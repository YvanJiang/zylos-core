import crypto from 'node:crypto';

import { createExecutorStore } from '../persistence/executor-store.js';

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

export function createExecutorService({
  database,
  adapter,
  provider,
  serviceInstanceId,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  leaseDurationMs,
  permissionHandler = null,
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (!adapter || typeof adapter.execute !== 'function') {
    throw new TypeError('adapter.execute must be a function');
  }
  if (!['claude', 'codex'].includes(provider)) {
    throw new TypeError('provider must be claude or codex');
  }
  if (typeof serviceInstanceId !== 'string' || serviceInstanceId.length === 0) {
    throw new TypeError('serviceInstanceId must be a non-empty string');
  }
  if (typeof generateId !== 'function') {
    throw new TypeError('generateId must be a function');
  }
  if (permissionHandler !== null && typeof permissionHandler !== 'function') {
    throw new TypeError('permissionHandler must be a function or null');
  }

  const store = createExecutorStore({
    database,
    provider,
    serviceInstanceId,
    now,
    generateId,
    leaseDurationMs,
  });
  let executors = [];
  let started = false;
  const activeTurns = new Map();
  const cancelledTurnIds = new Set();
  const closingPermissionTurnIds = new Set();
  const permissionControllers = new Map();
  const activeRunSettlements = new Set();
  const uncertainTurnIds = new Set();
  let lifecycle = 'open';
  let closePromise = null;

  function persistenceFailure(cause) {
    const error = new Error(`Runtime persistence failed: ${cause.message}`, { cause });
    error.persistenceFailure = true;
    return error;
  }

  function persist(operation) {
    try {
      return operation();
    } catch (cause) {
      throw persistenceFailure(cause);
    }
  }

  function refresh() {
    executors = store.rebuildExecutorCache();
  }

  function snapshot() {
    return {
      service_instance_id: serviceInstanceId,
      executors: executors.map((executor) => ({
        ...executor,
        queued_turn_ids: [...executor.queued_turn_ids],
      })),
    };
  }

  function start() {
    refresh();
    started = true;
    return snapshot();
  }

  async function runNext() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
    }
    if (!started) start();
    const turnContext = store.claimNextQueuedTurn();
    if (!turnContext) return { status: 'idle' };
    let resolveRunSettlement;
    const runSettlement = new Promise((resolve) => {
      resolveRunSettlement = resolve;
    });
    activeRunSettlements.add(runSettlement);
    activeTurns.set(turnContext.conversation_id, turnContext);
    let durableSettled = false;
    try {
      refresh();
      persist(() => store.transitionTurn(turnContext, 'starting', 'running'));
      const adapterContext = Object.freeze({
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        lineage_id: turnContext.lineage_id,
        provider_native_id: turnContext.provider_native_id,
        trace_id: turnContext.trace_id,
        input: turnContext.input,
        attempt: Object.freeze({ ...turnContext.attempt }),
      });
      const events = adapter.execute(adapterContext, Object.freeze({
        async requestPermission(request, { signal } = {}) {
          if (permissionHandler === null) {
            return Object.freeze({
              behavior: 'deny',
              message: 'No permission handler is configured for this executor service.',
              interrupt: false,
            });
          }
          let controllers = permissionControllers.get(turnContext.turn_id);
          if (!controllers) {
            controllers = new Set();
            permissionControllers.set(turnContext.turn_id, controllers);
            store.transitionTurn(turnContext, 'running', 'waiting_user');
          }
          const controller = new AbortController();
          controllers.add(controller);
          const abort = () => controller.abort();
          signal?.addEventListener?.('abort', abort, { once: true });
          let decision;
          try {
            decision = await Promise.race([
              permissionHandler(
                Object.freeze({ ...request }),
                adapterContext,
                Object.freeze({ signal: controller.signal }),
              ),
              new Promise((resolve, reject) => {
                controller.signal.addEventListener('abort', () => {
                  const error = new Error('Permission request was cancelled.');
                  error.name = 'AbortError';
                  reject(error);
                }, { once: true });
              }),
            ]);
          } finally {
            signal?.removeEventListener?.('abort', abort);
            controllers.delete(controller);
            if (controllers.size === 0) {
              permissionControllers.delete(turnContext.turn_id);
              if (
                !cancelledTurnIds.has(turnContext.turn_id)
                && !closingPermissionTurnIds.has(turnContext.turn_id)
              ) {
                store.transitionTurn(turnContext, 'waiting_user', 'running');
              }
              closingPermissionTurnIds.delete(turnContext.turn_id);
            }
          }
          if (!decision || !['allow', 'deny'].includes(decision.behavior)) {
            throw new TypeError('permissionHandler must return an allow or deny decision');
          }
          return Object.freeze({ ...decision });
        },
      }));
      let outcome = null;
      let usesManagedRecords = false;
      for await (const record of events) {
        if (record?.type === 'provider_native_id') {
          usesManagedRecords = true;
          try {
            persist(() => store.bindProviderNativeId(turnContext, record.provider_native_id));
            record.acknowledge();
          } catch (error) {
            record.acknowledge(error);
            throw error;
          }
          continue;
        }
        if (record?.type === 'normalized_event') {
          usesManagedRecords = true;
          persist(() => store.appendAdapterEvent(turnContext, record.event));
          continue;
        }
        if (record?.type === 'turn_result') {
          usesManagedRecords = true;
          outcome = record.outcome;
          continue;
        }
        persist(() => store.appendAdapterEvent(turnContext, record));
      }
      if (uncertainTurnIds.has(turnContext.turn_id)) {
        const { state } = store.assertCurrentFence(turnContext);
        persist(() => store.transitionTurn(turnContext, state, 'recovering', {
          error: null,
        }));
        durableSettled = true;
        refresh();
        return {
          status: 'recovering',
          conversation_id: turnContext.conversation_id,
          turn_id: turnContext.turn_id,
          ...turnContext.attempt,
        };
      }
      const terminalState = outcome === 'cancelled'
        ? 'stopped'
        : (outcome === 'failed' ? 'failed' : 'completed');
      const failure = outcome === 'failed' || (usesManagedRecords && outcome === null);
      persist(() => store.transitionTurn(turnContext, 'running', failure ? 'failed' : terminalState, {
        error: failure ? {
          code: outcome === null ? 'provider_stream_ended' : 'provider_execution_failed',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider turn did not complete successfully.',
        } : null,
      }));
      durableSettled = true;
      refresh();
      return {
        status: failure ? 'failed' : terminalState,
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        ...turnContext.attempt,
      };
    } catch (error) {
      if (durableSettled) throw error;
      if (error.persistenceFailure) {
        if (typeof adapter.abort === 'function') {
          await adapter.abort(turnContext);
        }
        throw error;
      }
      closingPermissionTurnIds.add(turnContext.turn_id);
      for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
        controller.abort();
      }
      const cancelled = cancelledTurnIds.has(turnContext.turn_id);
      const { state } = store.assertCurrentFence(turnContext);
      if (uncertainTurnIds.has(turnContext.turn_id)) {
        store.transitionTurn(turnContext, state, 'recovering', {
          error: null,
        });
        durableSettled = true;
        refresh();
        return {
          status: 'recovering',
          conversation_id: turnContext.conversation_id,
          turn_id: turnContext.turn_id,
          ...turnContext.attempt,
        };
      }
      const terminalState = cancelled ? 'stopped' : 'failed';
      store.transitionTurn(turnContext, state, terminalState, {
        error: cancelled ? null : {
          code: 'provider_stream_failed',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider stream ended before the turn completed.',
        },
      });
      durableSettled = true;
      refresh();
      return {
        status: terminalState,
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        ...turnContext.attempt,
      };
    } finally {
      if (activeTurns.get(turnContext.conversation_id) === turnContext) {
        activeTurns.delete(turnContext.conversation_id);
      }
      cancelledTurnIds.delete(turnContext.turn_id);
      uncertainTurnIds.delete(turnContext.turn_id);
      const controllers = permissionControllers.get(turnContext.turn_id);
      if (!controllers || controllers.size === 0) {
        permissionControllers.delete(turnContext.turn_id);
        closingPermissionTurnIds.delete(turnContext.turn_id);
      }
      activeRunSettlements.delete(runSettlement);
      resolveRunSettlement();
    }
  }

  async function cancel(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const turnContext = activeTurns.get(conversationId);
    if (!turnContext) return { status: 'idle', conversation_id: conversationId };
    store.assertCurrentFence(turnContext);
    if (typeof adapter.cancel !== 'function') {
      throw new TypeError('adapter.cancel must be a function to cancel an active turn');
    }
    cancelledTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    try {
      await adapter.cancel(turnContext);
    } catch (error) {
      cancelledTurnIds.delete(turnContext.turn_id);
      if (error.cancellationUncertain) {
        uncertainTurnIds.add(turnContext.turn_id);
        if (typeof adapter.abort === 'function') await adapter.abort(turnContext);
      }
      throw error;
    }
    return {
      status: 'cancellation_requested',
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      ...turnContext.attempt,
    };
  }

  async function evictIdleExecutors() {
    if (typeof adapter.evictIdle !== 'function') return [];
    return adapter.evictIdle({
      canEvict: (conversationId) => store.isConversationEvictable(conversationId),
    });
  }

  async function close() {
    if (closePromise) return closePromise;
    lifecycle = 'closing';
    const settlements = [...activeRunSettlements];
    closePromise = (async () => {
      let closeError = null;
      try {
        if (typeof adapter.close === 'function') await adapter.close();
      } catch (error) {
        closeError = error;
      }
      await Promise.allSettled(settlements);
      lifecycle = 'closed';
      if (closeError) throw closeError;
    })();
    return closePromise;
  }

  return Object.freeze({ cancel, close, evictIdleExecutors, runNext, snapshot, start });
}
