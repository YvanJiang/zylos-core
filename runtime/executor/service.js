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
  maxResidentExecutorsPerBot = 20,
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
  if (
    !Number.isSafeInteger(maxResidentExecutorsPerBot)
    || maxResidentExecutorsPerBot <= 0
  ) {
    throw new TypeError('maxResidentExecutorsPerBot must be a positive safe integer');
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
  const activeRuns = new Map();
  const activeTurns = new Map();
  const cancelledTurnIds = new Set();
  const closingPermissionTurnIds = new Set();
  const permissionControllers = new Map();
  const activeRunSettlements = new Set();
  const cancellationSettlements = new Map();
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
    store.reconcileExpiredResidents();
    refresh();
    started = true;
    return snapshot();
  }

  function resultFor(activeRun, status, extra = {}) {
    return {
      status,
      conversation_id: activeRun.turnContext.conversation_id,
      turn_id: activeRun.turnContext.turn_id,
      ...activeRun.turnContext.attempt,
      ...extra,
    };
  }

  function cleanupActiveRun(activeRun) {
    const { turnContext } = activeRun;
    if (activeRuns.get(turnContext.turn_id) === activeRun) {
      activeRuns.delete(turnContext.turn_id);
    }
    if (activeTurns.get(turnContext.conversation_id) === turnContext) {
      activeTurns.delete(turnContext.conversation_id);
    }
    cancelledTurnIds.delete(turnContext.turn_id);
    uncertainTurnIds.delete(turnContext.turn_id);
    cancellationSettlements.delete(turnContext.turn_id);
    const controllers = permissionControllers.get(turnContext.turn_id);
    if (!controllers || controllers.size === 0) {
      permissionControllers.delete(turnContext.turn_id);
      closingPermissionTurnIds.delete(turnContext.turn_id);
    }
    if (!activeRun.settled) {
      activeRun.settled = true;
      activeRunSettlements.delete(activeRun.settlement);
      activeRun.resolveSettlement();
    }
  }

  function releaseAbsentResident(conversationId) {
    if (
      typeof adapter.hasResident === 'function'
      && !adapter.hasResident(conversationId)
    ) {
      store.releaseExecutorResident(conversationId);
    }
  }

  async function awaitCancellationSettlement(turnId) {
    const settlement = cancellationSettlements.get(turnId);
    if (settlement) await settlement.promise;
  }

  function transitionToRecovery(activeRun) {
    const { state } = store.assertCurrentFence(activeRun.turnContext);
    persist(() => store.transitionTurn(activeRun.turnContext, state, 'recovering', {
      error: null,
    }));
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    return resultFor(activeRun, 'recovering');
  }

  async function handleRunFailure(activeRun, error) {
    const { turnContext } = activeRun;
    if (activeRun.durableSettled) {
      cleanupActiveRun(activeRun);
      throw error;
    }
    if (error.persistenceFailure) {
      if (typeof adapter.abort === 'function') {
        await adapter.abort(turnContext);
      }
      cleanupActiveRun(activeRun);
      throw error;
    }
    await awaitCancellationSettlement(turnContext.turn_id);
    closingPermissionTurnIds.add(turnContext.turn_id);
    for (const controller of permissionControllers.get(turnContext.turn_id) ?? []) {
      controller.abort();
    }
    if (uncertainTurnIds.has(turnContext.turn_id)) {
      return transitionToRecovery(activeRun);
    }
    const cancelled = cancelledTurnIds.has(turnContext.turn_id);
    const terminalState = cancelled ? 'stopped' : 'failed';
    const { state } = store.assertCurrentFence(turnContext);
    store.transitionTurn(turnContext, state, terminalState, {
      error: cancelled ? null : {
        code: 'provider_stream_failed',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider stream ended before the turn completed.',
      },
    });
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    releaseAbsentResident(turnContext.conversation_id);
    return resultFor(activeRun, terminalState);
  }

  async function finishRun(activeRun) {
    await awaitCancellationSettlement(activeRun.turnContext.turn_id);
    if (uncertainTurnIds.has(activeRun.turnContext.turn_id)) {
      return transitionToRecovery(activeRun);
    }
    const terminalState = activeRun.outcome === 'cancelled'
      ? 'stopped'
      : (activeRun.outcome === 'failed' ? 'failed' : 'completed');
    const failure = activeRun.outcome === 'failed'
      || (activeRun.usesManagedRecords && activeRun.outcome === null);
    const { state } = store.assertCurrentFence(activeRun.turnContext);
    persist(() => store.transitionTurn(
      activeRun.turnContext,
      state,
      failure ? 'failed' : terminalState,
      {
        error: failure ? {
          code: activeRun.outcome === null
            ? 'provider_stream_ended'
            : 'provider_execution_failed',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider turn did not complete successfully.',
        } : null,
      },
    ));
    activeRun.durableSettled = true;
    refresh();
    cleanupActiveRun(activeRun);
    releaseAbsentResident(activeRun.turnContext.conversation_id);
    return resultFor(activeRun, failure ? 'failed' : terminalState);
  }

  async function driveRun(activeRun) {
    try {
      while (true) {
        const next = await activeRun.iterator.next();
        if (next.done) return await finishRun(activeRun);
        const record = next.value;
        if (record?.kind === 'interaction_requested') {
          const request = persist(() => store.requestInteraction(
            activeRun.turnContext,
            record.payload,
          ));
          activeRun.pauseKind = 'interaction';
          refresh();
          return resultFor(activeRun, 'waiting_user', { request });
        }
        if (record?.type === 'provider_native_id') {
          activeRun.usesManagedRecords = true;
          try {
            persist(() => store.bindProviderNativeId(
              activeRun.turnContext,
              record.provider_native_id,
            ));
            record.acknowledge();
          } catch (error) {
            record.acknowledge(error);
            throw error;
          }
          continue;
        }
        if (record?.type === 'normalized_event') {
          activeRun.usesManagedRecords = true;
          persist(() => store.appendAdapterEvent(activeRun.turnContext, record.event));
          continue;
        }
        if (record?.type === 'turn_result') {
          activeRun.usesManagedRecords = true;
          activeRun.outcome = record.outcome;
          continue;
        }
        persist(() => store.appendAdapterEvent(activeRun.turnContext, record));
      }
    } catch (error) {
      return handleRunFailure(activeRun, error);
    }
  }

  function advanceRun(activeRun) {
    if (activeRun.advancing) return activeRun.advancing;
    const advancing = driveRun(activeRun);
    activeRun.advancing = advancing;
    advancing.finally(() => {
      if (activeRun.advancing === advancing) activeRun.advancing = null;
    }).catch(() => {});
    return advancing;
  }

  function createPermissionControls(activeRun, adapterContext) {
    const { turnContext } = activeRun;
    return Object.freeze({
      interactionPolicy: Object.freeze({
        allowed_sources: Object.freeze(['main_card_reply', 'card_action']),
        authorized_subjects: Object.freeze(
          turnContext.interaction_authority.map((subject) => Object.freeze({ ...subject })),
        ),
      }),
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
          persist(() => store.transitionTurn(turnContext, 'running', 'waiting_user'));
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
              const resumed = persist(() => store.resumeTurnAfterPermission(turnContext));
              if (resumed.resumed && activeRun.pauseKind === 'interaction') {
                activeRun.pauseKind = null;
                queueMicrotask(() => advanceRun(activeRun).catch(() => {}));
              }
            }
            closingPermissionTurnIds.delete(turnContext.turn_id);
          }
        }
        if (!decision || !['allow', 'deny'].includes(decision.behavior)) {
          throw new TypeError('permissionHandler must return an allow or deny decision');
        }
        return Object.freeze({ ...decision });
      },
    });
  }

  async function runNext() {
    if (lifecycle !== 'open') {
      throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
    }
    if (!started) start();
    let reservation = store.reserveNextExecutor({
      maxResidentExecutorsPerBot,
      markCapacityWait: false,
    });
    if (reservation.status === 'idle') return reservation;
    if (reservation.status === 'capacity_wait') {
      await evictIdleExecutors();
      if (lifecycle !== 'open') {
        throw new Error(`Executor service is ${lifecycle}; it cannot claim another turn.`);
      }
      reservation = store.reserveNextExecutor({ maxResidentExecutorsPerBot });
    }
    if (reservation.status === 'capacity_wait') {
      refresh();
      return reservation;
    }
    const turnContext = store.claimNextQueuedTurn({
      conversationId: reservation.conversation_id,
    });
    if (!turnContext) {
      refresh();
      return { status: 'idle' };
    }
    let resolveSettlement;
    const settlement = new Promise((resolve) => {
      resolveSettlement = resolve;
    });
    const activeRun = {
      advancing: null,
      durableSettled: false,
      iterator: null,
      outcome: null,
      pauseKind: null,
      resolveSettlement,
      settled: false,
      settlement,
      turnContext,
      usesManagedRecords: false,
    };
    activeRunSettlements.add(settlement);
    activeRuns.set(turnContext.turn_id, activeRun);
    activeTurns.set(turnContext.conversation_id, turnContext);
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
      const events = adapter.execute(
        adapterContext,
        createPermissionControls(activeRun, adapterContext),
      );
      if (!events || typeof events[Symbol.asyncIterator] !== 'function') {
        throw new TypeError('adapter.execute must return an async iterable');
      }
      activeRun.iterator = events[Symbol.asyncIterator]();
    } catch (error) {
      return handleRunFailure(activeRun, error);
    }
    return advanceRun(activeRun);
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
    if (cancellationSettlements.has(turnContext.turn_id)) {
      throw new Error('Cancellation is already pending for the active turn.');
    }
    let resolveCancellation;
    const cancellation = {
      promise: new Promise((resolve) => { resolveCancellation = resolve; }),
      resolve: () => resolveCancellation(),
      status: 'pending',
    };
    cancellationSettlements.set(turnContext.turn_id, cancellation);
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
        cancellation.status = 'uncertain';
        cancellation.resolve();
        if (typeof adapter.abort === 'function') {
          try {
            await adapter.abort(turnContext);
          } catch (abortFailure) {
            error.abortFailure = abortFailure;
          }
        }
      } else {
        cancellation.status = 'failed';
        cancellation.resolve();
      }
      throw error;
    }
    cancellation.status = 'confirmed';
    cancellation.resolve();
    const activeRun = activeRuns.get(turnContext.turn_id);
    let execution = null;
    if (activeRun?.pauseKind === 'interaction') {
      activeRun.pauseKind = null;
      execution = await advanceRun(activeRun);
    }
    return {
      status: 'cancellation_requested',
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      ...turnContext.attempt,
      ...(execution ? { execution } : {}),
    };
  }

  async function evictIdleExecutors() {
    if (typeof adapter.evictIdle !== 'function') return [];
    const evicted = await adapter.evictIdle({
      canEvict: (conversationId) => store.isConversationEvictable(conversationId),
    });
    for (const conversationId of evicted) {
      store.releaseExecutorResident(conversationId);
    }
    return evicted;
  }

  function submitInteractionAnswer(answer) {
    return store.commitInteractionAnswer(answer);
  }

  async function deliverInteractionAnswer(handoffId) {
    if (typeof adapter.handleInteractionAnswer !== 'function') {
      throw new TypeError('adapter.handleInteractionAnswer must be a function');
    }
    const delivery = store.claimInteractionHandoff(handoffId);
    const handlerAcknowledgement = await adapter.handleInteractionAnswer(
      Object.freeze(delivery),
    );
    const pendingPermissions = permissionControllers.get(delivery.request.turn_id)?.size ?? 0;
    const acknowledgement = store.acknowledgeInteractionHandoff(handlerAcknowledgement, {
      holdForPermission: pendingPermissions > 0,
    });
    const activeRun = activeRuns.get(delivery.request.turn_id);
    if (acknowledgement.resumed && activeRun) activeRun.pauseKind = null;
    const execution = acknowledgement.resumed && activeRun
      ? await advanceRun(activeRun)
      : null;
    return { acknowledgement, execution };
  }

  async function close() {
    if (closePromise) return closePromise;
    lifecycle = 'closing';
    closePromise = (async () => {
      let closeError = null;
      let closedConversationIds = [];
      try {
        if (typeof adapter.close === 'function') {
          closedConversationIds = await adapter.close() ?? [];
        }
      } catch (error) {
        closeError = error;
        closedConversationIds = error.closedConversationIds ?? [];
      }
      for (const activeRun of activeRuns.values()) {
        if (activeRun.pauseKind === 'interaction') {
          cleanupActiveRun(activeRun);
        } else if (!activeRun.advancing && activeRun.iterator) {
          advanceRun(activeRun);
        }
      }
      await Promise.allSettled([...activeRunSettlements]);
      for (const conversationId of closedConversationIds) {
        store.releaseExecutorResident(conversationId);
      }
      lifecycle = 'closed';
      if (closeError) throw closeError;
    })();
    return closePromise;
  }

  return Object.freeze({
    cancel,
    close,
    deliverInteractionAnswer,
    evictIdleExecutors,
    runNext,
    snapshot,
    start,
    submitInteractionAnswer,
  });
}
