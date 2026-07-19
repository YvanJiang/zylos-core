import crypto from 'node:crypto';

import { createContractError } from '../../contracts/public/index.js';
import { createExecutorStore } from '../persistence/executor-store.js';

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function normalizeProviderError(error, occurredAt) {
  const descriptor = error?.providerError;
  if (descriptor && typeof descriptor === 'object') {
    try {
      return createContractError({
        code: descriptor.code,
        category: descriptor.category,
        retryable: descriptor.retryable,
        sideEffectStatus: descriptor.side_effect_status,
        userMessage: descriptor.user_message,
        occurredAt,
      });
    } catch {
      // Invalid adapter error metadata is replaced with the safe generic provider failure below.
    }
  }
  return createContractError({
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    sideEffectStatus: 'unknown',
    userMessage: 'The provider execution failed after side effects may have occurred.',
    occurredAt,
  });
}

function isExplicitProviderError(error) {
  return error !== null
    && typeof error === 'object'
    && error.providerError !== null
    && typeof error.providerError === 'object';
}

export function createExecutorService({
  database,
  adapter,
  provider,
  serviceInstanceId,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  leaseDurationMs,
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

  function failTurn(turnContext, providerError) {
    store.transitionTurn(turnContext, 'running', 'failed', {
      reasonCode: 'executor_failed',
      error: normalizeProviderError(providerError, now()),
    });
    activeRuns.delete(turnContext.turn_id);
    refresh();
    return {
      status: 'failed',
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      ...turnContext.attempt,
    };
  }

  async function advanceRun(activeRun) {
    while (true) {
      let next;
      try {
        next = await activeRun.iterator.next();
      } catch (error) {
        if (isExplicitProviderError(error)) return failTurn(activeRun.turnContext, error);
        throw error;
      }
      if (next.done) {
        store.transitionTurn(activeRun.turnContext, 'running', 'completed');
        activeRuns.delete(activeRun.turnContext.turn_id);
        refresh();
        return {
          status: 'completed',
          conversation_id: activeRun.turnContext.conversation_id,
          turn_id: activeRun.turnContext.turn_id,
          ...activeRun.turnContext.attempt,
        };
      }
      try {
        const event = next.value;
        if (event?.kind === 'interaction_requested') {
          const request = store.requestInteraction(activeRun.turnContext, event.payload);
          refresh();
          return {
            status: 'waiting_user',
            conversation_id: activeRun.turnContext.conversation_id,
            turn_id: activeRun.turnContext.turn_id,
            ...activeRun.turnContext.attempt,
            request,
          };
        }
        store.appendAdapterEvent(activeRun.turnContext, event);
      } catch (persistenceError) {
        try {
          await activeRun.iterator.return?.();
        } catch {
          // Preserve the durable write failure; adapter cleanup is best-effort here.
        }
        activeRuns.delete(activeRun.turnContext.turn_id);
        throw persistenceError;
      }
    }
  }

  async function runNext() {
    if (!started) start();
    const reservation = store.reserveNextExecutor({ maxResidentExecutorsPerBot });
    if (reservation.status === 'idle') return reservation;
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
    refresh();
    store.transitionTurn(turnContext, 'starting', 'running');
    let events;
    try {
      events = adapter.execute(Object.freeze({
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        lineage_id: turnContext.lineage_id,
        trace_id: turnContext.trace_id,
        input: turnContext.input,
        lineage: Object.freeze({ ...turnContext.lineage }),
        bindProviderNativeId: (providerNativeId) => (
          store.bindProviderNativeId(turnContext, providerNativeId)
        ),
        attempt: Object.freeze({ ...turnContext.attempt }),
      }));
    } catch (error) {
      if (isExplicitProviderError(error)) return failTurn(turnContext, error);
      throw error;
    }
    if (!events || typeof events[Symbol.asyncIterator] !== 'function') {
      throw new TypeError('adapter.execute must return an async iterable');
    }
    const activeRun = {
      turnContext,
      iterator: events[Symbol.asyncIterator](),
    };
    activeRuns.set(turnContext.turn_id, activeRun);
    return advanceRun(activeRun);
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
    const acknowledgement = store.acknowledgeInteractionHandoff(handlerAcknowledgement);
    const activeRun = activeRuns.get(delivery.request.turn_id);
    const execution = acknowledgement.resumed && activeRun
      ? await advanceRun(activeRun)
      : null;
    return { acknowledgement, execution };
  }

  return Object.freeze({
    deliverInteractionAnswer,
    runNext,
    snapshot,
    start,
    submitInteractionAnswer,
  });
}
