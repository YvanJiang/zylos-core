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
  interactionTimeoutMs,
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
    interactionTimeoutMs,
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

  async function advanceRun(activeRun) {
    while (true) {
      const next = await activeRun.iterator.next();
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
    const events = adapter.execute(Object.freeze({
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      lineage_id: turnContext.lineage_id,
      trace_id: turnContext.trace_id,
      input: turnContext.input,
      attempt: Object.freeze({ ...turnContext.attempt }),
    }));
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

  function submitInteractionAnswer(answer, sourceEvidence) {
    return store.commitInteractionAnswer(answer, sourceEvidence);
  }

  async function expireInteraction(expiration) {
    const result = store.expireInteraction(expiration);
    if (result.status !== 'expired' || result.turn_state !== 'timed_out') return result;

    const activeRun = activeRuns.get(result.turn_id);
    if (activeRun) {
      if (typeof activeRun.iterator.return !== 'function') {
        return { ...result, lease_released: false };
      }
      await activeRun.iterator.return();
      activeRuns.delete(result.turn_id);
    }
    const leaseReleased = store.releaseTimedOutExecutorLease(result);
    refresh();
    return { ...result, lease_released: leaseReleased };
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
    expireInteraction,
    runNext,
    snapshot,
    start,
    submitInteractionAnswer,
  });
}
