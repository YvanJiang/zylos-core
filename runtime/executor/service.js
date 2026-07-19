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
    if (!started) start();
    const turnContext = store.claimNextQueuedTurn();
    if (!turnContext) return { status: 'idle' };
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
    for await (const event of events) {
      store.appendAdapterEvent(turnContext, event);
    }
    store.transitionTurn(turnContext, 'running', 'completed');
    refresh();
    return {
      status: 'completed',
      conversation_id: turnContext.conversation_id,
      turn_id: turnContext.turn_id,
      ...turnContext.attempt,
    };
  }

  return Object.freeze({ runNext, snapshot, start });
}
