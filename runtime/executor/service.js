import crypto from 'node:crypto';

import { createContractError } from '../../contracts/public/index.js';
import {
  createExecutorStore,
  ExecutorPersistenceError,
} from '../persistence/executor-store.js';

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
    try {
      const events = adapter.execute(Object.freeze({
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
      for await (const event of events) {
        store.appendAdapterEvent(turnContext, event);
      }
    } catch (error) {
      if (error instanceof ExecutorPersistenceError) throw error;
      store.transitionTurn(turnContext, 'running', 'failed', {
        reasonCode: 'executor_failed',
        error: normalizeProviderError(error, now()),
      });
      refresh();
      return {
        status: 'failed',
        conversation_id: turnContext.conversation_id,
        turn_id: turnContext.turn_id,
        ...turnContext.attempt,
      };
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
