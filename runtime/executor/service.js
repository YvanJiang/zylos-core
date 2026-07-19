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
  const residentConversations = new Map();

  function refresh() {
    executors = store.rebuildExecutorCache();
    const durableConversationIds = new Set(
      executors.map((executor) => executor.conversation_id),
    );
    for (const conversationId of residentConversations.keys()) {
      if (!durableConversationIds.has(conversationId)) {
        residentConversations.delete(conversationId);
      }
    }
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

  function residentCountForBot(botId) {
    let count = 0;
    for (const resident of residentConversations.values()) {
      if (resident.bot_id === botId) count += 1;
    }
    return count;
  }

  function selectCapacityCandidate(candidates) {
    const existingResident = candidates.find(
      (candidate) => residentConversations.has(candidate.conversation_id),
    );
    if (existingResident) return { candidate: existingResident, admitted: false };
    if (provider !== 'claude') return { candidate: candidates[0], admitted: false };
    const available = candidates.find(
      (candidate) => residentCountForBot(candidate.bot_id) < maxResidentExecutorsPerBot,
    );
    if (!available) return { candidate: null, admitted: false };
    residentConversations.set(available.conversation_id, { bot_id: available.bot_id });
    return { candidate: available, admitted: true };
  }

  async function runNext() {
    if (!started) start();
    const candidates = store.listClaimableQueuedTurns();
    if (candidates.length === 0) return { status: 'idle' };
    const selected = selectCapacityCandidate(candidates);
    if (!selected.candidate) {
      const wait = store.markCapacityWait(candidates[0].turn_id);
      refresh();
      return wait ? { status: 'capacity_wait', ...wait } : { status: 'idle' };
    }
    let turnContext;
    try {
      turnContext = store.claimNextQueuedTurn({
        conversationId: selected.candidate.conversation_id,
      });
    } catch (error) {
      if (selected.admitted) {
        residentConversations.delete(selected.candidate.conversation_id);
      }
      throw error;
    }
    if (!turnContext) {
      if (selected.admitted) {
        residentConversations.delete(selected.candidate.conversation_id);
      }
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
