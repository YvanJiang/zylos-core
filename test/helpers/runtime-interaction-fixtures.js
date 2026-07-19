import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import Database from '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../../contracts/public/index.js';
import { createExecutorStore } from '../../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../../runtime/persistence/inbound-acceptance.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const temporaryDirectories = [];

export function openInteractionTestDatabase(prefix = 'runtime-interaction') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), `zylos-${prefix}-`));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

export function cleanupInteractionTestDatabases() {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

export function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

export function interactionInboundEnvelope(suffix) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

export function acceptQueuedInteractionTurn(database, suffix, {
  acceptedAt = '2026-07-19T07:00:00Z',
} = {}) {
  return acceptNormalInbound(database, interactionInboundEnvelope(suffix), {
    now: () => acceptedAt,
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

export function createRunningInteractionTurn(database, suffix = 'request', {
  interactionTimeoutMs,
} = {}) {
  const envelope = interactionInboundEnvelope(suffix);
  const accepted = acceptNormalInbound(database, envelope, {
    now: () => '2026-07-19T07:00:00Z',
    generateId: deterministicIds(`inbound-${suffix}`),
  });
  const clock = { now: '2026-07-19T07:02:00Z' };
  const store = createExecutorStore({
    database,
    provider: 'claude',
    serviceInstanceId: `executor-service-${suffix}`,
    now: () => clock.now,
    generateId: deterministicIds(suffix),
    interactionTimeoutMs,
  });
  const turnContext = store.claimNextQueuedTurn();
  store.transitionTurn(turnContext, 'starting', 'running');
  return { accepted, clock, envelope, store, turnContext };
}

export function interactionAnswer(request, suffix = '1', overrides = {}) {
  const sourceEventId = `card-action-${suffix}`;
  const answer = {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-answer-${suffix}`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `answer-${suffix}`,
    source_event_or_action_id: sourceEventId,
    actor: {
      type: 'user',
      actor_id: 'user-123',
      authenticated: true,
      roles: ['member'],
    },
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    },
    source: 'card_action',
    value: { kind: 'decision', decision: 'approve' },
    answered_at: '2026-07-19T07:02:00Z',
  };
  Object.assign(answer, overrides);
  answer.idempotency_key = overrides.idempotency_key ?? createIdempotencyKey('interaction', {
    interaction_id: answer.interaction_id,
    source_event_or_action_id: answer.source_event_or_action_id,
  });
  return answer;
}
