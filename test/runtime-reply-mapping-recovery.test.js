import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, jest, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateInboundResult,
} from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptQueuedInbound as acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-reply-mapping-recovery-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function normalEnvelope(suffix) {
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

function replyEnvelope(source, suffix, platformMessageId) {
  const reply = normalEnvelope(suffix);
  reply.reply = {
    root_message_id: platformMessageId,
    parent_message_id: platformMessageId,
    reply_to_message_id: platformMessageId,
  };
  reply.chat_id = source.chat_id;
  return reply;
}

function accept(database, envelope, namespace) {
  return acceptNormalInbound(database, envelope, {
    now: () => '2026-07-20T01:00:00Z',
    generateId: deterministicIds(namespace),
  });
}

function deliverOnlyPending(
  database,
  namespace,
  deliveryAt = '2026-07-20T01:00:01Z',
) {
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `delivery-${namespace}`,
    now: () => deliveryAt,
    generateId: deterministicIds(`delivery-${namespace}`),
    throttleMs: 0,
  });
  const command = outbox.claimNext();
  expect(command).not.toBeNull();
  const result = deliveredResult(command, deliveryAt);
  expect(outbox.recordResult(result)).toEqual({
    status: 'applied',
    outbox_status: 'delivered',
  });
  return { command, result };
}

function retryableDeliveryFailure(command, resultAt) {
  return {
    ...deliveredResult(command, resultAt),
    status: 'retryable_failure',
    platform_message_id: null,
    delivered_at: null,
    error: {
      code: 'delivery_transient',
      category: 'channel',
      retryable: true,
      side_effect_status: 'none',
      user_message: 'The channel is temporarily unavailable.',
      occurred_at: resultAt,
    },
    result_at: resultAt,
  };
}

function permanentDeliveryFailure(command, resultAt) {
  return {
    ...retryableDeliveryFailure(command, resultAt),
    status: 'permanent_failure',
    platform_message_id: command.target_platform_message_id,
    error: {
      code: 'delivery_permanent',
      category: 'channel',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The channel permanently rejected the exact delivery target.',
      occurred_at: resultAt,
    },
  };
}

function deliverUntilCommand(database, namespace, predicate, deliveryAt) {
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    const delivered = deliverOnlyPending(
      database,
      `${namespace}-${attempt}`,
      deliveryAt,
    );
    if (predicate(delivered.command)) return delivered;
  }
  throw new Error(`No matching pending delivery was found for ${namespace}.`);
}

function deliverUntilTurn(database, turnId, namespace, deliveryAt) {
  return deliverUntilCommand(
    database,
    namespace,
    (command) => command.mapping.turn_id === turnId,
    deliveryAt,
  );
}

function createDeliveredSource(database, namespace) {
  const envelope = normalEnvelope(`${namespace}-source`);
  const accepted = accept(database, envelope, `${namespace}-source`);
  const delivery = deliverOnlyPending(database, `${namespace}-source`);
  return { envelope, accepted, ...delivery };
}

function simulateMissingDeliveredMapping(database, platformMessageId) {
  database.exec('DROP TRIGGER IF EXISTS runtime_bound_message_mapping_delete_immutable');
  database.prepare('DELETE FROM runtime_message_mappings WHERE platform_message_id = ?')
    .run(platformMessageId);
}

function allowSimulatedMappingCorruption(database) {
  database.exec('DROP TRIGGER IF EXISTS runtime_bound_message_mapping_immutable');
}

function markAssociatedSideEffectUnknown(database, turnId) {
  const lastEvent = database.prepare(`
    SELECT event_id, event_json FROM runtime_normalized_events
    WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
  `).get(turnId);
  const unknownEvent = JSON.parse(lastEvent.event_json);
  unknownEvent.error = {
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    side_effect_status: 'unknown',
    user_message: 'Associated work may have produced side effects.',
    occurred_at: '2026-07-20T01:00:00Z',
  };
  database.prepare('UPDATE runtime_normalized_events SET event_json = ? WHERE event_id = ?')
    .run(JSON.stringify(unknownEvent), lastEvent.event_id);
}

function recoveryDecisionAnswer(interaction, envelope, decision, namespace) {
  const sourceEventOrActionId = `recovery-decision-${namespace}`;
  return {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-recovery-decision-${namespace}`,
    interaction_id: interaction.interaction_id,
    interaction_version: interaction.version,
    answer_id: `answer-recovery-decision-${namespace}`,
    source_event_or_action_id: sourceEventOrActionId,
    actor: structuredClone(envelope.actor),
    source_context: {
      region: envelope.region,
      tenant_id: envelope.tenant_id,
      channel: envelope.channel,
      bot_id: envelope.bot_id,
      chat_id: envelope.chat_id,
      native_thread_or_topic_id: envelope.native_thread_or_topic_id,
      platform_message_or_action_id: sourceEventOrActionId,
    },
    source: 'main_card_reply',
    value: { kind: 'decision', decision },
    answered_at: '2026-07-20T01:00:03Z',
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: interaction.interaction_id,
      source_event_or_action_id: sourceEventOrActionId,
    }),
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('reply mapping provisional-lineage recovery', () => {
  test('persists a missing mapping as one pending blocked turn and provisional delivery', () => {
    const database = openTestDatabase();
    const originalEnvelope = normalEnvelope('mapping-source');
    const original = accept(database, originalEnvelope, 'mapping-source');
    const { result: delivered } = deliverOnlyPending(database, 'mapping-source');
    simulateMissingDeliveredMapping(database, delivered.platform_message_id);

    const pending = accept(
      database,
      replyEnvelope(originalEnvelope, 'mapping-missing', delivered.platform_message_id),
      'mapping-missing',
    );

    expect(validateInboundResult(pending).forwarded).toEqual(pending);
    expect(pending).toMatchObject({
      status: 'accepted',
      conversation_id: original.conversation_id,
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
      turn_version: 2,
      error: null,
    });
    expect(pending.turn_id).not.toBe(original.turn_id);
    expect(database.prepare(`
      SELECT turn.state, turn.lineage_id, queue.status, queue.wait_reason
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: 'queued',
      lineage_id: null,
      status: 'queued',
      wait_reason: 'lineage_resolution_pending',
    });

    const recovery = database.prepare(`
      SELECT mapping_id, reason, candidate_lineage_id, side_effect_status,
        state, native_recovery_attempt_count
      FROM runtime_reply_mapping_recoveries
      WHERE turn_id = ?
    `).get(pending.turn_id);
    expect(recovery).toEqual({
      mapping_id: expect.any(String),
      reason: 'mapping_missing',
      candidate_lineage_id: original.lineage_id,
      side_effect_status: 'none',
      state: 'queued',
      native_recovery_attempt_count: 0,
    });

    const command = JSON.parse(database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
    `).get(pending.turn_id).command_json);
    expect(validateDeliveryCommand(command).forwarded).toEqual(command);
    expect(command.mapping).toEqual({
      mapping_id: recovery.mapping_id,
      conversation_id: original.conversation_id,
      turn_id: pending.turn_id,
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
      reason: 'mapping_missing',
    });

    database.close();
  });

  test('admits recovery beyond normal capacity without leapfrogging FIFO or steer priority', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'fifo-capacity');
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    const pendingEnvelope = replyEnvelope(
      source.envelope,
      'fifo-capacity-pending',
      source.result.platform_message_id,
    );
    const pending = acceptNormalInbound(database, pendingEnvelope, {
      now: () => '2026-07-20T01:00:00Z',
      generateId: deterministicIds('fifo-capacity-pending'),
      maxQueuedTurns: 1,
    });
    expect(pending).toMatchObject({
      status: 'accepted',
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
      error: null,
    });

    const executedTurnIds = [];
    const execute = jest.fn((context) => (async function* executeProvider() {
      executedTurnIds.push(context.turn_id);
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const recoverLineage = jest.fn();
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-fifo-capacity',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('fifo-capacity-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: source.accepted.turn_id,
    });
    expect(execute).toHaveBeenCalledTimes(1);
    expect(executedTurnIds).toEqual([source.accepted.turn_id]);
    expect(recoverLineage).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT recovery.state, queue.status, queue.wait_reason
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: 'queued',
      status: 'queued',
      wait_reason: 'lineage_resolution_pending',
    });

    const priorityEnvelope = normalEnvelope('fifo-capacity-priority');
    priorityEnvelope.chat_id = source.envelope.chat_id;
    const priority = accept(database, priorityEnvelope, 'fifo-capacity-priority');
    database.prepare(`
      UPDATE runtime_turn_queue SET priority = 1 WHERE turn_id = ?
    `).run(priority.turn_id);
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: priority.turn_id,
    });
    expect(executedTurnIds).toEqual([source.accepted.turn_id, priority.turn_id]);
    expect(recoverLineage).not.toHaveBeenCalled();

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'lineage_resolution_pending',
      turn_id: pending.turn_id,
      wait_reason: 'reply_mapping_notice_delivery',
    });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(recoverLineage).not.toHaveBeenCalled();

    await service.close();
    database.close();
  });

  test.each([
    {
      reason: 'mapping_corrupt',
      corrupt(database, source) {
        allowSimulatedMappingCorruption(database);
        database.prepare(`
          UPDATE runtime_message_mappings
          SET binding_state = 'corrupt'
          WHERE platform_message_id = ?
        `).run(source.result.platform_message_id);
      },
    },
    {
      reason: 'mapping_unbound',
      corrupt(database, source) {
        allowSimulatedMappingCorruption(database);
        database.prepare(`
          UPDATE runtime_message_mappings
          SET lineage_id = NULL, binding_state = 'pending',
            reason = 'mapping_unbound', mapping_version = 1
          WHERE platform_message_id = ?
        `).run(source.result.platform_message_id);
      },
    },
    {
      reason: 'provider_lineage_invalid',
      corrupt(database, source) {
        database.prepare(`
          UPDATE runtime_lineages
          SET provider = 'claude', provider_native_id = 'claude-session-invalid',
            provider_native_id_bound_at = '2026-07-20T00:59:00Z',
            provider_native_state = 'invalid'
          WHERE lineage_id = ?
        `).run(source.accepted.lineage_id);
      },
    },
  ])('persists $reason as a pending provisional recovery', ({ reason, corrupt }) => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, reason);
    corrupt(database, source);

    const pending = accept(
      database,
      replyEnvelope(source.envelope, reason, source.result.platform_message_id),
      reason,
    );

    expect(pending).toMatchObject({
      status: 'accepted',
      conversation_id: source.accepted.conversation_id,
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
      error: null,
    });
    expect(database.prepare(`
      SELECT recovery.reason, recovery.candidate_lineage_id,
        recovery.side_effect_status, recovery.state, queue.wait_reason
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      reason,
      candidate_lineage_id: source.accepted.lineage_id,
      side_effect_status: 'none',
      state: 'queued',
      wait_reason: 'lineage_resolution_pending',
    });
    const command = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE turn_id = ?
    `).get(pending.turn_id).command_json);
    expect(command.mapping).toMatchObject({
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
      reason,
    });

    database.close();
  });

  test('marks a failed provider-native context invalid so the next reply enters recovery', () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-invalid-production');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'codex', provider_native_id = 'codex-thread-native-invalid',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId: 'executor-native-invalid-production',
      now: () => '2026-07-20T01:00:01Z',
      generateId: deterministicIds('native-invalid-production'),
    });
    const turn = store.claimNextQueuedTurn();
    store.transitionTurn(turn, 'starting', 'running');
    store.transitionTurn(turn, 'running', 'failed', {
      reasonCode: 'executor_failed',
      error: {
        code: 'provider_context_invalid',
        category: 'provider',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'The persisted provider context is invalid.',
        occurred_at: '2026-07-20T01:00:01Z',
      },
    });
    expect(database.prepare(`
      SELECT provider_native_state FROM runtime_lineages WHERE lineage_id = ?
    `).get(source.accepted.lineage_id)).toEqual({ provider_native_state: 'invalid' });

    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-invalid-production-reply',
        source.result.platform_message_id,
      ),
      'native-invalid-production-reply',
    );
    expect(pending).toMatchObject({
      status: 'accepted',
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
    });
    expect(database.prepare(`
      SELECT reason, candidate_lineage_id FROM runtime_reply_mapping_recoveries
      WHERE turn_id = ?
    `).get(pending.turn_id)).toEqual({
      reason: 'provider_lineage_invalid',
      candidate_lineage_id: source.accepted.lineage_id,
    });

    database.close();
  });

  test('rejects ordinary replies while recovery is pending without creating another turn', () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'pending-reply');
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'pending-reply-first', source.result.platform_message_id),
      'pending-reply-first',
    );
    const turnCount = database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
    `).get().count;

    const repeatedSourceReply = accept(
      database,
      replyEnvelope(source.envelope, 'pending-reply-source-repeat', source.result.platform_message_id),
      'pending-reply-source-repeat',
    );
    expect(repeatedSourceReply).toMatchObject({
      status: 'rejected',
      conversation_id: pending.conversation_id,
      turn_id: pending.turn_id,
      lineage_id: null,
      turn_version: 2,
      lineage_resolution_state: 'pending_recovery',
      error: {
        code: 'lineage_resolution_pending',
        category: 'conflict',
        retryable: false,
        side_effect_status: 'none',
      },
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count)
      .toBe(turnCount);

    const pendingDelivery = deliverOnlyPending(database, 'pending-reply-provisional');
    expect(pendingDelivery.command.mapping.turn_id).toBe(pending.turn_id);
    const provisionalReply = accept(
      database,
      replyEnvelope(
        source.envelope,
        'pending-reply-provisional-repeat',
        pendingDelivery.result.platform_message_id,
      ),
      'pending-reply-provisional-repeat',
    );
    expect(provisionalReply).toMatchObject({
      status: 'rejected',
      turn_id: pending.turn_id,
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
      error: { code: 'lineage_resolution_pending' },
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count)
      .toBe(turnCount);

    database.close();
  });

  test('associates and binds a delivered text fallback mapping with the same recovery', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'pending-fallback-reply');
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'pending-fallback-reply-first',
        source.result.platform_message_id,
      ),
      'pending-fallback-reply-first',
    );
    const turnCount = database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count;
    let deliveryTime = '2026-07-20T01:00:01Z';
    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-pending-fallback-reply',
      now: () => deliveryTime,
      generateId: deterministicIds('delivery-pending-fallback-reply'),
      throttleMs: 0,
    });
    let createCommand = null;
    for (let index = 0; index < 10 && createCommand === null; index += 1) {
      const command = outbox.claimNext();
      expect(command).not.toBeNull();
      if (command.mapping.turn_id === pending.turn_id && command.operation === 'create_main') {
        createCommand = command;
      } else {
        expect(outbox.recordResult(deliveredResult(
          command,
          '2026-07-20T01:00:01Z',
        )).status).toBe('applied');
      }
    }
    expect(createCommand).not.toBeNull();
    expect(outbox.recordResult(retryableDeliveryFailure(
      createCommand,
      '2026-07-20T01:00:01Z',
    ))).toMatchObject({ status: 'applied', outbox_status: 'retry_wait' });
    const fallbackCommand = outbox.claimNext();
    expect(fallbackCommand).toMatchObject({
      operation: 'send_text',
      mapping: {
        turn_id: pending.turn_id,
        binding_state: 'pending',
        lineage_id: null,
      },
    });
    expect(fallbackCommand.mapping.mapping_id).not.toBe(createCommand.mapping.mapping_id);
    const fallbackResult = deliveredResult(fallbackCommand, '2026-07-20T01:00:02Z');
    expect(outbox.recordResult(fallbackResult)).toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });

    const repeated = accept(
      database,
      replyEnvelope(
        source.envelope,
        'pending-fallback-reply-repeat',
        fallbackResult.platform_message_id,
      ),
      'pending-fallback-reply-repeat',
    );
    expect(repeated).toMatchObject({
      status: 'rejected',
      turn_id: pending.turn_id,
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
      error: { code: 'lineage_resolution_pending' },
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count)
      .toBe(turnCount);

    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, close: async () => {} },
      serviceInstanceId: 'executor-pending-fallback-reply',
      now: () => '2026-07-20T01:00:03Z',
      generateId: deterministicIds('executor-pending-fallback-reply'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'lineage_resolution_pending',
      turn_id: pending.turn_id,
    });
    deliveryTime = '2026-07-20T01:10:00Z';
    const staleCreateRetry = outbox.claimNext();
    expect(staleCreateRetry).toMatchObject({ operation: 'create_main' });
    expect(outbox.recordResult(permanentDeliveryFailure(
      staleCreateRetry,
      deliveryTime,
    ))).toMatchObject({ status: 'applied', outbox_status: 'dead_letter' });
    deliverUntilTurn(
      database,
      pending.turn_id,
      'pending-fallback-reply-notice',
      deliveryTime,
    );
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: pending.turn_id,
    });
    expect(execute).toHaveBeenCalledTimes(1);
    expect(database.prepare(`
      SELECT state, bound_lineage_id
      FROM runtime_reply_mapping_recoveries
      WHERE turn_id = ?
    `).get(pending.turn_id)).toMatchObject({
      state: 'bound',
      bound_lineage_id: expect.any(String),
    });
    await service.close();

    database.close();
  });

  test('claims a pending lineage only to publish its notice and never calls the provider before delivery', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'notice-barrier');
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'notice-barrier-pending', source.result.platform_message_id),
      'notice-barrier-pending',
    );
    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const nativeRecover = jest.fn(async () => ({ status: 'recovered' }));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: {
        execute,
        recoverLineage: nativeRecover,
        close: async () => {},
      },
      serviceInstanceId: 'executor-notice-barrier',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('notice-barrier-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'lineage_resolution_pending',
      turn_id: pending.turn_id,
      wait_reason: 'reply_mapping_notice_delivery',
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'lineage_resolution_pending',
      turn_id: pending.turn_id,
      wait_reason: 'reply_mapping_notice_delivery',
    });
    expect(execute).not.toHaveBeenCalled();
    expect(nativeRecover).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT turn.state AS turn_state, turn.attempt_id, turn.attempt_no, turn.lease_epoch,
        queue.status, queue.wait_reason,
        recovery.state AS recovery_state, recovery.notice_event_sequence
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      JOIN runtime_reply_mapping_recoveries AS recovery ON recovery.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      turn_state: 'starting',
      attempt_id: null,
      attempt_no: null,
      lease_epoch: null,
      status: 'claimed',
      wait_reason: 'reply_mapping_notice_delivery',
      recovery_state: 'notice_pending',
      notice_event_sequence: 3,
    });
    const noticeCommand = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE turn_id = ? AND status = 'pending'
    `).get(pending.turn_id).command_json);
    expect(noticeCommand).toMatchObject({
      aggregate_version: 3,
      event_sequence_through: 3,
      render_model: {
        phase: 'starting',
        text: expect.stringContaining('lineage'),
      },
    });
    expect(noticeCommand.mapping).toMatchObject({
      lineage_id: null,
      binding_state: 'pending',
      reason: 'mapping_missing',
    });

    await service.close();
    database.close();
  });

  test.each([
    { claimed: false, expectedTurnState: 'cancelled', expectedQueueState: 'cancelled' },
    { claimed: true, expectedTurnState: 'stopped', expectedQueueState: 'stopped' },
  ])('stop terminalizes a $expectedTurnState reply recovery instead of stranding it', async ({
    claimed,
    expectedTurnState,
    expectedQueueState,
  }) => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, `stop-recovery-${expectedTurnState}`);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        `stop-recovery-${expectedTurnState}-pending`,
        source.result.platform_message_id,
      ),
      `stop-recovery-${expectedTurnState}-pending`,
    );
    const execute = jest.fn();
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: { execute, recoverLineage: jest.fn(), close: async () => {} },
      serviceInstanceId: `executor-stop-recovery-${expectedTurnState}`,
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds(`stop-recovery-${expectedTurnState}`),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });
    if (claimed) {
      await expect(service.runNext()).resolves.toMatchObject({
        status: 'lineage_resolution_pending',
        turn_id: pending.turn_id,
      });
    }

    await expect(service.stop({
      conversation_id: pending.conversation_id,
      stop_id: `stop-recovery-${expectedTurnState}`,
    })).resolves.toMatchObject({
      conversation_id: pending.conversation_id,
      provider_stop_status: 'not_applicable',
      lease_released: true,
    });
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT recovery.state AS recovery_state,
        turn.state AS turn_state, queue.status AS queue_state
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      recovery_state: 'rejected',
      turn_state: expectedTurnState,
      queue_state: expectedQueueState,
    });
    const afterStop = accept(
      database,
      replyEnvelope(
        source.envelope,
        `stop-recovery-${expectedTurnState}-after`,
        source.result.platform_message_id,
      ),
      `stop-recovery-${expectedTurnState}-after`,
    );
    expect(afterStop).toMatchObject({
      status: 'accepted',
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
    });
    expect(afterStop.turn_id).not.toBe(pending.turn_id);

    await service.close();
    database.close();
  });

  test('attempts one unique native candidate only after notice delivery and binds before provider execution', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-success');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'claude', provider_native_id = 'claude-session-native-success',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'native-success-pending', source.result.platform_message_id),
      'native-success-pending',
    );

    const execute = jest.fn((context) => (async function* executeProvider() {
      expect(context.lineage_id).toBe(source.accepted.lineage_id);
      expect(context.lineage).toEqual({
        provider_native_id: 'claude-session-native-success',
      });
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const recoverLineage = jest.fn(async (request) => ({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: request.candidate.lineage_id,
      provider: request.candidate.provider,
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    }));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: {
        execute,
        recoverLineage,
        close: async () => {},
      },
      serviceInstanceId: 'executor-native-success',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-success-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'lineage_resolution_pending',
      turn_id: pending.turn_id,
      wait_reason: 'reply_mapping_notice_delivery',
    });
    expect(recoverLineage).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();

    const noticeDelivery = deliverUntilTurn(
      database,
      pending.turn_id,
      'native-success-notice',
    );
    expect(noticeDelivery.command.mapping.turn_id).toBe(pending.turn_id);
    expect(noticeDelivery.command.event_sequence_through).toBe(3);

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: pending.turn_id,
    });
    expect(recoverLineage).toHaveBeenCalledTimes(1);
    expect(recoverLineage).toHaveBeenCalledWith(expect.objectContaining({
      turn_id: pending.turn_id,
      reason: 'mapping_missing',
      native_recovery_attempt_no: 1,
      candidate: {
        lineage_id: source.accepted.lineage_id,
        provider: 'claude',
        provider_native_id: 'claude-session-native-success',
      },
    }));
    expect(execute).toHaveBeenCalledTimes(1);

    const durable = database.prepare(`
      SELECT turn.lineage_id, turn.state AS turn_state,
        recovery.state AS recovery_state, recovery.native_recovery_attempt_count,
        recovery.bound_lineage_id, lane.mapping_json
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_delivery_lanes AS lane ON lane.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id);
    expect(durable).toMatchObject({
      lineage_id: source.accepted.lineage_id,
      turn_state: 'completed',
      recovery_state: 'bound',
      native_recovery_attempt_count: 1,
      bound_lineage_id: source.accepted.lineage_id,
    });
    expect(JSON.parse(durable.mapping_json)).toMatchObject({
      lineage_id: source.accepted.lineage_id,
      binding_state: 'bound',
      mapping_version: 2,
      reason: 'mapping_missing',
    });
    expect(database.prepare(`
      SELECT lineage_id, binding_state, mapping_version, reason
      FROM runtime_message_mappings
      WHERE platform_message_id = ?
    `).get(noticeDelivery.result.platform_message_id)).toEqual({
      lineage_id: source.accepted.lineage_id,
      binding_state: 'bound',
      mapping_version: 2,
      reason: 'mapping_missing',
    });

    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    expect(recoverLineage).toHaveBeenCalledTimes(1);
    const recoveredReply = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-success-recovered-reply',
        source.result.platform_message_id,
      ),
      'native-success-recovered-reply',
    );
    expect(recoveredReply).toMatchObject({
      status: 'accepted',
      lineage_id: source.accepted.lineage_id,
      lineage_resolution_state: 'bound',
      error: null,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_reply_mapping_recoveries
    `).get()).toEqual({ count: 1 });
    database.prepare(`
      UPDATE runtime_lineages
      SET provider_native_state = 'invalid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    const invalidAgain = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-success-invalid-again',
        source.result.platform_message_id,
      ),
      'native-success-invalid-again',
    );
    expect(invalidAgain).toMatchObject({
      status: 'accepted',
      lineage_id: null,
      lineage_resolution_state: 'pending_recovery',
    });
    expect(database.prepare(`
      SELECT reason, candidate_lineage_id
      FROM runtime_reply_mapping_recoveries
      WHERE turn_id = ?
    `).get(invalidAgain.turn_id)).toEqual({
      reason: 'provider_lineage_invalid',
      candidate_lineage_id: source.accepted.lineage_id,
    });
    await service.close();
    database.close();
  });

  test('does not guess without a unique candidate and creates an explicit recovery lineage', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'no-candidate');
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES ('lineage-no-candidate-alternate', ?, 'normal', 0, ?)
    `).run(source.accepted.conversation_id, source.accepted.committed_at);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pendingEnvelope = replyEnvelope(
      source.envelope,
      'no-candidate-pending',
      source.result.platform_message_id,
    );
    pendingEnvelope.content.text = 'Continue this request after safe lineage recovery.';
    const pending = accept(database, pendingEnvelope, 'no-candidate-pending');
    expect(database.prepare(`
      SELECT candidate_lineage_id FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(pending.turn_id)).toEqual({ candidate_lineage_id: null });

    const recoverLineage = jest.fn();
    const execute = jest.fn((context) => (async function* executeProvider() {
      expect(context.lineage_id).not.toBe(source.accepted.lineage_id);
      expect(context.provider_native_id).toBeNull();
      expect(context.input.text).toContain('Zylos recovery handoff');
      expect(context.input.text).toContain('Please inspect the attached report.');
      expect(context.input.text).toContain('Continue this request after safe lineage recovery.');
      expect(context.input.text).toContain('Existing Zylos memory');
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-no-candidate',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('no-candidate-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'no-candidate-notice');
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: pending.turn_id,
    });
    expect(recoverLineage).not.toHaveBeenCalled();
    const recoveryLineage = database.prepare(`
      SELECT lineage.lineage_kind, lineage.recovery_of_lineage_id,
        recovery.native_recovery_attempt_count, recovery.native_recovery_status,
        recovery.bound_lineage_id
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_lineages AS lineage ON lineage.lineage_id = recovery.bound_lineage_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id);
    expect(recoveryLineage).toEqual({
      lineage_kind: 'recovery',
      recovery_of_lineage_id: null,
      native_recovery_attempt_count: 0,
      native_recovery_status: 'not_applicable',
      bound_lineage_id: expect.any(String),
    });

    await service.close();
    database.close();
  });

  test('fences an in-flight native recovery against concurrent executor services', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-concurrent');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'codex', provider_native_id = 'codex-thread-native-concurrent',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-concurrent-pending',
        source.result.platform_message_id,
      ),
      'native-concurrent-pending',
    );
    const unrelatedEnvelope = normalEnvelope('native-concurrent-unrelated');
    unrelatedEnvelope.chat_id = 'chat-native-concurrent-unrelated';
    const unrelated = accept(
      database,
      unrelatedEnvelope,
      'native-concurrent-unrelated',
    );
    const nativeStarted = createDeferred();
    const nativeResult = createDeferred();
    const executeA = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const executeB = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const recoverA = jest.fn(async (request) => {
      nativeStarted.resolve(request);
      return nativeResult.promise;
    });
    const serviceA = createExecutorService({
      database,
      provider: 'codex',
      adapter: { execute: executeA, recoverLineage: recoverA, close: async () => {} },
      serviceInstanceId: 'executor-native-concurrent-A',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-concurrent-A'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });
    const recoverB = jest.fn();
    const serviceB = createExecutorService({
      database,
      provider: 'codex',
      adapter: { execute: executeB, recoverLineage: recoverB, close: async () => {} },
      serviceInstanceId: 'executor-native-concurrent-B',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-concurrent-B'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await serviceA.runNext();
    deliverUntilTurn(database, pending.turn_id, 'native-concurrent-notice');
    const firstRun = serviceA.runNext();
    const nativeRequest = await nativeStarted.promise;
    await expect(serviceB.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: unrelated.turn_id,
    });
    expect(executeA).not.toHaveBeenCalled();
    expect(executeB).toHaveBeenCalledTimes(1);
    expect(recoverB).not.toHaveBeenCalled();
    nativeResult.resolve({
      status: 'recovered',
      recovery_id: nativeRequest.recovery_id,
      lineage_id: nativeRequest.candidate.lineage_id,
      provider: nativeRequest.candidate.provider,
      provider_native_id: nativeRequest.candidate.provider_native_id,
      native_recovery_attempt_id: nativeRequest.native_recovery_attempt_id,
      native_recovery_attempt_no: nativeRequest.native_recovery_attempt_no,
      side_effect_status: 'none',
    });
    await expect(firstRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: pending.turn_id,
    });
    expect(recoverA).toHaveBeenCalledTimes(1);
    expect(executeA).toHaveBeenCalledTimes(1);
    expect(executeB).toHaveBeenCalledTimes(1);

    await serviceA.close();
    await serviceB.close();
    database.close();
  });

  test('rejects a native recovery result after its durable owner lease expires', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-expired-result');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'codex', provider_native_id = 'codex-thread-native-expired-result',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-expired-result-pending',
        source.result.platform_message_id,
      ),
      'native-expired-result-pending',
    );
    let currentTime = '2026-07-20T01:00:02Z';
    const nativeStarted = createDeferred();
    const nativeResult = createDeferred();
    const execute = jest.fn();
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: {
        execute,
        recoverLineage: jest.fn(async (request) => {
          nativeStarted.resolve(request);
          return nativeResult.promise;
        }),
        close: async () => {},
      },
      serviceInstanceId: 'executor-native-expired-result',
      now: () => currentTime,
      generateId: deterministicIds('native-expired-result'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'native-expired-result-notice');
    const running = service.runNext();
    const request = await nativeStarted.promise;
    currentTime = '2026-07-20T01:02:00Z';
    nativeResult.resolve({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: request.candidate.lineage_id,
      provider: request.candidate.provider,
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    });
    await expect(running).rejects.toMatchObject({
      persistenceFailure: true,
      code: 'stale_attempt',
    });
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT state, bound_lineage_id
      FROM runtime_reply_mapping_recoveries
      WHERE turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: 'native_recovery_claimed',
      bound_lineage_id: null,
    });

    await service.close();
    database.close();
  });

  test('waits for native recovery during close and never starts provider work afterward', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-close-race');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'codex', provider_native_id = 'codex-thread-native-close-race',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'native-close-race-pending',
        source.result.platform_message_id,
      ),
      'native-close-race-pending',
    );
    const nativeStarted = createDeferred();
    const nativeResult = createDeferred();
    const execute = jest.fn();
    const closeAdapter = jest.fn(async () => []);
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: {
        execute,
        recoverLineage: jest.fn(async (request) => {
          nativeStarted.resolve(request);
          return nativeResult.promise;
        }),
        close: closeAdapter,
      },
      serviceInstanceId: 'executor-native-close-race',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-close-race'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'native-close-race-notice');
    const running = service.runNext();
    const request = await nativeStarted.promise;
    let closeSettled = false;
    const closing = service.close().then(() => {
      closeSettled = true;
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(closeAdapter).toHaveBeenCalledTimes(1);
    expect(closeSettled).toBe(false);
    nativeResult.resolve({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: request.candidate.lineage_id,
      provider: request.candidate.provider,
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    });
    await expect(running).resolves.toMatchObject({
      status: 'service_closing',
      turn_id: pending.turn_id,
      recovery_status: 'bound',
    });
    await closing;
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT recovery.state, recovery.bound_lineage_id,
        turn.state AS turn_state, queue.status AS queue_status
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toMatchObject({
      state: 'bound',
      bound_lineage_id: source.accepted.lineage_id,
      turn_state: 'recovering',
      queue_status: 'queued',
    });

    database.close();
  });

  test('rejects a mismatched recovered result instead of silently creating a fallback lineage', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-mismatch');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'codex', provider_native_id = 'codex-thread-native-mismatch',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'native-mismatch-pending', source.result.platform_message_id),
      'native-mismatch-pending',
    );
    const execute = jest.fn();
    const recoverLineage = jest.fn(async (request) => ({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: 'lineage-mismatched-provider-result',
      provider: request.candidate.provider,
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    }));
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-native-mismatch',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-mismatch'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'native-mismatch-notice');
    await expect(service.runNext()).rejects.toMatchObject({
      persistenceFailure: true,
      code: 'provider_context_invalid',
    });
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT recovery.state, recovery.bound_lineage_id, turn.lineage_id
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: 'native_recovery_claimed',
      bound_lineage_id: null,
      lineage_id: null,
    });

    await service.close();
    database.close();
  });

  test('falls back to a linked recovery lineage after the single native attempt fails', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'native-failure');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'claude', provider_native_id = 'claude-session-native-failure',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'native-failure-pending', source.result.platform_message_id),
      'native-failure-pending',
    );
    const recoverLineage = jest.fn(async (request) => ({
      status: 'failed',
      recovery_id: request.recovery_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    }));
    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-native-failure',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('native-failure-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'native-failure-notice');
    await expect(service.runNext()).resolves.toMatchObject({ status: 'completed' });
    expect(recoverLineage).toHaveBeenCalledTimes(1);
    const durable = database.prepare(`
      SELECT recovery.native_recovery_attempt_count, recovery.native_recovery_status,
        recovery.bound_lineage_id, lineage.lineage_kind, lineage.recovery_of_lineage_id
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_lineages AS lineage ON lineage.lineage_id = recovery.bound_lineage_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id);
    expect(durable).toEqual({
      native_recovery_attempt_count: 1,
      native_recovery_status: 'failed',
      bound_lineage_id: expect.any(String),
      lineage_kind: 'recovery',
      recovery_of_lineage_id: source.accepted.lineage_id,
    });

    await service.close();
    database.close();
  });

  test('rolls back a failed final binding and never repeats the claimed native recovery', async () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const source = createDeliveredSource(database, 'binding-rollback');
    database.prepare(`
      UPDATE runtime_lineages
      SET provider = 'claude', provider_native_id = 'claude-session-binding-rollback',
        provider_native_id_bound_at = '2026-07-20T00:59:00Z',
        provider_native_state = 'valid'
      WHERE lineage_id = ?
    `).run(source.accepted.lineage_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'binding-rollback-pending', source.result.platform_message_id),
      'binding-rollback-pending',
    );
    const recoverLineage = jest.fn(async (request) => ({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: request.candidate.lineage_id,
      provider: request.candidate.provider,
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    }));
    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-binding-rollback',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('binding-rollback-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    const notice = deliverUntilTurn(database, pending.turn_id, 'binding-rollback-notice');
    database.exec(`
      CREATE TEMP TRIGGER fail_reply_mapping_lane_binding
      BEFORE UPDATE OF mapping_json ON runtime_delivery_lanes
      BEGIN
        SELECT RAISE(ABORT, 'injected lane binding failure');
      END;
    `);
    await expect(service.runNext()).rejects.toMatchObject({ persistenceFailure: true });
    expect(recoverLineage).toHaveBeenCalledTimes(1);
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT turn.lineage_id, turn.state AS turn_state, recovery.state AS recovery_state,
        recovery.native_recovery_attempt_count, recovery.bound_lineage_id, lane.mapping_json
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_delivery_lanes AS lane ON lane.turn_id = recovery.turn_id
      WHERE recovery.turn_id = ?
    `).get(pending.turn_id)).toMatchObject({
      lineage_id: null,
      turn_state: 'recovering',
      recovery_state: 'native_recovery_claimed',
      native_recovery_attempt_count: 1,
      bound_lineage_id: null,
      mapping_json: expect.stringContaining('"binding_state":"pending"'),
    });
    expect(database.prepare(`
      SELECT lineage_id, binding_state, mapping_version
      FROM runtime_message_mappings WHERE platform_message_id = ?
    `).get(notice.result.platform_message_id)).toEqual({
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
    });

    database.exec('DROP TRIGGER fail_reply_mapping_lane_binding');
    await service.close();
    database.close();

    const reopenedDatabase = new Database(databasePath);
    const restartedService = createExecutorService({
      database: reopenedDatabase,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-binding-rollback-restarted',
      now: () => '2026-07-20T01:01:03Z',
      generateId: deterministicIds('binding-rollback-restarted-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });
    await expect(restartedService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: pending.turn_id,
    });
    expect(recoverLineage).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(reopenedDatabase.prepare(`
      SELECT state, native_recovery_attempt_count, native_recovery_status,
        bound_lineage_id FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(pending.turn_id)).toMatchObject({
      state: 'bound',
      native_recovery_attempt_count: 1,
      native_recovery_status: 'lost',
      bound_lineage_id: expect.any(String),
    });

    await restartedService.close();
    expect(reopenedDatabase.pragma('foreign_key_check')).toEqual([]);
    expect(reopenedDatabase.pragma('integrity_check')).toEqual([{ integrity_check: 'ok' }]);
    reopenedDatabase.close();
  });

  test('makes a bound recovery idempotent for the same lineage and immutable for any other value', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'binding-immutable');
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(source.envelope, 'binding-immutable-pending', source.result.platform_message_id),
      'binding-immutable-pending',
    );
    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, close: async () => {} },
      serviceInstanceId: 'executor-binding-immutable',
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds('binding-immutable-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });
    await service.runNext();
    const notice = deliverUntilTurn(database, pending.turn_id, 'binding-immutable-notice');
    await service.runNext();
    await service.close();

    const recovery = database.prepare(`
      SELECT recovery_id, mapping_id, bound_lineage_id
      FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(pending.turn_id);
    const store = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-binding-immutable-retry',
      now: () => '2026-07-20T01:00:04Z',
      generateId: deterministicIds('binding-immutable-retry'),
    });
    expect(store.completeReplyMappingRecovery({
      recovery_id: recovery.recovery_id,
      turn_id: pending.turn_id,
      lineage_id: recovery.bound_lineage_id,
    }, null)).toEqual({
      status: 'duplicate',
      turn_id: pending.turn_id,
      lineage_id: recovery.bound_lineage_id,
    });
    const otherLineageId = 'lineage-binding-immutable-other';
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES (?, ?, 'recovery', 0, '2026-07-20T01:00:04Z')
    `).run(otherLineageId, pending.conversation_id);
    expect(() => store.completeReplyMappingRecovery({
      recovery_id: recovery.recovery_id,
      turn_id: pending.turn_id,
      lineage_id: otherLineageId,
    }, null)).toThrow(expect.objectContaining({ code: 'version_conflict' }));

    expect(() => database.prepare(`
      UPDATE runtime_message_mappings SET lineage_id = ? WHERE platform_message_id = ?
    `).run(otherLineageId, notice.result.platform_message_id)).toThrow(/immutable/);
    expect(() => database.prepare(`
      UPDATE runtime_message_mappings
      SET platform_message_id = platform_message_id || '-moved'
      WHERE platform_message_id = ?
    `).run(notice.result.platform_message_id)).toThrow(/immutable/);
    expect(() => database.prepare(`
      UPDATE runtime_message_mappings SET reason = 'mapping_corrupt'
      WHERE platform_message_id = ?
    `).run(notice.result.platform_message_id)).toThrow(/immutable/);
    expect(() => database.prepare(`
      DELETE FROM runtime_message_mappings WHERE platform_message_id = ?
    `).run(notice.result.platform_message_id)).toThrow(/cannot be deleted/);
    expect(() => database.prepare(`
      UPDATE runtime_turns SET lineage_id = ? WHERE turn_id = ?
    `).run(otherLineageId, pending.turn_id)).toThrow(/immutable/);
    const lane = database.prepare(`
      SELECT lane_key, mapping_json FROM runtime_delivery_lanes WHERE turn_id = ?
    `).get(pending.turn_id);
    const changedLaneMapping = { ...JSON.parse(lane.mapping_json), lineage_id: otherLineageId };
    expect(() => database.prepare(`
      UPDATE runtime_delivery_lanes SET mapping_json = ? WHERE lane_key = ?
    `).run(JSON.stringify(changedLaneMapping), lane.lane_key)).toThrow(/immutable/);
    const changedLaneProvenance = {
      ...JSON.parse(lane.mapping_json),
      reason: 'mapping_corrupt',
    };
    expect(() => database.prepare(`
      UPDATE runtime_delivery_lanes SET mapping_json = ? WHERE lane_key = ?
    `).run(JSON.stringify(changedLaneProvenance), lane.lane_key)).toThrow(/immutable/);
    expect(() => database.prepare(`
      UPDATE runtime_reply_mapping_recoveries SET bound_lineage_id = ? WHERE recovery_id = ?
    `).run(otherLineageId, recovery.recovery_id)).toThrow(/immutable/);
    const pendingOutbox = database.prepare(`
      SELECT outbox_id, command_json FROM runtime_outbox
      WHERE turn_id = ? AND status IN ('pending', 'retry_wait')
        AND json_extract(command_json, '$.mapping.binding_state') = 'bound'
      LIMIT 1
    `).get(pending.turn_id);
    expect(pendingOutbox).toBeDefined();
    const changedCommand = JSON.parse(pendingOutbox.command_json);
    changedCommand.mapping.lineage_id = otherLineageId;
    expect(() => database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify(changedCommand), pendingOutbox.outbox_id)).toThrow(/immutable/);
    const changedOutboxReason = JSON.parse(pendingOutbox.command_json);
    changedOutboxReason.mapping.reason = 'mapping_corrupt';
    expect(() => database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify(changedOutboxReason), pendingOutbox.outbox_id)).toThrow(/immutable/);

    database.close();
  });

  test.each([
    { decision: 'approve', expectedState: 'completed', expectedCalls: 1 },
    { decision: 'deny', expectedState: 'stopped', expectedCalls: 0 },
  ])('waits for an authorized $decision decision when associated side effects are unknown', async ({
    decision,
    expectedState,
    expectedCalls,
  }) => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, `unknown-${decision}`);
    markAssociatedSideEffectUnknown(database, source.accepted.turn_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        `unknown-${decision}-pending`,
        source.result.platform_message_id,
      ),
      `unknown-${decision}-pending`,
    );
    expect(database.prepare(`
      SELECT side_effect_status FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(pending.turn_id)).toEqual({ side_effect_status: 'unknown' });

    const recoverLineage = jest.fn();
    const execute = jest.fn(() => (async function* executeProvider() {
      yield { type: 'turn_result', outcome: 'completed' };
    }()));
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: `executor-unknown-${decision}`,
      now: () => '2026-07-20T01:00:02Z',
      generateId: deterministicIds(`unknown-${decision}-service`),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    const notice = deliverUntilTurn(database, pending.turn_id, `unknown-${decision}-notice`);
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'waiting_decision',
      turn_id: pending.turn_id,
      side_effect_status: 'unknown',
      interaction_id: expect.any(String),
    });
    expect(recoverLineage).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
    const interaction = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions
      WHERE turn_id = ? AND parent_type = 'recovery_control'
    `).get(pending.turn_id).request_json);
    expect(interaction).toMatchObject({
      interaction_id: expect.any(String),
      control_id: expect.any(String),
      kind: 'recovery_decision',
      lineage_id: null,
      authorized_subjects: [{
        type: 'actor',
        actor_id: source.envelope.actor.actor_id,
      }],
      state: 'pending',
    });
    const decisionCard = deliverUntilCommand(
      database,
      `unknown-${decision}-decision-card`,
      (command) => command.mapping.turn_id === pending.turn_id
        && command.render_model.interactions.some(
          ({ interaction_id: interactionId }) => interactionId === interaction.interaction_id,
        ),
      '2026-07-20T01:00:10Z',
    );
    expect(decisionCard.command).toMatchObject({
      operation: 'update_main',
      render_model: {
        user_action_required: true,
        interactions: expect.arrayContaining([
          expect.objectContaining({ interaction_id: interaction.interaction_id }),
        ]),
      },
    });

    if (decision === 'approve') {
      const unauthorized = recoveryDecisionAnswer(
        interaction,
        source.envelope,
        decision,
        'unauthorized',
      );
      unauthorized.actor.actor_id = 'unauthorized-actor';
      expect(service.submitInteractionAnswer(unauthorized, {
        replyToMessageId: decisionCard.result.platform_message_id,
      })).toMatchObject({
        status: 'rejected',
        error: { code: 'interaction_actor_forbidden' },
      });
      expect(database.prepare(`
        SELECT state FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
      `).get(pending.turn_id)).toEqual({ state: 'waiting_decision' });
    }

    const acceptedDecision = service.submitInteractionAnswer(
      recoveryDecisionAnswer(interaction, source.envelope, decision, decision),
      { replyToMessageId: decisionCard.result.platform_message_id },
    );
    expect(acceptedDecision.status).toBe('accepted');
    const resolution = await service.deliverInteractionAnswer(
      database.prepare(`
        SELECT handoff_id FROM runtime_interaction_handoffs WHERE interaction_id = ?
      `).get(interaction.interaction_id).handoff_id,
    );
    expect(resolution.acknowledgement.status).toBe('accepted');

    if (decision === 'approve') {
      await expect(service.runNext()).resolves.toMatchObject({
        status: 'completed',
        turn_id: pending.turn_id,
      });
    }
    expect(execute).toHaveBeenCalledTimes(expectedCalls);
    expect(recoverLineage).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT turn.state, recovery.state AS recovery_state
      FROM runtime_turns AS turn
      JOIN runtime_reply_mapping_recoveries AS recovery ON recovery.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: expectedState,
      recovery_state: decision === 'approve' ? 'bound' : 'rejected',
    });

    await service.close();
    database.close();
  });

  test('expires an unanswered unknown-side-effect decision as a safe rejection', async () => {
    const database = openTestDatabase();
    const source = createDeliveredSource(database, 'unknown-expired');
    markAssociatedSideEffectUnknown(database, source.accepted.turn_id);
    simulateMissingDeliveredMapping(database, source.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(source.accepted.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.accepted.turn_id);
    const pending = accept(
      database,
      replyEnvelope(
        source.envelope,
        'unknown-expired-pending',
        source.result.platform_message_id,
      ),
      'unknown-expired-pending',
    );
    let currentTime = '2026-07-20T01:00:02Z';
    const recoverLineage = jest.fn();
    const execute = jest.fn();
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: { execute, recoverLineage, close: async () => {} },
      serviceInstanceId: 'executor-unknown-expired',
      now: () => currentTime,
      generateId: deterministicIds('unknown-expired-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'unknown-expired-notice');
    const waiting = await service.runNext();
    currentTime = '2026-07-20T01:11:00Z';
    await expect(service.expireInteraction({
      interaction_id: waiting.interaction_id,
      interaction_version: 1,
    })).resolves.toMatchObject({
      status: 'expired',
      turn_id: pending.turn_id,
      turn_state: 'stopped',
    });
    expect(recoverLineage).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
    expect(database.prepare(`
      SELECT turn.state, queue.status AS queue_status, queue.wait_reason,
        recovery.state AS recovery_state, recovery.native_recovery_status
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      JOIN runtime_reply_mapping_recoveries AS recovery ON recovery.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.turn_id)).toEqual({
      state: 'stopped',
      queue_status: 'stopped',
      wait_reason: null,
      recovery_state: 'rejected',
      native_recovery_status: 'expired',
    });

    await service.close();
    database.close();
  });
});
