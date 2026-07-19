import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateDeliveryMapping,
  validateInboundResult,
  validateNormalizedEvent,
} from '../contracts/public/index.js';
import {
  acceptNormalInbound,
  initializeRuntimePersistence,
} from '../runtime/persistence/inbound-acceptance.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-inbound-acceptance-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function normalEnvelope() {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  return JSON.parse(JSON.stringify(fixture));
}

function deterministicOptions() {
  const counts = new Map();
  return {
    now: () => '2026-07-19T06:00:00Z',
    generateId(kind) {
      const next = (counts.get(kind) ?? 0) + 1;
      counts.set(kind, next);
      return `${kind}-test-${next}`;
    },
  };
}

function refreshInboundIdempotencyKey(envelope) {
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function nextEnvelope(envelope, suffix) {
  const next = JSON.parse(JSON.stringify(envelope));
  next.inbound_event_id = `evt-${suffix}`;
  next.trace_id = `trace-${suffix}`;
  next.message_id = `message-${suffix}`;
  next.occurred_at = '2026-07-19T06:10:00Z';
  next.received_at = '2026-07-19T06:10:01Z';
  return refreshInboundIdempotencyKey(next);
}

const RUNTIME_TABLES = Object.freeze([
  'runtime_conversations',
  'runtime_lineages',
  'runtime_inbound_events',
  'runtime_inbound_idempotency',
  'runtime_turns',
  'runtime_turn_queue',
  'runtime_normalized_events',
  'runtime_outbox',
  'runtime_delivery_lanes',
  'runtime_projection_snapshots',
  'runtime_message_mappings',
]);

function readTableCounts(database) {
  return Object.fromEntries(RUNTIME_TABLES.map((table) => [
    table,
    database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count,
  ]));
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('acceptNormalInbound', () => {
  test('atomically persists a normal queued turn and its initial user notification', () => {
    const database = openTestDatabase();
    const result = acceptNormalInbound(database, normalEnvelope(), deterministicOptions());

    expect(validateInboundResult(result).forwarded).toEqual(result);
    expect(result).toMatchObject({
      status: 'accepted',
      turn_version: 2,
      lineage_resolution_state: 'bound',
      deduplicated: false,
      error: null,
      committed_at: '2026-07-19T06:00:00Z',
    });

    const tableCounts = readTableCounts(database);
    expect(tableCounts).toEqual({
      runtime_conversations: 1,
      runtime_lineages: 1,
      runtime_inbound_events: 1,
      runtime_inbound_idempotency: 1,
      runtime_turns: 1,
      runtime_turn_queue: 1,
      runtime_normalized_events: 2,
      runtime_outbox: 1,
      runtime_delivery_lanes: 1,
      runtime_projection_snapshots: 1,
      runtime_message_mappings: 0,
    });

    expect(database.prepare(`
      SELECT state, turn_version, queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(result.turn_id)).toEqual({
      state: 'queued',
      turn_version: 2,
      queue_sequence: 1,
    });
    expect(database.prepare(`
      SELECT aggregate_version, event_sequence_through, status
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
    `).get(result.turn_id)).toEqual({
      aggregate_version: 2,
      event_sequence_through: 2,
      status: 'staged',
    });
    expect(database.prepare(`
      SELECT status, queue_sequence
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(result.turn_id)).toEqual({ status: 'queued', queue_sequence: 1 });

    const events = database.prepare(`
      SELECT event_json
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence ASC
    `).all(result.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson));
    expect(events.map((event) => event.phase)).toEqual(['received', 'queued']);
    for (const event of events) {
      expect(validateNormalizedEvent(event).forwarded).toEqual(event);
    }

    const outbox = database.prepare(`
      SELECT aggregate_type, aggregate_id, status, command_json
      FROM runtime_outbox
      WHERE turn_id = ?
    `).get(result.turn_id);
    const command = JSON.parse(outbox.command_json);
    expect(outbox).toMatchObject({
      aggregate_type: 'turn_main',
      aggregate_id: result.turn_id,
      status: 'pending',
    });
    expect(validateDeliveryCommand(command).forwarded).toEqual(command);
    expect(command).toMatchObject({
      aggregate_type: 'turn_main',
      aggregate_id: result.turn_id,
      operation: 'create_main',
      aggregate_version: 1,
      event_sequence_through: 1,
      render_model: {
        phase: 'received',
        text: 'Message received.',
        terminal: false,
      },
      mapping: {
        conversation_id: result.conversation_id,
        turn_id: result.turn_id,
        lineage_id: result.lineage_id,
        binding_state: 'bound',
        mapping_version: 1,
      },
    });

    database.close();
  });

  test('persists queue-full admission as received then failed with a user notification', () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope();
    const dependencies = deterministicOptions();
    const first = acceptNormalInbound(database, envelope, {
      ...dependencies,
      maxQueuedTurns: 1,
    });
    const rejected = acceptNormalInbound(
      database,
      nextEnvelope(envelope, 'queue-full'),
      {
        ...dependencies,
        maxQueuedTurns: 1,
      },
    );

    expect(first.status).toBe('accepted');
    expect(validateInboundResult(rejected).forwarded).toEqual(rejected);
    expect(rejected).toMatchObject({
      status: 'rejected',
      conversation_id: first.conversation_id,
      lineage_id: first.lineage_id,
      turn_version: 2,
      lineage_resolution_state: 'bound',
      deduplicated: false,
      error: {
        code: 'queue_full',
        category: 'capacity',
        retryable: true,
        side_effect_status: 'none',
        user_message: 'The conversation queue is full.',
      },
      committed_at: '2026-07-19T06:00:00Z',
    });
    expect(rejected.turn_id).not.toBe(first.turn_id);

    expect(readTableCounts(database)).toEqual({
      runtime_conversations: 1,
      runtime_lineages: 1,
      runtime_inbound_events: 2,
      runtime_inbound_idempotency: 2,
      runtime_turns: 2,
      runtime_turn_queue: 1,
      runtime_normalized_events: 4,
      runtime_outbox: 2,
      runtime_delivery_lanes: 2,
      runtime_projection_snapshots: 2,
      runtime_message_mappings: 0,
    });
    expect(database.prepare(`
      SELECT state, turn_version, queue_sequence, attempt_id
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(rejected.turn_id)).toEqual({
      state: 'failed',
      turn_version: 2,
      queue_sequence: 2,
      attempt_id: null,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(rejected.turn_id).count).toBe(0);

    const events = database.prepare(`
      SELECT event_json
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence ASC
    `).all(rejected.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson));
    expect(events.map((event) => event.phase)).toEqual(['received', 'failed']);
    expect(events[0].error).toBeNull();
    expect(events[1]).toMatchObject({
      turn_version: 2,
      error: rejected.error,
      payload: {
        from_state: 'received',
        to_state: 'failed',
        reason_code: 'queue_full',
      },
    });
    for (const event of events) {
      expect(validateNormalizedEvent(event).forwarded).toEqual(event);
    }

    const outbox = database.prepare(`
      SELECT aggregate_version, status, command_json
      FROM runtime_outbox
      WHERE turn_id = ?
    `).get(rejected.turn_id);
    const command = JSON.parse(outbox.command_json);
    expect(outbox).toMatchObject({ aggregate_version: 2, status: 'pending' });
    expect(validateDeliveryCommand(command).forwarded).toEqual(command);
    expect(command).toMatchObject({
      aggregate_type: 'turn_main',
      aggregate_id: rejected.turn_id,
      operation: 'create_main',
      aggregate_version: 2,
      event_sequence_through: 2,
      render_model: {
        phase: 'failed',
        text: 'The conversation queue is full.',
        error: rejected.error,
        terminal: true,
      },
      mapping: {
        conversation_id: first.conversation_id,
        turn_id: rejected.turn_id,
        lineage_id: first.lineage_id,
        binding_state: 'bound',
      },
    });

    database.close();
  });

  test('replays the first result after restart and rejects a changed payload without side effects', () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const envelope = normalEnvelope();
    const first = acceptNormalInbound(database, envelope, deterministicOptions());
    const committedCounts = readTableCounts(database);
    database.close();

    const restarted = new Database(databasePath);
    const duplicate = normalEnvelope();
    duplicate.trace_id = 'trace-dm-retry-002';
    duplicate.received_at = '2026-07-19T06:01:00Z';
    const replayed = acceptNormalInbound(restarted, duplicate, {
      now: () => '2026-07-19T06:02:00Z',
      generateId: () => {
        throw new Error('a duplicate replay must not allocate new IDs');
      },
    });

    expect(replayed).toEqual({
      ...first,
      trace_id: duplicate.trace_id,
      deduplicated: true,
    });
    expect(validateInboundResult(replayed).forwarded).toEqual(replayed);
    expect(readTableCounts(restarted)).toEqual(committedCounts);

    const conflicting = normalEnvelope();
    conflicting.content.text = 'This is a different business payload.';
    const rejected = acceptNormalInbound(restarted, conflicting, {
      now: () => {
        throw new Error('an idempotency conflict must not request a commit timestamp');
      },
      generateId: () => {
        throw new Error('an idempotency conflict must not allocate IDs');
      },
    });
    expect(validateInboundResult(rejected).forwarded).toEqual(rejected);
    expect(rejected).toMatchObject({
      trace_id: conflicting.trace_id,
      inbound_event_id: conflicting.inbound_event_id,
      idempotency_key: conflicting.idempotency_key,
      status: 'rejected',
      conversation_id: null,
      turn_id: null,
      lineage_id: null,
      control_id: null,
      turn_version: null,
      lineage_resolution_state: 'not_applicable',
      deduplicated: false,
      error: {
        code: 'idempotency_conflict',
        side_effect_status: 'none',
      },
      committed_at: null,
    });
    expect(readTableCounts(restarted)).toEqual(committedCounts);

    restarted.close();
  });

  test('keeps reply identity in the conversation while selecting its mapped lineage', () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope();
    const deterministicDependencies = deterministicOptions();
    const first = acceptNormalInbound(database, envelope, deterministicDependencies);
    const alternateLineageId = 'lineage-alternate-history';
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES (?, ?, 'normal', 0, ?)
    `).run(alternateLineageId, first.conversation_id, first.committed_at);
    database.prepare(`
      INSERT INTO runtime_message_mappings (
        region, tenant_id, channel, bot_id, platform_message_id,
        conversation_id, turn_id, lineage_id, binding_state, reason,
        mapping_id, mapping_version, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'bound', NULL, ?, 1, ?)
    `).run(
      envelope.region,
      envelope.tenant_id,
      envelope.channel,
      envelope.bot_id,
      'model-message-alternate',
      first.conversation_id,
      first.turn_id,
      alternateLineageId,
      'mapping-alternate-history',
      first.committed_at,
    );

    const reply = nextEnvelope(envelope, 'reply-alternate');
    reply.reply = {
      root_message_id: 'model-message-root',
      parent_message_id: 'model-message-alternate',
      reply_to_message_id: 'model-message-alternate',
    };
    const acceptedReply = acceptNormalInbound(database, reply, deterministicDependencies);

    expect(acceptedReply).toMatchObject({
      conversation_id: first.conversation_id,
      lineage_id: alternateLineageId,
      turn_version: 2,
    });
    expect(acceptedReply.turn_id).not.toBe(first.turn_id);
    expect(database.prepare(`
      SELECT queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(acceptedReply.turn_id).queue_sequence).toBe(2);
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_conversations').get().count).toBe(1);

    database.close();
  });

  test('uses collision-free conversation identity and allocates FIFO within that identity', () => {
    const database = openTestDatabase();
    const deterministicDependencies = deterministicOptions();
    const regionDelimitedEnvelope = normalEnvelope();
    regionDelimitedEnvelope.region = 'region:tenant';
    regionDelimitedEnvelope.tenant_id = 'tenant';
    refreshInboundIdempotencyKey(regionDelimitedEnvelope);
    const tenantDelimitedEnvelope = nextEnvelope(
      regionDelimitedEnvelope,
      'identity-tenant-delimited',
    );
    tenantDelimitedEnvelope.region = 'region';
    tenantDelimitedEnvelope.tenant_id = 'tenant:tenant';
    refreshInboundIdempotencyKey(tenantDelimitedEnvelope);

    const acceptedRegionDelimited = acceptNormalInbound(
      database,
      regionDelimitedEnvelope,
      deterministicDependencies,
    );
    const acceptedTenantDelimited = acceptNormalInbound(
      database,
      tenantDelimitedEnvelope,
      deterministicDependencies,
    );
    expect(acceptedTenantDelimited.conversation_id)
      .not.toBe(acceptedRegionDelimited.conversation_id);

    const identityVariants = [
      ['bot', (candidate) => { candidate.bot_id = 'bot-other'; }],
      ['chat-type', (candidate) => { candidate.chat_type = 'group'; }],
      ['chat', (candidate) => { candidate.chat_id = 'chat-other'; }],
      ['native-thread', (candidate) => {
        candidate.chat_type = 'thread';
        candidate.native_thread_or_topic_id = 'thread-other';
      }],
    ];
    const distinctConversationIds = new Set([
      acceptedRegionDelimited.conversation_id,
      acceptedTenantDelimited.conversation_id,
    ]);
    for (const [suffix, mutateIdentity] of identityVariants) {
      const candidate = nextEnvelope(regionDelimitedEnvelope, `identity-${suffix}`);
      mutateIdentity(candidate);
      refreshInboundIdempotencyKey(candidate);
      distinctConversationIds.add(acceptNormalInbound(
        database,
        candidate,
        deterministicDependencies,
      ).conversation_id);
    }
    expect(distinctConversationIds.size).toBe(6);

    const sameConversationEnvelope = nextEnvelope(
      regionDelimitedEnvelope,
      'identity-same-conversation',
    );
    sameConversationEnvelope.channel = 'lark';
    sameConversationEnvelope.reply.root_message_id = 'root-only-does-not-split';
    refreshInboundIdempotencyKey(sameConversationEnvelope);
    const acceptedSameConversation = acceptNormalInbound(
      database,
      sameConversationEnvelope,
      deterministicDependencies,
    );

    expect(acceptedSameConversation).toMatchObject({
      conversation_id: acceptedRegionDelimited.conversation_id,
      lineage_id: acceptedRegionDelimited.lineage_id,
    });
    expect(database.prepare(`
      SELECT queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(acceptedSameConversation.turn_id).queue_sequence).toBe(2);
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_conversations').get().count)
      .toBe(6);

    database.close();
  });

  test('never reuses a FIFO sequence after an earlier queue entry is consumed', () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope();
    const deterministicDependencies = deterministicOptions();
    const first = acceptNormalInbound(database, envelope, deterministicDependencies);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?').run(first.turn_id);
    const databasePath = database.name;
    database.close();
    const restarted = new Database(databasePath);

    const second = acceptNormalInbound(
      restarted,
      nextEnvelope(envelope, 'after-consumed-entry'),
      deterministicDependencies,
    );

    expect(restarted.prepare(`
      SELECT queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(second.turn_id).queue_sequence).toBe(2);
    expect(restarted.prepare(`
      SELECT queue_sequence
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(second.turn_id).queue_sequence).toBe(2);

    restarted.close();
  });

  test('does not freeze persistence rows in their issue 06 initial states', () => {
    const database = openTestDatabase();
    const result = acceptNormalInbound(database, normalEnvelope(), deterministicOptions());

    expect(() => database.transaction(() => {
      database.prepare(`
        UPDATE runtime_turns
        SET state = 'recovering', turn_version = 3, lineage_id = NULL
        WHERE turn_id = ?
      `).run(result.turn_id);
      database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'claimed'
        WHERE turn_id = ?
      `).run(result.turn_id);
      database.prepare(`
        UPDATE runtime_outbox
        SET status = 'delivering'
        WHERE turn_id = ?
      `).run(result.turn_id);
      database.prepare(`
        INSERT INTO runtime_lineages (
          lineage_id, conversation_id, lineage_kind, is_default, created_at
        ) VALUES ('lineage-recovery-test', ?, 'recovery', 0, ?)
      `).run(result.conversation_id, result.committed_at);
    })()).not.toThrow();
    expect(database.prepare(`
      SELECT state, turn_version, lineage_id
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(result.turn_id)).toEqual({
      state: 'recovering',
      turn_version: 3,
      lineage_id: null,
    });

    database.close();
  });

  test('stores provider-neutral outbox aggregates and every public mapping binding shape', () => {
    const database = openTestDatabase();
    const result = acceptNormalInbound(database, normalEnvelope(), deterministicOptions());

    expect(() => database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
        aggregate_version, status, command_json, created_at
      ) VALUES (?, ?, 'interaction', ?, ?, NULL, 1, 'pending', '{}', ?)
    `).run(
      'outbox-interaction-test',
      'delivery-interaction-test',
      'interaction-test',
      result.turn_id,
      result.committed_at,
    )).not.toThrow();
    expect(() => database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
        aggregate_version, status, command_json, created_at
      ) VALUES (?, ?, 'security_notice', ?, NULL, ?, 1, 'pending', '{}', ?)
    `).run(
      'outbox-security-test',
      'delivery-security-test',
      'security-notice-test',
      'control-test',
      result.committed_at,
    )).not.toThrow();

    const pendingMapping = {
      mapping_id: 'mapping-pending-test',
      conversation_id: result.conversation_id,
      turn_id: result.turn_id,
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
      reason: 'mapping_missing',
    };
    const notApplicableMapping = {
      mapping_id: 'mapping-not-applicable-test',
      conversation_id: null,
      turn_id: null,
      lineage_id: null,
      binding_state: 'not_applicable',
      mapping_version: 1,
      reason: null,
    };
    expect(validateDeliveryMapping(pendingMapping)).toEqual(pendingMapping);
    expect(validateDeliveryMapping(notApplicableMapping)).toEqual(notApplicableMapping);
    for (const [platformMessageId, mapping] of [
      ['platform-pending-test', pendingMapping],
      ['platform-not-applicable-test', notApplicableMapping],
    ]) {
      database.prepare(`
        INSERT INTO runtime_message_mappings (
          region, tenant_id, channel, bot_id, platform_message_id,
          conversation_id, turn_id, lineage_id, binding_state, reason,
          mapping_id, mapping_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        'cn',
        'tenant-A',
        'feishu',
        'bot-A',
        platformMessageId,
        mapping.conversation_id,
        mapping.turn_id,
        mapping.lineage_id,
        mapping.binding_state,
        mapping.reason,
        mapping.mapping_id,
        mapping.mapping_version,
        result.committed_at,
      );
    }

    expect(database.prepare(`
      SELECT binding_state, reason, lineage_id
      FROM runtime_message_mappings
      WHERE mapping_id = ?
    `).get(pendingMapping.mapping_id)).toEqual({
      binding_state: 'pending',
      reason: 'mapping_missing',
      lineage_id: null,
    });
    expect(database.prepare(`
      SELECT conversation_id, turn_id, lineage_id, binding_state, reason
      FROM runtime_message_mappings
      WHERE mapping_id = ?
    `).get(notApplicableMapping.mapping_id)).toEqual({
      conversation_id: null,
      turn_id: null,
      lineage_id: null,
      binding_state: 'not_applicable',
      reason: null,
    });

    database.close();
  });

  test('rolls back every business row when the transaction fails after outbox insertion', () => {
    const database = openTestDatabase();
    initializeRuntimePersistence(database);
    database.exec(`
      CREATE TRIGGER force_idempotency_failure
      BEFORE INSERT ON runtime_inbound_idempotency
      BEGIN
        SELECT RAISE(ABORT, 'forced idempotency persistence failure');
      END;
    `);

    expect(() => acceptNormalInbound(database, normalEnvelope(), deterministicOptions()))
      .toThrow(/forced idempotency persistence failure/);
    expect(readTableCounts(database)).toEqual(Object.fromEntries(
      RUNTIME_TABLES.map((table) => [table, 0]),
    ));

    database.close();
  });
});
