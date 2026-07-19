import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  canonicalizeJson,
  ContractKernelError,
  validateDeliveryCommand,
} from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import {
  createDeliveryLaneKeyFromIdentity,
} from '../runtime/persistence/delivery-lane-key.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import {
  initializeMainProjection,
  stageMainProjection,
} from '../runtime/persistence/main-projection.js';
import { initializeRuntimePersistence } from '../runtime/persistence/schema.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-thread-delivery-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function nativeThreadEnvelope() {
  return structuredClone(inboundFixture.valid.find(
    ({ name }) => name === 'native_thread_or_topic',
  ).document);
}

function nonThreadEnvelope() {
  return structuredClone(inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document);
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function acceptanceOptions(namespace) {
  return {
    now: () => '2026-07-19T13:00:00Z',
    generateId: deterministicIds(namespace),
  };
}

function readLane(database, turnId) {
  const row = database.prepare(`
    SELECT lane_key, target_json
    FROM runtime_delivery_lanes
    WHERE turn_id = ?
  `).get(turnId);
  return { ...row, target: JSON.parse(row.target_json) };
}

function readInitialCommand(database, turnId) {
  return JSON.parse(database.prepare(`
    SELECT command_json
    FROM runtime_outbox
    WHERE turn_id = ?
    ORDER BY created_at, outbox_id
    LIMIT 1
  `).get(turnId).command_json);
}

function downgradePersistedDeliveryToV10(database, turnId) {
  const lane = readLane(database, turnId);
  const target = structuredClone(lane.target);
  delete target.native_thread_root_message_id;
  delete target.native_thread_reply_target_message_id;
  database.prepare(`
    UPDATE runtime_delivery_lanes SET target_json = ? WHERE turn_id = ?
  `).run(JSON.stringify(target), turnId);

  const rows = database.prepare(`
    SELECT outbox_id, command_json
    FROM runtime_outbox
    WHERE turn_id = ?
  `).all(turnId);
  for (const row of rows) {
    const command = JSON.parse(row.command_json);
    command.contract_version = '1.0';
    command.target = target;
    database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify(command), row.outbox_id);
  }
  return target;
}

function permanentFailure(command, resultAt) {
  return {
    contract: 'zylos.delivery-result',
    contract_version: '1.0',
    trace_id: command.trace_id,
    outbox_id: command.outbox_id,
    delivery_id: command.delivery_id,
    idempotency_key: command.idempotency_key,
    delivery_attempt_id: command.delivery_attempt_id,
    delivery_attempt_no: command.delivery_attempt_no,
    outbox_lease_epoch: command.outbox_lease_epoch,
    mapping_id: command.mapping.mapping_id,
    operation: command.operation,
    aggregate_version: command.aggregate_version,
    status: 'permanent_failure',
    platform_message_id: command.target_platform_message_id,
    applied_platform_version: null,
    delivered_at: null,
    error: {
      code: 'delivery_permanent',
      category: 'channel',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The exact platform update target rejected the delivery.',
      occurred_at: resultAt,
    },
    renderer_capabilities: {
      supports_update: true,
      supports_actions: true,
      supports_platform_idempotency: true,
      supports_platform_version: true,
    },
    result_at: resultAt,
  };
}

function retryableFailure(command, resultAt) {
  return {
    ...permanentFailure(command, resultAt),
    status: 'retryable_failure',
    platform_message_id: null,
    error: {
      code: 'delivery_transient',
      category: 'channel',
      retryable: true,
      side_effect_status: 'none',
      user_message: 'The native-thread create is temporarily unavailable.',
      occurred_at: resultAt,
    },
  };
}

function expectContractFailure(operation, expectedCode) {
  try {
    operation();
    throw new Error(`expected ${expectedCode}`);
  } catch (error) {
    expect(error).toBeInstanceOf(ContractKernelError);
    expect(error.contractError.code).toBe(expectedCode);
  }
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('native-thread delivery authority', () => {
  test('fences v1.0 native-thread conversation identity in the lane key', () => {
    const target = nativeThreadEnvelope();
    const legacyTarget = {
      region: target.region,
      tenant_id: target.tenant_id,
      channel: target.channel,
      bot_id: target.bot_id,
      chat_type: target.chat_type,
      chat_id: target.chat_id,
      native_thread_or_topic_id: target.native_thread_or_topic_id,
    };
    const laneIdentity = {
      target: legacyTarget,
      turnId: 'turn-v1-thread-update',
      aggregateType: 'turn_main',
    };

    expect(createDeliveryLaneKeyFromIdentity(laneIdentity)).not.toBe(
      createDeliveryLaneKeyFromIdentity({
        ...laneIdentity,
        target: {
          ...legacyTarget,
          native_thread_or_topic_id: 'native-thread-tampered',
        },
      }),
    );
    expect(createDeliveryLaneKeyFromIdentity(laneIdentity)).not.toBe(
      createDeliveryLaneKeyFromIdentity({
        ...laneIdentity,
        target: { ...legacyTarget, region: 'region-tampered' },
      }),
    );
    const nonThreadTarget = {
      ...legacyTarget,
      chat_type: 'group',
      native_thread_or_topic_id: null,
    };
    expect(createDeliveryLaneKeyFromIdentity({
      ...laneIdentity,
      target: nonThreadTarget,
    })).not.toBe(createDeliveryLaneKeyFromIdentity({
      ...laneIdentity,
      target: { ...nonThreadTarget, chat_type: 'synthetic' },
    }));
  });

  test('rekeys a persisted v1.0 native-thread lane once before enforcing its identity', () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const accepted = acceptNormalInbound(
      database,
      nativeThreadEnvelope(),
      acceptanceOptions('legacy-thread-rekey'),
    );
    const command = readInitialCommand(database, accepted.turn_id);
    const legacyTarget = structuredClone(command.target);
    delete legacyTarget.native_thread_root_message_id;
    delete legacyTarget.native_thread_reply_target_message_id;
    const legacyCommand = {
      ...command,
      contract_version: '1.0',
      target: legacyTarget,
      operation: 'update_main',
      target_platform_message_id: 'platform-main-existing',
      predecessor_delivery_id: 'delivery-main-existing',
      expected_platform_version: 1,
    };
    expect(validateDeliveryCommand(legacyCommand).forwarded).toEqual(legacyCommand);
    const legacyLaneKey = canonicalizeJson([
      legacyTarget.channel,
      legacyTarget.tenant_id,
      legacyTarget.bot_id,
      legacyTarget.chat_id,
      accepted.turn_id,
      legacyCommand.aggregate_type,
    ]);

    database.pragma('foreign_keys = OFF');
    database.prepare(`
      UPDATE runtime_delivery_lanes
      SET lane_key = ?, target_json = ?, lane_identity_version = 0
      WHERE turn_id = ?
    `).run(legacyLaneKey, JSON.stringify(legacyTarget), accepted.turn_id);
    database.prepare(`
      UPDATE runtime_outbox
      SET lane_key = ?, command_json = ?
      WHERE turn_id = ?
    `).run(legacyLaneKey, JSON.stringify(legacyCommand), accepted.turn_id);
    database.prepare(`
      UPDATE runtime_projection_snapshots SET lane_key = ? WHERE turn_id = ?
    `).run(legacyLaneKey, accepted.turn_id);
    database.pragma('foreign_keys = ON');
    database.close();

    const restarted = new Database(databasePath);
    initializeRuntimePersistence(restarted);
    const migratedLane = readLane(restarted, accepted.turn_id);
    expect(migratedLane.lane_key).toBe(createDeliveryLaneKeyFromIdentity({
      target: legacyTarget,
      turnId: accepted.turn_id,
      aggregateType: legacyCommand.aggregate_type,
    }));
    expect(migratedLane.lane_key).not.toBe(legacyLaneKey);
    expect(restarted.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_outbox
      WHERE turn_id = ? AND lane_key = ?
    `).get(accepted.turn_id, migratedLane.lane_key).count).toBeGreaterThan(0);
    expect(restarted.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_projection_snapshots
      WHERE turn_id = ? AND lane_key = ?
    `).get(accepted.turn_id, migratedLane.lane_key).count).toBeGreaterThan(0);

    const tamperedTarget = {
      ...legacyTarget,
      native_thread_or_topic_id: 'native-thread-tampered-after-migration',
    };
    restarted.prepare(`
      UPDATE runtime_delivery_lanes SET target_json = ? WHERE turn_id = ?
    `).run(JSON.stringify(tamperedTarget), accepted.turn_id);
    restarted.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE turn_id = ?
    `).run(JSON.stringify({
      ...legacyCommand,
      target: tamperedTarget,
    }), accepted.turn_id);
    const outbox = createOutboxService({
      database: restarted,
      serviceInstanceId: 'delivery-service-legacy-thread-rekey',
      now: () => '2026-07-19T13:00:01Z',
      generateId: deterministicIds('delivery-legacy-thread-rekey'),
    });
    expectContractFailure(() => outbox.claimNext(), 'version_conflict');

    restarted.close();
  });

  test('persists authenticated inbound anchors and reuses them after restart for update and fallback', () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const envelope = nativeThreadEnvelope();
    const accepted = acceptNormalInbound(database, envelope, acceptanceOptions('restart'));
    const authoritativeTarget = {
      region: envelope.region,
      tenant_id: envelope.tenant_id,
      channel: envelope.channel,
      bot_id: envelope.bot_id,
      chat_type: 'thread',
      chat_id: envelope.chat_id,
      native_thread_or_topic_id: envelope.native_thread_or_topic_id,
      native_thread_root_message_id: envelope.reply.root_message_id,
      native_thread_reply_target_message_id: envelope.message_id,
    };

    expect(readLane(database, accepted.turn_id).target).toEqual(authoritativeTarget);
    expect(readInitialCommand(database, accepted.turn_id)).toMatchObject({
      contract_version: '1.1',
      operation: 'create_main',
      target: authoritativeTarget,
    });
    database.close();

    const restarted = new Database(databasePath);
    let currentTime = '2026-07-19T13:00:01Z';
    const outbox = createOutboxService({
      database: restarted,
      serviceInstanceId: 'delivery-service-thread-restart',
      now: () => currentTime,
      generateId: deterministicIds('delivery-restart'),
      throttleMs: 0,
    });
    const create = outbox.claimNext();
    expect(create).toMatchObject({ contract_version: '1.1', target: authoritativeTarget });
    const channelResult = {
      ...deliveredResult(create, '2026-07-19T13:00:01Z'),
      target: {
        ...authoritativeTarget,
        native_thread_reply_target_message_id: 'channel-attempted-rewrite',
      },
    };
    expect(outbox.recordResult(channelResult)).toEqual({
      status: 'applied',
      outbox_status: 'delivered',
    });
    expect(readLane(restarted, accepted.turn_id).target).toEqual(authoritativeTarget);

    stageMainProjection(restarted, { turn_id: accepted.turn_id }, {
      event_id: 'event-thread-terminal',
      trace_id: 'trace-thread-terminal',
      conversation_id: accepted.conversation_id,
      turn_id: accepted.turn_id,
      lineage_id: accepted.lineage_id,
      event_sequence: 3,
      turn_version: 3,
      kind: 'turn_state_changed',
      phase: 'completed',
      payload: {
        from_state: 'queued',
        to_state: 'completed',
        reason_code: 'test_terminal_projection',
      },
      error: null,
      persisted_at: '2026-07-19T13:00:02Z',
    }, {
      generateId: deterministicIds('projection-restart'),
      throttleMs: 0,
    });

    currentTime = '2026-07-19T13:00:02Z';
    const update = outbox.claimNext();
    expect(update).toMatchObject({
      contract_version: '1.1',
      operation: 'update_main',
      target: authoritativeTarget,
      render_model: { terminal: true },
    });
    expect(outbox.recordResult(permanentFailure(update, '2026-07-19T13:00:03Z')))
      .toEqual({ status: 'applied', outbox_status: 'dead_letter' });

    currentTime = '2026-07-19T13:00:03Z';
    const fallback = outbox.claimNext();
    expect(fallback).toMatchObject({
      contract_version: '1.1',
      operation: 'send_fallback',
      predecessor_delivery_id: update.delivery_id,
      target: authoritativeTarget,
    });
    expect(fallback.target.native_thread_reply_target_message_id).toBe(envelope.message_id);
    expect(fallback.target.native_thread_reply_target_message_id)
      .not.toBe(envelope.native_thread_or_topic_id);
    expect(fallback.target.native_thread_reply_target_message_id).not.toBe(envelope.chat_id);

    restarted.close();
  });

  test('fails closed when authenticated native-thread acceptance lacks a root anchor', () => {
    const database = openTestDatabase();
    const envelope = nativeThreadEnvelope();
    envelope.reply.root_message_id = null;

    expectContractFailure(
      () => acceptNormalInbound(database, envelope, acceptanceOptions('missing-root')),
      'unsupported_capability',
    );
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_outbox').get().count).toBe(0);
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_delivery_lanes').get().count)
      .toBe(0);

    database.close();
  });

  test('rejects unauthenticated native-thread facts before creating durable state', () => {
    const database = openTestDatabase();
    const envelope = nativeThreadEnvelope();
    envelope.actor.authenticated = false;

    expectContractFailure(
      () => acceptNormalInbound(database, envelope, acceptanceOptions('unauthenticated')),
      'unauthenticated',
    );
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM sqlite_master
      WHERE type = 'table' AND name LIKE 'runtime_%'
    `).get().count).toBe(0);

    database.close();
  });

  test('reuses the durable reply target for the initial text acknowledgement', () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const envelope = nativeThreadEnvelope();
    acceptNormalInbound(database, envelope, acceptanceOptions('text-ack'));
    database.close();

    const restarted = new Database(databasePath);
    const outbox = createOutboxService({
      database: restarted,
      serviceInstanceId: 'delivery-service-thread-text-ack',
      now: () => '2026-07-19T13:00:01Z',
      generateId: deterministicIds('delivery-text-ack'),
    });
    const create = outbox.claimNext();
    expect(outbox.recordResult(retryableFailure(create, '2026-07-19T13:00:01Z')))
      .toEqual({ status: 'applied', outbox_status: 'retry_wait' });

    const text = outbox.claimNext();
    expect(text).toMatchObject({
      contract_version: '1.1',
      operation: 'send_text',
      target: {
        native_thread_or_topic_id: envelope.native_thread_or_topic_id,
        native_thread_root_message_id: envelope.reply.root_message_id,
        native_thread_reply_target_message_id: envelope.message_id,
      },
    });
    expect(text.target.native_thread_reply_target_message_id)
      .not.toBe(text.target.native_thread_or_topic_id);
    expect(text.target.native_thread_reply_target_message_id).not.toBe(text.target.chat_id);

    restarted.close();
  });

  test('preserves persisted v1.0 non-thread lanes for update and text acknowledgement', () => {
    const updateDatabase = openTestDatabase();
    const updateAccepted = acceptNormalInbound(
      updateDatabase,
      nonThreadEnvelope(),
      acceptanceOptions('legacy-update'),
    );
    const legacyUpdateTarget = downgradePersistedDeliveryToV10(
      updateDatabase,
      updateAccepted.turn_id,
    );
    const updateOutbox = createOutboxService({
      database: updateDatabase,
      serviceInstanceId: 'delivery-service-legacy-update',
      now: () => '2026-07-19T13:00:01Z',
      generateId: deterministicIds('delivery-legacy-update'),
      throttleMs: 0,
    });
    const create = updateOutbox.claimNext();
    expect(create).toMatchObject({ contract_version: '1.0', target: legacyUpdateTarget });
    expect(updateOutbox.recordResult(deliveredResult(create, '2026-07-19T13:00:01Z')))
      .toEqual({ status: 'applied', outbox_status: 'delivered' });
    const update = updateOutbox.claimNext();
    expect(update).toMatchObject({
      contract_version: '1.0',
      operation: 'update_main',
      target: legacyUpdateTarget,
    });
    expect(update.target).not.toHaveProperty('native_thread_root_message_id');
    expect(update.target).not.toHaveProperty('native_thread_reply_target_message_id');
    updateDatabase.close();

    const textDatabase = openTestDatabase();
    const textAccepted = acceptNormalInbound(
      textDatabase,
      nonThreadEnvelope(),
      acceptanceOptions('legacy-text'),
    );
    const legacyTextTarget = downgradePersistedDeliveryToV10(
      textDatabase,
      textAccepted.turn_id,
    );
    const textOutbox = createOutboxService({
      database: textDatabase,
      serviceInstanceId: 'delivery-service-legacy-text',
      now: () => '2026-07-19T13:00:01Z',
      generateId: deterministicIds('delivery-legacy-text'),
    });
    const failedCreate = textOutbox.claimNext();
    expect(failedCreate).toMatchObject({ contract_version: '1.0', target: legacyTextTarget });
    expect(textOutbox.recordResult(
      retryableFailure(failedCreate, '2026-07-19T13:00:01Z'),
    )).toEqual({ status: 'applied', outbox_status: 'retry_wait' });
    const acknowledgement = textOutbox.claimNext();
    expect(acknowledgement).toMatchObject({
      contract_version: '1.0',
      operation: 'send_text',
      target: legacyTextTarget,
    });
    textDatabase.close();
  });

  test('keeps duplicate acceptance idempotent and rejects the same inbound key with changed anchors', () => {
    const database = openTestDatabase();
    const envelope = nativeThreadEnvelope();
    const first = acceptNormalInbound(database, envelope, acceptanceOptions('idempotency'));
    const duplicate = nativeThreadEnvelope();
    duplicate.trace_id = 'trace-thread-duplicate';
    duplicate.received_at = '2026-07-19T13:01:00Z';
    expect(acceptNormalInbound(database, duplicate, {
      now: () => { throw new Error('duplicate must not allocate a timestamp'); },
      generateId: () => { throw new Error('duplicate must not allocate IDs'); },
    })).toEqual({ ...first, trace_id: duplicate.trace_id, deduplicated: true });

    const changedRoot = nativeThreadEnvelope();
    changedRoot.reply.root_message_id = 'platform-root-message-tampered';
    const rejected = acceptNormalInbound(database, changedRoot, {
      now: () => { throw new Error('conflict must not allocate a timestamp'); },
      generateId: () => { throw new Error('conflict must not allocate IDs'); },
    });
    expect(rejected).toMatchObject({
      status: 'rejected',
      error: { code: 'idempotency_conflict' },
      committed_at: null,
    });
    expect(readLane(database, first.turn_id).target.native_thread_root_message_id)
      .toBe(envelope.reply.root_message_id);

    database.close();
  });

  test('returns version_conflict for same-lane target changes and persisted target tampering', () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(
      database,
      nativeThreadEnvelope(),
      acceptanceOptions('target-conflict'),
    );
    const command = readInitialCommand(database, accepted.turn_id);
    const changedTargetCommand = {
      ...command,
      target: {
        ...command.target,
        native_thread_reply_target_message_id: 'platform-message-other',
      },
    };
    expect(validateDeliveryCommand(changedTargetCommand).forwarded).toEqual(changedTargetCommand);
    expectContractFailure(
      () => initializeMainProjection(database, changedTargetCommand),
      'version_conflict',
    );

    database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify(changedTargetCommand), command.outbox_id);
    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-target-conflict',
      now: () => '2026-07-19T13:02:00Z',
      generateId: deterministicIds('delivery-target-conflict'),
    });
    expectContractFailure(() => outbox.claimNext(), 'version_conflict');

    database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify(command), command.outbox_id);
    const lane = readLane(database, accepted.turn_id);
    database.prepare(`
      UPDATE runtime_delivery_lanes SET target_json = ? WHERE lane_key = ?
    `).run(JSON.stringify({
      ...lane.target,
      native_thread_root_message_id: 'platform-root-message-other',
    }), lane.lane_key);
    expectContractFailure(() => outbox.claimNext(), 'version_conflict');

    database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify({
      ...command,
      target: {
        ...command.target,
        native_thread_root_message_id: 'platform-root-message-other',
      },
    }), command.outbox_id);
    expectContractFailure(() => outbox.claimNext(), 'version_conflict');

    const changedConversationTarget = {
      ...command.target,
      native_thread_or_topic_id: 'native-thread-other',
    };
    database.prepare(`
      UPDATE runtime_delivery_lanes SET target_json = ? WHERE lane_key = ?
    `).run(JSON.stringify(changedConversationTarget), lane.lane_key);
    database.prepare(`
      UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
    `).run(JSON.stringify({
      ...command,
      target: changedConversationTarget,
    }), command.outbox_id);
    expectContractFailure(() => outbox.claimNext(), 'version_conflict');

    database.close();
  });
});
