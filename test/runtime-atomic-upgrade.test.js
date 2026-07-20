import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { canonicalizeJson, createIdempotencyKey } from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createRuntimeUpgradeService } from '../runtime/migration/runtime-upgrade-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOperationsControlService } from '../runtime/control/operations-control-service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-runtime-upgrade-'));
  temporaryDirectories.push(directory);
  const databasePath = path.join(directory, 'runtime.db');
  return { database: new Database(databasePath), databasePath };
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function envelope(suffix, { botId = 'bot-upgrade' } = {}) {
  const inboundEventId = `event-${suffix}`;
  return {
    contract: 'zylos.inbound-envelope',
    contract_version: '1.0',
    inbound_event_id: inboundEventId,
    idempotency_key: createIdempotencyKey('inbound', {
      region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
      bot_id: botId, inbound_event_id: inboundEventId,
    }),
    trace_id: `trace-${suffix}`,
    occurred_at: '2026-07-20T10:00:00.000Z',
    received_at: '2026-07-20T10:00:01.000Z',
    region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram', bot_id: botId,
    chat_type: 'group', chat_id: 'chat-upgrade', native_thread_or_topic_id: null,
    message_id: `message-${suffix}`,
    actor: { type: 'user', actor_id: 'user-upgrade', authenticated: true, roles: [] },
    content: { kind: 'text', text: 'Queue this during maintenance.', attachments: [] },
    reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
    source: { kind: 'platform_original', source_ref: null },
  };
}

function upgradeService(database, suffix = 'A') {
  return createRuntimeUpgradeService({
    database,
    now: () => '2026-07-20T10:00:02.000Z',
    generateId: deterministicIds(`upgrade-${suffix}`),
  });
}

function legacyEnvelope(legacyRecordId) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'legacy_compatibility_fields',
  ).document;
  const document = structuredClone(fixture);
  document.inbound_event_id = `legacy-event-${legacyRecordId}`;
  document.idempotency_key = `legacy-c4:${legacyRecordId}`;
  document.trace_id = `legacy-trace-${legacyRecordId}`;
  document.message_id = `legacy-message-${legacyRecordId}`;
  document.legacy.legacy_record_id = legacyRecordId;
  document.legacy.migration_batch_id = 'legacy-batch-A';
  return document;
}

function legacyScheduledOccurrence(suffix) {
  return {
    schedule_id: `legacy-schedule-${suffix}`,
    task_id: `legacy-task-${suffix}`,
    occurrence_id: `legacy-occurrence-${suffix}`,
    prompt: `Run legacy schedule ${suffix}.`,
    occurred_at: '2026-07-20T09:59:00.000Z',
    received_at: '2026-07-20T10:00:00.000Z',
    region: 'global', tenant_id: 'tenant-upgrade', bot_id: 'bot-upgrade',
    bound_conversation: null,
  };
}

function notificationTarget(suffix) {
  return {
    region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
    bot_id: 'bot-upgrade', chat_type: 'group', chat_id: `notice-${suffix}`,
    native_thread_or_topic_id: null,
    native_thread_root_message_id: null,
    native_thread_reply_target_message_id: null,
  };
}

function testExecutorService(database, {
  serviceInstanceId, hostId = serviceInstanceId, releaseRef = null, upgradeId = null,
  now = () => '2026-07-20T10:00:02.000Z',
}) {
  return createExecutorService({
    database,
    adapter: { provider: 'codex', provider_transport: 'official_app_server', async *execute() {} },
    provider: 'codex', serviceInstanceId, hostId, releaseRef, upgradeId,
    serviceStartedAt: now(), now, generateId: deterministicIds(serviceInstanceId),
    scheduleResidentHeartbeat: () => ({ unref() {} }), cancelResidentHeartbeat: () => {},
    scheduleWorkspaceHeartbeat: () => ({ unref() {} }), cancelWorkspaceHeartbeat: () => {},
    scheduleTurnLeaseRenewal: () => ({ unref() {} }), cancelTurnLeaseRenewal: () => {},
    scheduleNonterminalSweep: () => ({ unref() {} }), cancelNonterminalSweep: () => {},
    schedulePermissionSweep: () => ({ unref() {} }), cancelPermissionSweep: () => {},
  });
}

function preflightChecks(overrides = {}) {
  return {
    sqlite_integrity: 'ok',
    codex_transport: 'official_app_server_only',
    delivery_contract: 'zylos.delivery-command@1.1',
    workspace_lease_fencing: 'intact',
    retention_cleanup: 'intact',
    normal_runtime_paths: 'new_only',
    ...overrides,
  };
}

function enterMaintenance(database, upgradeId, suffix) {
  const upgrade = upgradeService(database, suffix);
  upgrade.preflight({
    upgrade_id: upgradeId, from_release: '0.6.0', to_release: '0.7.0',
    scope: { kind: 'installation', bot_id: null }, checks: preflightChecks(),
  });
  upgrade.recordSnapshot(upgradeId, {
    package_release_ref: `release-${suffix}`,
    database_snapshot_ref: `database-${suffix}`,
    snapshot_sha256: 'd'.repeat(64),
  });
  upgrade.enterMaintenance(upgradeId);
  return upgrade;
}

function recordVerifiedRestoreEffect(database, upgradeId, snapshot) {
  database.prepare(`
    INSERT INTO runtime_upgrade_effects (
      upgrade_id, step_key, step_id, input_hash, state, claim_owner,
      claim_attempt, claim_expires_at, result_json, committed_at, updated_at
    ) VALUES (?, 'rollback-restore', ?, ?, 'completed', 'fixture',
      1, NULL, ?, '2026-07-20T10:00:02.000Z', '2026-07-20T10:00:02.000Z')
  `).run(
    upgradeId,
    `${upgradeId}:rollback-restore`,
    'fixture-input-hash',
    JSON.stringify({
      release_ref: snapshot.package_release_ref,
      database_snapshot_ref: snapshot.database_snapshot_ref,
      snapshot_sha256: snapshot.snapshot_sha256,
      database_integrity: 'ok',
      foreign_key_violations: 0,
    }),
  );
  const sourceInvalidation = database.prepare(`
    SELECT result_json FROM runtime_upgrade_effects
    WHERE upgrade_id = ? AND step_key = 'legacy-source-invalidate' AND state = 'completed'
  `).get(upgradeId);
  if (sourceInvalidation !== undefined) {
    const sourceProof = JSON.parse(sourceInvalidation.result_json);
    for (const stepKey of [
      'legacy-source-seal', 'legacy-source-restore', 'legacy-dispatcher-restart',
    ]) {
      const result = stepKey === 'legacy-source-seal'
        ? {
            audit_queue_ref: sourceProof.audit_queue_ref,
            audit_sha256: sourceProof.audit_sha256,
            audit_queue_removed: true,
          }
        : (stepKey === 'legacy-source-restore'
            ? {
                source_queue_ref: sourceProof.source_queue_ref,
                rollback_queue_sha256: sourceProof.rollback_queue_sha256,
                source_queue_restored: true,
              }
            : {
                source_queue_ref: sourceProof.source_queue_ref,
                legacy_dispatcher_restarted: true,
                dispatcher_restart_idempotency_key: `${upgradeId}:${stepKey}`,
              });
      database.prepare(`
        INSERT OR REPLACE INTO runtime_upgrade_effects (
          upgrade_id, step_key, step_id, input_hash, state, claim_owner,
          claim_attempt, claim_expires_at, result_json, committed_at, updated_at
        ) VALUES (?, ?, ?, 'fixture-input-hash', 'completed', 'fixture',
          1, NULL, ?, '2026-07-20T10:00:02.000Z', '2026-07-20T10:00:02.000Z')
      `).run(upgradeId, stepKey, `${upgradeId}:${stepKey}`, JSON.stringify(result));
    }
  }
}

function recordVerifiedActivationEffect(database, upgradeId, releaseRef) {
  database.prepare(`
    INSERT INTO runtime_upgrade_effects (
      upgrade_id, step_key, step_id, input_hash, state, claim_owner,
      claim_attempt, claim_expires_at, result_json, committed_at, updated_at
    ) VALUES (?, 'release-activate', ?, 'fixture-input-hash', 'completed', 'fixture',
      1, NULL, ?, '2026-07-20T10:00:02.000Z', '2026-07-20T10:00:02.000Z')
  `).run(
    upgradeId,
    `${upgradeId}:release-activate`,
    JSON.stringify({ release_ref: releaseRef }),
  );
}

function authorizeLegacyBatch(database, upgradeId, batch) {
  const batchHash = crypto.createHash('sha256').update(canonicalizeJson(batch)).digest('hex');
  database.prepare(`
    INSERT OR REPLACE INTO runtime_upgrade_effects (
      upgrade_id, step_key, step_id, input_hash, state, claim_owner,
      claim_attempt, claim_expires_at, result_json, committed_at, updated_at
    ) VALUES (?, 'legacy-source-invalidate', ?, ?, 'completed', 'fixture',
      1, NULL, ?, '2026-07-20T10:00:02.000Z', '2026-07-20T10:00:02.000Z')
  `).run(
    upgradeId,
    `${upgradeId}:legacy-source-invalidate`,
    batchHash,
    JSON.stringify({
      batch_id: batch.batch_id,
      batch_hash: batchHash,
      source_queue_read_only: true,
      legacy_dispatcher_stopped: true,
      legacy_dispatcher_stopped_at: '2026-07-20T10:00:01.500Z',
      source_queue_ref: '/disposable/legacy-control-queue.json',
      audit_queue_ref: '/disposable/audit/legacy-control-queue.json',
      audit_sha256: batchHash,
      rollback_queue_ref: '/disposable/audit/legacy-rollback-safe.json',
      rollback_queue_sha256: batchHash,
    }),
  );
}

function migrateLegacyWithSourceProof(upgrade, database, upgradeId, batch) {
  authorizeLegacyBatch(database, upgradeId, batch);
  return upgrade.migrateLegacy(upgradeId, batch);
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('atomic runtime upgrade state machine', () => {
  test('durably fences execution while maintenance ingress remains queued and visible after reopen', () => {
    const { database, databasePath } = openTestDatabase();
    const upgrade = upgradeService(database);
    upgrade.preflight({
      upgrade_id: 'upgrade-A',
      from_release: '0.6.0',
      to_release: '0.7.0',
      scope: { kind: 'installation', bot_id: null },
      checks: preflightChecks(),
    });
    upgrade.recordSnapshot('upgrade-A', {
      package_release_ref: 'release-snapshot-A',
      database_snapshot_ref: 'sqlite-snapshot-A',
      snapshot_sha256: 'a'.repeat(64),
    });
    expect(upgrade.enterMaintenance('upgrade-A')).toMatchObject({
      upgrade_id: 'upgrade-A', state: 'maintenance', state_version: 3,
    });

    const accepted = acceptNormalInbound(database, envelope('maintenance'), {
      now: () => '2026-07-20T10:00:03.000Z',
      generateId: deterministicIds('inbound-maintenance'),
    });
    expect(accepted).toMatchObject({ status: 'accepted', deduplicated: false });
    expect(database.prepare(`
      SELECT status, wait_reason FROM runtime_turn_queue WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'queued', wait_reason: 'maintenance' });
    const command = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE turn_id = ? ORDER BY created_at LIMIT 1
    `).get(accepted.turn_id).command_json);
    expect(command.render_model).toMatchObject({
      phase: 'received',
      text: 'Zylos is in maintenance; your message is durably queued.',
      terminal: false,
    });

    const store = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'executor-before-reopen',
      now: () => '2026-07-20T10:00:04.000Z',
      generateId: deterministicIds('executor-before-reopen'),
    });
    expect(store.claimNextQueuedTurn()).toBeNull();

    const permissionLike = envelope('permission-during-maintenance');
    permissionLike.content.text = '/permission trusted';
    permissionLike.idempotency_key = createIdempotencyKey('inbound', {
      region: permissionLike.region, tenant_id: permissionLike.tenant_id,
      channel: permissionLike.channel, bot_id: permissionLike.bot_id,
      inbound_event_id: permissionLike.inbound_event_id,
    });
    const queuedPermissionLike = acceptNormalInbound(database, permissionLike, {
      now: () => '2026-07-20T10:00:04.000Z',
      generateId: deterministicIds('permission-during-maintenance'),
    });
    expect(queuedPermissionLike).toMatchObject({ status: 'accepted', turn_id: expect.any(String) });
    expect(queuedPermissionLike.control_id).toBeNull();
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_controls
    `).get().count).toBe(0);
    const maintenanceBurst = [];
    for (let index = 0; index < 6; index += 1) {
      maintenanceBurst.push(acceptNormalInbound(database, envelope(`maintenance-burst-${index}`), {
        now: () => '2026-07-20T10:00:04.000Z',
        generateId: deterministicIds(`maintenance-burst-${index}`),
      }));
    }
    expect(maintenanceBurst).toHaveLength(6);
    expect(maintenanceBurst.every(({ status }) => status === 'accepted')).toBe(true);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turn_queue
      WHERE status = 'queued' AND wait_reason = 'maintenance'
    `).get().count).toBe(8);

    const executor = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'executor-observability-maintenance',
      now: () => '2026-07-20T10:00:04.000Z',
      generateId: deterministicIds('observability-maintenance'),
    });
    expect(executor.publishObservabilitySnapshot().service).toMatchObject({
      maintenance: true, draining: true,
    });
    database.close();

    const reopened = new Database(databasePath);
    expect(upgradeService(reopened, 'reopen').get('upgrade-A')).toMatchObject({
      state: 'maintenance', state_version: 3,
    });
    const restartedStore = createExecutorStore({
      database: reopened, provider: 'claude', serviceInstanceId: 'executor-after-reopen',
      now: () => '2026-07-20T10:00:05.000Z',
      generateId: deterministicIds('executor-after-reopen'),
    });
    expect(restartedStore.claimNextQueuedTurn()).toBeNull();
    reopened.close();
  });

  test('requires matching snapshot proof for idempotent pre-commit rollback and reopens one execution path', () => {
    const { database, databasePath } = openTestDatabase();
    const upgrade = upgradeService(database, 'rollback');
    upgrade.preflight({
      upgrade_id: 'upgrade-rollback', from_release: '0.6.0', to_release: '0.7.0',
      scope: { kind: 'installation', bot_id: null }, checks: preflightChecks(),
    });
    const snapshot = {
      package_release_ref: 'release-snapshot-rollback',
      database_snapshot_ref: 'sqlite-snapshot-rollback',
      snapshot_sha256: 'b'.repeat(64),
    };
    upgrade.recordSnapshot('upgrade-rollback', snapshot);
    upgrade.enterMaintenance('upgrade-rollback');
    const accepted = acceptNormalInbound(database, envelope('rollback'), {
      now: () => '2026-07-20T10:00:03.000Z', generateId: deterministicIds('rollback-inbound'),
    });

    expect(upgrade.fail('upgrade-rollback', {
      boundary: 'data_migration', code: 'fixture_failure', message: 'migration failed',
    })).toMatchObject({ state: 'rollback_required', state_version: 4 });
    expect(() => upgrade.completeRollback('upgrade-rollback'))
      .toThrow('requires the coordinator durable restore effect');
    recordVerifiedRestoreEffect(database, 'upgrade-rollback', {
      ...snapshot, snapshot_sha256: 'c'.repeat(64),
    });
    expect(() => upgrade.completeRollback('upgrade-rollback'))
      .toThrow('does not match the verified snapshot');
    database.prepare(`DELETE FROM runtime_upgrade_effects WHERE upgrade_id = ?`)
      .run('upgrade-rollback');
    recordVerifiedRestoreEffect(database, 'upgrade-rollback', snapshot);
    const rolledBack = upgrade.completeRollback('upgrade-rollback');
    expect(rolledBack).toMatchObject({ state: 'rolled_back', state_version: 5 });
    expect(upgrade.completeRollback('upgrade-rollback')).toEqual(rolledBack);
    database.close();

    const reopened = new Database(databasePath);
    const store = createExecutorStore({
      database: reopened, provider: 'claude', serviceInstanceId: 'executor-after-rollback',
      now: () => '2026-07-20T10:00:06.000Z',
      generateId: deterministicIds('executor-after-rollback'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: accepted.turn_id });
    expect(upgradeService(reopened, 'rollback-reopen').get('upgrade-rollback')).toMatchObject({
      state: 'rolled_back', rolled_back_at: '2026-07-20T10:00:02.000Z',
    });
    reopened.close();
  });

  test('atomically migrates or quarantines every legacy class and gates health on unknown-side-effect notices', async () => {
    const { database, databasePath } = openTestDatabase();
    const oldLoadedExecutor = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'executor-loaded-before-maintenance',
      now: () => '2026-07-20T10:00:00.000Z', generateId: deterministicIds('old-loaded'),
    });
    const oldLoadedService = createExecutorService({
      database,
      adapter: {
        provider: 'codex', provider_transport: 'official_app_server', async *execute() {},
      },
      provider: 'codex', serviceInstanceId: 'old-service-before-maintenance',
      hostId: 'old-host-before-maintenance',
      serviceStartedAt: '2026-07-20T10:00:00.000Z',
      now: () => '2026-07-20T10:00:00.000Z', generateId: deterministicIds('old-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }), cancelResidentHeartbeat: () => {},
      scheduleWorkspaceHeartbeat: () => ({ unref() {} }), cancelWorkspaceHeartbeat: () => {},
      scheduleTurnLeaseRenewal: () => ({ unref() {} }), cancelTurnLeaseRenewal: () => {},
      scheduleNonterminalSweep: () => ({ unref() {} }), cancelNonterminalSweep: () => {},
      schedulePermissionSweep: () => ({ unref() {} }), cancelPermissionSweep: () => {},
    });
    oldLoadedService.start();
    const upgrade = enterMaintenance(database, 'upgrade-legacy', 'legacy');
    expect(upgrade.completeDrain('upgrade-legacy')).toMatchObject({ state: 'drained' });
    recordVerifiedActivationEffect(database, 'upgrade-legacy', '0.7.0');
    upgrade.activateReleaseFence('upgrade-legacy');

    const migrated = migrateLegacyWithSourceProof(upgrade, database, 'upgrade-legacy', {
      batch_id: 'legacy-batch-A',
      records: [
        { kind: 'c4', legacy_record_id: 'pending-unique', legacy_state: 'pending',
          route: 'unique', legacy_queue_sequence: 1,
          envelope: legacyEnvelope('pending-unique') },
        { kind: 'c4', legacy_record_id: 'pending-ambiguous', legacy_state: 'pending',
          route: 'ambiguous' },
        { kind: 'c4', legacy_record_id: 'pending-\ninvalid', legacy_state: 'pending',
          route: 'unique' },
        { kind: 'c4', legacy_record_id: 'running-A', legacy_state: 'running',
          notification_target: notificationTarget('c4-running-A') },
        { kind: 'c4', legacy_record_id: 'delivered-A', legacy_state: 'delivered',
          history: { final_status: 'delivered', conversation_summary: 'Delivered history.' } },
        { kind: 'c4', legacy_record_id: 'failed-A', legacy_state: 'failed',
          history: { final_status: 'failed', conversation_summary: 'Failed history.' } },
        { kind: 'global_provider_lineage', legacy_record_id: 'global-claude-A',
          legacy_state: 'active',
          recent_c4_context: ['Earlier durable C4 message.'],
          memory_handoff: 'Existing Zylos memory remains authoritative.',
          outbound_messages: [{
            region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
            bot_id: 'bot-upgrade', chat_type: 'group', chat_id: 'chat-upgrade',
            native_thread_or_topic_id: null,
            platform_message_id: 'legacy-platform-global-A',
          }] },
        { kind: 'scheduler', legacy_record_id: 'schedule-running-A', legacy_state: 'running',
          notification_target: notificationTarget('schedule-running-A') },
        { kind: 'scheduler', legacy_record_id: 'schedule-pending-A', legacy_state: 'pending',
          schedule_type: 'one-time', scheduled_for: '2026-07-20T09:59:00.000Z',
          observed_at: '2026-07-20T10:00:00.000Z', miss_threshold_ms: 120_000,
          occurrence: legacyScheduledOccurrence('pending-A') },
        { kind: 'scheduler', legacy_record_id: 'schedule-stale-A', legacy_state: 'pending',
          schedule_type: 'recurring', scheduled_for: '2026-07-20T08:00:00.000Z',
          observed_at: '2026-07-20T10:00:00.000Z', miss_threshold_ms: 120_000,
          occurrence: legacyScheduledOccurrence('stale-A'),
          notification_target: notificationTarget('schedule-stale-A') },
        { kind: 'scheduler', legacy_record_id: 'schedule-history-A', legacy_state: 'delivered',
          definition: { schedule_type: 'recurring', cron: '0 9 * * *', timezone: 'UTC' },
          history: [{ occurred_at: '2026-07-19T09:00:00.000Z', status: 'delivered' }] },
        { kind: 'runtime_control', legacy_record_id: 'control-stop-A', legacy_state: 'pending' },
      ],
    });
    expect(migrated).toMatchObject({
      state: 'health_check',
      migration: { migrated: 2, quarantined: 4, retained: 6, notices_pending: 3 },
    });
    const records = database.prepare(`
      SELECT legacy_kind, legacy_record_id, legacy_state, disposition,
        payload.payload_json, audit_json, migrated_turn_id, executable, read_only
      FROM runtime_legacy_migration_records AS record
      LEFT JOIN runtime_legacy_migration_payloads AS payload
        USING (upgrade_id, legacy_kind, legacy_record_id)
      WHERE upgrade_id = 'upgrade-legacy'
      ORDER BY legacy_kind, legacy_record_id
    `).all();
    expect(records).toEqual(expect.arrayContaining([
      expect.objectContaining({
        legacy_kind: 'c4', legacy_record_id: 'pending-unique',
        disposition: 'migrated_pending', executable: 0, read_only: 1,
      }),
      expect.objectContaining({
        legacy_kind: 'c4', legacy_record_id: 'pending-ambiguous',
        disposition: 'quarantined_ambiguous', migrated_turn_id: null,
      }),
      expect.objectContaining({
        legacy_kind: 'c4', legacy_record_id: 'pending-\ninvalid',
        disposition: 'quarantined_invalid_identity', migrated_turn_id: null,
        audit_json: JSON.stringify({
          batch_id: 'legacy-batch-A', reason: 'legacy_record_id_invalid',
        }),
      }),
      expect.objectContaining({
        legacy_kind: 'c4', legacy_record_id: 'running-A',
        disposition: 'quarantined_side_effect_unknown', migrated_turn_id: null,
        audit_json: JSON.stringify({
          batch_id: 'legacy-batch-A', side_effect_status: 'unknown',
          terminal_state: 'interrupted',
        }),
      }),
      expect.objectContaining({
        legacy_kind: 'global_provider_lineage', disposition: 'archived_unmapped',
        migrated_turn_id: null,
        audit_json: JSON.stringify({
          batch_id: 'legacy-batch-A', mapping_status: 'legacy_unmapped', importable: false,
        }),
      }),
      expect.objectContaining({
        legacy_kind: 'runtime_control', disposition: 'invalidated_audit_only',
        migrated_turn_id: null, executable: 0, read_only: 1,
        audit_json: JSON.stringify({
          batch_id: 'legacy-batch-A', reason: 'legacy_runtime_control_invalidated',
          source_queue_read_only: true,
        }),
      }),
      expect.objectContaining({
        legacy_kind: 'scheduler', legacy_record_id: 'schedule-pending-A',
        disposition: 'migrated_scheduler', executable: 0, read_only: 1,
      }),
      expect.objectContaining({
        legacy_kind: 'scheduler', legacy_record_id: 'schedule-stale-A',
        disposition: 'skipped_missed', migrated_turn_id: null,
      }),
    ]));
    const delivered = records.find(({ legacy_record_id: id }) => id === 'delivered-A');
    expect(JSON.parse(delivered.payload_json)).toEqual({
      kind: 'c4', legacy_record_id: 'delivered-A', legacy_state: 'delivered',
      history: { final_status: 'delivered', conversation_summary: 'Delivered history.' },
    });
    const scheduler = records.find(({ legacy_record_id: id }) => id === 'schedule-pending-A');
    expect(JSON.parse(scheduler.audit_json)).toMatchObject({
      schedule_decision: 'enqueue', permission_mode: 'safe',
    });
    const pending = records.find(({ legacy_record_id: id }) => id === 'pending-unique');
    expect(database.prepare(`
      SELECT inbound.idempotency_key, queue.status, queue.wait_reason
      FROM runtime_turns AS turn
      JOIN runtime_inbound_events AS inbound ON inbound.inbound_event_id = turn.inbound_event_id
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.migrated_turn_id)).toEqual({
      idempotency_key: 'legacy-c4:pending-unique', status: 'queued', wait_reason: 'maintenance',
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_operations_controls').get().count)
      .toBe(0);
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_permission_controls').get().count)
      .toBe(0);
    for (const table of [
      'runtime_stop_controls',
      'runtime_steer_controls',
      'runtime_steer_requests',
      'runtime_operations_reconciliation_intents',
      'runtime_reply_mapping_recoveries',
    ]) {
      expect(database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count).toBe(0);
    }
    expect(() => database.prepare(`
      UPDATE runtime_legacy_migration_records SET executable = 1
      WHERE legacy_kind = 'runtime_control' AND legacy_record_id = 'control-stop-A'
    `).run()).toThrow('legacy migration audit is immutable');
    expect(() => upgrade.recordExecutorHealth('upgrade-legacy', {
      service_instance_id: 'executor-new', snapshot_version: 1,
      health: 'healthy', reconciliation: 'complete',
    })).toThrow('delivered notice proof');

    expect(() => upgrade.recordNoticeDelivered('upgrade-legacy', 'c4', 'running-A'))
      .toThrow('durable outbox delivery');
    const noticeOutbox = createOutboxService({
      database, serviceInstanceId: 'delivery-legacy-notices',
      now: () => '2026-07-20T10:00:10.000Z',
      generateId: deterministicIds('delivery-legacy-notices'), throttleMs: 0,
    });
    for (let index = 0; index < 3; index += 1) {
      const noticeCommand = noticeOutbox.claimNext();
      expect(noticeCommand).toMatchObject({ aggregate_type: 'text_notice', operation: 'send_text' });
      noticeOutbox.recordResult(deliveredResult(
        noticeCommand,
        `2026-07-20T10:00:1${index}.000Z`,
      ));
    }
    upgrade.recordNoticeDelivered('upgrade-legacy', 'c4', 'running-A');
    upgrade.recordNoticeDelivered('upgrade-legacy', 'scheduler', 'schedule-running-A');
    upgrade.recordNoticeDelivered('upgrade-legacy', 'scheduler', 'schedule-stale-A');
    const healthExecutor = createExecutorService({
      database,
      adapter: {
        provider: 'codex', provider_transport: 'official_app_server',
        async *execute() {},
      },
      provider: 'codex', serviceInstanceId: 'executor-new', hostId: 'host-new',
      serviceStartedAt: '2026-07-20T10:00:11.750Z',
      releaseRef: '0.7.0', upgradeId: 'upgrade-legacy',
      now: () => '2026-07-20T10:00:11.750Z',
      generateId: deterministicIds('health-executor'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
      scheduleWorkspaceHeartbeat: () => ({ unref() {} }),
      cancelWorkspaceHeartbeat: () => {},
      scheduleTurnLeaseRenewal: () => ({ unref() {} }),
      cancelTurnLeaseRenewal: () => {},
      scheduleNonterminalSweep: () => ({ unref() {} }),
      cancelNonterminalSweep: () => {},
      schedulePermissionSweep: () => ({ unref() {} }),
      cancelPermissionSweep: () => {},
    });
    healthExecutor.start();
    const healthSnapshot = healthExecutor.publishObservabilitySnapshot();
    expect(upgrade.recordExecutorHealth('upgrade-legacy', {
      service_instance_id: 'executor-new', snapshot_version: healthSnapshot.snapshot_version,
      health: healthSnapshot.service.health, reconciliation: 'complete',
    })).toMatchObject({ state: 'ready_to_commit' });
    expect(upgrade.commit('upgrade-legacy')).toMatchObject({ state: 'committed' });
    expect(oldLoadedExecutor.claimNextQueuedTurn()).toBeNull();
    expect(await oldLoadedService.runNext()).toMatchObject({ status: 'idle' });
    expect(() => upgrade.fail('upgrade-legacy', {
      boundary: 'after_commit', code: 'late_failure', message: 'must not reopen old release',
    })).toThrow('committed and cannot roll back');
    expect(upgrade.get('upgrade-legacy')).toMatchObject({ state: 'committed' });
    const legacyReply = envelope('legacy-global-reply');
    legacyReply.reply.reply_to_message_id = 'legacy-platform-global-A';
    legacyReply.idempotency_key = createIdempotencyKey('inbound', {
      region: legacyReply.region, tenant_id: legacyReply.tenant_id,
      channel: legacyReply.channel, bot_id: legacyReply.bot_id,
      inbound_event_id: legacyReply.inbound_event_id,
    });
    const acceptedLegacyReply = acceptNormalInbound(database, legacyReply, {
      now: () => '2026-07-20T10:00:12.000Z',
      generateId: deterministicIds('legacy-global-reply'),
    });
    expect(acceptedLegacyReply).toMatchObject({
      status: 'accepted', lineage_resolution_state: 'bound', lineage_id: expect.any(String),
    });
    expect(JSON.parse(database.prepare(`
      SELECT provider_input_json FROM runtime_turns WHERE turn_id = ?
    `).get(acceptedLegacyReply.turn_id).provider_input_json).text).toContain(
      'The old global provider lineage cannot be assigned to this chat.',
    );
    expect(JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE turn_id = ? ORDER BY created_at LIMIT 1
    `).get(acceptedLegacyReply.turn_id).command_json).render_model.text).toBe(
      'That message came from the legacy global provider lineage. Continuing safely in this chat.',
    );
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(acceptedLegacyReply.turn_id).count).toBe(0);
    expect(database.pragma('foreign_key_check')).toEqual([]);
    const cleanup = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'cleanup-legacy-payload',
      now: () => '2026-08-20T10:00:12.000Z', generateId: deterministicIds('cleanup-legacy'),
    });
    expect(cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 86_400_000 })
      .deleted_by_kind.legacy_migration_payload).toBe(12);
    expect(database.prepare(`SELECT COUNT(*) AS count FROM runtime_legacy_migration_payloads`)
      .get().count).toBe(0);
    expect(database.prepare(`SELECT COUNT(*) AS count FROM runtime_legacy_migration_records`)
      .get().count).toBe(12);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_audit_payloads
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_durable_facts
    `).get().count).toBe(6);
    expect(JSON.parse(database.prepare(`
      SELECT fact_json FROM runtime_legacy_migration_durable_facts
      WHERE legacy_kind = 'c4' AND legacy_record_id = 'delivered-A'
    `).get().fact_json)).toMatchObject({
      legacy_state: 'delivered', disposition: 'retained_delivered',
      history: { final_status: 'delivered', conversation_summary: 'Delivered history.' },
    });
    expect(JSON.parse(database.prepare(`
      SELECT fact_json FROM runtime_legacy_migration_durable_facts
      WHERE legacy_kind = 'scheduler' AND legacy_record_id = 'schedule-history-A'
    `).get().fact_json)).toEqual({
      kind: 'scheduler', legacy_record_id: 'schedule-history-A',
      legacy_state: 'delivered', disposition: 'retained_history',
      schedule_type: null, scheduled_for: null, observed_at: null, occurrence: null,
      definition: { schedule_type: 'recurring', cron: '0 9 * * *', timezone: 'UTC' },
      history: [{ occurred_at: '2026-07-19T09:00:00.000Z', status: 'delivered' }],
    });
    expect(JSON.parse(database.prepare(`
      SELECT audit_payload_json FROM runtime_legacy_migration_audit_payloads
      WHERE legacy_kind = 'runtime_control' AND legacy_record_id = 'control-stop-A'
    `).get().audit_payload_json)).toEqual({
      kind: 'runtime_control', legacy_record_id: 'control-stop-A', legacy_state: 'pending',
    });
    const retainedLegacyReply = envelope('legacy-global-reply-after-retention');
    retainedLegacyReply.reply.reply_to_message_id = 'legacy-platform-global-A';
    const retainedReplyResult = acceptNormalInbound(database, retainedLegacyReply, {
      now: () => '2026-08-20T10:00:13.000Z',
      generateId: deterministicIds('legacy-global-reply-after-retention'),
    });
    expect(JSON.parse(database.prepare(`
      SELECT provider_input_json FROM runtime_turns WHERE turn_id = ?
    `).get(retainedReplyResult.turn_id).provider_input_json).text).toContain(
      'Existing Zylos memory remains authoritative.',
    );
    const auditCleanup = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'cleanup-legacy-audit',
      now: () => '2027-01-17T10:00:12.000Z', generateId: deterministicIds('cleanup-audit'),
    });
    expect(auditCleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 86_400_000 })
      .deleted_by_kind.legacy_migration_audit_payload).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_audit_payloads
    `).get().count).toBe(0);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_durable_facts
    `).get().count).toBe(6);
    expect(() => database.prepare(`
      UPDATE runtime_legacy_migration_durable_facts SET fact_json = '{}'
      WHERE legacy_record_id = 'schedule-history-A'
    `).run()).toThrow('durable fact is immutable');
    await oldLoadedService.close();
    await healthExecutor.close();
    database.close();

    const reopened = new Database(databasePath);
    const oldLoaded = createExecutorStore({
      database: reopened, provider: 'claude', serviceInstanceId: 'executor-after-commit',
      now: () => '2026-07-20T10:00:12.000Z', generateId: deterministicIds('after-commit'),
    });
    expect(oldLoaded.claimNextQueuedTurn()).toBeNull();
    const targetRelease = testExecutorService(reopened, {
      serviceInstanceId: 'executor-new-after-commit-restart',
      releaseRef: '0.7.0', upgradeId: 'upgrade-legacy',
      now: () => '2026-07-20T10:00:12.000Z',
    });
    targetRelease.start();
    expect(await targetRelease.runNext()).toMatchObject({ turn_id: pending.migrated_turn_id });
    await targetRelease.close();
    reopened.close();
  });

  test('rolls back a partially failing legacy batch atomically and resumes the same batch after reopen', () => {
    const { database, databasePath } = openTestDatabase();
    const upgrade = enterMaintenance(database, 'upgrade-retry', 'retry');
    upgrade.completeDrain('upgrade-retry');
    database.exec(`
      CREATE TRIGGER reject_second_legacy_record
      BEFORE INSERT ON runtime_legacy_migration_records
      WHEN NEW.legacy_record_id = 'failed-A'
      BEGIN SELECT RAISE(ABORT, 'injected legacy audit failure'); END;
    `);
    const batch = {
      batch_id: 'legacy-batch-retry',
      records: [
        { kind: 'c4', legacy_record_id: 'delivered-A', legacy_state: 'delivered' },
        { kind: 'c4', legacy_record_id: 'failed-A', legacy_state: 'failed' },
      ],
    };
    authorizeLegacyBatch(database, 'upgrade-retry', batch);
    expect(() => upgrade.migrateLegacy('upgrade-retry', batch))
      .toThrow('injected legacy audit failure');
    expect(upgrade.get('upgrade-retry')).toMatchObject({ state: 'migrating' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-retry'
    `).get().count).toBe(0);
    database.exec('DROP TRIGGER reject_second_legacy_record');
    database.close();

    const reopened = new Database(databasePath);
    const resumed = upgradeService(reopened, 'retry-reopen');
    expect(resumed.migrateLegacy('upgrade-retry', batch)).toMatchObject({
      state: 'health_check', migration: { retained: 2 },
    });
    expect(resumed.migrateLegacy('upgrade-retry', batch)).toMatchObject({
      state: 'health_check', migration: { retained: 2 },
    });
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-retry'
    `).get().count).toBe(2);
    reopened.close();
  });

  test('rollback parks only imported legacy work while preserving maintenance ingress', async () => {
    const { database } = openTestDatabase();
    const upgrade = enterMaintenance(database, 'upgrade-data-rollback', 'data-rollback');
    const concurrent = acceptNormalInbound(database, envelope('preserve-on-rollback'), {
      now: () => '2026-07-20T10:00:03.000Z', generateId: deterministicIds('preserve'),
    });
    upgrade.completeDrain('upgrade-data-rollback');
    const migrated = migrateLegacyWithSourceProof(
      upgrade, database, 'upgrade-data-rollback', {
      batch_id: 'legacy-batch-data-rollback',
      records: [{
        kind: 'c4', legacy_record_id: 'pending-data-rollback', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1,
        envelope: legacyEnvelope('pending-data-rollback'),
      }],
      },
    );
    const imported = database.prepare(`
      SELECT migrated_turn_id FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-data-rollback'
    `).get().migrated_turn_id;
    expect(migrated).toMatchObject({ state: 'health_check' });
    const targetExecutor = testExecutorService(database, {
      serviceInstanceId: 'rollback-target-executor',
      releaseRef: '0.7.0', upgradeId: 'upgrade-data-rollback',
      now: () => '2026-07-20T10:00:04.000Z',
    });
    targetExecutor.start();
    upgrade.fail('upgrade-data-rollback', {
      boundary: 'health_check', code: 'executor_unhealthy', message: 'health check failed',
    });
    const dataRollbackSnapshot = {
      package_release_ref: 'release-data-rollback',
      database_snapshot_ref: 'database-data-rollback',
      snapshot_sha256: 'd'.repeat(64),
    };
    recordVerifiedRestoreEffect(database, 'upgrade-data-rollback', dataRollbackSnapshot);
    database.prepare(`
      DELETE FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-data-rollback' AND step_key = 'legacy-dispatcher-restart'
    `).run();
    expect(() => upgrade.completeRollback('upgrade-data-rollback'))
      .toThrow('completed legacy-dispatcher-restart effect');
    database.prepare(`
      INSERT INTO runtime_upgrade_effects (
        upgrade_id, step_key, step_id, input_hash, state, claim_owner,
        claim_attempt, claim_expires_at, result_json, committed_at, updated_at
      ) VALUES ('upgrade-data-rollback', 'legacy-dispatcher-restart',
        'upgrade-data-rollback:legacy-dispatcher-restart', 'fixture-input-hash',
        'completed', 'fixture', 1, NULL, ?,
        '2026-07-20T10:00:02.000Z', '2026-07-20T10:00:02.000Z')
    `).run(JSON.stringify({
      source_queue_ref: '/disposable/legacy-control-queue.json',
      legacy_dispatcher_restarted: true,
      dispatcher_restart_idempotency_key: 'upgrade-data-rollback:legacy-dispatcher-restart',
    }));
    upgrade.completeRollback('upgrade-data-rollback');
    expect(database.prepare(`
      SELECT turn.state, queue.status
      FROM runtime_turns AS turn JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(imported)).toEqual({ state: 'queued', status: 'cancelled' });
    expect(database.prepare(`
      SELECT revoked_at FROM runtime_executor_service_instances
      WHERE service_instance_id = 'rollback-target-executor'
    `).get().revoked_at).not.toBeNull();
    expect(await targetExecutor.runNext()).toMatchObject({ status: 'idle' });
    expect(database.prepare(`
      SELECT state, disposition FROM runtime_legacy_migration_records
      JOIN runtime_upgrade_runs USING (upgrade_id)
      WHERE migrated_turn_id = ?
    `).get(imported)).toEqual({ state: 'rolled_back', disposition: 'migrated_pending' });
    const store = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'executor-data-rollback',
      now: () => '2026-07-20T10:00:08.000Z', generateId: deterministicIds('data-rollback'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: concurrent.turn_id });
    await targetExecutor.close();
    database.close();
  });

  test('times out drain into rollback_required instead of force-stopping an active turn', () => {
    const { database } = openTestDatabase();
    const clock = { now: '2026-07-20T10:00:00.000Z' };
    const upgrade = createRuntimeUpgradeService({
      database, now: () => clock.now, generateId: deterministicIds('drain-timeout'),
    });
    const accepted = acceptNormalInbound(database, envelope('active-before-maintenance'), {
      now: () => clock.now, generateId: deterministicIds('active-before-maintenance'),
    });
    const store = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'executor-active-before-maintenance',
      now: () => clock.now, generateId: deterministicIds('active-before-maintenance-store'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: accepted.turn_id });
    upgrade.preflight({
      upgrade_id: 'upgrade-drain-timeout', from_release: '0.6.0', to_release: '0.7.0',
      scope: { kind: 'installation', bot_id: null }, checks: preflightChecks(),
    });
    upgrade.recordSnapshot('upgrade-drain-timeout', {
      package_release_ref: 'release-drain-timeout',
      database_snapshot_ref: 'database-drain-timeout', snapshot_sha256: 'e'.repeat(64),
    });
    upgrade.enterMaintenance('upgrade-drain-timeout');
    expect(upgrade.completeDrain('upgrade-drain-timeout')).toMatchObject({
      status: 'waiting', active_turn_ids: [accepted.turn_id],
      deadline_at: '2026-07-20T10:10:00.000Z',
    });
    clock.now = '2026-07-20T10:10:00.001Z';
    expect(upgrade.completeDrain('upgrade-drain-timeout')).toMatchObject({
      state: 'rollback_required', failure: { boundary: 'drain', code: 'drain_timeout' },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'starting' });
    database.close();
  });

  test('allows force drain only after canonical notice delivery and capability-authenticated admin stop', async () => {
    const { database } = openTestDatabase();
    const active = acceptNormalInbound(database, envelope('force-drain-active'), {
      now: () => '2026-07-20T10:00:00.000Z', generateId: deterministicIds('force-active'),
    });
    const store = createExecutorStore({
      database, provider: 'claude', serviceInstanceId: 'force-drain-store',
      now: () => '2026-07-20T10:00:01.000Z', generateId: deterministicIds('force-store'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: active.turn_id });
    const upgrade = enterMaintenance(database, 'upgrade-force-drain', 'force-drain');
    expect(upgrade.prepareForcedDrain('upgrade-force-drain')).toMatchObject({
      state: 'maintenance', notice_turn_ids: [active.turn_id],
    });
    expect(() => upgrade.completeForcedDrain('upgrade-force-drain', [{
      caller_namespace: 'dashboard.prod', control_id: 'force-stop-active',
    }])).toThrow('deliver the user notice before stop authorization');

    const delivery = createOutboxService({
      database, serviceInstanceId: 'force-notice-delivery',
      now: () => '2026-07-20T10:00:02.000Z',
      generateId: deterministicIds('force-delivery'), throttleMs: 0,
    });
    let forceCommand = null;
    for (let attempt = 0; attempt < 5 && forceCommand === null; attempt += 1) {
      const command = delivery.claimNext();
      if (command === null) break;
      delivery.recordResult(deliveredResult(command, '2026-07-20T10:00:02.000Z'));
      if (command.aggregate_id.startsWith('upgrade-force-')) forceCommand = command;
    }
    expect(forceCommand).toMatchObject({
      contract_version: '1.1', operation: 'send_text', mapping: { turn_id: active.turn_id },
    });
    expect(() => upgrade.completeForcedDrain('upgrade-force-drain', [{
      caller_namespace: 'dashboard.prod', control_id: 'force-stop-active',
    }])).toThrow('not in the canonical operations audit');

    const turn = database.prepare(`
      SELECT conversation_id, turn_version FROM runtime_turns WHERE turn_id = ?
    `).get(active.turn_id);
    const policy = {
      policy_id: 'runtime-upgrade-force', policy_version: 1,
      grants: [{
        grant_id: 'grant-upgrade-force-stop',
        subject: { type: 'user', subject_id: 'operator-admin' },
        capability: 'turn.stop',
        scope: {
          scope_type: 'conversation', region: 'global', tenant_id: 'tenant-upgrade',
          bot_id: 'bot-upgrade', conversation_id: turn.conversation_id,
          service_instance_id: null, recovery_id: null,
        },
        state: 'active', expires_at: null,
      }],
    };
    const operations = createOperationsControlService({
      database, serviceInstanceId: 'force-drain-operations',
      deploymentPolicy: policy, runtimeStore: store,
      now: () => '2026-07-20T10:00:03.000Z',
      generateId: deterministicIds('force-operations'),
    });
    const controlResult = await operations.execute({
      contract: 'zylos.control-request', contract_version: '1.0',
      trace_id: 'trace-force-stop-active', caller_namespace: 'dashboard.prod',
      control_id: 'force-stop-active', action: 'stop_active_turn',
      target: {
        aggregate_type: 'turn', conversation_id: turn.conversation_id,
        turn_id: active.turn_id,
      },
      expected_version: {
        aggregate_type: 'turn', aggregate_id: active.turn_id, version: turn.turn_version,
      },
      actor: {
        type: 'user', actor_id: 'forged', authenticated: true,
        roles: [], capabilities: [],
      },
      auth_context: {
        source: 'dashboard_session', auth_subject_id: 'forged',
        tenant_id: 'forged', bot_id: 'forged',
        authorization_policy_id: 'forged', authorization_policy_version: 99,
        authenticated_at: '2026-07-20T09:00:00.000Z',
      },
      reason: 'Administrator explicitly forces the upgrade after durable user notice.',
      idempotency_key: createIdempotencyKey('control', {
        caller_namespace: 'dashboard.prod', control_id: 'force-stop-active',
      }),
      created_at: '2026-07-20T10:00:03.000Z',
    }, {
      source: 'dashboard_session',
      verified_subject: {
        type: 'user', subject_id: 'operator-admin', roles: ['tenant-admin'],
      },
      authorization_policy_id: 'runtime-upgrade-force', authorization_policy_version: 1,
      authenticated_at: '2026-07-20T10:00:03.000Z',
    });
    expect(controlResult).toMatchObject({ status: 'completed' });
    expect(upgrade.completeForcedDrain('upgrade-force-drain', [{
      caller_namespace: 'dashboard.prod', control_id: 'force-stop-active',
    }])).toMatchObject({ state: 'drained' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(active.turn_id)).toEqual({ state: 'stopped' });
    database.close();
  });

  test('bot-scoped maintenance fences only that bot across concurrent SQLite connections', () => {
    const { database, databasePath } = openTestDatabase();
    const upgrade = createRuntimeUpgradeService({
      database, now: () => '2026-07-20T10:00:00.000Z',
      generateId: deterministicIds('bot-scope'),
    });
    upgrade.preflight({
      upgrade_id: 'upgrade-bot-A', from_release: '0.6.0', to_release: '0.7.0',
      scope: { kind: 'bot', bot_id: 'bot-upgrade' }, checks: preflightChecks(),
    });
    upgrade.recordSnapshot('upgrade-bot-A', {
      package_release_ref: 'release-bot-A', database_snapshot_ref: 'database-bot-A',
      snapshot_sha256: 'f'.repeat(64),
    });
    upgrade.enterMaintenance('upgrade-bot-A');
    const writer = new Database(databasePath);
    const blocked = acceptNormalInbound(writer, envelope('bot-A'), {
      now: () => '2026-07-20T10:00:01.000Z', generateId: deterministicIds('bot-A'),
    });
    const otherEnvelope = envelope('bot-B', { botId: 'bot-other' });
    otherEnvelope.idempotency_key = createIdempotencyKey('inbound', {
      region: otherEnvelope.region, tenant_id: otherEnvelope.tenant_id,
      channel: otherEnvelope.channel, bot_id: otherEnvelope.bot_id,
      inbound_event_id: otherEnvelope.inbound_event_id,
    });
    const runnable = acceptNormalInbound(writer, otherEnvelope, {
      now: () => '2026-07-20T10:00:02.000Z', generateId: deterministicIds('bot-B'),
    });
    const store = createExecutorStore({
      database: writer, provider: 'claude', serviceInstanceId: 'executor-bot-scope',
      now: () => '2026-07-20T10:00:03.000Z', generateId: deterministicIds('bot-scope-store'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: runnable.turn_id });
    expect(writer.prepare(`SELECT wait_reason FROM runtime_turn_queue WHERE turn_id = ?`)
      .get(blocked.turn_id)).toEqual({ wait_reason: 'maintenance' });
    expect(writer.prepare(`SELECT wait_reason FROM runtime_turn_queue WHERE turn_id = ?`)
      .get(runnable.turn_id)).toEqual({ wait_reason: null });
    writer.close();
    database.close();
  });

  test('fails preflight closed on any forbidden transport, delivery, lease, retention, or dual path', () => {
    const { database } = openTestDatabase();
    const upgrade = upgradeService(database, 'preflight-hard-decisions');
    for (const [field, value] of [
      ['codex_transport', 'exec_resume'],
      ['delivery_contract', 'zylos.delivery-command@1.0'],
      ['workspace_lease_fencing', 'unknown'],
      ['retention_cleanup', 'disabled'],
      ['normal_runtime_paths', 'dual'],
    ]) {
      expect(() => upgrade.preflight({
        upgrade_id: `upgrade-reject-${field}`,
        from_release: '0.6.0', to_release: '0.7.0',
        scope: { kind: 'installation', bot_id: null },
        checks: preflightChecks({ [field]: value }),
      })).toThrow(`preflight check ${field}`);
    }
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_upgrade_runs').get().count)
      .toBe(0);
    database.close();
  });

  test('terminates a pre-snapshot failure without claiming that snapshot rollback is required', () => {
    const { database } = openTestDatabase();
    const upgrade = upgradeService(database, 'pre-snapshot-failure');
    upgrade.preflight({
      upgrade_id: 'upgrade-pre-snapshot-failure',
      from_release: '0.6.0', to_release: '0.7.0',
      scope: { kind: 'installation', bot_id: null }, checks: preflightChecks(),
    });

    expect(upgrade.fail('upgrade-pre-snapshot-failure', {
      boundary: 'snapshot', code: 'snapshot_failed', message: 'snapshot was not created',
    })).toMatchObject({
      state: 'rolled_back', rolled_back_at: '2026-07-20T10:00:02.000Z',
      failure: { code: 'snapshot_failed' },
    });
    expect(() => upgrade.completeRollback('upgrade-pre-snapshot-failure'))
      .toThrow('already rolled_back');
    expect(database.prepare(`
      SELECT from_state, to_state FROM runtime_upgrade_events
      WHERE upgrade_id = 'upgrade-pre-snapshot-failure' AND step_key = 'failure'
    `).get()).toEqual({ from_state: 'preflight', to_state: 'rolled_back' });
    database.close();
  });

  test('drain waits for scoped workspace leases and background work even without an active turn', () => {
    const { database } = openTestDatabase();
    const accepted = acceptNormalInbound(database, envelope('lease-drain'), {
      now: () => '2026-07-20T09:59:00.000Z', generateId: deterministicIds('lease-drain'),
    });
    const holder = database.prepare(`
      SELECT conversation_id FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id);
    const upgrade = enterMaintenance(database, 'upgrade-lease-drain', 'lease-drain');
    database.prepare(`
      INSERT INTO runtime_workspace_lease_fences (workspace_root, last_epoch, updated_at)
      VALUES ('/workspace/lease-drain', 1, '2026-07-20T09:59:01.000Z')
    `).run();
    database.prepare(`
      INSERT INTO runtime_workspace_leases (
        workspace_lease_id, workspace_root, mode, holder_service_instance_id,
        holder_conversation_id, holder_turn_id, lease_epoch, lease_expires_at,
        state, acquired_at, updated_at, released_at
      ) VALUES (?, '/workspace/lease-drain', 'writable', 'executor-before-upgrade',
        ?, ?, 1, '2026-07-20T10:20:00.000Z', 'active',
        '2026-07-20T09:59:01.000Z', '2026-07-20T09:59:01.000Z', NULL)
    `).run('workspace-lease-drain', holder.conversation_id, accepted.turn_id);
    database.prepare(`
      INSERT INTO runtime_workspace_background_work (
        background_work_id, workspace_lease_id, holder_turn_id, provider_task_id,
        state, started_at, ended_at, error_json
      ) VALUES ('background-lease-drain', 'workspace-lease-drain', ?,
        'provider-task-lease-drain', 'active', '2026-07-20T09:59:02.000Z', NULL, NULL)
    `).run(accepted.turn_id);

    expect(upgrade.completeDrain('upgrade-lease-drain')).toMatchObject({
      status: 'waiting', active_turn_ids: [],
      workspace_lease_ids: ['workspace-lease-drain'],
      background_work_ids: ['background-lease-drain'],
    });
    database.prepare(`
      UPDATE runtime_workspace_background_work
      SET state = 'completed', ended_at = '2026-07-20T10:00:01.000Z'
      WHERE background_work_id = 'background-lease-drain'
    `).run();
    database.prepare(`
      UPDATE runtime_workspace_leases
      SET state = 'released', released_at = '2026-07-20T10:00:01.000Z',
        updated_at = '2026-07-20T10:00:01.000Z'
      WHERE workspace_lease_id = 'workspace-lease-drain'
    `).run();
    expect(upgrade.completeDrain('upgrade-lease-drain')).toMatchObject({ state: 'drained' });
    database.close();
  });

  test('rejects missing or unsorted pending C4 FIFO evidence before entering migration', () => {
    for (const [suffix, records] of [
      ['missing', [{
        kind: 'c4', legacy_record_id: 'fifo-missing', legacy_state: 'pending',
        route: 'unique', envelope: legacyEnvelope('fifo-missing'),
      }]],
      ['unsorted', [
        { kind: 'c4', legacy_record_id: 'fifo-second', legacy_state: 'pending',
          route: 'unique', legacy_queue_sequence: 2, envelope: legacyEnvelope('fifo-second') },
        { kind: 'c4', legacy_record_id: 'fifo-first', legacy_state: 'pending',
          route: 'unique', legacy_queue_sequence: 1, envelope: legacyEnvelope('fifo-first') },
      ]],
    ]) {
      const { database } = openTestDatabase();
      const upgrade = enterMaintenance(database, `upgrade-fifo-${suffix}`, `fifo-${suffix}`);
      upgrade.completeDrain(`upgrade-fifo-${suffix}`);
      const fifoBatch = {
        batch_id: `batch-fifo-${suffix}`, records,
      };
      authorizeLegacyBatch(database, `upgrade-fifo-${suffix}`, fifoBatch);
      expect(() => upgrade.migrateLegacy(`upgrade-fifo-${suffix}`, fifoBatch))
        .toThrow('legacy_queue_sequence');
      expect(upgrade.get(`upgrade-fifo-${suffix}`)).toMatchObject({ state: 'drained' });
      database.close();
    }
  });

  test('parks and adopts multiple same-chat C4 imports across rollback, reopen, and retry rollback', async () => {
    const { database, databasePath } = openTestDatabase();
    const batch = {
      batch_id: 'retry-import-batch',
      records: [{
        kind: 'c4', legacy_record_id: 'retry-import-c4', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1, envelope: legacyEnvelope('retry-import-c4'),
      }, {
        kind: 'c4', legacy_record_id: 'retry-import-c4-second', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 2,
        envelope: legacyEnvelope('retry-import-c4-second'),
      }],
    };
    const first = enterMaintenance(database, 'upgrade-retry-import-1', 'retry-import-1');
    first.completeDrain('upgrade-retry-import-1');
    migrateLegacyWithSourceProof(first, database, 'upgrade-retry-import-1', batch);
    const firstImports = database.prepare(`
      SELECT legacy_kind, migrated_turn_id FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-retry-import-1' ORDER BY legacy_kind
    `).all();
    first.fail('upgrade-retry-import-1', {
      boundary: 'health_check', code: 'retry_fixture', message: 'retry exact imports',
    });
    const firstSnapshot = {
      package_release_ref: 'release-retry-import-1',
      database_snapshot_ref: 'database-retry-import-1', snapshot_sha256: 'd'.repeat(64),
    };
    recordVerifiedRestoreEffect(database, 'upgrade-retry-import-1', firstSnapshot);
    first.completeRollback('upgrade-retry-import-1');
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
      WHERE turn_id IN (?, ?) AND state = 'queued'
    `).get(...firstImports.map(({ migrated_turn_id: turnId }) => turnId)).count).toBe(2);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turn_queue
      WHERE turn_id IN (?, ?) AND status = 'cancelled'
        AND wait_reason = 'upgrade_rollback_parked'
    `).get(...firstImports.map(({ migrated_turn_id: turnId }) => turnId)).count).toBe(2);
    const concurrent = acceptNormalInbound(database, envelope('between-retry-upgrades'), {
      now: () => '2026-07-20T10:00:03.000Z',
      generateId: deterministicIds('between-retry-upgrades'),
    });
    const normalExecutor = testExecutorService(database, {
      serviceInstanceId: 'rollback-normal-ingress-probe',
      now: () => '2026-07-20T10:00:03.500Z',
    });
    normalExecutor.start();
    expect(await normalExecutor.runNext()).toMatchObject({ turn_id: concurrent.turn_id });
    await normalExecutor.close();
    database.close();

    const reopened = new Database(databasePath);
    const second = enterMaintenance(reopened, 'upgrade-retry-import-2', 'retry-import-2');
    second.completeDrain('upgrade-retry-import-2');
    authorizeLegacyBatch(reopened, 'upgrade-retry-import-2', batch);
    const wrongFirstAttempt = structuredClone(batch);
    wrongFirstAttempt.records[0].envelope.content.text = 'mismatched prior payload';
    expect(() => second.migrateLegacy('upgrade-retry-import-2', wrongFirstAttempt))
      .toThrow('exact durable source-queue invalidation proof');
    expect(second.get('upgrade-retry-import-2')).toMatchObject({ state: 'drained' });
    expect(second.migrateLegacy('upgrade-retry-import-2', batch)).toMatchObject({
      state: 'health_check', migration: { migrated: 2 },
    });
    expect(second.migrateLegacy('upgrade-retry-import-2', batch)).toMatchObject({
      state: 'health_check', migration: { migrated: 2 },
    });
    const adopted = reopened.prepare(`
      SELECT legacy_kind, migrated_turn_id, imported_by_upgrade,
        turn.state, queue.status, queue.wait_reason
      FROM runtime_legacy_migration_records AS legacy
      JOIN runtime_turns AS turn ON turn.turn_id = legacy.migrated_turn_id
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE legacy.upgrade_id = 'upgrade-retry-import-2'
      ORDER BY legacy_kind
    `).all();
    expect(adopted).toEqual(firstImports.map((prior) => ({
      ...prior, imported_by_upgrade: 1,
      state: 'queued', status: 'queued', wait_reason: 'maintenance',
    })));
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_provider_attempts
      WHERE turn_id IN (?, ?)
    `).get(...adopted.map(({ migrated_turn_id: turnId }) => turnId)).count).toBe(0);
    const mismatched = structuredClone(batch);
    mismatched.records[0].envelope.content.text = 'different legacy payload';
    expect(() => second.migrateLegacy('upgrade-retry-import-2', mismatched))
      .toThrow('exact durable source-queue invalidation proof');
    second.fail('upgrade-retry-import-2', {
      boundary: 'health_check', code: 'repeat_rollback', message: 'repeat rollback fixture',
    });
    recordVerifiedRestoreEffect(reopened, 'upgrade-retry-import-2', {
      package_release_ref: 'release-retry-import-2',
      database_snapshot_ref: 'database-retry-import-2', snapshot_sha256: 'd'.repeat(64),
    });
    expect(second.completeRollback('upgrade-retry-import-2')).toMatchObject({ state: 'rolled_back' });
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turns AS turn JOIN runtime_turn_queue AS queue USING (turn_id)
      WHERE turn.turn_id IN (?, ?) AND turn.state = 'queued'
        AND queue.status = 'cancelled' AND queue.wait_reason = 'upgrade_rollback_parked'
    `).get(...adopted.map(({ migrated_turn_id: turnId }) => turnId)).count).toBe(2);
    expect(reopened.pragma('foreign_key_check')).toEqual([]);
    reopened.close();
  });

  test('does not claim rollback ownership of a deduplicated pre-existing legacy turn', () => {
    const { database } = openTestDatabase();
    const existing = acceptNormalInbound(database, legacyEnvelope('deduplicated-existing'), {
      now: () => '2026-07-20T09:59:00.000Z', generateId: deterministicIds('dedup-existing'),
    });
    const upgrade = enterMaintenance(database, 'upgrade-dedup-existing', 'dedup-existing');
    upgrade.completeDrain('upgrade-dedup-existing');
    migrateLegacyWithSourceProof(upgrade, database, 'upgrade-dedup-existing', {
      batch_id: 'batch-dedup-existing',
      records: [{
        kind: 'c4', legacy_record_id: 'deduplicated-existing', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1,
        envelope: legacyEnvelope('deduplicated-existing'),
      }],
    });
    expect(database.prepare(`
      SELECT migrated_turn_id, imported_by_upgrade
      FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-dedup-existing'
    `).get()).toEqual({ migrated_turn_id: existing.turn_id, imported_by_upgrade: 0 });
    upgrade.fail('upgrade-dedup-existing', {
      boundary: 'health_check', code: 'fixture_failure', message: 'rollback dedup fixture',
    });
    const dedupSnapshot = {
      package_release_ref: 'release-dedup-existing',
      database_snapshot_ref: 'database-dedup-existing', snapshot_sha256: 'd'.repeat(64),
    };
    recordVerifiedRestoreEffect(database, 'upgrade-dedup-existing', dedupSnapshot);
    upgrade.completeRollback('upgrade-dedup-existing');
    expect(database.prepare(`
      SELECT turn.state, queue.status FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(existing.turn_id)).toEqual({ state: 'queued', status: 'queued' });
    database.close();
  });
});
