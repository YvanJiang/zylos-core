import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import {
  createAtomicReleaseAdapter,
  createLegacySourceQueueAdapter,
  createRuntimeUpgradeCoordinator,
  createSqliteSnapshotAdapter,
} from '../runtime/migration/runtime-upgrade-coordinator.js';
import { createInstalledRuntimeUpgradeHost } from '../runtime/migration/installed-runtime-upgrade-host.js';
import { createRuntimeUpgradeService } from '../runtime/migration/runtime-upgrade-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { runSelfUpgrade } from '../cli/lib/self-upgrade.js';
import { runtimeUpgradeOwnershipLockPath } from '../runtime/migration/upgrade-state.js';

const inboundFixtures = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function createFixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-upgrade-coordinator-'));
  temporaryDirectories.push(directory);
  const databasePath = path.join(directory, 'runtime.db');
  const releaseA = path.join(directory, 'release-A');
  const releaseB = path.join(directory, 'release-B');
  const legacyPath = path.join(directory, 'legacy-runtime-service');
  fs.mkdirSync(releaseA);
  fs.mkdirSync(releaseB);
  fs.mkdirSync(legacyPath);
  fs.writeFileSync(path.join(releaseA, 'version'), 'A\n');
  fs.writeFileSync(path.join(releaseB, 'version'), 'B\n');
  return {
    directory,
    databasePath,
    database: new Database(databasePath),
    releaseA,
    releaseB,
    legacyPath,
    activeReleaseFile: path.join(directory, 'active-release.json'),
    snapshotDirectory: path.join(directory, 'snapshots'),
    legacyQueueFile: path.join(directory, 'legacy-control-queue.json'),
    legacyAuditDirectory: path.join(directory, 'legacy-audit'),
    legacyDispatcher: { running: true, stop_count: 0, restart_count: 0 },
  };
}

function legacySourceAdapter(fixture, { restartLegacyDispatcher } = {}) {
  return createLegacySourceQueueAdapter({
    sourceQueueFile: fixture.legacyQueueFile,
    auditDirectory: fixture.legacyAuditDirectory,
    stopLegacyDispatcher: async () => {
      fixture.legacyDispatcher.running = false;
      fixture.legacyDispatcher.stop_count += 1;
      return { stopped: true, stopped_at: '2026-07-20T10:00:05.500Z' };
    },
    restartLegacyDispatcher: restartLegacyDispatcher ?? (async ({ step_id: stepId }) => {
      fixture.legacyDispatcher.running = true;
      fixture.legacyDispatcher.restart_count += 1;
      return {
        step_id: stepId, restarted: true, restarted_at: '2026-07-20T10:00:06.750Z',
      };
    }),
  });
}

function writeLegacySource(fixture, batch) {
  fs.writeFileSync(fixture.legacyQueueFile, `${JSON.stringify(batch)}\n`, { mode: 0o600 });
}

function deliveredNoticeAdapter(database, deliveries = []) {
  return {
    async deliver({ upgrade_id: upgradeId, notice_ids: noticeIds }) {
      for (const notice of noticeIds) {
        const outbox = database.prepare(`
          SELECT outbox.outbox_id, outbox.delivery_id
          FROM runtime_legacy_migration_notices AS notice
          JOIN runtime_outbox AS outbox ON outbox.outbox_id = notice.outbox_id
          WHERE notice.upgrade_id = ? AND notice.legacy_kind = ?
            AND notice.legacy_record_id = ?
        `).get(upgradeId, notice.legacy_kind, notice.legacy_record_id);
        const proof = {
          status: 'delivered', delivery_id: outbox.delivery_id,
          delivered_at: '2026-07-20T10:00:06.500Z',
        };
        database.prepare(`
          UPDATE runtime_outbox SET status = 'delivered', result_json = ?, updated_at = ?
          WHERE outbox_id = ?
        `).run(JSON.stringify(proof), proof.delivered_at, outbox.outbox_id);
        deliveries.push({ ...notice, ...proof });
      }
    },
  };
}

function ids(prefix) {
  let sequence = 0;
  return (kind) => `${kind}-${prefix}-${++sequence}`;
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => { resolve = resolvePromise; });
  return { promise, resolve };
}

function checks() {
  return {
    sqlite_integrity: 'ok',
    codex_transport: 'official_app_server_only',
    delivery_contract: 'zylos.delivery-command@1.1',
    workspace_lease_fencing: 'intact',
    retention_cleanup: 'intact',
    normal_runtime_paths: 'new_only',
  };
}

function envelope(suffix) {
  const inboundEventId = `event-${suffix}`;
  return {
    contract: 'zylos.inbound-envelope', contract_version: '1.0',
    inbound_event_id: inboundEventId,
    idempotency_key: createIdempotencyKey('inbound', {
      region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
      bot_id: 'bot-upgrade', inbound_event_id: inboundEventId,
    }),
    trace_id: `trace-${suffix}`,
    occurred_at: '2026-07-20T10:00:00.000Z',
    received_at: '2026-07-20T10:00:01.000Z',
    region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
    bot_id: 'bot-upgrade', chat_type: 'group', chat_id: 'chat-upgrade',
    native_thread_or_topic_id: null, message_id: `message-${suffix}`,
    actor: { type: 'user', actor_id: 'user-upgrade', authenticated: true, roles: [] },
    content: { kind: 'text', text: 'Preserve this concurrent ingress.', attachments: [] },
    reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
    source: { kind: 'platform_original', source_ref: null },
  };
}

function legacyEnvelope(legacyRecordId) {
  const document = structuredClone(inboundFixtures.valid.find(
    ({ name }) => name === 'legacy_compatibility_fields',
  ).document);
  document.inbound_event_id = `legacy-event-${legacyRecordId}`;
  document.idempotency_key = `legacy-c4:${legacyRecordId}`;
  document.trace_id = `legacy-trace-${legacyRecordId}`;
  document.message_id = `legacy-message-${legacyRecordId}`;
  document.legacy.legacy_record_id = legacyRecordId;
  document.legacy.migration_batch_id = 'physical-batch';
  return document;
}

function legacyScheduledOccurrence(suffix) {
  return {
    schedule_id: `schedule-${suffix}`,
    task_id: `task-${suffix}`,
    occurrence_id: `occurrence-${suffix}`,
    prompt: `Run ${suffix}.`,
    occurred_at: '2026-07-20T09:59:00.000Z',
    received_at: '2026-07-20T10:00:00.000Z',
    region: 'global', tenant_id: 'tenant-upgrade', bot_id: 'bot-upgrade',
    bound_conversation: null,
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('runtime upgrade coordinator', () => {
  test('derives rollback-safe work only through canonical legacy validation', () => {
    const fixture = createFixture();
    const service = createRuntimeUpgradeService({ database: fixture.database, generateId: ids('plan') });
    const recurring = {
      kind: 'scheduler', legacy_record_id: 'recurring-next', legacy_state: 'pending',
      schedule_type: 'recurring', scheduled_for: '2026-07-20T09:59:00.000Z',
      observed_at: '2026-07-20T10:00:00.000Z', miss_threshold_ms: 120_000,
      occurrence: legacyScheduledOccurrence('recurring-next'),
    };
    expect(service.planLegacyRollbackBatch({
      batch_id: 'canonical-plan',
      records: [
        { kind: 'c4', legacy_record_id: 'safe-c4', legacy_state: 'pending', route: 'unique',
          legacy_queue_sequence: 1, envelope: legacyEnvelope('safe-c4') },
        recurring,
        { kind: 'runtime_control', legacy_record_id: 'never-safe', legacy_state: 'pending' },
      ],
    })).toEqual({
      batch_id: 'canonical-plan', rollback_reconciled: true,
      records: [expect.objectContaining({ legacy_record_id: 'safe-c4' }), recurring],
    });
    const malformed = legacyEnvelope('wrong-identity');
    expect(() => service.planLegacyRollbackBatch({
      batch_id: 'malformed-plan',
      records: [{
        kind: 'c4', legacy_record_id: 'expected-identity', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1, envelope: malformed,
      }],
    })).toThrow('exact legacy record identity');
    expect(() => service.planLegacyRollbackBatch({
      batch_id: 'bad-scheduler-plan',
      records: [{ ...recurring, occurrence: { schedule_id: 'incomplete' } }],
    })).toThrow();
    expect(() => service.planLegacyRollbackBatch({
      batch_id: 'bad-fifo-plan',
      records: [
        { kind: 'c4', legacy_record_id: 'fifo-one', legacy_state: 'pending', route: 'unique',
          legacy_queue_sequence: 1, envelope: legacyEnvelope('fifo-one') },
        { kind: 'c4', legacy_record_id: 'fifo-two', legacy_state: 'pending', route: 'unique',
          legacy_queue_sequence: 1, envelope: legacyEnvelope('fifo-two') },
      ],
    })).toThrow('positive and strictly FIFO ordered');
    expect(() => service.planLegacyRollbackBatch({
      batch_id: 'trusted-scheduler-plan',
      records: [{ ...recurring, permission_mode: 'trusted' }],
    })).toThrow('permission_mode must be safe');
    fixture.database.close();
  });

  test('hashes upgrade IDs so snapshot files cannot escape the configured directory', async () => {
    const fixture = createFixture();
    const adapter = createSqliteSnapshotAdapter({
      database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const descriptor = await adapter.capture({
      step_id: '../escape:snapshot-capture', upgrade_id: '../escape', from_release: 'release-A',
    });
    expect(path.dirname(descriptor.database_snapshot_ref)).toBe(fixture.snapshotDirectory);
    expect(path.basename(descriptor.database_snapshot_ref)).toMatch(/^[a-f0-9]{64}\.sqlite$/);
    expect(fs.existsSync(path.join(fixture.directory, 'escape.sqlite'))).toBe(false);
    fixture.database.close();
  });

  test('durably claims each external effect before invocation across SQLite connections', async () => {
    const fixture = createFixture();
    const otherDatabase = new Database(fixture.databasePath);
    const serviceA = createRuntimeUpgradeService({
      database: fixture.database, now: () => '2026-07-20T08:00:00.000Z', generateId: ids('race-A'),
    });
    serviceA.preflight({
      upgrade_id: 'upgrade-effect-race', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    const serviceB = createRuntimeUpgradeService({
      database: otherDatabase, now: () => '2026-07-20T08:00:00.000Z', generateId: ids('race-B'),
    });
    const entered = deferred();
    const release = deferred();
    let invocationCount = 0;
    const snapshotAdapter = {
      async capture(request) {
        invocationCount += 1;
        entered.resolve();
        await release.promise;
        return {
          package_release_ref: request.from_release,
          database_snapshot_ref: '/disposable/snapshot.sqlite',
          snapshot_sha256: 'a'.repeat(64), step_id: request.step_id,
        };
      },
      async verify({ descriptor }) {
        return { ...descriptor, database_integrity: 'ok', foreign_key_violations: 0 };
      },
    };
    const releaseAdapter = {
      async activate() { return {}; }, async restore() { return {}; }, async cleanup() { return {}; },
    };
    const coordinatorA = createRuntimeUpgradeCoordinator({
      database: fixture.database, upgradeService: serviceA, snapshotAdapter, releaseAdapter,
      now: () => '2026-07-20T08:00:01.000Z',
    });
    const coordinatorB = createRuntimeUpgradeCoordinator({
      database: otherDatabase, upgradeService: serviceB, snapshotAdapter, releaseAdapter,
      now: () => '2026-07-20T08:00:01.000Z',
    });
    const first = coordinatorA.advance('upgrade-effect-race');
    await entered.promise;
    await expect(coordinatorB.advance('upgrade-effect-race')).rejects.toThrow(
      'already claimed by another coordinator',
    );
    expect(invocationCount).toBe(1);
    release.resolve();
    expect(await first).toMatchObject({ completed_step: 'snapshot-capture' });
    expect(otherDatabase.prepare(`
      SELECT state, claim_attempt FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-effect-race' AND step_key = 'snapshot-capture'
    `).get()).toEqual({ state: 'completed', claim_attempt: 1 });
    otherDatabase.close();
    fixture.database.close();
  });

  test('renews a live external-effect claim so no coordinator can take it over after expiry', async () => {
    const fixture = createFixture();
    const otherDatabase = new Database(fixture.databasePath);
    const serviceA = createRuntimeUpgradeService({ database: fixture.database, generateId: ids('long-A') });
    serviceA.preflight({
      upgrade_id: 'upgrade-long-effect', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    const serviceB = createRuntimeUpgradeService({ database: otherDatabase, generateId: ids('long-B') });
    const entered = deferred();
    const release = deferred();
    let invocationCount = 0;
    const snapshotAdapter = {
      async capture(request) {
        invocationCount += 1;
        entered.resolve();
        await release.promise;
        return {
          package_release_ref: request.from_release,
          database_snapshot_ref: '/disposable/long.sqlite',
          snapshot_sha256: 'b'.repeat(64), step_id: request.step_id,
        };
      },
      async verify({ descriptor }) {
        return { ...descriptor, database_integrity: 'ok', foreign_key_violations: 0 };
      },
    };
    const releaseAdapter = {
      async activate() { return {}; }, async restore() { return {}; }, async cleanup() { return {}; },
    };
    const coordinatorA = createRuntimeUpgradeCoordinator({
      database: fixture.database, upgradeService: serviceA, snapshotAdapter, releaseAdapter,
      effectClaimLeaseMs: 30,
    });
    const coordinatorB = createRuntimeUpgradeCoordinator({
      database: otherDatabase, upgradeService: serviceB, snapshotAdapter, releaseAdapter,
      effectClaimLeaseMs: 30,
    });
    const first = coordinatorA.advance('upgrade-long-effect');
    await entered.promise;
    await new Promise((resolve) => setTimeout(resolve, 75));
    await expect(coordinatorB.advance('upgrade-long-effect')).rejects.toThrow(
      'already claimed by another coordinator',
    );
    expect(invocationCount).toBe(1);
    release.resolve();
    await first;
    otherDatabase.close();
    fixture.database.close();
  });

  test('resumes an exact hash-authorized migrating batch through the coordinator after reopen', async () => {
    const fixture = createFixture();
    const service = createRuntimeUpgradeService({ database: fixture.database, generateId: ids('retry') });
    service.preflight({
      upgrade_id: 'upgrade-migrating-retry', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    const snapshotAdapter = createSqliteSnapshotAdapter({
      database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
    });
    const batch = {
      batch_id: 'coordinator-retry-batch',
      records: [
        { kind: 'c4', legacy_record_id: 'retry-first', legacy_state: 'delivered' },
        { kind: 'c4', legacy_record_id: 'retry-second', legacy_state: 'failed' },
      ],
    };
    writeLegacySource(fixture, batch);
    let coordinator = createRuntimeUpgradeCoordinator({
      database: fixture.database, upgradeService: service, snapshotAdapter, releaseAdapter,
      legacySourceAdapter: legacySourceAdapter(fixture),
    });
    for (let step = 0; step < 8 && !service.isReleaseFenceActive('upgrade-migrating-retry'); step += 1) {
      await coordinator.advance('upgrade-migrating-retry', { legacyBatch: batch });
    }
    expect(service.isReleaseFenceActive('upgrade-migrating-retry')).toBe(true);
    expect(service.get('upgrade-migrating-retry')).toMatchObject({ state: 'drained' });
    fixture.database.exec(`
      CREATE TRIGGER fail_coordinator_migration
      BEFORE INSERT ON runtime_legacy_migration_records
      WHEN NEW.legacy_record_id = 'retry-second'
      BEGIN SELECT RAISE(ABORT, 'injected coordinator migration failure'); END;
    `);
    let migrationError = null;
    try {
      await coordinator.advance('upgrade-migrating-retry', { legacyBatch: batch });
    } catch (error) {
      migrationError = error;
    }
    expect(migrationError?.message).toContain('injected coordinator migration failure');
    expect(service.get('upgrade-migrating-retry')).toMatchObject({ state: 'migrating' });
    fixture.database.close();

    const reopened = new Database(fixture.databasePath);
    reopened.exec('DROP TRIGGER fail_coordinator_migration');
    const resumedService = createRuntimeUpgradeService({ database: reopened, generateId: ids('retry-reopen') });
    coordinator = createRuntimeUpgradeCoordinator({
      database: reopened, upgradeService: resumedService,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: reopened, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter, legacySourceAdapter: legacySourceAdapter(fixture),
    });
    expect(await coordinator.advance('upgrade-migrating-retry', { legacyBatch: batch }))
      .toMatchObject({ state: 'health_check', migration: { retained: 2 } });
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_records
      WHERE upgrade_id = 'upgrade-migrating-retry'
    `).get().count).toBe(2);
    reopened.close();
  });

  test('installed host requests rollback and restores a reconciled source after activation failure', async () => {
    const fixture = createFixture();
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
    });
    fs.writeFileSync(fixture.activeReleaseFile, JSON.stringify({
      release_ref: 'release-A', release_path: fixture.releaseA,
    }));
    let activationAttempts = 0;
    const failingReleaseAdapter = {
      ...releaseAdapter,
      async activate() {
        activationAttempts += 1;
        throw new Error('permanent activation failure');
      },
    };
    const preflight = {
      upgrade_id: 'upgrade-host-rollback', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    };
    const batch = {
      batch_id: 'host-rollback-batch',
      records: [
        { kind: 'c4', legacy_record_id: 'safe-pending', legacy_state: 'pending',
          route: 'unique', legacy_queue_sequence: 1, envelope: legacyEnvelope('safe-pending') },
        { kind: 'c4', legacy_record_id: 'unknown-running', legacy_state: 'running',
          notification_target: {
            region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
            bot_id: 'bot-upgrade', chat_type: 'group', chat_id: 'chat-upgrade',
            native_thread_or_topic_id: null,
            native_thread_root_message_id: null,
            native_thread_reply_target_message_id: null,
          } },
        { kind: 'scheduler', legacy_record_id: 'recurring-next', legacy_state: 'pending',
          schedule_type: 'recurring', scheduled_for: '2026-07-20T09:59:00.000Z',
          observed_at: '2026-07-20T10:00:00.000Z', miss_threshold_ms: 120_000,
          occurrence: legacyScheduledOccurrence('host-recurring-next') },
        { kind: 'runtime_control', legacy_record_id: 'forbidden-stop', legacy_state: 'pending' },
      ],
    };
    writeLegacySource(fixture, batch);
    let host = createInstalledRuntimeUpgradeHost({
      database: fixture.database,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter: failingReleaseAdapter,
      legacySourceAdapter: legacySourceAdapter(fixture),
      zylosDir: fixture.directory, generateId: ids('host-rollback'),
    });
    await host.attach(preflight);
    await host.advance(preflight.upgrade_id);
    await host.advance(preflight.upgrade_id);
    await host.advance(preflight.upgrade_id);
    await host.advance(preflight.upgrade_id);
    await host.advance(preflight.upgrade_id, { legacyBatch: batch });
    await expect(host.advance(preflight.upgrade_id, { legacyBatch: batch }))
      .rejects.toThrow('permanent activation failure');
    expect(host.requestRollback(preflight.upgrade_id, {
      boundary: 'release_activation', code: 'activation_failed',
      message: 'target release could not activate',
    })).toMatchObject({ state: 'rollback_required' });
    fixture.database.close();

    let reopened = new Database(fixture.databasePath);
    const noticeDeliveries = [];
    const durableRestartProofs = new Map();
    let crashAfterDispatcherRestart = true;
    const crashSafeLegacyAdapter = legacySourceAdapter(fixture, {
      async restartLegacyDispatcher({ step_id: stepId }) {
        if (durableRestartProofs.has(stepId)) return durableRestartProofs.get(stepId);
        fixture.legacyDispatcher.running = true;
        fixture.legacyDispatcher.restart_count += 1;
        const proof = {
          step_id: stepId, restarted: true, restarted_at: '2026-07-20T10:00:06.750Z',
        };
        durableRestartProofs.set(stepId, proof);
        if (crashAfterDispatcherRestart) {
          crashAfterDispatcherRestart = false;
          throw new Error('injected crash after dispatcher restart');
        }
        return proof;
      },
    });
    host = createInstalledRuntimeUpgradeHost({
      database: reopened,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: reopened, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter,
      legacySourceAdapter: crashSafeLegacyAdapter,
      noticeAdapter: deliveredNoticeAdapter(reopened, noticeDeliveries),
      zylosDir: fixture.directory, generateId: ids('host-rollback-reopen'),
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-rollback-reconciliation',
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-source-seal',
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'notice-delivery',
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'rollback-restore',
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-source-restore',
    });
    expect(JSON.parse(fs.readFileSync(fixture.legacyQueueFile, 'utf8')).records).toEqual([
      expect.objectContaining({ legacy_record_id: 'safe-pending' }),
      expect.objectContaining({ legacy_record_id: 'recurring-next' }),
    ]);
    await expect(host.advance(preflight.upgrade_id))
      .rejects.toThrow('injected crash after dispatcher restart');
    expect(reopened.prepare(`
      SELECT state FROM runtime_upgrade_effects
      WHERE upgrade_id = ? AND step_key = 'legacy-dispatcher-restart'
    `).get(preflight.upgrade_id)).toEqual({ state: 'claimed' });
    const consumedAfterRestart = JSON.parse(fs.readFileSync(fixture.legacyQueueFile, 'utf8'));
    consumedAfterRestart.records = [];
    fs.writeFileSync(fixture.legacyQueueFile, `${JSON.stringify(consumedAfterRestart)}\n`);
    reopened.close();
    reopened = new Database(fixture.databasePath);
    host = createInstalledRuntimeUpgradeHost({
      database: reopened,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: reopened, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter,
      legacySourceAdapter: crashSafeLegacyAdapter,
      noticeAdapter: deliveredNoticeAdapter(reopened, noticeDeliveries),
      zylosDir: fixture.directory, generateId: ids('host-rollback-restart-reopen'),
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-dispatcher-restart',
    });
    expect(await host.advance(preflight.upgrade_id)).toMatchObject({ state: 'rolled_back' });
    expect(activationAttempts).toBe(1);
    expect(fixture.legacyDispatcher).toMatchObject({ running: true, restart_count: 1 });
    expect(JSON.parse(fs.readFileSync(fixture.legacyQueueFile, 'utf8')).records).toEqual([]);
    expect(noticeDeliveries).toEqual([
      expect.objectContaining({ legacy_record_id: 'unknown-running', status: 'delivered' }),
    ]);
    expect(reopened.prepare(`
      SELECT legacy_record_id, disposition FROM runtime_legacy_migration_records
      WHERE upgrade_id = ? ORDER BY legacy_record_id
    `).all(preflight.upgrade_id)).toEqual([
      { legacy_record_id: 'forbidden-stop', disposition: 'invalidated_audit_only' },
      { legacy_record_id: 'recurring-next', disposition: 'restored_scheduler' },
      { legacy_record_id: 'safe-pending', disposition: 'restored_pending' },
      { legacy_record_id: 'unknown-running', disposition: 'quarantined_side_effect_unknown' },
    ]);
    expect(reopened.prepare(`
      SELECT record_kind, retention_class FROM runtime_retention_entries
      WHERE record_kind LIKE 'legacy_migration_%'
      ORDER BY record_kind, retention_class
    `).all()).toEqual([
      { record_kind: 'legacy_migration_audit_payload', retention_class: 'security_audit_180d' },
      ...Array.from({ length: 4 }, () => ({
        record_kind: 'legacy_migration_payload', retention_class: 'terminal_detail_30d',
      })),
    ]);
    reopened.close();
  });

  test('finishes a crash-interrupted source invalidation before rollback consumes its proof', async () => {
    const fixture = createFixture();
    const service = createRuntimeUpgradeService({ database: fixture.database, generateId: ids('partial') });
    service.preflight({
      upgrade_id: 'upgrade-partial-invalidation', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    fs.writeFileSync(fixture.activeReleaseFile, JSON.stringify({
      release_ref: 'release-A', release_path: fixture.releaseA,
    }));
    const batch = {
      batch_id: 'partial-invalidation-batch',
      records: [{
        kind: 'c4', legacy_record_id: 'partial-safe', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1, envelope: legacyEnvelope('partial-safe'),
      }],
    };
    writeLegacySource(fixture, batch);
    const durableAdapter = legacySourceAdapter(fixture);
    let crashAfterRename = true;
    const crashingAdapter = {
      ...durableAdapter,
      async invalidate(request) {
        const proof = await durableAdapter.invalidate(request);
        if (crashAfterRename) {
          crashAfterRename = false;
          throw new Error('injected crash after source invalidation');
        }
        return proof;
      },
    };
    const snapshotAdapter = createSqliteSnapshotAdapter({
      database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
    });
    let coordinator = createRuntimeUpgradeCoordinator({
      database: fixture.database, upgradeService: service, snapshotAdapter, releaseAdapter,
      legacySourceAdapter: crashingAdapter,
    });
    for (let step = 0; step < 4; step += 1) await coordinator.advance('upgrade-partial-invalidation');
    await expect(coordinator.advance('upgrade-partial-invalidation', { legacyBatch: batch }))
      .rejects.toThrow('injected crash after source invalidation');
    expect(coordinator.loadEffect('upgrade-partial-invalidation', 'legacy-source-invalidate'))
      .toMatchObject({ state: 'claimed', result: null, input: { batch_id: batch.batch_id } });
    service.fail('upgrade-partial-invalidation', {
      boundary: 'legacy_source_invalidation', code: 'host_crash',
      message: 'host stopped after source rename',
    });
    fixture.database.close();

    const reopened = new Database(fixture.databasePath);
    const resumedService = createRuntimeUpgradeService({ database: reopened, generateId: ids('partial-reopen') });
    coordinator = createRuntimeUpgradeCoordinator({
      database: reopened, upgradeService: resumedService,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: reopened, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter, legacySourceAdapter: durableAdapter,
    });
    expect(await coordinator.advance('upgrade-partial-invalidation')).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-source-invalidate',
    });
    expect(coordinator.loadEffect('upgrade-partial-invalidation', 'legacy-source-invalidate'))
      .toMatchObject({ state: 'completed', claim_attempt: 2 });
    expect((await coordinator.advance('upgrade-partial-invalidation')).completed_step)
      .toBe('legacy-rollback-reconciliation');
    expect((await coordinator.advance('upgrade-partial-invalidation')).completed_step)
      .toBe('legacy-source-seal');
    expect((await coordinator.advance('upgrade-partial-invalidation')).completed_step)
      .toBe('rollback-restore');
    expect((await coordinator.advance('upgrade-partial-invalidation')).completed_step)
      .toBe('legacy-source-restore');
    expect((await coordinator.advance('upgrade-partial-invalidation')).completed_step)
      .toBe('legacy-dispatcher-restart');
    expect(await coordinator.advance('upgrade-partial-invalidation')).toMatchObject({
      state: 'rolled_back',
    });
    reopened.close();
  });

  test('installed host attaches and resumes the same durable upgrade ID after reopen', async () => {
    const fixture = createFixture();
    const markerPath = path.join(fixture.directory, 'runtime', 'atomic-upgrade-owner.json');
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
    });
    let host = createInstalledRuntimeUpgradeHost({
      database: fixture.database,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter,
      zylosDir: fixture.directory,
      now: () => '2026-07-20T09:00:00.000Z', generateId: ids('installed-host'),
    });
    const preflight = {
      upgrade_id: 'upgrade-installed-host', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    };
    const ownershipLock = runtimeUpgradeOwnershipLockPath(fixture.directory);
    fs.mkdirSync(ownershipLock, { recursive: true });
    await expect(host.attach(preflight)).rejects.toThrow('owns the installation lock');
    expect(fs.existsSync(markerPath)).toBe(false);
    fs.rmSync(ownershipLock, { recursive: true, force: true });
    fs.mkdirSync(ownershipLock, { recursive: true });
    fs.writeFileSync(path.join(ownershipLock, 'owner.json'), JSON.stringify({
      schema_version: 1, host: os.hostname(), process_id: process.pid,
      owner_id: 'live-owner', acquired_at: '2026-07-20T08:00:00.000Z',
    }));
    fs.utimesSync(ownershipLock, new Date(0), new Date(0));
    await expect(host.attach(preflight)).rejects.toThrow('owns the installation lock');
    fs.rmSync(ownershipLock, { recursive: true, force: true });
    fs.mkdirSync(ownershipLock, { recursive: true });
    fs.writeFileSync(path.join(ownershipLock, 'owner.json'), JSON.stringify({
      schema_version: 1, host: os.hostname(), process_id: 99999999,
      owner_id: 'crashed-owner', acquired_at: '2026-07-20T08:00:00.000Z',
    }));
    expect(await host.attach(preflight)).toMatchObject({
      upgrade_id: 'upgrade-installed-host', state: 'preflight', state_version: 1,
      ownership_marker_path: markerPath,
    });
    expect(JSON.parse(fs.readFileSync(markerPath, 'utf8'))).toMatchObject({
      runtime_owner: 'durable_executor', legacy_self_upgrade: 'disabled',
    });
    const legacyMutations = [];
    expect(runSelfUpgrade({ tempDir: fixture.directory, newVersion: 'release-B' }, {
      zylosDir: fixture.directory,
      preInstallSteps: [() => legacyMutations.push('legacy-pre-install')],
      runInstalledFinalizer: () => legacyMutations.push('legacy-finalizer'),
      rollbackSelf: () => legacyMutations.push('legacy-rollback'),
    })).toMatchObject({ success: false, failedStep: 0, durableRuntimeOwner: true });
    expect(legacyMutations).toEqual([]);
    expect(await host.advance('upgrade-installed-host')).toMatchObject({
      state: 'preflight', completed_step: 'snapshot-capture',
    });
    expect(await host.advance('upgrade-installed-host')).toMatchObject({ state: 'snapshotted' });
    fixture.database.close();

    const reopened = new Database(fixture.databasePath);
    host = createInstalledRuntimeUpgradeHost({
      database: reopened,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database: reopened, snapshotDirectory: fixture.snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter,
      zylosDir: fixture.directory,
      now: () => '2026-07-20T09:00:01.000Z', generateId: ids('installed-host-reopen'),
    });
    expect(await host.attach(preflight)).toMatchObject({
      upgrade_id: 'upgrade-installed-host', state: 'snapshotted', state_version: 2,
    });
    expect(await host.advance('upgrade-installed-host')).toMatchObject({
      upgrade_id: 'upgrade-installed-host', state: 'maintenance',
    });
    reopened.close();
  });

  test('replays physical snapshot and release effects, then rolls back without replacing live SQLite', async () => {
    const fixture = createFixture();
    const service = createRuntimeUpgradeService({
      database: fixture.database,
      now: () => '2026-07-20T10:00:02.000Z',
      generateId: ids('coordinator'),
    });
    service.preflight({
      upgrade_id: 'upgrade-physical', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    fs.writeFileSync(fixture.activeReleaseFile, JSON.stringify({
      release_ref: 'release-A', release_path: fixture.releaseA,
    }));
    const physicalSnapshotAdapter = createSqliteSnapshotAdapter({
      database: fixture.database,
      snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
      cleanupPaths: [fixture.legacyPath],
    });
    const physicalBatch = {
      batch_id: 'physical-batch',
      records: [{
        kind: 'c4', legacy_record_id: 'physical-import', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1, envelope: legacyEnvelope('physical-import'),
      }, {
        kind: 'c4', legacy_record_id: 'physical-running-unknown', legacy_state: 'running',
        notification_target: {
          region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
          bot_id: 'bot-upgrade', chat_type: 'group', chat_id: 'chat-upgrade',
          native_thread_or_topic_id: null,
          native_thread_root_message_id: null,
          native_thread_reply_target_message_id: null,
        },
      }, {
        kind: 'global_provider_lineage', legacy_record_id: 'rolled-back-global-lineage',
        legacy_state: 'delivered', recent_c4_context: ['rolled back context'],
        memory_handoff: 'rolled back memory',
        outbound_messages: [{
          region: 'global', tenant_id: 'tenant-upgrade', channel: 'telegram',
          bot_id: 'bot-upgrade', chat_type: 'group', chat_id: 'chat-upgrade',
          native_thread_or_topic_id: null,
          platform_message_id: 'rolled-back-legacy-message',
        }],
      }],
    };
    writeLegacySource(fixture, physicalBatch);
    const adapterCalls = [];
    const snapshotAdapter = {
      async capture(request) {
        adapterCalls.push(['snapshot-capture', request.step_id]);
        return physicalSnapshotAdapter.capture(request);
      },
      async verify(request) {
        adapterCalls.push(['snapshot-verify', request.step_id]);
        return physicalSnapshotAdapter.verify(request);
      },
    };
    let injectedActivationCrash = true;
    const flakyReleaseAdapter = {
      ...releaseAdapter,
      async activate(request) {
        adapterCalls.push(['release-activate', request.step_id]);
        const result = await releaseAdapter.activate(request);
        if (injectedActivationCrash) {
          injectedActivationCrash = false;
          throw new Error('injected crash after physical release activation');
        }
        return result;
      },
    };
    let coordinator = createRuntimeUpgradeCoordinator({
      database: fixture.database, upgradeService: service,
      snapshotAdapter, releaseAdapter: flakyReleaseAdapter,
      legacySourceAdapter: legacySourceAdapter(fixture),
      now: () => '2026-07-20T10:00:03.000Z',
    });

    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'preflight', completed_step: 'snapshot-capture',
    });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({ state: 'snapshotted' });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({ state: 'maintenance' });
    const concurrent = acceptNormalInbound(fixture.database, envelope('during-maintenance'), {
      now: () => '2026-07-20T10:00:04.000Z', generateId: ids('concurrent'),
    });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({ state: 'drained' });
    expect(await coordinator.advance('upgrade-physical', {
      legacyBatch: physicalBatch,
    })).toMatchObject({ state: 'drained', completed_step: 'legacy-source-invalidate' });
    await expect(coordinator.advance('upgrade-physical', { legacyBatch: physicalBatch }))
      .rejects.toThrow('injected crash after physical release activation');
    expect(JSON.parse(fs.readFileSync(fixture.activeReleaseFile, 'utf8'))).toMatchObject({
      release_ref: 'release-B', release_path: fixture.releaseB,
    });
    expect(service.get('upgrade-physical')).toMatchObject({ state: 'drained' });
    expect(fixture.database.prepare(`
      SELECT state, claim_attempt FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-physical' AND step_key = 'release-activate'
    `).get()).toEqual({ state: 'claimed', claim_attempt: 1 });

    fixture.database.close();
    const reopened = new Database(fixture.databasePath);
    const resumedService = createRuntimeUpgradeService({
      database: reopened,
      now: () => '2026-07-20T10:00:05.000Z', generateId: ids('resumed'),
    });
    const resumedPhysicalSnapshotAdapter = createSqliteSnapshotAdapter({
      database: reopened,
      snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const resumedSnapshotAdapter = {
      async capture(request) {
        adapterCalls.push(['snapshot-capture', request.step_id]);
        return resumedPhysicalSnapshotAdapter.capture(request);
      },
      async verify(request) {
        adapterCalls.push(['snapshot-verify', request.step_id]);
        return resumedPhysicalSnapshotAdapter.verify(request);
      },
    };
    const resumedReleaseAdapter = {
      ...releaseAdapter,
      async activate(request) {
        adapterCalls.push(['release-activate', request.step_id]);
        return releaseAdapter.activate(request);
      },
      async restore(request) {
        adapterCalls.push(['rollback-restore', request.step_id]);
        return releaseAdapter.restore(request);
      },
    };
    const rollbackNoticeDeliveries = [];
    const durableNoticeAdapter = deliveredNoticeAdapter(reopened, rollbackNoticeDeliveries);
    let allowRollbackNoticeDelivery = false;
    const gatedRollbackNoticeAdapter = {
      async deliver(request) {
        if (!allowRollbackNoticeDelivery) throw new Error('notice transport unavailable');
        return durableNoticeAdapter.deliver(request);
      },
    };
    coordinator = createRuntimeUpgradeCoordinator({
      database: reopened, upgradeService: resumedService,
      snapshotAdapter: resumedSnapshotAdapter, releaseAdapter: resumedReleaseAdapter,
      now: () => '2026-07-20T10:00:06.000Z',
    });
    coordinator = createRuntimeUpgradeCoordinator({
      database: reopened, upgradeService: resumedService,
      snapshotAdapter: resumedSnapshotAdapter, releaseAdapter: resumedReleaseAdapter,
      legacySourceAdapter: legacySourceAdapter(fixture),
      noticeAdapter: gatedRollbackNoticeAdapter,
      now: () => '2026-07-20T10:00:06.000Z',
    });
    expect(await coordinator.advance('upgrade-physical', {
      legacyBatch: physicalBatch,
    })).toMatchObject({ state: 'drained', completed_step: 'release-activate' });
    expect(await coordinator.advance('upgrade-physical', {
      legacyBatch: physicalBatch,
    })).toMatchObject({ state: 'drained', completed_step: 'release-generation-fence' });
    expect(fs.existsSync(fixture.legacyQueueFile)).toBe(false);
    const invalidation = coordinator.loadEffect('upgrade-physical', 'legacy-source-invalidate');
    expect(invalidation.result).toMatchObject({
      batch_id: 'physical-batch', source_queue_read_only: true,
      legacy_dispatcher_stopped: true,
    });
    expect(fs.existsSync(invalidation.result.audit_queue_ref)).toBe(true);
    expect(fs.statSync(invalidation.result.audit_queue_ref).mode & 0o222).toBe(0);
    expect(() => fs.readFileSync(fixture.legacyQueueFile, 'utf8')).toThrow();
    expect(await coordinator.advance('upgrade-physical', {
      legacyBatch: physicalBatch,
    })).toMatchObject({ state: 'health_check' });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'health_check', completed_step: 'legacy-source-seal',
    });
    expect(fs.existsSync(invalidation.result.audit_queue_ref)).toBe(false);
    resumedService.fail('upgrade-physical', {
      boundary: 'executor_health', code: 'fixture_failure', message: 'force rollback fixture',
    });
    await expect(coordinator.advance('upgrade-physical'))
      .rejects.toThrow('notice transport unavailable');
    expect(JSON.parse(fs.readFileSync(fixture.activeReleaseFile, 'utf8'))).toMatchObject({
      release_ref: 'release-B', release_path: fixture.releaseB,
    });
    expect(fixture.legacyDispatcher.running).toBe(false);
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-physical' AND step_key = 'rollback-restore'
    `).get().count).toBe(0);
    allowRollbackNoticeDelivery = true;
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'rollback_required', completed_step: 'notice-delivery',
    });
    expect(rollbackNoticeDeliveries).toEqual([
      expect.objectContaining({ legacy_record_id: 'physical-running-unknown' }),
    ]);
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'rollback_required', completed_step: 'rollback-restore',
    });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'rollback_required', completed_step: 'release-generation-restore',
    });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-source-restore',
    });
    expect(await coordinator.advance('upgrade-physical')).toMatchObject({
      state: 'rollback_required', completed_step: 'legacy-dispatcher-restart',
    });
    const rolledBack = await coordinator.advance('upgrade-physical');
    expect(rolledBack).toMatchObject({ state: 'rolled_back' });
    expect(await coordinator.advance('upgrade-physical')).toEqual(rolledBack);
    expect(JSON.parse(fs.readFileSync(fixture.activeReleaseFile, 'utf8'))).toMatchObject({
      release_ref: 'release-A', release_path: fixture.releaseA,
    });
    expect(fixture.legacyDispatcher).toMatchObject({
      running: true, stop_count: 1, restart_count: 1,
    });
    expect(JSON.parse(fs.readFileSync(fixture.legacyQueueFile, 'utf8'))).toMatchObject({
      batch_id: 'physical-batch', rollback_reconciled: true,
      records: [expect.objectContaining({ legacy_record_id: 'physical-import' })],
    });
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_active_release_fences
      WHERE upgrade_id = 'upgrade-physical'
    `).get().count).toBe(0);
    expect(reopened.prepare(`
      SELECT restored_at FROM runtime_upgrade_release_fence_history
      WHERE upgrade_id = 'upgrade-physical'
    `).get().restored_at).not.toBeNull();
    expect(reopened.prepare(`
      SELECT turn.state, queue.status, queue.wait_reason
      FROM runtime_turns AS turn JOIN runtime_turn_queue AS queue USING (turn_id)
      WHERE turn.turn_id = ?
    `).get(concurrent.turn_id)).toEqual({
      state: 'queued', status: 'queued', wait_reason: 'maintenance',
    });
    expect(reopened.prepare(`
      SELECT turn.state, queue.status
      FROM runtime_legacy_migration_records AS legacy
      JOIN runtime_turns AS turn ON turn.turn_id = legacy.migrated_turn_id
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE legacy.upgrade_id = 'upgrade-physical' AND legacy.legacy_record_id = 'physical-import'
    `).get()).toEqual({ state: 'queued', status: 'cancelled' });
    const rolledBackReply = envelope('rolled-back-legacy-reply');
    rolledBackReply.reply.reply_to_message_id = 'rolled-back-legacy-message';
    const replyResult = acceptNormalInbound(reopened, rolledBackReply, {
      now: () => '2026-07-20T10:00:06.500Z', generateId: ids('rolled-back-reply'),
    });
    expect(replyResult).toMatchObject({
      status: 'accepted', lineage_id: null, lineage_resolution_state: 'pending_recovery',
    });
    expect(reopened.prepare(`
      SELECT provider_input_json FROM runtime_turns WHERE turn_id = ?
    `).get(replyResult.turn_id)).toEqual({ provider_input_json: null });
    const store = createExecutorStore({
      database: reopened, provider: 'claude', serviceInstanceId: 'after-physical-rollback',
      now: () => '2026-07-20T10:00:07.000Z', generateId: ids('after-rollback'),
    });
    expect(store.claimNextQueuedTurn()).toMatchObject({ turn_id: concurrent.turn_id });
    const snapshot = resumedService.get('upgrade-physical').snapshot;
    const snapshotDb = new Database(snapshot.database_snapshot_ref, {
      readonly: true, fileMustExist: true,
    });
    expect(snapshotDb.pragma('integrity_check', { simple: true })).toBe('ok');
    expect(snapshotDb.pragma('foreign_key_check')).toEqual([]);
    snapshotDb.close();
    expect(reopened.pragma('foreign_key_check')).toEqual([]);
    expect(reopened.prepare(`
      SELECT step_key, COUNT(*) AS count FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-physical'
      GROUP BY step_key ORDER BY step_key
    `).all()).toEqual([
      { step_key: 'legacy-dispatcher-restart', count: 1 },
      { step_key: 'legacy-source-invalidate', count: 1 },
      { step_key: 'legacy-source-restore', count: 1 },
      { step_key: 'legacy-source-seal', count: 1 },
      { step_key: 'release-activate', count: 1 },
      { step_key: 'rollback-restore', count: 1 },
      { step_key: 'snapshot-capture', count: 1 },
    ]);
    expect(fs.existsSync(fixture.legacyPath)).toBe(true);
    expect(adapterCalls).toEqual([
      ['snapshot-capture', 'upgrade-physical:snapshot-capture'],
      ['snapshot-verify', 'upgrade-physical:snapshot-record'],
      ['release-activate', 'upgrade-physical:release-activate'],
      ['release-activate', 'upgrade-physical:release-activate'],
      ['snapshot-verify', 'upgrade-physical:rollback-restore'],
      ['rollback-restore', 'upgrade-physical:rollback-restore'],
    ]);
    reopened.close();
  });

  test('commits only after target executor reconciliation and only then removes legacy paths', async () => {
    const fixture = createFixture();
    const service = createRuntimeUpgradeService({
      database: fixture.database,
      now: () => '2026-07-20T11:00:02.000Z', generateId: ids('commit-service'),
    });
    service.preflight({
      upgrade_id: 'upgrade-commit', from_release: 'release-A', to_release: 'release-B',
      scope: { kind: 'installation', bot_id: null }, checks: checks(),
    });
    fs.writeFileSync(fixture.activeReleaseFile, JSON.stringify({
      release_ref: 'release-A', release_path: fixture.releaseA,
    }));
    const snapshotAdapter = createSqliteSnapshotAdapter({
      database: fixture.database, snapshotDirectory: fixture.snapshotDirectory,
      openDatabase: (file, options) => new Database(file, options),
    });
    const releaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile: fixture.activeReleaseFile,
      releases: { 'release-A': fixture.releaseA, 'release-B': fixture.releaseB },
      cleanupPaths: [fixture.legacyPath],
    });
    let executor = null;
    const coordinator = createRuntimeUpgradeCoordinator({
      database: fixture.database,
      upgradeService: service,
      snapshotAdapter,
      releaseAdapter,
      legacySourceAdapter: legacySourceAdapter(fixture),
      executorAdapter: {
        health(request) {
          executor = createExecutorService({
            database: fixture.database,
            adapter: {
              provider: 'codex', provider_transport: 'official_app_server',
              async *execute() {},
            },
            provider: 'codex', serviceInstanceId: 'executor-upgrade-commit',
            hostId: 'host-upgrade-commit',
            serviceStartedAt: '2026-07-20T11:00:10.000Z',
            releaseRef: request.release_ref, upgradeId: request.upgrade_id,
            now: () => '2026-07-20T11:00:10.000Z', generateId: ids('commit-executor'),
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
          executor.start();
          const snapshot = executor.publishObservabilitySnapshot();
          return {
            service_instance_id: 'executor-upgrade-commit',
            snapshot_version: snapshot.snapshot_version,
            health: snapshot.service.health,
            reconciliation: 'complete',
          };
        },
      },
      now: () => '2026-07-20T11:00:11.000Z',
    });
    await coordinator.advance('upgrade-commit');
    await coordinator.advance('upgrade-commit');
    await coordinator.advance('upgrade-commit');
    await coordinator.advance('upgrade-commit');
    const commitBatch = { batch_id: 'commit-empty-batch', records: [] };
    writeLegacySource(fixture, commitBatch);
    await coordinator.advance('upgrade-commit', {
      legacyBatch: commitBatch,
    });
    await coordinator.advance('upgrade-commit', {
      legacyBatch: commitBatch,
    });
    await coordinator.advance('upgrade-commit', {
      legacyBatch: commitBatch,
    });
    await coordinator.advance('upgrade-commit', {
      legacyBatch: commitBatch,
    });
    expect((await coordinator.advance('upgrade-commit')).completed_step).toBe('legacy-source-seal');
    expect((await coordinator.advance('upgrade-commit')).completed_step).toBe('executor-health');
    expect(await coordinator.advance('upgrade-commit')).toMatchObject({ state: 'ready_to_commit' });
    expect(await coordinator.advance('upgrade-commit')).toMatchObject({ state: 'committed' });
    const restartedExecutor = createExecutorService({
      database: fixture.database,
      adapter: { provider: 'codex', provider_transport: 'official_app_server', async *execute() {} },
      provider: 'codex', serviceInstanceId: 'executor-upgrade-commit-restart',
      hostId: 'host-upgrade-commit-restart',
      serviceStartedAt: '2026-07-20T11:00:12.000Z',
      releaseRef: 'release-B', upgradeId: 'upgrade-commit',
      now: () => '2026-07-20T11:00:12.000Z', generateId: ids('restart-executor'),
      scheduleResidentHeartbeat: () => ({ unref() {} }), cancelResidentHeartbeat: () => {},
      scheduleWorkspaceHeartbeat: () => ({ unref() {} }), cancelWorkspaceHeartbeat: () => {},
      scheduleTurnLeaseRenewal: () => ({ unref() {} }), cancelTurnLeaseRenewal: () => {},
      scheduleNonterminalSweep: () => ({ unref() {} }), cancelNonterminalSweep: () => {},
      schedulePermissionSweep: () => ({ unref() {} }), cancelPermissionSweep: () => {},
    });
    restartedExecutor.start();
    expect(fixture.database.prepare(`
      SELECT upgrade_id, release_ref, generation FROM runtime_active_release_fences
      WHERE scope_key = 'installation'
    `).get()).toEqual({
      upgrade_id: 'upgrade-commit', release_ref: 'release-B', generation: 1,
    });
    expect(fs.existsSync(fixture.legacyPath)).toBe(true);
    expect(await coordinator.advance('upgrade-commit')).toMatchObject({
      state: 'committed', completed_step: 'legacy-source-commit',
    });
    expect(await coordinator.advance('upgrade-commit')).toMatchObject({
      state: 'committed', completed_step: 'postcommit-cleanup',
    });
    expect(fs.existsSync(fixture.legacyPath)).toBe(false);
    expect(() => service.fail('upgrade-commit', {
      boundary: 'after_commit', code: 'late_failure', message: 'must stay on release B',
    })).toThrow('committed and cannot roll back');
    expect(JSON.parse(fs.readFileSync(fixture.activeReleaseFile, 'utf8'))).toMatchObject({
      release_ref: 'release-B', release_path: fixture.releaseB,
    });
    expect(fixture.database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_upgrade_effects
      WHERE upgrade_id = 'upgrade-commit' AND step_key = 'rollback-restore'
    `).get().count).toBe(0);
    await executor.close();
    await restartedExecutor.close();
    fixture.database.close();
  });
});
