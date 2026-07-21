import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  extractAndFenceLegacyBaseBatch,
  reconcileLegacyBaseRollback,
} from '../runtime/migration/legacy-base-source.js';
import { createInstalledExecutorUpgradeHandler } from '../runtime/migration/installed-executor-upgrade.js';
import { readChannelAuthorityManifest } from '../runtime/migration/channel-authority-manifest.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { validateInboundEnvelope } from '../contracts/public/index.js';
import { deliveredResult } from './helpers/delivered-result.js';

const roots = [];

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function exactBaseDatabase() {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'zylos-exact-base-source-'));
  roots.push(root);
  const database = new Database(path.join(root, 'c4.db'));
  database.exec(`
    CREATE TABLE conversations (
      id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
      direction TEXT NOT NULL, channel TEXT NOT NULL, endpoint_id TEXT, content TEXT NOT NULL,
      status TEXT DEFAULT 'pending', delivery_action TEXT, priority INTEGER DEFAULT 3,
      require_idle INTEGER DEFAULT 0, retry_count INTEGER DEFAULT 0
    );
    CREATE TABLE control_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT, raw_content TEXT NOT NULL, content TEXT NOT NULL,
      priority INTEGER DEFAULT 3, require_idle INTEGER DEFAULT 0, bypass_state INTEGER DEFAULT 0,
      ack_deadline_at INTEGER, status TEXT DEFAULT 'pending', retry_count INTEGER DEFAULT 0,
      available_at INTEGER, last_error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
  `);
  return database;
}

function writeRelease(root, { legacy = false } = {}) {
  fs.mkdirSync(path.join(root, 'cli'), { recursive: true });
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ name: 'zylos', version: '0.6.0' }));
  fs.writeFileSync(path.join(root, 'cli', 'zylos.js'), 'export {};\n');
  if (legacy) return;
  for (const directory of ['runtime/executor', 'scripts', 'templates/pm2']) {
    fs.mkdirSync(path.join(root, directory), { recursive: true });
  }
  for (const file of [
    'cli/launcher.js', 'runtime/executor/daemon.js', 'runtime/executor/health-probe.js',
    'runtime/executor/launcher.js', 'scripts/postinstall.js',
  ]) fs.writeFileSync(path.join(root, file), 'export {};\n');
  fs.writeFileSync(
    path.join(root, 'templates/pm2/ecosystem.config.cjs'),
    'module.exports={apps:[{name:"zylos-executor"}]};\n',
  );
  for (const skill of ['comm-bridge', 'scheduler', 'web-console']) {
    fs.mkdirSync(path.join(root, 'skills', skill), { recursive: true });
    fs.writeFileSync(path.join(root, 'skills', skill, 'package.json'), JSON.stringify({
      name: `fixture-${skill}`, version: '1.0.0',
    }));
  }
  fs.writeFileSync(path.join(root, 'retired-runtime.js'), 'must not enter active release\n');
  fs.writeFileSync(path.join(root, '.npmignore'), 'retired-runtime.js\n');
}

function channelAuthority() {
  return {
    schema_version: 1,
    contract: 'zylos.channel-authority',
    scopes: [
      {
        channel: 'feishu', region: 'cn', tenant_id: 'tenant-feishu', bot_id: 'app-feishu',
        verified_at: '2026-07-20T00:00:00.000Z',
        verification_source: 'authenticated_event', provider_instance_id: 'feishu-owner-one',
      },
      {
        channel: 'lark', region: 'global', tenant_id: 'tenant-lark', bot_id: 'app-lark',
        verified_at: '2026-07-20T00:00:00.000Z',
        verification_source: 'authenticated_event', provider_instance_id: 'lark-owner-one',
      },
    ],
  };
}

describe('exact-base durable source fencing', () => {
  test('fails before fencing when a routed channel lacks authenticated authority', () => {
    const database = exactBaseDatabase();
    database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content, status)
      VALUES ('in', 'feishu', 'chat-no-authority|type:p2p|msg:message-one', 'pending', 'pending')
    `).run();
    expect(() => extractAndFenceLegacyBaseBatch({
      database,
      batchId: 'upgrade-missing-authority',
      provider: 'claude',
      channelAuthority: {
        schema_version: 1, contract: 'zylos.channel-authority', scopes: [],
      },
      observedAt: '2026-07-21T00:00:00.000Z',
    })).toThrow('no unique authenticated authority scope');
    expect(() => database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content)
      VALUES ('in', 'feishu', 'still-open', 'not fenced')
    `).run()).not.toThrow();
    database.close();
  });

  test('atomically captures pending/running/control state and rejects late ingress', () => {
    const database = exactBaseDatabase();
    database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content, status)
      VALUES ('in', 'feishu', 'chat-one|type:group|root:root-one|parent:parent-one|msg:message-one|thread:thread-one', 'pending message', 'pending'),
             ('in', 'lark', 'chat-two|type:group|root:root-two|parent:parent-two|msg:message-two|thread:thread-two', 'running message', 'running'),
             ('out', 'feishu', 'chat-one|type:group|root:root-one|msg:message-out|thread:thread-one', 'delivered reply', 'delivered'),
             ('in', 'feishu', 'chat-incomplete|type:group|root:root-incomplete|msg:message-incomplete', 'ambiguous pending', 'pending')
    `).run();
    database.prepare(`
      INSERT INTO control_queue (raw_content, content, status, created_at, updated_at)
      VALUES ('/exit', '/exit', 'pending', 1, 1)
    `).run();
    const batch = extractAndFenceLegacyBaseBatch({
      database,
      batchId: 'upgrade-exact-base-fixture',
      provider: 'codex',
      channelAuthority: channelAuthority(),
      observedAt: '2026-07-21T00:00:00.000Z',
    });
    expect(batch.records).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'conversation:1', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1,
        envelope: expect.objectContaining({
          chat_type: 'thread', chat_id: 'chat-one',
          native_thread_or_topic_id: 'thread-one', message_id: 'message-one',
          reply: {
            root_message_id: 'root-one', parent_message_id: 'parent-one',
            reply_to_message_id: 'parent-one',
          },
        }),
      }),
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'conversation:2', legacy_state: 'running',
        notification_target: {
          region: 'global', tenant_id: 'tenant-lark', channel: 'lark',
          bot_id: 'app-lark', chat_type: 'thread', chat_id: 'chat-two',
          native_thread_or_topic_id: 'thread-two',
          native_thread_root_message_id: 'root-two',
          native_thread_reply_target_message_id: 'message-two',
        },
      }),
      expect.objectContaining({
        kind: 'runtime_control', legacy_record_id: 'control:1', legacy_state: 'pending',
      }),
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'conversation:4', legacy_state: 'pending',
        route: 'ambiguous',
      }),
    ]));
    const pending = batch.records.find(({ legacy_record_id: id }) => id === 'conversation:1');
    expect(validateInboundEnvelope(pending.envelope).forwarded).toEqual(pending.envelope);
    expect(pending.envelope).toMatchObject({
      region: 'cn', tenant_id: 'tenant-feishu', bot_id: 'app-feishu', channel: 'feishu',
    });
    expect(batch.records.some(({ legacy_record_id: id }) => id.startsWith('provider-session:')))
      .toBe(false);
    expect(() => database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content) VALUES ('in','lark','late','late')
    `).run()).toThrow('legacy source fenced');

    reconcileLegacyBaseRollback({
      database, rollbackBatch: batch,
    });
    expect(database.prepare('SELECT id, status FROM conversations ORDER BY id').all()).toEqual([
      { id: 1, status: 'pending' },
      { id: 2, status: 'failed' },
      { id: 3, status: 'delivered' },
      { id: 4, status: 'pending' },
    ]);
    expect(database.prepare('SELECT status FROM control_queue WHERE id = 1').get().status).toBe('failed');
    expect(() => database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content) VALUES ('in','lark','after','after')
    `).run()).not.toThrow();
    database.close();
  });

  test('keeps exact-base data fenced and old services inert through rollback and commit', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'zylos-exact-base-commit-'));
    roots.push(root);
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    writeRelease(fromRelease, { legacy: true });
    writeRelease(targetRelease);
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, 'pm2'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts'), { recursive: true });
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    database.exec(`
      CREATE TABLE conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        direction TEXT NOT NULL, channel TEXT NOT NULL, endpoint_id TEXT, content TEXT NOT NULL,
        status TEXT DEFAULT 'pending', delivery_action TEXT, priority INTEGER DEFAULT 3,
        require_idle INTEGER DEFAULT 0, retry_count INTEGER DEFAULT 0
      );
      CREATE TABLE control_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT, raw_content TEXT NOT NULL, content TEXT NOT NULL,
        priority INTEGER DEFAULT 3, require_idle INTEGER DEFAULT 0, bypass_state INTEGER DEFAULT 0,
        ack_deadline_at INTEGER, status TEXT DEFAULT 'pending', retry_count INTEGER DEFAULT 0,
        available_at INTEGER, last_error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
      );
      INSERT INTO conversations (direction, channel, endpoint_id, content, status)
      VALUES ('in', 'feishu', 'chat-running|type:p2p|msg:message-running', 'uncertain running work', 'running'),
             ('in', 'feishu', 'chat-commit|type:p2p|msg:message-commit', 'pending commit work', 'pending');
    `);
    const expectedScripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const processes = new Map([['activity-monitor', 'stopped'], ['c4-dispatcher', 'stopped']]);
    const calls = [];
    const fixtureExec = (file, args, options) => {
      calls.push([file, args]);
      if (file === 'pm2') {
        if (args[0] === 'jlist') return JSON.stringify([...processes].map(([name, status]) => ({
          name, pm2_env: { status, pm_exec_path: expectedScripts.get(name) },
        })));
        if (args[0] === 'delete') processes.delete(args[1]);
        return '';
      }
      if (file === 'npm' || (file === process.execPath && args[0] === '-e')
        || (file === process.execPath && args[0]?.endsWith('/scripts/postinstall.js'))) return '';
      return execFileSync(file, args, options);
    };
    const packageCalls = [];
    let healthShouldPass = false;
    let deliveryEnabled = false;
    let stopDeliveryOwner = false;
    let deliveryOwnerError = null;
    const deliveredNotices = [];
    const deliveryService = createOutboxService({
      database,
      renderer: {
        async deliver(command) {
          deliveredNotices.push(command);
          return deliveredResult(command, '2026-07-23T00:00:00.000Z');
        },
      },
      serviceInstanceId: 'exact-base-channel-delivery-owner',
      now: () => '2026-07-23T00:00:00.000Z',
      leaseDurationMs: 100,
      throttleMs: 0,
    });
    const deliveryOwner = (async () => {
      while (!stopDeliveryOwner) {
        if (deliveryEnabled) await deliveryService.dispatchNext();
        await new Promise((resolve) => setTimeout(resolve, 2));
      }
    })().catch((error) => { deliveryOwnerError = error; });
    try {
      expect(() => createInstalledExecutorUpgradeHandler({
        database, Database, zylosDir,
        currentReleasePath: fromRelease,
        currentReleaseRef: 'branch:exact-base-bootstrap',
        provider: 'claude', allowLegacyFromRelease: true,
        legacyChannelAuthority: {
          document: channelAuthority(),
          provider_binding: 'owner_only_exact_path_authenticated_event',
        },
      })).toThrow('requires verified channel prerequisite authority');
      const authorityFile = path.join(zylosDir, 'runtime', 'channel-authority.json');
      fs.mkdirSync(path.dirname(authorityFile), { recursive: true });
      fs.writeFileSync(authorityFile, JSON.stringify(channelAuthority()), { mode: 0o600 });
      const certifiedAuthority = readChannelAuthorityManifest(authorityFile, {
        expectedPath: authorityFile,
      });
      const handler = createInstalledExecutorUpgradeHandler({
        database, Database, zylosDir,
        currentReleasePath: fromRelease,
        currentReleaseRef: 'branch:exact-base-bootstrap',
        provider: 'claude', allowLegacyFromRelease: true,
        legacyChannelAuthority: certifiedAuthority,
        execFileSyncFn: fixtureExec,
        noticeDeliveryTimeoutMs: 50,
        packageLifecycle: {
          async activate() { packageCalls.push('activate'); return { installed: true }; },
          async restore() { packageCalls.push('restore'); return { restored: true }; },
        },
        startTargetHealth: async ({ request }) => {
          if (!healthShouldPass) {
            return {
              proof: {
                service_instance_id: 'invalid-target', snapshot_version: 1,
                health: 'unhealthy', reconciliation: 'incomplete',
              },
              close: async () => {},
            };
          }
          const service = createExecutorService({
            database,
            adapter: {
              provider: 'claude', provider_transport: 'claude_agent_sdk',
              async *execute() {}, async close() { return []; },
            },
            provider: 'claude', serviceInstanceId: 'target-exact-base-commit',
            hostId: 'target-exact-base-commit', workspaceRoot: zylosDir,
            releaseRef: request.release_ref, upgradeId: request.upgrade_id,
            serviceStartedAt: '2026-07-22T00:00:00.000Z',
            now: () => '2026-07-22T00:00:00.000Z',
          });
          service.start();
          const snapshot = service.publishObservabilitySnapshot();
          return {
            proof: {
              service_instance_id: snapshot.core_service_instance_id,
              snapshot_version: snapshot.snapshot_version,
              health: snapshot.service.health, reconciliation: 'complete',
            },
            close: () => service.close(),
          };
        },
        now: (() => {
          let tick = 0;
          const origin = Date.parse('2026-07-21T00:00:00.000Z');
          return () => new Date(origin + (tick++ * 1_000)).toISOString();
        })(),
      });
      const rollback = await handler({
        action: 'upgrade',
        target: {
          release: 'branch:executor-lifecycle', branch: 'executor-lifecycle',
          downloaded_source: targetRelease,
        },
      });
      expect(rollback.error).toContain('not durably delivered');
      expect(rollback.rollback?.error).toContain('not durably delivered');
      expect(rollback).toMatchObject({ success: false, state: 'rollback_failed' });
      expect(deliveredNotices).toHaveLength(0);
      expect(packageCalls).toEqual(['activate']);
      expect(processes.size).toBe(0);

      deliveryEnabled = true;
      const recoveredRollback = await handler.resumeBlocking();
      expect(database.prepare('SELECT status FROM conversations WHERE id = 1').get().status)
        .toBe('failed');
      expect(database.prepare('SELECT status FROM conversations WHERE id = 2').get().status)
        .toBe('pending');
      expect(deliveryOwnerError).toBeNull();
      expect(database.prepare(`
        SELECT status FROM runtime_outbox ORDER BY created_at, outbox_id
      `).all()).toEqual(expect.arrayContaining([{ status: 'delivered' }]));
      expect(recoveredRollback.rollback?.error ?? null).toBeNull();
      expect(recoveredRollback).toMatchObject({ success: false, state: 'rolled_back' });
      expect(packageCalls).toEqual(['activate', 'restore']);
      expect(processes).toEqual(new Map());
      expect(database.prepare('SELECT status FROM conversations WHERE id = 1').get().status)
        .toBe('failed');
      expect(database.prepare('SELECT status FROM conversations WHERE id = 2').get().status)
        .toBe('pending');
      database.prepare(`
        INSERT INTO conversations (direction, channel, endpoint_id, content, status)
        VALUES ('in', 'feishu', 'chat-late|type:p2p|msg:message-late', 'late rollback-safe work', 'pending')
      `).run();

      healthShouldPass = true;
      const result = await handler({
        action: 'upgrade',
        target: {
          release: 'branch:executor-lifecycle', branch: 'executor-lifecycle',
          downloaded_source: targetRelease,
        },
      });
      expect(result.error ?? null).toBeNull();
      expect(result).toMatchObject({ success: true, state: 'committed' });
      expect(packageCalls).toEqual(['activate', 'restore', 'activate']);
      expect(deliveredNotices).not.toHaveLength(0);
      expect(deliveredNotices).toEqual(expect.arrayContaining([
        expect.objectContaining({
          target: expect.objectContaining({
            region: 'cn', tenant_id: 'tenant-feishu', bot_id: 'app-feishu',
            chat_type: 'dm', chat_id: 'chat-running',
            native_thread_root_message_id: null,
            native_thread_reply_target_message_id: null,
          }),
        }),
      ]));
      expect(processes.size).toBe(0);
      const active = JSON.parse(fs.readFileSync(
        path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
      ));
      expect(fs.existsSync(path.join(active.release_path, 'retired-runtime.js'))).toBe(false);
      expect(database.prepare(`
        SELECT disposition FROM runtime_legacy_migration_records
        WHERE legacy_kind = 'c4' AND legacy_record_id = 'conversation:2'
      `).all()).toContainEqual({ disposition: 'migrated_pending' });
      expect(database.prepare(`
        SELECT disposition FROM runtime_legacy_migration_records
        WHERE legacy_kind = 'c4' AND legacy_record_id = 'conversation:3'
      `).get()).toEqual({ disposition: 'migrated_pending' });
    } finally {
      stopDeliveryOwner = true;
      await deliveryOwner;
      database.close();
    }
  }, 15_000);
});
