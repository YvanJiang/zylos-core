import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  extractAndFenceLegacyBaseBatch,
  reconcileLegacyBaseRollback,
} from '../runtime/migration/legacy-base-source.js';
import { createLegacyProviderQuiescence } from '../runtime/migration/legacy-provider-quiescence.js';
import { createInstalledExecutorUpgradeHandler } from '../runtime/migration/installed-executor-upgrade.js';
import { createExecutorService } from '../runtime/executor/service.js';

const roots = [];
const tmuxServers = new Map();

afterEach(() => {
  for (const [server, serverPid] of tmuxServers) {
    try { process.kill(serverPid, 'SIGCONT'); } catch (error) {
      if (error?.code !== 'ESRCH') throw error;
    }
    try { execFileSync('tmux', ['-L', server, 'kill-server'], { stdio: 'ignore' }); } catch {}
  }
  tmuxServers.clear();
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
  fs.writeFileSync(path.join(root, 'retired-tmux-runtime.js'), 'must not enter active release\n');
  fs.writeFileSync(path.join(root, '.npmignore'), 'retired-tmux-runtime.js\n');
}

describe('exact-base durable source fencing', () => {
  test('atomically captures pending/running/control state and rejects late ingress', () => {
    const database = exactBaseDatabase();
    database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content, status)
      VALUES ('in', 'feishu', 'chat-one', 'pending message', 'pending'),
             ('in', 'lark', 'chat-two', 'running message', 'running'),
             ('out', 'feishu', 'chat-one', 'delivered reply', 'delivered')
    `).run();
    database.prepare(`
      INSERT INTO control_queue (raw_content, content, status, created_at, updated_at)
      VALUES ('/exit', '/exit', 'pending', 1, 1)
    `).run();
    const batch = extractAndFenceLegacyBaseBatch({
      database,
      batchId: 'upgrade-exact-base-fixture',
      provider: 'codex',
      providerActive: true,
      observedAt: '2026-07-21T00:00:00.000Z',
    });
    expect(batch.records).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'conversation:1', legacy_state: 'pending',
        route: 'unique', legacy_queue_sequence: 1,
      }),
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'conversation:2', legacy_state: 'running',
      }),
      expect.objectContaining({
        kind: 'runtime_control', legacy_record_id: 'control:1', legacy_state: 'pending',
      }),
      expect.objectContaining({
        kind: 'c4', legacy_record_id: 'provider-session:codex', legacy_state: 'running',
      }),
    ]));
    expect(() => database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content) VALUES ('in','lark','late','late')
    `).run()).toThrow('legacy source fenced');

    reconcileLegacyBaseRollback({
      database,
      rollbackBatch: { batch_id: batch.batch_id, records: [batch.records[0]] },
    });
    expect(database.prepare('SELECT id, status FROM conversations ORDER BY id').all()).toEqual([
      { id: 1, status: 'pending' },
      { id: 2, status: 'failed' },
      { id: 3, status: 'delivered' },
    ]);
    expect(database.prepare('SELECT status FROM control_queue WHERE id = 1').get().status).toBe('failed');
    expect(() => database.prepare(`
      INSERT INTO conversations (direction, channel, endpoint_id, content) VALUES ('in','lark','after','after')
    `).run()).not.toThrow();
    database.close();
  });

  test('suspends and resumes the exact disposable provider process, then removes it only on commit', () => {
    const server = `zylos-issue27-${process.pid}-${Date.now()}`;
    execFileSync('tmux', [
      '-L', server, '-f', '/dev/null', 'new-session', '-d', '-s', 'claude-main',
      'while :; do sleep 1; done',
    ]);
    tmuxServers.set(server, Number(execFileSync(
      'tmux', ['-L', server, 'display-message', '-p', '#{pid}'], { encoding: 'utf8' },
    ).trim()));
    const quiescence = createLegacyProviderQuiescence({
      provider: 'claude', execFileSyncFn: execFileSync, tmuxArgsPrefix: ['-L', server],
    });
    const suspended = quiescence.suspend();
    expect(suspended).toMatchObject({ active: true, suspended: true, session: 'claude-main' });
    const state = execFileSync('ps', ['-o', 'state=', '-p', String(suspended.pane_pid)], {
      encoding: 'utf8',
    }).trim();
    expect(state).toMatch(/^T/);
    expect(quiescence.resume(suspended)).toMatchObject({ resumed: true });
    expect(execFileSync('ps', ['-o', 'state=', '-p', String(suspended.pane_pid)], {
      encoding: 'utf8',
    }).trim()).not.toMatch(/^T/);
    const suspendedAgain = quiescence.suspend();
    expect(quiescence.commit(suspendedAgain)).toMatchObject({ removed: true });
    expect(quiescence.inspect()).toMatchObject({ active: false });
  });

  test('commits an exact-base SQLite, PM2, and disposable provider fixture without old/new overlap', async () => {
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
      VALUES ('in', 'feishu', 'chat-commit', 'pending commit work', 'pending');
    `);
    const server = `zylos-issue27-commit-${process.pid}-${Date.now()}`;
    execFileSync('tmux', [
      '-L', server, '-f', '/dev/null', 'new-session', '-d', '-s', 'claude-main',
      'while :; do sleep 1; done',
    ]);
    tmuxServers.set(server, Number(execFileSync(
      'tmux', ['-L', server, 'display-message', '-p', '#{pid}'], { encoding: 'utf8' },
    ).trim()));
    const expectedScripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const processes = new Map([['activity-monitor', 'online'], ['c4-dispatcher', 'online']]);
    const calls = [];
    const fixtureExec = (file, args, options) => {
      calls.push([file, args]);
      if (file === 'pm2') {
        if (args[0] === 'jlist') return JSON.stringify([...processes].map(([name, status]) => ({
          name, pm2_env: { status, pm_exec_path: expectedScripts.get(name) },
        })));
        if (args[0] === 'delete') processes.delete(args[1]);
        if (args[0] === 'start') processes.set(args[args.indexOf('--only') + 1], 'online');
        if (args[0] === 'stop') processes.set(args[1], 'stopped');
        return '';
      }
      if (file === 'npm' || (file === process.execPath && args[0] === '-e')
        || (file === process.execPath && args[0]?.endsWith('/scripts/postinstall.js'))) return '';
      return execFileSync(file, args, options);
    };
    const packageCalls = [];
    let healthShouldPass = false;
    const deliveryTimer = setInterval(() => {
      for (const row of database.prepare(`
        SELECT outbox_id, delivery_id FROM runtime_outbox WHERE status = 'pending'
      `).all()) {
        const proof = {
          status: 'delivered', delivery_id: row.delivery_id,
          delivered_at: '2026-07-21T00:00:20.000Z',
        };
        database.prepare(`
          UPDATE runtime_outbox SET status = 'delivered', result_json = ?, updated_at = ?
          WHERE outbox_id = ?
        `).run(JSON.stringify(proof), proof.delivered_at, row.outbox_id);
      }
    }, 5);
    try {
      const handler = createInstalledExecutorUpgradeHandler({
        database, Database, zylosDir,
        currentReleasePath: fromRelease,
        currentReleaseRef: 'branch:exact-base-bootstrap',
        provider: 'claude', allowLegacyFromRelease: true,
        execFileSyncFn: fixtureExec,
        legacyProviderQuiescence: createLegacyProviderQuiescence({
          provider: 'claude', execFileSyncFn: fixtureExec, tmuxArgsPrefix: ['-L', server],
        }),
        noticeDeliveryTimeoutMs: 1_000,
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
      expect(rollback).toMatchObject({ success: false, state: 'rolled_back' });
      expect(packageCalls).toEqual(['activate', 'restore']);
      expect(processes).toEqual(new Map([
        ['activity-monitor', 'online'], ['c4-dispatcher', 'online'],
      ]));
      expect(() => execFileSync(
        'tmux', ['-L', server, 'has-session', '-t', '=claude-main'], { stdio: 'ignore' },
      )).not.toThrow();
      expect(database.prepare('SELECT status FROM conversations WHERE id = 1').get().status)
        .toBe('pending');
      database.prepare(`
        INSERT INTO conversations (direction, channel, endpoint_id, content, status)
        VALUES ('in', 'feishu', 'chat-late', 'late rollback-safe work', 'pending')
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
      expect(processes.size).toBe(0);
      expect(() => execFileSync(
        'tmux', ['-L', server, 'has-session', '-t', '=claude-main'], { stdio: 'ignore' },
      ))
        .toThrow();
      const active = JSON.parse(fs.readFileSync(
        path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
      ));
      expect(fs.existsSync(path.join(active.release_path, 'retired-tmux-runtime.js'))).toBe(false);
      expect(database.prepare(`
        SELECT disposition FROM runtime_legacy_migration_records
        WHERE legacy_kind = 'c4' AND legacy_record_id = 'conversation:1'
      `).all()).toContainEqual({ disposition: 'migrated_pending' });
      expect(database.prepare(`
        SELECT disposition FROM runtime_legacy_migration_records
        WHERE legacy_kind = 'c4' AND legacy_record_id = 'conversation:2'
      `).get()).toEqual({ disposition: 'migrated_pending' });
    } finally {
      clearInterval(deliveryTimer);
      database.close();
    }
  }, 15_000);
});
