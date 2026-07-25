import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';

import { driveInstalledRuntimeUpgrade } from '../runtime/migration/executor-upgrade-driver.js';
import {
  createInstalledExecutorUpgradeHandler,
  reconcileLegacyServicesForExecutorStart,
  retireOwnedLegacyServices,
} from '../runtime/migration/installed-executor-upgrade.js';
import { legacyLifecycleArtifactPaths } from '../runtime/migration/legacy-lifecycle-artifacts.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { startExecutorService } from '../cli/lib/executor-service-lifecycle.js';
import { createRuntimeUpgradeService } from '../runtime/migration/runtime-upgrade-service.js';
import { createSqliteSnapshotAdapter } from '../runtime/migration/runtime-upgrade-coordinator.js';

const require = createRequire(new URL('../skills/comm-bridge/package.json', import.meta.url));
const Database = require('better-sqlite3');
const directories = [];

function processIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

async function waitForProcessExit(pid, timeoutMs = 1_000) {
  const deadline = Date.now() + timeoutMs;
  while (processIsAlive(pid) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function settleWithin(promise, timeoutMs, timeoutValue) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => { timer = setTimeout(() => resolve(timeoutValue), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

function preflight() {
  return {
    upgrade_id: 'upgrade-fixture',
    from_release: 'release-A',
    to_release: 'release-B',
    scope: { kind: 'installation', bot_id: null },
    checks: {
      sqlite_integrity: 'ok',
      codex_transport: 'official_app_server_only',
      delivery_contract: 'zylos.delivery-command@1.1',
      workspace_lease_fencing: 'intact',
      retention_cleanup: 'intact',
      normal_runtime_paths: 'new_only',
    },
  };
}

function writeActiveExecutorRelease(zylosDir, releasePath) {
  fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
  fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
    release_ref: 'release-A',
    release_path: releasePath,
    upgrade_id: 'upgrade-base-release',
  }));
}

describe('installed executor upgrade driver', () => {
  test('drives the canonical durable host through commit and postcommit cleanup', async () => {
    const calls = [];
    const states = [
      { state: 'preflight', completed_step: 'snapshot-capture' },
      { state: 'snapshotted' },
      { state: 'maintenance' },
      { state: 'drained' },
      { state: 'drained', completed_step: 'legacy-source-invalidate' },
      { state: 'drained', completed_step: 'release-activate' },
      { state: 'drained', completed_step: 'release-generation-fence' },
      { state: 'health_check' },
      { state: 'health_check', completed_step: 'legacy-source-seal' },
      { state: 'health_check', completed_step: 'executor-health' },
      { state: 'ready_to_commit' },
      { state: 'committed' },
      { state: 'committed', completed_step: 'legacy-source-commit' },
      { state: 'committed', completed_step: 'postcommit-cleanup' },
    ];
    const host = {
      async attach(value) { calls.push(['attach', value]); return { state: 'preflight' }; },
      async advance(id, input) { calls.push(['advance', id, input]); return states.shift(); },
      requestRollback: () => { throw new Error('unexpected rollback'); },
    };
    const legacyBatch = { batch_id: 'empty-upgrade-fixture', records: [] };

    await expect(driveInstalledRuntimeUpgrade({ host, preflight: preflight(), legacyBatch }))
      .resolves.toMatchObject({
        success: true,
        state: 'committed',
        completedStep: 'postcommit-cleanup',
        from: 'release-A',
        to: 'release-B',
      });
    expect(calls[0]).toEqual(['attach', preflight()]);
    expect(calls.slice(1).every((call) => call[1] === 'upgrade-fixture')).toBe(true);
    expect(calls.slice(1).every((call) => call[2]?.legacyBatch === legacyBatch)).toBe(true);
  });

  test('waits for a transient drain without spending the durable transition budget', async () => {
    let remainingWaits = 65;
    let rollbackRequests = 0;
    const host = {
      async attach() { return { state: 'preflight' }; },
      async advance() {
        if (remainingWaits > 0) {
          remainingWaits -= 1;
          return {
            state: 'maintenance',
            status: 'waiting',
            active_turn_ids: ['turn-active'],
            deadline_at: '2026-07-20T10:10:00.000Z',
          };
        }
        return { state: 'committed', completed_step: 'postcommit-cleanup' };
      },
      requestRollback() { rollbackRequests += 1; },
    };

    await expect(driveInstalledRuntimeUpgrade({
      host,
      preflight: preflight(),
      legacyBatch: { batch_id: 'empty-upgrade-fixture', records: [] },
      waitForPoll: async () => {},
    })).resolves.toMatchObject({ success: true, state: 'committed' });
    expect(remainingWaits).toBe(0);
    expect(rollbackRequests).toBe(0);
  });

  test('reports the durable rolled-back result instead of claiming upgrade success', async () => {
    const rollbackRequests = [];
    let invocation = 0;
    const host = {
      async attach() { return { state: 'preflight' }; },
      async advance() {
        invocation += 1;
        if (invocation === 1) throw new Error('activation exploded');
        return invocation === 2
          ? { state: 'rollback_required', completed_step: 'rollback-restore' }
          : { state: 'rolled_back' };
      },
      requestRollback(id, failure) {
        rollbackRequests.push([id, failure]);
        return { state: 'rollback_required' };
      },
    };

    await expect(driveInstalledRuntimeUpgrade({
      host,
      preflight: preflight(),
      legacyBatch: { batch_id: 'empty-upgrade-fixture', records: [] },
    })).resolves.toMatchObject({
      success: false,
      state: 'rolled_back',
      error: 'activation exploded',
      rollback: { performed: true },
    });
    expect(rollbackRequests).toHaveLength(1);
  });

  test('reports a postcommit cleanup failure without attempting an impossible rollback', async () => {
    const rollbackRequests = [];
    const states = [
      { state: 'committed' },
      new Error('obsolete artifact cleanup failed'),
    ];
    const host = {
      async attach() { return { state: 'ready_to_commit' }; },
      async advance() {
        const next = states.shift();
        if (next instanceof Error) throw next;
        return next;
      },
      requestRollback(...args) { rollbackRequests.push(args); },
    };

    await expect(driveInstalledRuntimeUpgrade({
      host,
      preflight: preflight(),
      legacyBatch: { batch_id: 'empty-upgrade-fixture', records: [] },
    })).resolves.toMatchObject({
      success: false,
      state: 'committed',
      committed: true,
      error: 'obsolete artifact cleanup failed',
      rollback: { performed: false, reason: 'already_committed' },
    });
    expect(rollbackRequests).toEqual([]);
  });
});

describe('installed executor production upgrade owner', () => {
  test('durably retires identity-verified running services before executor migration', () => {
    const zylosDir = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-pm2-running-'));
    directories.push(zylosDir);
    const auditFile = path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'upgrade-running-services.json');
    const scripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const processes = new Map([
      ['activity-monitor', 'online'],
      ['c4-dispatcher', 'online'],
    ]);
    const commands = [];
    const result = retireOwnedLegacyServices({
      zylosDir,
      upgradeId: 'upgrade-running',
      stepId: 'upgrade-running:legacy-source-invalidate',
      stateFile: auditFile,
      now: () => '2026-07-21T00:00:00.000Z',
      execFileSyncFn: (file, args) => {
        commands.push([file, args]);
        if (args[0] === 'jlist') {
          return JSON.stringify([...processes].map(([name, status]) => ({
            name, pm2_env: { status, pm_exec_path: scripts.get(name) },
          })));
        }
        if (args[0] === 'stop') {
          const state = JSON.parse(fs.readFileSync(auditFile, 'utf8'));
          expect(state).toMatchObject({
            upgrade_id: 'upgrade-running', step_id: 'upgrade-running:legacy-source-invalidate',
            phase: 'services_quiescing',
          });
          expect(state.services).toEqual(expect.arrayContaining([
            expect.objectContaining({ name: args[1], observed_stop_command: ['pm2', 'stop', args[1]] }),
          ]));
          processes.set(args[1], 'stopped');
        }
        if (args[0] === 'delete') processes.delete(args[1]);
        return '';
      },
    });

    expect(result).toMatchObject({ stopped: true, deleted: true, pm2_saved: true });
    expect(commands).toContainEqual(['pm2', ['stop', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['stop', 'c4-dispatcher']]);
    expect(commands).toContainEqual(['pm2', ['delete', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['delete', 'c4-dispatcher']]);
    expect(commands).toContainEqual(['pm2', ['save']]);
    expect(processes).toEqual(new Map());
  });

  test('fails closed on identity drift and treats an unproven stop as a recovery barrier', () => {
    const zylosDir = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-pm2-unknown-'));
    directories.push(zylosDir);
    const auditFile = path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'upgrade-unknown-services.json');
    const expected = path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js');
    let inspection = 0;
    const driftCommands = [];
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-unknown', stepId: 'upgrade-unknown:legacy-source-invalidate',
      stateFile: auditFile,
      execFileSyncFn: (file, args) => {
        driftCommands.push([file, args]);
        if (args[0] !== 'jlist') return '';
        inspection += 1;
        return JSON.stringify([{
          name: 'activity-monitor',
          pm2_env: {
            status: 'online',
            pm_exec_path: inspection === 1 ? expected : '/opt/user/activity-monitor.js',
          },
        }]);
      },
    })).toThrow('ambiguous PM2 service name collisions: activity-monitor');
    expect(driftCommands).toEqual([['pm2', ['jlist']], ['pm2', ['jlist']]]);

    let status = 'online';
    const unknownCommands = [];
    const unknownExec = (file, args) => {
      unknownCommands.push([file, args]);
      if (args[0] === 'jlist') return JSON.stringify([{
        name: 'activity-monitor', pm2_env: { status, pm_exec_path: expected },
      }]);
      if (args[0] === 'stop') throw new Error('PM2 stop timed out');
      return '';
    };
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-unknown-stop', stepId: 'upgrade-unknown-stop:legacy-source-invalidate',
      stateFile: path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'unknown-stop.json'),
      execFileSyncFn: unknownExec,
    })).toThrow('PM2 stop timed out');
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-unknown-stop', stepId: 'upgrade-unknown-stop:legacy-source-invalidate',
      stateFile: path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'unknown-stop.json'),
      execFileSyncFn: unknownExec,
    })).toThrow('Unknown PM2 stop outcome requires authorized disposition');
    expect(unknownCommands.filter(([, args]) => args[0] === 'stop')).toHaveLength(1);
  });

  test('does not replay partial stop or delete effects without durable PM2 proof', () => {
    const zylosDir = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-pm2-partial-'));
    directories.push(zylosDir);
    const scripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const processes = new Map([['activity-monitor', 'online'], ['c4-dispatcher', 'online']]);
    const partialFile = path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'partial.json');
    const stopCommands = [];
    const partialExec = (file, args) => {
      if (args[0] === 'jlist') return JSON.stringify([...processes].map(([name, status]) => ({
        name, pm2_env: { status, pm_exec_path: scripts.get(name) },
      })));
      if (args[0] === 'stop') {
        stopCommands.push(args[1]);
        if (args[1] === 'activity-monitor') processes.set(args[1], 'stopped');
        else throw new Error('PM2 stop timeout after partial retirement');
      }
      return '';
    };
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-partial', stepId: 'upgrade-partial:legacy-source-invalidate',
      stateFile: partialFile, execFileSyncFn: partialExec,
    })).toThrow('PM2 stop timeout after partial retirement');
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-partial', stepId: 'upgrade-partial:legacy-source-invalidate',
      stateFile: partialFile, execFileSyncFn: partialExec,
    })).toThrow('Unknown PM2 stop outcome requires authorized disposition: c4-dispatcher');
    expect(stopCommands).toEqual(['activity-monitor', 'c4-dispatcher']);

    const deleteFile = path.join(zylosDir, 'runtime', 'legacy-upgrade-audit', 'delete-proof.json');
    processes.clear();
    processes.set('activity-monitor', 'stopped');
    let deleteAttempts = 0;
    const deleteExec = (file, args) => {
      if (args[0] === 'jlist') return JSON.stringify([...processes].map(([name, status]) => ({
        name, pm2_env: { status, pm_exec_path: scripts.get(name) },
      })));
      if (args[0] === 'delete') {
        deleteAttempts += 1;
        processes.delete(args[1]);
        throw new Error('PM2 delete timed out after registration removal');
      }
      return '';
    };
    expect(() => retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-delete-proof', stepId: 'upgrade-delete-proof:legacy-source-invalidate',
      stateFile: deleteFile, execFileSyncFn: deleteExec,
    })).toThrow('PM2 delete timed out after registration removal');
    expect(retireOwnedLegacyServices({
      zylosDir, upgradeId: 'upgrade-delete-proof', stepId: 'upgrade-delete-proof:legacy-source-invalidate',
      stateFile: deleteFile, execFileSyncFn: deleteExec,
    })).toMatchObject({ stopped: true, deleted: true, pm2_saved: true });
    expect(deleteAttempts).toBe(1);
  });

  test('postcommit reconciliation refuses reappeared registrations instead of deleting them', () => {
    const zylosDir = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-init-legacy-'));
    directories.push(zylosDir);
    const scriptPath = path.join(
      zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js',
    );
    for (const status of ['stopped', 'online']) {
      const commands = [];
      const execFileSyncFn = (file, args) => {
        commands.push([file, args]);
        if (args[0] === 'jlist') {
          return JSON.stringify([{
            name: 'c4-dispatcher', pm2_env: { status, pm_exec_path: scriptPath },
          }]);
        }
        return '';
      };
      if (status === 'stopped') {
        expect(() => reconcileLegacyServicesForExecutorStart({
          zylosDir, upgradeId: 'upgrade-fence-fixture', execFileSyncFn,
        })).toThrow('registrations reappeared after verified retirement');
        expect(fs.existsSync(path.join(zylosDir, 'runtime', 'executor-start-fence.json'))).toBe(false);
        expect(commands).toEqual([['pm2', ['jlist']], ['pm2', ['jlist']]]);
      } else {
        expect(() => reconcileLegacyServicesForExecutorStart({
          zylosDir, upgradeId: 'upgrade-fence-fixture', execFileSyncFn,
        }))
          .toThrow('still active');
        expect(commands).toEqual([['pm2', ['jlist']]]);
      }
    }
  });

  test('commits a prepared release through Global26 and removes obsolete artifacts only afterward', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-installed-upgrade-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const downloadedSource = path.join(directory, 'downloaded-B');
    const zylosDir = path.join(directory, 'installation');
    for (const release of [currentRelease, downloadedSource]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.mkdirSync(path.join(release, 'scripts'), { recursive: true });
      fs.mkdirSync(path.join(release, 'templates', 'pm2'), { recursive: true });
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({
        name: 'zylos', version: release === currentRelease ? 'release-A' : 'release-B',
      }));
      if (release === downloadedSource) {
        fs.writeFileSync(path.join(release, 'package-lock.json'), JSON.stringify({
          name: 'zylos', version: 'release-B', lockfileVersion: 3, requires: true,
          packages: { '': { name: 'zylos', version: 'release-B' } },
        }));
      }
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'health-probe.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'scripts', 'postinstall.js'), 'export {};\n');
      fs.writeFileSync(
        path.join(release, 'templates', 'pm2', 'ecosystem.config.cjs'),
        release === currentRelease
          ? 'module.exports = { apps: [{ name: "legacy-runtime" }] };\n'
          : 'module.exports = { apps: [{ name: "zylos-executor" }] };\n',
      );
    }
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, 'pm2'), { recursive: true });
    fs.writeFileSync(
      path.join(zylosDir, 'pm2', 'ecosystem.config.cjs'),
      'module.exports = { apps: [{ name: "legacy-runtime" }] };\n',
    );
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const obsoleteArtifacts = legacyLifecycleArtifactPaths(zylosDir);
    for (const artifact of obsoleteArtifacts) {
      if (artifact.endsWith('activity-monitor')) {
        fs.mkdirSync(artifact, { recursive: true });
        fs.writeFileSync(path.join(artifact, 'legacy-service.js'), 'obsolete');
      } else {
        fs.mkdirSync(path.dirname(artifact), { recursive: true });
        fs.writeFileSync(artifact, 'obsolete');
      }
    }
    writeActiveExecutorRelease(zylosDir, currentRelease);
    const codexHooks = path.join(zylosDir, '.codex', 'hooks.json');
    fs.mkdirSync(path.dirname(codexHooks), { recursive: true });
    fs.writeFileSync(codexHooks, JSON.stringify({
      hooks: {
        SessionStart: [{ hooks: [
          {
            type: 'command',
            command: `node ${path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-session-init.js')}`,
          },
          { type: 'command', command: 'node retained-hook.js' },
        ] }],
      },
    }));
    const claudeSettings = path.join(zylosDir, '.claude', 'settings.json');
    fs.writeFileSync(claudeSettings, JSON.stringify({
      hooks: {
        PreToolUse: [{ hooks: [
          {
            type: 'command',
            command: `node ${path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'hook-activity.js')}`,
          },
          { type: 'command', command: 'node retained-claude-hook.js' },
        ] }],
      },
    }));
    const commands = [];
    const commandStates = [];
    const commandDirectories = [];
    const commandOptions = [];
    const legacyScripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const legacyProcesses = new Map([
      ['activity-monitor', 'online'],
      ['c4-dispatcher', 'online'],
    ]);
    let preActionOwnershipRecord = null;
    let failStrictPostinstallOnce = true;
    const handler = createInstalledExecutorUpgradeHandler({
      database,
      Database,
      zylosDir,
      currentReleasePath: currentRelease,
      currentReleaseRef: 'release-A',
      provider: 'codex',
      startTargetHealth: async ({ request }) => {
        const service = createExecutorService({
          database,
          adapter: {
            provider: 'codex', provider_transport: 'official_app_server',
            async *execute() {}, async close() { return []; },
          },
          provider: 'codex',
          serviceInstanceId: 'target-health-fixture',
          hostId: 'target-health-fixture',
          workspaceRoot: zylosDir,
          releaseRef: request.release_ref,
          upgradeId: request.upgrade_id,
          serviceStartedAt: '2026-07-20T12:05:30.000Z',
          now: () => '2026-07-20T12:05:30.000Z',
        });
        service.start();
        const snapshot = service.publishObservabilitySnapshot();
        return {
          proof: {
            service_instance_id: snapshot.core_service_instance_id,
            snapshot_version: snapshot.snapshot_version,
            health: snapshot.service.health,
            reconciliation: 'complete',
          },
          close: () => service.close(),
        };
      },
      execFileSyncFn: (file, args, options = {}) => {
        commands.push([file, args]);
        commandDirectories.push(options.cwd ?? null);
        commandOptions.push(options);
        if (file === process.execPath && args[0]?.endsWith('/scripts/postinstall.js')) {
          commandStates.push(database.prepare(
            'SELECT state FROM runtime_upgrade_runs ORDER BY created_at DESC LIMIT 1',
          ).get()?.state ?? null);
          if (failStrictPostinstallOnce) {
            failStrictPostinstallOnce = false;
            throw new Error('strict postinstall fixture failed');
          }
        }
        if (file === 'pm2' && args[0] === 'jlist') {
          return JSON.stringify([...legacyProcesses].map(([name, status]) => ({
            name,
            pm2_env: { status, pm_exec_path: legacyScripts.get(name) },
          })));
        }
        if (file === 'pm2' && args[0] === 'stop') {
          const auditDirectory = path.join(zylosDir, 'runtime', 'legacy-upgrade-audit');
          const auditEntry = fs.readdirSync(auditDirectory).find((entry) => entry.endsWith('-services.json'));
          preActionOwnershipRecord ??= JSON.parse(fs.readFileSync(path.join(auditDirectory, auditEntry), 'utf8'));
          legacyProcesses.set(args[1], 'stopped');
        }
        if (file === 'pm2' && args[0] === 'delete') legacyProcesses.delete(args[1]);
        return '';
      },
      now: (() => {
        let tick = 0;
        return () => `2026-07-20T12:00:${String(tick++).padStart(2, '0')}.000Z`;
      })(),
    });

    const firstResult = await handler({
      action: 'upgrade',
      target: { release: 'release-B', downloaded_source: downloadedSource },
    });
    expect(firstResult).toMatchObject({
      success: false,
      committed: true,
      state: 'committed',
      error: 'strict postinstall fixture failed',
    });
    expect(fs.existsSync(path.join(zylosDir, 'runtime', 'executor-start-fence.json'))).toBe(false);
    expect(fs.readdirSync(path.join(zylosDir, 'runtime', 'upgrade-plans'))).toHaveLength(1);

    const result = await handler.resumeBlocking();

    expect(result).toMatchObject({ success: true, state: 'committed', to: 'release-B' });
    expect(preActionOwnershipRecord).toMatchObject({
      schema_version: 2,
      phase: 'services_quiescing',
      services: expect.arrayContaining([
        expect.objectContaining({
          name: 'activity-monitor',
          observed_was_running: true,
          observed_script_path: legacyScripts.get('activity-monitor'),
        }),
        expect.objectContaining({
          name: 'c4-dispatcher',
          observed_was_running: true,
          observed_script_path: legacyScripts.get('c4-dispatcher'),
        }),
      ]),
    });
    expect(legacyProcesses).toEqual(new Map());
    expect(commands).toContainEqual(['pm2', ['stop', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['stop', 'c4-dispatcher']]);
    expect(commands).toContainEqual(['pm2', ['delete', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['delete', 'c4-dispatcher']]);
    expect(commands).toContainEqual(['pm2', ['save']]);
    expect(commands.filter(([file, args]) => file === 'npm' && args[0] === 'ci'))
      .toHaveLength(7);
    expect(commands).toContainEqual(['npm', ['ci', '--omit=dev', '--no-audit', '--no-fund']]);
    expect(commands).toContainEqual([
      process.execPath,
      [expect.stringMatching(/scripts\/postinstall\.js$/)],
    ]);
    expect(commandStates).toEqual(['committed', 'committed']);
    const precommitDependencyDirectories = commands
      .map((command, index) => ({ command, cwd: commandDirectories[index] }))
      .filter(({ command: [file, args], cwd }) => (
        file === 'npm' && args[0] === 'ci' && cwd?.startsWith(path.join(zylosDir, 'runtime', 'releases'))
      ))
      .map(({ cwd }) => cwd);
    expect(precommitDependencyDirectories).toHaveLength(4);
    expect(precommitDependencyDirectories.some((cwd) => cwd.endsWith('/skills/comm-bridge'))).toBe(true);
    const preparedReleaseRoot = precommitDependencyDirectories.find((cwd) => !cwd.includes('/skills/'));
    expect(JSON.parse(fs.readFileSync(
      path.join(preparedReleaseRoot, 'npm-shrinkwrap.json'), 'utf8',
    ))).toMatchObject({ name: 'zylos', version: 'release-B', lockfileVersion: 3 });
    expect(commands.filter(([file, args]) => file === 'npm' && args[0] === 'ci')
      .every(([, args]) => !args.includes('--ignore-scripts'))).toBe(true);
    expect(commands.filter(([file, args]) => file === process.execPath && args[0] === '-e'))
      .toHaveLength(6);
    const prepareRootIndex = commandDirectories.findIndex((cwd) => (
      cwd?.startsWith(path.join(zylosDir, 'runtime', 'releases'))
      && !cwd.includes('/skills/')
    ));
    expect(commandOptions[prepareRootIndex].env.ZYLOS_PACKAGE_PREPARE).toBe('1');
    const strictPostinstallIndex = commands.findIndex(([file, args]) => (
      file === process.execPath && args[0]?.endsWith('/scripts/postinstall.js')
    ));
    expect(commandOptions[strictPostinstallIndex].env.ZYLOS_POSTINSTALL_STRICT).toBe('1');
    expect(obsoleteArtifacts.every((artifact) => !fs.existsSync(artifact))).toBe(true);
    expect(JSON.parse(fs.readFileSync(codexHooks, 'utf8'))).toEqual({
      hooks: {
        SessionStart: [{ hooks: [{ type: 'command', command: 'node retained-hook.js' }] }],
      },
    });
    expect(JSON.parse(fs.readFileSync(claudeSettings, 'utf8'))).toEqual({
      hooks: {
        PreToolUse: [{ hooks: [{ type: 'command', command: 'node retained-claude-hook.js' }] }],
      },
    });
    expect(JSON.parse(fs.readFileSync(
      path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
    ))).toMatchObject({ release_ref: 'release-B', upgrade_id: expect.stringMatching(/^upgrade-/) });
    await expect(startExecutorService({
      zylosDir,
      execFileSyncFn: (file, args) => (file === 'pm2' && args[0] === 'jlist' ? '[]' : ''),
      requestFn: async () => ({
        ok: true,
        result: {
          executor: { service_instance_id: 'committed-executor' },
          snapshot: {
            contract: 'zylos.observability-snapshot',
            core_service_instance_id: 'committed-executor',
            service: { health: 'healthy', service_instance_id: 'committed-executor' },
          },
        },
      }),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({ ok: true, serviceInstanceId: 'committed-executor' });
    expect(JSON.parse(fs.readFileSync(
      path.join(zylosDir, 'runtime', 'executor-start-fence.json'), 'utf8',
    ))).toMatchObject({
      issuance_kind: 'committed_reconciliation',
      upgrade_id: expect.stringMatching(/^upgrade-/),
      legacy_services_quiesced: true,
      legacy_registrations_absent: true,
      legacy_artifacts_reconciled: true,
    });
    expect(fs.readFileSync(path.join(zylosDir, 'pm2', 'ecosystem.config.cjs'), 'utf8'))
      .toBe('module.exports = { apps: [{ name: "zylos-executor" }] };\n');
    database.close();
  });

  test('removes inactive legacy registrations before activation and keeps them removed on rollback', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-legacy-rollback-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const downloadedSource = path.join(directory, 'downloaded-B');
    const zylosDir = path.join(directory, 'installation');
    for (const [release, version] of [[currentRelease, 'release-A'], [downloadedSource, 'release-B']]) {
      for (const entry of ['runtime/executor', 'cli', 'scripts', 'templates/pm2']) {
        fs.mkdirSync(path.join(release, entry), { recursive: true });
      }
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({ name: 'zylos', version }));
      for (const entry of [
        'runtime/executor/daemon.js', 'runtime/executor/health-probe.js',
        'runtime/executor/launcher.js', 'cli/launcher.js', 'cli/zylos.js', 'scripts/postinstall.js',
      ]) fs.writeFileSync(path.join(release, entry), 'export {};\n');
      fs.writeFileSync(
        path.join(release, 'templates', 'pm2', 'ecosystem.config.cjs'),
        release === currentRelease
          ? 'module.exports = { apps: [{ name: "activity-monitor" }, { name: "c4-dispatcher" }] };\n'
          : 'module.exports = { apps: [{ name: "zylos-executor" }] };\n',
      );
    }
    fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
    fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
      release_ref: 'release-A',
      release_path: currentRelease,
      upgrade_id: 'upgrade-base-release',
    }));
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, 'pm2'), { recursive: true });
    fs.writeFileSync(
      path.join(zylosDir, 'pm2', 'ecosystem.config.cjs'),
      fs.readFileSync(path.join(currentRelease, 'templates', 'pm2', 'ecosystem.config.cjs')),
    );
    const scripts = new Map([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ]);
    const processes = new Map([
      ['activity-monitor', 'online'],
      ['c4-dispatcher', 'online'],
    ]);
    const commands = [];
    const execFileSyncFn = (file, args) => {
      commands.push([file, args]);
      if (file !== 'pm2') return '';
      if (args[0] === 'jlist') {
        return JSON.stringify([...processes].map(([name, status]) => ({
          name,
          pm2_env: { status, pm_exec_path: scripts.get(name) },
        })));
      }
      if (args[0] === 'delete') processes.delete(args[1]);
      if (args[0] === 'start') processes.set(args[args.indexOf('--only') + 1], 'online');
      if (args[0] === 'stop') processes.set(args[1], 'stopped');
      return '';
    };
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const handler = createInstalledExecutorUpgradeHandler({
      database,
      Database,
      zylosDir,
      currentReleasePath: currentRelease,
      currentReleaseRef: 'release-A',
      provider: 'codex',
      execFileSyncFn,
      startTargetHealth: async () => ({
        proof: {
          service_instance_id: 'invalid-target', snapshot_version: 1,
          health: 'unhealthy', reconciliation: 'incomplete',
        },
        close: async () => {},
      }),
    });

    await expect(handler({
      action: 'upgrade',
      target: { release: 'release-B', downloaded_source: downloadedSource },
    })).resolves.toMatchObject({ success: false, state: 'rolled_back' });
    expect(processes).toEqual(new Map());
    expect(commands).toContainEqual(['pm2', ['delete', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['delete', 'c4-dispatcher']]);
    expect(commands).not.toContainEqual([
      'pm2', ['start', path.join(zylosDir, 'pm2', 'ecosystem.config.cjs'), '--only', 'activity-monitor'],
    ]);
    expect(commands).toContainEqual(['pm2', ['stop', 'activity-monitor']]);
    expect(commands).toContainEqual(['pm2', ['stop', 'c4-dispatcher']]);
    expect(JSON.parse(fs.readFileSync(
      path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
    ))).toMatchObject({ release_ref: 'release-A', upgrade_id: 'upgrade-base-release' });
    database.close();
  });

  test('reaps a malformed target health process before rollback completes', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-health-reap-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const downloadedSource = path.join(directory, 'downloaded-B');
    const zylosDir = path.join(directory, 'installation');
    const pidFile = path.join(directory, 'health-probe.pid');
    for (const [release, version] of [[currentRelease, 'release-A'], [downloadedSource, 'release-B']]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.mkdirSync(path.join(release, 'scripts'), { recursive: true });
      fs.mkdirSync(path.join(release, 'templates', 'pm2'), { recursive: true });
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({ name: 'zylos', version }));
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'scripts', 'postinstall.js'), 'export {};\n');
      fs.writeFileSync(
        path.join(release, 'templates', 'pm2', 'ecosystem.config.cjs'),
        'module.exports = { apps: [{ name: "zylos-executor" }] };\n',
      );
      fs.writeFileSync(
        path.join(release, 'runtime', 'executor', 'health-probe.js'),
        release === currentRelease
          ? 'export {};\n'
          : `import fs from 'node:fs';\nfs.writeFileSync(process.env.ZYLOS_TEST_HEALTH_PID_FILE, String(process.pid));\nprocess.on('SIGTERM', () => {});\nprocess.stdout.write('not-json\\n');\nsetInterval(() => {}, 1_000);\n`,
      );
    }
    writeActiveExecutorRelease(zylosDir, currentRelease);
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const previousPidFile = process.env.ZYLOS_TEST_HEALTH_PID_FILE;
    process.env.ZYLOS_TEST_HEALTH_PID_FILE = pidFile;
    let childPid = null;
    try {
      const handler = createInstalledExecutorUpgradeHandler({
        database,
        Database,
        zylosDir,
        currentReleasePath: currentRelease,
        currentReleaseRef: 'release-A',
        provider: 'codex',
        execFileSyncFn: (file) => (file === 'pm2' ? '[]' : ''),
      });

      await expect(handler({
        action: 'upgrade',
        target: { release: 'release-B', downloaded_source: downloadedSource },
      })).resolves.toMatchObject({ success: false, state: 'rolled_back' });

      childPid = Number(fs.readFileSync(pidFile, 'utf8'));
      await waitForProcessExit(childPid);
      expect(processIsAlive(childPid)).toBe(false);
    } finally {
      if (Number.isSafeInteger(childPid) && processIsAlive(childPid)) {
        process.kill(childPid, 'SIGKILL');
        await waitForProcessExit(childPid);
      }
      database.close();
      if (previousPidFile === undefined) delete process.env.ZYLOS_TEST_HEALTH_PID_FILE;
      else process.env.ZYLOS_TEST_HEALTH_PID_FILE = previousPidFile;
    }
  }, 10_000);

  test('times out and reaps a silent target health process', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-health-timeout-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const downloadedSource = path.join(directory, 'downloaded-B');
    const zylosDir = path.join(directory, 'installation');
    const pidFile = path.join(directory, 'health-probe.pid');
    for (const [release, version] of [[currentRelease, 'release-A'], [downloadedSource, 'release-B']]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.mkdirSync(path.join(release, 'scripts'), { recursive: true });
      fs.mkdirSync(path.join(release, 'templates', 'pm2'), { recursive: true });
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({ name: 'zylos', version }));
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'scripts', 'postinstall.js'), 'export {};\n');
      fs.writeFileSync(
        path.join(release, 'templates', 'pm2', 'ecosystem.config.cjs'),
        'module.exports = { apps: [{ name: "zylos-executor" }] };\n',
      );
      fs.writeFileSync(
        path.join(release, 'runtime', 'executor', 'health-probe.js'),
        release === currentRelease
          ? 'export {};\n'
          : `import fs from 'node:fs';\nfs.writeFileSync(process.env.ZYLOS_TEST_HEALTH_PID_FILE, String(process.pid));\nprocess.on('SIGTERM', () => {});\nsetInterval(() => {}, 1_000);\n`,
      );
    }
    writeActiveExecutorRelease(zylosDir, currentRelease);
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const previousPidFile = process.env.ZYLOS_TEST_HEALTH_PID_FILE;
    process.env.ZYLOS_TEST_HEALTH_PID_FILE = pidFile;
    let childPid = null;
    let operation = null;
    try {
      const handler = createInstalledExecutorUpgradeHandler({
        database,
        Database,
        zylosDir,
        currentReleasePath: currentRelease,
        currentReleaseRef: 'release-A',
        provider: 'codex',
        execFileSyncFn: (file) => (file === 'pm2' ? '[]' : ''),
        // Allow the child to reach its PID proof before exercising the
        // timeout/reap path; a 50ms budget can expire during Node startup.
        targetHealthProofTimeoutMs: 500,
        targetHealthTerminationGraceMs: 50,
      });
      operation = handler({
        action: 'upgrade',
        target: { release: 'release-B', downloaded_source: downloadedSource },
      });

      const result = await settleWithin(operation, 1_200, { blocked: true });
      expect(result).toMatchObject({ success: false, state: 'rolled_back' });
      childPid = Number(fs.readFileSync(pidFile, 'utf8'));
      await waitForProcessExit(childPid);
      expect(processIsAlive(childPid)).toBe(false);
    } finally {
      if (childPid === null && fs.existsSync(pidFile)) childPid = Number(fs.readFileSync(pidFile, 'utf8'));
      if (Number.isSafeInteger(childPid) && processIsAlive(childPid)) {
        process.kill(childPid, 'SIGKILL');
        await waitForProcessExit(childPid);
      }
      if (operation !== null) await Promise.allSettled([operation]);
      database.close();
      if (previousPidFile === undefined) delete process.env.ZYLOS_TEST_HEALTH_PID_FILE;
      else process.env.ZYLOS_TEST_HEALTH_PID_FILE = previousPidFile;
    }
  }, 10_000);

  test('reconstructs the same durable plan and resumes from maintenance after restart', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-upgrade-resume-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const zylosDir = path.join(directory, 'installation');
    const targetRelease = path.join(zylosDir, 'runtime', 'releases', 'release-B-fixture');
    for (const [release, version] of [[currentRelease, 'release-A'], [targetRelease, 'release-B']]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.mkdirSync(path.join(release, 'scripts'), { recursive: true });
      fs.mkdirSync(path.join(release, 'templates', 'pm2'), { recursive: true });
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({ name: 'zylos', version }));
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'health-probe.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'scripts', 'postinstall.js'), 'export {};\n');
      fs.writeFileSync(
        path.join(release, 'templates', 'pm2', 'ecosystem.config.cjs'),
        'module.exports = { apps: [{ name: "zylos-executor" }] };\n',
      );
    }
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const upgradeId = 'upgrade-resume-fixture';
    const upgradePreflight = { ...preflight(), upgrade_id: upgradeId };
    const upgradeService = createRuntimeUpgradeService({
      database,
      now: (() => {
        let tick = 0;
        return () => `2026-07-20T12:00:${String(tick++).padStart(2, '0')}.000Z`;
      })(),
    });
    upgradeService.preflight(upgradePreflight);
    const snapshot = await createSqliteSnapshotAdapter({
      database,
      snapshotDirectory: path.join(zylosDir, 'runtime', 'upgrade-snapshots'),
      openDatabase: (file, options) => new Database(file, options),
    }).capture({
      step_id: `${upgradeId}:snapshot-capture`,
      upgrade_id: upgradeId,
      from_release: 'release-A',
    });
    upgradeService.recordSnapshot(upgradeId, snapshot);
    upgradeService.enterMaintenance(upgradeId);

    const legacyBatch = { batch_id: `${upgradeId}-empty`, records: [] };
    const legacyQueueFile = path.join(zylosDir, 'runtime', 'upgrade-input', `${upgradeId}.json`);
    const planFile = path.join(zylosDir, 'runtime', 'upgrade-plans', `${upgradeId}.json`);
    fs.mkdirSync(path.dirname(legacyQueueFile), { recursive: true });
    fs.mkdirSync(path.dirname(planFile), { recursive: true });
    fs.writeFileSync(legacyQueueFile, `${JSON.stringify(legacyBatch)}\n`);
    const durablePlan = {
      schema_version: 1,
      upgrade_id: upgradeId,
      preflight: upgradePreflight,
      legacy_batch: legacyBatch,
      legacy_queue_file: legacyQueueFile,
      from_release_path: currentRelease,
      from_upgrade_id: 'upgrade-base-release',
      to_release_path: targetRelease,
      from_package_version: 'release-A',
      to_package_version: 'release-B',
      provider: 'codex',
    };
    fs.writeFileSync(planFile, `${JSON.stringify({
      ...durablePlan,
      to_package_version: 'conflicting-release',
    })}\n`);
    fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
    fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
      release_ref: 'release-A', release_path: currentRelease, upgrade_id: 'upgrade-base-release',
    }));

    const handler = createInstalledExecutorUpgradeHandler({
      database,
      Database,
      zylosDir,
      currentReleasePath: currentRelease,
      currentReleaseRef: 'release-A',
      provider: 'codex',
      execFileSyncFn: (file) => (file === 'pm2' ? '[]' : ''),
      startTargetHealth: async ({ request }) => {
        const service = createExecutorService({
          database,
          adapter: {
            provider: 'codex', provider_transport: 'official_app_server',
            async *execute() {}, async close() { return []; },
          },
          provider: 'codex',
          serviceInstanceId: 'resumed-target-health',
          hostId: 'resumed-target-health',
          workspaceRoot: zylosDir,
          releaseRef: request.release_ref,
          upgradeId: request.upgrade_id,
          serviceStartedAt: '2026-07-20T13:00:00.000Z',
          now: () => '2026-07-20T13:00:00.000Z',
        });
        service.start();
        const observed = service.publishObservabilitySnapshot();
        return {
          proof: {
            service_instance_id: observed.core_service_instance_id,
            snapshot_version: observed.snapshot_version,
            health: observed.service.health,
            reconciliation: 'complete',
          },
          close: () => service.close(),
        };
      },
      now: (() => {
        let tick = 30;
        return () => `2026-07-20T12:01:${String(tick++).padStart(2, '0')}.000Z`;
      })(),
    });

    await expect(handler.resumeBlocking()).rejects.toThrow('package identity conflicts');
    expect(upgradeService.get(upgradeId).state).toBe('maintenance');
    fs.writeFileSync(planFile, `${JSON.stringify(durablePlan)}\n`);

    await expect(handler.resumeBlocking()).resolves.toMatchObject({
      success: true,
      state: 'committed',
      to: 'release-B',
    });
    expect(fs.existsSync(planFile)).toBe(false);
    expect(fs.existsSync(legacyQueueFile)).toBe(false);
    expect(JSON.parse(fs.readFileSync(
      path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
    ))).toMatchObject({ release_ref: 'release-B', upgrade_id: upgradeId });
    database.close();
  });
});
