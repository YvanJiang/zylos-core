import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';

import { driveInstalledRuntimeUpgrade } from '../runtime/migration/executor-upgrade-driver.js';
import {
  createInstalledExecutorUpgradeHandler,
  removeLegacyServiceRegistrations,
} from '../runtime/migration/installed-executor-upgrade.js';
import { legacyLifecycleArtifactPaths } from '../runtime/migration/legacy-lifecycle-artifacts.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createRuntimeUpgradeService } from '../runtime/migration/runtime-upgrade-service.js';
import { createSqliteSnapshotAdapter } from '../runtime/migration/runtime-upgrade-coordinator.js';

const require = createRequire(new URL('../skills/comm-bridge/package.json', import.meta.url));
const Database = require('better-sqlite3');
const directories = [];

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
  test('postcommit cleanup removes only obsolete supervisor registrations', () => {
    const commands = [];
    const result = removeLegacyServiceRegistrations((file, args) => {
      commands.push([file, args]);
      if (args[0] === 'jlist') {
        return JSON.stringify([
          { name: 'c4-dispatcher', pm2_env: { status: 'stopped' } },
          { name: 'activity-monitor', pm2_env: { status: 'errored' } },
          { name: 'zylos-executor', pm2_env: { status: 'online' } },
        ]);
      }
      return '';
    });

    expect(result).toEqual({ removed_services: ['c4-dispatcher', 'activity-monitor'] });
    expect(commands).toEqual([
      ['pm2', ['jlist']],
      ['pm2', ['delete', 'c4-dispatcher']],
      ['pm2', ['delete', 'activity-monitor']],
      ['pm2', ['save']],
    ]);
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
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({
        name: 'zylos', version: release === currentRelease ? 'release-A' : 'release-B',
      }));
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'health-probe.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
    }
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
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
    const commands = [];
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
          serviceStartedAt: '2026-07-20T12:00:30.000Z',
          now: () => '2026-07-20T12:00:30.000Z',
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
      execFileSyncFn: (file, args) => {
        commands.push([file, args]);
        if (file === 'pm2') return '[]';
        return '';
      },
      now: (() => {
        let tick = 0;
        return () => `2026-07-20T12:00:${String(tick++).padStart(2, '0')}.000Z`;
      })(),
    });

    const result = await handler({
      action: 'upgrade',
      target: { release: 'release-B', downloaded_source: downloadedSource },
    });

    expect(result).toMatchObject({ success: true, state: 'committed', to: 'release-B' });
    expect(commands).toContainEqual(['npm', ['install', '--omit=dev', '--no-audit', '--no-fund']]);
    expect(obsoleteArtifacts.every((artifact) => !fs.existsSync(artifact))).toBe(true);
    expect(JSON.parse(fs.readFileSync(
      path.join(zylosDir, 'runtime', 'active-release.json'), 'utf8',
    ))).toMatchObject({ release_ref: 'release-B', upgrade_id: expect.stringMatching(/^upgrade-/) });
    database.close();
  });

  test('reconstructs the same durable plan and resumes from maintenance after restart', async () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-upgrade-resume-'));
    directories.push(directory);
    const currentRelease = path.join(directory, 'release-A');
    const zylosDir = path.join(directory, 'installation');
    const targetRelease = path.join(zylosDir, 'runtime', 'releases', 'release-B-fixture');
    for (const [release, version] of [[currentRelease, 'release-A'], [targetRelease, 'release-B']]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.writeFileSync(path.join(release, 'package.json'), JSON.stringify({ name: 'zylos', version }));
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'health-probe.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'launcher.js'), 'export {};\n');
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), 'export {};\n');
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
      release_ref: 'release-A', release_path: currentRelease, upgrade_id: null,
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
