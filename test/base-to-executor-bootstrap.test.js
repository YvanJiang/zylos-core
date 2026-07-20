import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { describe, expect, test } from '@jest/globals';

import {
  bootstrapFailureExitCode,
  runBaseToExecutorBootstrap,
} from '../scripts/bootstrap-executor-lifecycle.js';
import { cleanupCompletedBootstrapStaging } from '../scripts/cleanup-bootstrap-staging.js';

function writeChannelAuthority(root) {
  const file = path.join(root, 'channel-authority.json');
  fs.writeFileSync(file, `${JSON.stringify({
    schema_version: 1,
    contract: 'zylos.channel-authority',
    scopes: [{
      channel: 'feishu', region: 'cn', tenant_id: 'tenant-bootstrap', bot_id: 'app-bootstrap',
      verified_at: '2026-07-21T00:00:00.000Z',
      verification_source: 'authenticated_event', provider_instance_id: 'provider-bootstrap',
    }],
  })}\n`, { mode: 0o600 });
  return file;
}

describe('exact-base executor lifecycle bootstrap', () => {
  test('hands a legacy source to the canonical durable installed upgrade owner', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-bootstrap-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, '.zylos'), { recursive: true });
    fs.mkdirSync(fromRelease, { recursive: true });
    fs.mkdirSync(targetRelease, { recursive: true });
    fs.writeFileSync(path.join(zylosDir, '.zylos', 'config.json'), '{"runtime":"codex"}\n');
    const calls = [];
    class DatabaseFixture {
      constructor(file) { calls.push(['database', file]); }
      close() { calls.push(['database-close']); }
    }
    try {
      const result = await runBaseToExecutorBootstrap({
        zylosDir,
        fromReleasePath: fromRelease,
        targetReleasePath: targetRelease,
        Database: DatabaseFixture,
        createHandler: (options) => {
          calls.push(['handler', options]);
          return async (request) => {
            calls.push(['request', request]);
            return { success: true, state: 'committed', to: request.target.release };
          };
        },
      });
      expect(result).toMatchObject({ success: true, state: 'committed' });
      expect(calls[1][1]).toMatchObject({
        currentReleasePath: fs.realpathSync(fromRelease),
        currentReleaseRef: 'branch:exact-base-bootstrap',
        provider: 'codex',
        allowLegacyFromRelease: true,
      });
      expect(calls[2][1]).toMatchObject({
        action: 'upgrade',
        target: {
          release: 'branch:executor-lifecycle',
          branch: 'executor-lifecycle',
          downloaded_source: fs.realpathSync(targetRelease),
        },
      });
      expect(calls.at(-1)).toEqual(['database-close']);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('reports committed post-commit failure without permitting package rollback', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-bootstrap-committed-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    fs.mkdirSync(path.join(zylosDir, 'comm-bridge'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, '.zylos'), { recursive: true });
    fs.mkdirSync(fromRelease, { recursive: true });
    fs.mkdirSync(targetRelease, { recursive: true });
    let closed = false;
    class DatabaseFixture {
      close() { closed = true; }
    }
    try {
      await expect(runBaseToExecutorBootstrap({
        zylosDir,
        fromReleasePath: fromRelease,
        targetReleasePath: targetRelease,
        Database: DatabaseFixture,
        createHandler: () => async () => ({
          success: false,
          state: 'committed',
          error: 'strict postinstall failed',
        }),
      })).rejects.toMatchObject({ committed: true });
      expect(closed).toBe(true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('distinguishes an incomplete rollback from a completed rollback in exit status', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-rollback-pending-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    for (const directory of [path.join(zylosDir, 'comm-bridge'), fromRelease, targetRelease]) {
      fs.mkdirSync(directory, { recursive: true });
    }
    class DatabaseFixture { close() {} }
    try {
      let observed;
      try {
        await runBaseToExecutorBootstrap({
          zylosDir, fromReleasePath: fromRelease, targetReleasePath: targetRelease,
          Database: DatabaseFixture,
          createHandler: () => async () => ({
            success: false, state: 'rollback_failed', error: 'delivery owner unavailable',
          }),
        });
      } catch (error) {
        observed = error;
      }
      expect(observed).toMatchObject({ rollbackPending: true, rollbackState: 'rollback_failed' });
      expect(bootstrapFailureExitCode(observed)).toBe(3);
      expect(bootstrapFailureExitCode({ committed: true })).toBe(2);
      expect(bootstrapFailureExitCode(new Error('rolled back'))).toBe(1);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('persists package transaction before activation and starts the executor after commit', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-package-transaction-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    const fromPackage = path.join(root, 'exact-base.tgz');
    const targetPackage = path.join(root, 'candidate.tgz');
    const authorityManifest = writeChannelAuthority(root);
    const installedRoot = path.join(root, 'installed');
    const installedBin = path.join(root, 'bin', 'zylos');
    for (const directory of [
      path.join(zylosDir, 'comm-bridge'), path.join(zylosDir, '.zylos'),
      fromRelease, targetRelease, path.join(installedRoot, 'cli'), path.dirname(installedBin),
    ]) fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(fromPackage, 'exact-base-package');
    fs.writeFileSync(targetPackage, 'candidate-package');
    fs.writeFileSync(path.join(installedRoot, 'cli', 'zylos.js'), '');
    fs.symlinkSync(path.join(installedRoot, 'cli', 'zylos.js'), installedBin);
    const calls = [];
    class DatabaseFixture { close() { calls.push(['database-close']); } }
    try {
      const result = await runBaseToExecutorBootstrap({
        zylosDir, fromReleasePath: fromRelease, targetReleasePath: targetRelease,
        fromPackageTarball: fromPackage, targetPackageTarball: targetPackage,
        channelAuthorityManifest: authorityManifest,
        installMode: 'direct', Database: DatabaseFixture,
        execFileSyncFn: (file, args) => {
          calls.push([file, args]);
          if (file === 'sh') return `${installedBin}\n`;
          return '';
        },
        startService: async () => ({ ok: true, serviceInstanceId: 'executor-after-bootstrap' }),
        createHandler: (options) => {
          const handler = async () => {
            const manifest = JSON.parse(fs.readFileSync(
              path.join(zylosDir, 'runtime', 'base-executor-bootstrap.json'), 'utf8',
            ));
            expect(manifest.state).toBe('prepared');
            await options.packageLifecycle.activate({ upgrade_id: 'upgrade-fixture' });
            return { success: true, state: 'committed' };
          };
          handler.resumeBlocking = async () => null;
          return handler;
        },
      });
      expect(result).toMatchObject({
        state: 'committed', executor_service_instance_id: 'executor-after-bootstrap',
      });
      expect(calls).toContainEqual([
        'npm', ['install', '-g', '--install-links', targetPackage],
      ]);
      expect(JSON.parse(fs.readFileSync(
        path.join(zylosDir, 'runtime', 'base-executor-bootstrap.json'), 'utf8',
      ))).toMatchObject({
        state: 'supervisor_started',
        channel_authority_manifest: fs.realpathSync(authorityManifest),
        channel_authority_sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
      });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('restores the exact package inside rollback and never starts executor supervisor', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-package-rollback-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    const fromPackage = path.join(root, 'exact-base.tgz');
    const targetPackage = path.join(root, 'candidate.tgz');
    const authorityManifest = writeChannelAuthority(root);
    for (const directory of [path.join(zylosDir, 'comm-bridge'), fromRelease, targetRelease]) {
      fs.mkdirSync(directory, { recursive: true });
    }
    fs.writeFileSync(fromPackage, 'exact-base-package');
    fs.writeFileSync(targetPackage, 'candidate-package');
    const calls = [];
    let started = false;
    class DatabaseFixture { close() {} }
    try {
      await expect(runBaseToExecutorBootstrap({
        zylosDir, fromReleasePath: fromRelease, targetReleasePath: targetRelease,
        fromPackageTarball: fromPackage, targetPackageTarball: targetPackage,
        channelAuthorityManifest: authorityManifest,
        installMode: 'direct', Database: DatabaseFixture,
        execFileSyncFn: (file, args) => { calls.push([file, args]); return ''; },
        startService: async () => { started = true; return { ok: true }; },
        createHandler: (options) => {
          const handler = async () => {
            await options.packageLifecycle.activate({ upgrade_id: 'upgrade-fixture' });
            await options.packageLifecycle.restore({ upgrade_id: 'upgrade-fixture' });
            return { success: false, state: 'rolled_back', error: 'target health failed' };
          };
          handler.resumeBlocking = async () => null;
          return handler;
        },
      })).rejects.toThrow('rollback state is rolled_back');
      expect(calls).toEqual([
        ['npm', ['install', '-g', '--install-links', targetPackage]],
        ['npm', ['install', '-g', '--install-links', '--ignore-scripts', fromPackage]],
      ]);
      expect(started).toBe(false);
      expect(JSON.parse(fs.readFileSync(
        path.join(zylosDir, 'runtime', 'base-executor-bootstrap.json'), 'utf8',
      )).state).toBe('base_restored');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('resume rejects a changed authority manifest before reopening migration state', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-authority-resume-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    const fromPackage = path.join(root, 'exact-base.tgz');
    const targetPackage = path.join(root, 'candidate.tgz');
    const authorityManifest = writeChannelAuthority(root);
    for (const directory of [path.join(zylosDir, 'comm-bridge'), fromRelease, targetRelease]) {
      fs.mkdirSync(directory, { recursive: true });
    }
    fs.writeFileSync(fromPackage, 'exact-base-package');
    fs.writeFileSync(targetPackage, 'candidate-package');
    class DatabaseFixture { close() {} }
    try {
      await expect(runBaseToExecutorBootstrap({
        zylosDir, fromReleasePath: fromRelease, targetReleasePath: targetRelease,
        fromPackageTarball: fromPackage, targetPackageTarball: targetPackage,
        channelAuthorityManifest: authorityManifest,
        installMode: 'direct', Database: DatabaseFixture,
        createHandler: () => {
          const handler = async () => { throw new Error('fixture interruption'); };
          handler.resumeBlocking = async () => null;
          return handler;
        },
      })).rejects.toThrow('fixture interruption');
      fs.appendFileSync(authorityManifest, ' ');
      await expect(runBaseToExecutorBootstrap({
        zylosDir, resume: true, Database: DatabaseFixture,
      })).rejects.toThrow('Channel authority manifest changed after bootstrap preparation');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('legacy package postinstall fails closed and directs upgrades to the bootstrap installer', () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-postinstall-legacy-'));
    const zylosDir = path.join(root, 'zylos');
    fs.mkdirSync(path.join(zylosDir, '.claude'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, 'pm2'), { recursive: true });
    fs.writeFileSync(
      path.join(zylosDir, 'pm2', 'ecosystem.config.cjs'),
      'module.exports={apps:[{name:"activity-monitor"}]};\n',
    );
    try {
      const child = spawnSync(process.execPath, ['scripts/postinstall.js'], {
        cwd: path.resolve('.'),
        env: { ...process.env, HOME: root, ZYLOS_DIR: zylosDir, CI: '' },
        encoding: 'utf8',
      });
      expect(child.status).not.toBe(0);
      expect(`${child.stdout}\n${child.stderr}`).toContain('one-time executor migration installer');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('successful resume removes only its disposable staging root', () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-bootstrap-cleanup-'));
    const zylosDir = path.join(root, 'zylos');
    const runtime = path.join(zylosDir, 'runtime');
    const staging = path.join(runtime, 'install-inventory.A1b2c3');
    const targetRelease = path.join(staging, 'target-release');
    const activeRelease = path.join(runtime, 'releases', 'executor-active');
    for (const directory of [targetRelease, activeRelease]) fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(targetRelease, 'obsolete-staging-file'), 'obsolete');
    fs.writeFileSync(path.join(activeRelease, 'executor-file'), 'active');
    fs.writeFileSync(path.join(runtime, 'base-executor-bootstrap.json'), JSON.stringify({
      state: 'supervisor_started', target_release_path: targetRelease,
    }));
    fs.writeFileSync(path.join(runtime, 'active-release.json'), JSON.stringify({
      release_path: activeRelease,
    }));
    try {
      expect(cleanupCompletedBootstrapStaging({ zylosDir })).toEqual({
        removed_staging_path: fs.realpathSync(runtime) + '/install-inventory.A1b2c3',
      });
      expect(fs.existsSync(staging)).toBe(false);
      expect(fs.readFileSync(path.join(activeRelease, 'executor-file'), 'utf8')).toBe('active');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('the installer stages exact packages and enters durable bootstrap before global activation or init', () => {
    const installer = fs.readFileSync(new URL('../scripts/install.sh', import.meta.url), 'utf8');
    const stage = installer.indexOf('npm pack --ignore-scripts');
    const bootstrap = installer.lastIndexOf('bootstrap-executor-lifecycle.js');
    const globalInstall = installer.indexOf('ZYLOS_PACKAGE_PREPARE=1 npm install');
    const init = installer.indexOf('info "Running zylos init..."');
    expect(stage).toBeGreaterThan(0);
    expect(bootstrap).toBeGreaterThan(stage);
    expect(globalInstall).toBeGreaterThan(bootstrap);
    expect(init).toBeGreaterThan(bootstrap);
    expect(installer).toContain('delegating upgrade to authoritative Core control');
    expect(installer).toContain('zylos upgrade --self --yes --branch "$BRANCH"');
    expect(installer).toContain('--from-package "$bootstrap_backup/exact-base-package.tgz"');
    expect(installer).toContain('--target-package "$bootstrap_backup/executor-package.tgz"');
    expect(installer).toContain('--channel-authority-manifest "$channel_authority_manifest"');
  });
});
