import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { describe, expect, test } from '@jest/globals';

import { runBaseToExecutorBootstrap } from '../scripts/bootstrap-executor-lifecycle.js';

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

  test('persists package transaction before activation and starts the executor after commit', async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-base-package-transaction-'));
    const zylosDir = path.join(root, 'zylos');
    const fromRelease = path.join(root, 'exact-base');
    const targetRelease = path.join(root, 'candidate');
    const fromPackage = path.join(root, 'exact-base.tgz');
    const targetPackage = path.join(root, 'candidate.tgz');
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
      )).state).toBe('supervisor_started');
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
  });
});
