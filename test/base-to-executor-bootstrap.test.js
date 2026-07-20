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

  test('the installer prepares the package inertly and runs durable bootstrap before init', () => {
    const installer = fs.readFileSync(new URL('../scripts/install.sh', import.meta.url), 'utf8');
    const prepare = installer.indexOf('ZYLOS_PACKAGE_PREPARE=1 npm install');
    const bootstrap = installer.indexOf('bootstrap-executor-lifecycle.js');
    const init = installer.indexOf('info "Running zylos init..."');
    expect(prepare).toBeGreaterThan(0);
    expect(bootstrap).toBeGreaterThan(prepare);
    expect(init).toBeGreaterThan(bootstrap);
    expect(installer).toContain('New package installation failed; restoring the previous package.');
    expect(installer).toContain('Executor migration rolled back; restoring the previous package.');
    expect(installer).toContain('Executor migration committed, but post-commit completion failed;');
    expect(installer).toContain('if [ "$bootstrap_status" -eq 2 ]; then');
    expect(installer).toMatch(/return "\$restore_status"/);
  });
});
