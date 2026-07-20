import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { describe, expect, jest, test } from '@jest/globals';

import {
  EXECUTOR_SERVICE_NAME,
  executorServiceSocketPath,
  restartExecutorService,
  removeExecutorServiceRegistration,
  reconcileExecutorService,
  selfHealExecutorService,
  startExecutorService,
  stopExecutorService,
} from '../cli/lib/executor-service-lifecycle.js';

import { resolveCliEntry } from '../cli/launcher.js';
import { runtimeCommand } from '../cli/commands/runtime.js';

function health(instanceId, status = 'healthy', provider = 'codex') {
  return {
    ok: true,
    result: {
      executor: { provider, service_instance_id: instanceId },
      snapshot: {
        contract: 'zylos.observability-snapshot',
        core_service_instance_id: instanceId,
        service: { health: status, service_instance_id: instanceId },
      },
    },
  };
}

describe('executor lifecycle CLI boundary', () => {
  test('runtime status reports the provider and identity from Core health', async () => {
    const log = jest.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await expect(runtimeCommand(['status'], {
        zylosDir: '/tmp/zylos-cli-fixture',
        getHealth: async () => ({
          ok: true,
          health: 'healthy',
          provider: 'codex',
          serviceInstanceId: 'executor-authoritative',
        }),
      })).resolves.toEqual({
        ok: true,
        health: 'healthy',
        provider: 'codex',
        serviceInstanceId: 'executor-authoritative',
      });
    } finally {
      log.mockRestore();
    }
  });

  test('starts through the exact executor ecosystem target and trusts Core health', async () => {
    const commands = [];
    const result = await startExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (file, args) => commands.push([file, args]),
      requestFn: async () => health('executor-new'),
      retryDelaysMs: [0],
    });

    expect(commands).toEqual([
      ['pm2', ['start', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
    expect(result).toMatchObject({ ok: true, serviceInstanceId: 'executor-new', health: 'healthy' });
  });

  test('stop reports failure when Core cannot acknowledge shutdown', async () => {
    const commands = [];
    const result = await stopExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (file, args) => commands.push([file, args]),
      requestFn: async () => ({ ok: false, error: 'shutdown_rejected' }),
    });

    expect(result).toEqual({ ok: false, error: 'shutdown_rejected' });
    expect(commands).toEqual([]);
  });

  test('restart succeeds only after Core reports a new healthy identity', async () => {
    const commands = [];
    const replies = [health('executor-old'), health('executor-old'), health('executor-new')];
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (file, args) => commands.push([file, args]),
      requestFn: async () => replies.shift(),
      retryDelaysMs: [0, 0],
    });

    expect(commands).toEqual([
      ['pm2', ['restart', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
    expect(result).toMatchObject({ ok: true, previousServiceInstanceId: 'executor-old', serviceInstanceId: 'executor-new' });
  });

  test('self-heal is a no-op for healthy Core and starts an unavailable service', async () => {
    const healthyCommands = [];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (...args) => healthyCommands.push(args),
      requestFn: async () => health('executor-current'),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({ ok: true, repaired: false });
    expect(healthyCommands).toEqual([]);

    const repairedCommands = [];
    const replies = [Promise.reject(Object.assign(new Error('offline'), { code: 'ENOENT' })), health('executor-repaired')];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (file, args) => repairedCommands.push([file, args]),
      requestFn: () => replies.shift(),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({ ok: true, repaired: true, serviceInstanceId: 'executor-repaired' });
    expect(repairedCommands[0]).toEqual([
      'pm2', ['start', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME],
    ]);
  });

  test('uses an explicit fixed Core socket under the selected ZYLOS_DIR', () => {
    expect(executorServiceSocketPath('/tmp/zylos-cli-fixture'))
      .toBe('/tmp/zylos-cli-fixture/runtime/executor-service.sock');
  });

  test('uninstall removes only the exact executor supervisor registration', () => {
    const commands = [];
    expect(removeExecutorServiceRegistration({
      execFileSyncFn: (file, args) => commands.push([file, args]),
    })).toEqual({ ok: true });
    expect(commands).toEqual([
      ['pm2', ['delete', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
  });

  test('init reconciliation replaces a healthy old identity with the deployed executor config', async () => {
    const commands = [];
    const replies = [
      health('executor-before-init'),
      health('executor-before-init'),
      health('executor-after-init'),
    ];
    await expect(reconcileExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: (file, args) => commands.push([file, args]),
      requestFn: async () => replies.shift(),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({
      ok: true,
      previousServiceInstanceId: 'executor-before-init',
      serviceInstanceId: 'executor-after-init',
    });
    expect(commands[0]).toEqual([
      'pm2', ['restart', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME],
    ]);
  });
});

describe('installed service template', () => {
  test('defines executor as the only built-in Core service', () => {
    const home = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-template-home-'));
    const zylosDir = path.join(home, 'zylos');
    fs.mkdirSync(path.join(zylosDir, '.zylos'), { recursive: true });
    const template = path.resolve('templates/pm2/ecosystem.config.cjs');
    const child = spawnSync(process.execPath, ['-e', `
      const config = require(${JSON.stringify(template)});
      process.stdout.write(JSON.stringify(config.apps.map((app) => ({ name: app.name, script: app.script }))));
    `], {
      encoding: 'utf8',
      env: { ...process.env, HOME: home, ZYLOS_DIR: zylosDir, ZYLOS_PACKAGE_ROOT: process.cwd() },
    });
    fs.rmSync(home, { recursive: true, force: true });

    expect(child.status).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual([{
      name: EXECUTOR_SERVICE_NAME,
      script: path.resolve('runtime/executor/launcher.js'),
    }]);
  });
});

describe('installed CLI release dispatcher', () => {
  test('uses the same durable active release as the executor launcher', () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-cli-release-'));
    const current = path.join(directory, 'current');
    const target = path.join(directory, 'target');
    const zylosDir = path.join(directory, 'installation');
    for (const release of [current, target]) {
      fs.mkdirSync(path.join(release, 'cli'), { recursive: true });
      fs.writeFileSync(path.join(release, 'cli', 'zylos.js'), '#!/usr/bin/env node\n');
    }
    fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
    fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
      release_ref: 'release-B', release_path: target,
    }));

    expect(resolveCliEntry({ zylosDir, packageRoot: current }))
      .toBe(path.join(target, 'cli', 'zylos.js'));
    fs.rmSync(directory, { recursive: true, force: true });
  });
});
