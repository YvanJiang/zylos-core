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
import { resolveActiveRelease } from '../runtime/executor/launcher.js';
import { runtimeCommand } from '../cli/commands/runtime.js';
import { classifyExecutorUpgradeControlFailure } from '../cli/commands/component.js';

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

function shutdown(instanceId) {
  return { ok: true, result: { status: 'completed', service_instance_id: instanceId } };
}

function pm2Fixture(commands, { registered = true, foreign = false } = {}) {
  return (file, args) => {
    commands.push([file, args]);
    if (file === 'pm2' && args[0] === 'jlist') {
      return JSON.stringify(registered ? [{
        name: EXECUTOR_SERVICE_NAME,
        pm2_env: {
          status: 'online',
          pm_exec_path: foreign ? '/opt/user/executor.js' : path.resolve('runtime/executor/launcher.js'),
          pm_cwd: '/tmp/zylos-cli-fixture',
          ZYLOS_DIR: '/tmp/zylos-cli-fixture',
        },
      }] : []);
    }
    return '';
  };
}

describe('executor lifecycle CLI boundary', () => {
  test('classifies a control timeout as an unknown durable outcome without a fake failed step', () => {
    const timeout = Object.assign(new Error('timed out'), {
      code: 'ETIMEDOUT', outcome: 'unknown',
    });
    expect(classifyExecutorUpgradeControlFailure(timeout)).toEqual({
      action: 'self_upgrade',
      success: false,
      state: 'uncertain',
      uncertain: true,
      failedStep: null,
      error: 'Upgrade result is uncertain; Core may still commit. Reconnect and query the authoritative upgrade state before retrying.',
    });
  });
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
      execFileSyncFn: pm2Fixture(commands, { registered: false }),
      requestFn: async () => health('executor-new'),
      retryDelaysMs: [0],
      assertStartFence: () => {},
    });

    expect(commands).toEqual([
      ['pm2', ['jlist']],
      ['pm2', ['start', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
    expect(result).toMatchObject({ ok: true, serviceInstanceId: 'executor-new', health: 'healthy' });
  });

  test('does not start an executor until one-time reconciliation has written its fence', async () => {
    const commands = [];
    const result = await startExecutorService({
      zylosDir: '/tmp/zylos-cli-fence-missing',
      execFileSyncFn: pm2Fixture(commands, { registered: false }),
      requestFn: async () => health('executor-new'),
      retryDelaysMs: [0],
    });

    expect(result).toMatchObject({ ok: false, error: expect.stringContaining('one-time runtime reconciliation') });
    expect(commands).toEqual([]);
  });

  test('stop reports failure when Core cannot acknowledge shutdown', async () => {
    const commands = [];
    const result = await stopExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(commands),
      requestFn: async () => ({ ok: false, error: 'shutdown_rejected' }),
    });

    expect(result).toEqual({ ok: false, error: 'shutdown_rejected' });
    expect(commands).toEqual([['pm2', ['jlist']]]);
  });

  test('restart succeeds only after Core reports a new healthy identity', async () => {
    const commands = [];
    const replies = [
      health('executor-old'), shutdown('executor-old'),
      health('executor-old'), health('executor-new'),
    ];
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(commands),
      requestFn: async () => replies.shift(),
      retryDelaysMs: [0, 0],
    });

    expect(commands).toEqual([
      ['pm2', ['jlist']],
      ['pm2', ['restart', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
    expect(result).toMatchObject({ ok: true, previousServiceInstanceId: 'executor-old', serviceInstanceId: 'executor-new' });
  });

  test('restart rejects a new healthy identity for the wrong provider', async () => {
    const replies = [
      health('executor-old', 'healthy', 'claude'),
      shutdown('executor-old'),
      health('executor-wrong-provider', 'healthy', 'claude'),
    ];
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      expectedProvider: 'codex',
      execFileSyncFn: pm2Fixture([]),
      requestFn: async () => replies.shift(),
      retryDelaysMs: [0],
    });

    expect(result).toMatchObject({
      ok: false,
      error: 'executor_provider_mismatch',
      expectedProvider: 'codex',
      provider: 'claude',
    });
  });

  test('restart does not invoke the supervisor while Core is in lifecycle maintenance', async () => {
    const commands = [];
    const busy = health('executor-busy');
    busy.result.snapshot.service.maintenance = true;
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(commands),
      requestFn: async () => busy,
      retryDelaysMs: [0],
    });

    expect(result).toEqual({ ok: false, error: 'executor_lifecycle_operation_in_progress' });
    expect(commands).toEqual([['pm2', ['jlist']]]);
  });

  test('provider reconfiguration rolls back the configuration and old provider on failed health', async () => {
    const events = [];
    const replies = [
      health('executor-old', 'healthy', 'claude'),
      shutdown('executor-old'),
      health('executor-wrong', 'healthy', 'claude'),
      health('executor-restored', 'healthy', 'claude'),
    ];
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      expectedProvider: 'codex',
      beforeSupervisorRestart: async () => events.push('configure-codex'),
      restoreConfiguration: async () => events.push('restore-claude'),
      execFileSyncFn: (file, args) => {
        events.push(`${file}:${args[0]}`);
        if (file === 'pm2' && args[0] === 'jlist') return JSON.stringify([{
          name: EXECUTOR_SERVICE_NAME,
          pm2_env: {
            pm_exec_path: path.resolve('runtime/executor/launcher.js'),
            pm_cwd: '/tmp/zylos-cli-fixture', ZYLOS_DIR: '/tmp/zylos-cli-fixture',
          },
        }]);
        return '';
      },
      requestFn: async (_socket, request) => {
        events.push(`core:${request.action}`);
        return replies.shift();
      },
      retryDelaysMs: [0],
    });

    expect(result).toMatchObject({
      ok: false,
      error: 'executor_provider_mismatch',
      configurationRollback: {
        ok: true,
        provider: 'claude',
        serviceInstanceId: 'executor-restored',
      },
    });
    expect(events).toEqual([
      'pm2:jlist', 'core:health', 'core:shutdown', 'configure-codex',
      'pm2:restart', 'pm2:save', 'core:health', 'restore-claude',
      'pm2:restart', 'pm2:save', 'core:health',
    ]);
  });

  test('configuration preparation failure restarts the acknowledged old executor after restoring config', async () => {
    const events = [];
    const replies = [
      health('executor-old', 'healthy', 'claude'),
      shutdown('executor-old'),
      health('executor-restored', 'healthy', 'claude'),
    ];
    const result = await restartExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      expectedProvider: 'codex',
      beforeSupervisorRestart: async () => {
        events.push('configure-codex');
        throw new Error('config write failed');
      },
      restoreConfiguration: async () => events.push('restore-claude'),
      execFileSyncFn: (file, args) => {
        events.push(`${file}:${args[0]}`);
        if (file === 'pm2' && args[0] === 'jlist') return JSON.stringify([{
          name: EXECUTOR_SERVICE_NAME,
          pm2_env: {
            pm_exec_path: path.resolve('runtime/executor/launcher.js'),
            pm_cwd: '/tmp/zylos-cli-fixture', ZYLOS_DIR: '/tmp/zylos-cli-fixture',
          },
        }]);
        return '';
      },
      requestFn: async (_socket, request) => {
        events.push(`core:${request.action}`);
        return replies.shift();
      },
      retryDelaysMs: [0],
    });

    expect(result).toMatchObject({
      ok: false,
      error: 'config write failed',
      configurationRollback: {
        ok: true, provider: 'claude', serviceInstanceId: 'executor-restored',
      },
    });
    expect(events).toEqual([
      'pm2:jlist', 'core:health', 'core:shutdown', 'configure-codex', 'restore-claude',
      'pm2:restart', 'pm2:save', 'core:health',
    ]);
  });

  test('self-heal is a no-op for healthy Core and starts an unavailable service', async () => {
    const healthyCommands = [];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(healthyCommands),
      requestFn: async () => health('executor-current'),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({ ok: true, repaired: false });
    expect(healthyCommands).toEqual([]);

    const repairedCommands = [];
    const replies = [Promise.reject(Object.assign(new Error('offline'), { code: 'ENOENT' })), health('executor-repaired')];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(repairedCommands, { registered: false }),
      requestFn: () => replies.shift(),
      retryDelaysMs: [0],
      assertStartFence: () => {},
    })).resolves.toMatchObject({ ok: true, repaired: true, serviceInstanceId: 'executor-repaired' });
    expect(repairedCommands[1]).toEqual([
      'pm2', ['start', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME],
    ]);

    const degradedCommands = [];
    const degradedReplies = [
      health('executor-degraded', 'degraded'),
      health('executor-degraded', 'degraded'),
      shutdown('executor-degraded'),
      health('executor-healed'),
    ];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(degradedCommands),
      requestFn: async () => degradedReplies.shift(),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({
      ok: true,
      repaired: true,
      previousServiceInstanceId: 'executor-degraded',
      serviceInstanceId: 'executor-healed',
    });
    expect(degradedCommands[1]).toEqual([
      'pm2', ['restart', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME],
    ]);

    const mismatchCommands = [];
    const mismatchReplies = [
      health('executor-claude', 'healthy', 'claude'),
      health('executor-claude', 'healthy', 'claude'),
      shutdown('executor-claude'),
      health('executor-codex', 'healthy', 'codex'),
    ];
    await expect(selfHealExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      expectedProvider: 'codex',
      execFileSyncFn: pm2Fixture(mismatchCommands),
      requestFn: async () => mismatchReplies.shift(),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({
      ok: true, repaired: true, provider: 'codex', serviceInstanceId: 'executor-codex',
    });
    expect(mismatchCommands[1]).toEqual([
      'pm2', ['restart', '/tmp/zylos-cli-fixture/pm2/ecosystem.config.cjs', '--only', EXECUTOR_SERVICE_NAME],
    ]);
  });

  test('uses an explicit fixed Core socket under the selected ZYLOS_DIR', () => {
    expect(executorServiceSocketPath('/tmp/zylos-cli-fixture'))
      .toBe('/tmp/zylos-cli-fixture/runtime/executor-service.sock');
  });

  test('uninstall removes only the exact executor supervisor registration', () => {
    const commands = [];
    expect(removeExecutorServiceRegistration({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(commands),
    })).toEqual({ ok: true });
    expect(commands).toEqual([
      ['pm2', ['jlist']],
      ['pm2', ['delete', EXECUTOR_SERVICE_NAME]],
      ['pm2', ['save']],
    ]);
  });

  test('all supervisor mutations fail closed on a foreign generic-name collision', async () => {
    for (const operation of [
      (options) => startExecutorService({ ...options, assertStartFence: () => {} }),
      (options) => stopExecutorService(options),
      (options) => restartExecutorService(options),
      (options) => Promise.resolve(removeExecutorServiceRegistration(options)),
    ]) {
      const commands = [];
      const result = await operation({
        zylosDir: '/tmp/zylos-cli-fixture',
        execFileSyncFn: pm2Fixture(commands, { foreign: true }),
        requestFn: async () => { throw new Error('Core must not be contacted'); },
        retryDelaysMs: [0],
      });
      expect(result).toMatchObject({
        ok: false,
        error: 'Refusing to control a foreign zylos-executor PM2 registration.',
      });
      expect(commands).toEqual([['pm2', ['jlist']]]);
    }
  });

  test('init reconciliation replaces a healthy old identity with the deployed executor config', async () => {
    const commands = [];
    const replies = [
      health('executor-before-init'),
      health('executor-before-init'),
      shutdown('executor-before-init'),
      health('executor-after-init'),
    ];
    await expect(reconcileExecutorService({
      zylosDir: '/tmp/zylos-cli-fixture',
      execFileSyncFn: pm2Fixture(commands),
      requestFn: async () => replies.shift(),
      retryDelaysMs: [0],
    })).resolves.toMatchObject({
      ok: true,
      previousServiceInstanceId: 'executor-before-init',
      serviceInstanceId: 'executor-after-init',
    });
    expect(commands[1]).toEqual([
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
      process.stdout.write(JSON.stringify(config.apps.map((app) => ({
        name: app.name, script: app.script, interpreter: app.interpreter,
      }))));
    `], {
      encoding: 'utf8',
      env: { ...process.env, HOME: home, ZYLOS_DIR: zylosDir, ZYLOS_PACKAGE_ROOT: process.cwd() },
    });
    fs.rmSync(home, { recursive: true, force: true });

    expect(child.status).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual([{
      name: EXECUTOR_SERVICE_NAME,
      script: path.resolve('runtime/executor/launcher.js'),
      interpreter: 'none',
    }]);
  });
});

describe('installed CLI release dispatcher', () => {
  test('runs when invoked through the installed npm bin symlink', () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-cli-bin-link-'));
    const target = path.join(directory, 'target');
    const zylosDir = path.join(directory, 'installation');
    const binDir = path.join(directory, 'bin');
    fs.mkdirSync(path.join(target, 'cli'), { recursive: true });
    fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
    fs.mkdirSync(binDir, { recursive: true });
    fs.writeFileSync(
      path.join(target, 'cli', 'zylos.js'),
      '#!/usr/bin/env node\nconsole.log(`forwarded:${process.argv[2]}`);\n',
      { mode: 0o755 },
    );
    fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
      release_ref: 'release-B', release_path: target,
    }));
    const command = path.join(binDir, 'zylos');
    fs.symlinkSync(path.resolve('cli/launcher.js'), command);
    try {
      const child = spawnSync(command, ['probe'], {
        encoding: 'utf8', env: { ...process.env, ZYLOS_DIR: zylosDir },
      });
      expect(child.status).toBe(0);
      expect(child.stdout.trim()).toBe('forwarded:probe');
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

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

describe('installed executor release dispatcher', () => {
  test('can ignore active release for Docker sidecar manifests', () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-executor-release-'));
    const current = path.join(directory, 'current');
    const target = path.join(directory, 'target');
    const zylosDir = path.join(directory, 'installation');
    for (const release of [current, target]) {
      fs.mkdirSync(path.join(release, 'runtime', 'executor'), { recursive: true });
      fs.writeFileSync(path.join(release, 'runtime', 'executor', 'daemon.js'), '#!/usr/bin/env node\n');
    }
    fs.mkdirSync(path.join(zylosDir, 'runtime'), { recursive: true });
    fs.writeFileSync(path.join(zylosDir, 'runtime', 'active-release.json'), JSON.stringify({
      release_ref: 'release-B',
      release_path: target,
      upgrade_id: 'upgrade-B',
    }));

    expect(resolveActiveRelease({ zylosDir, packageRoot: current, useActiveRelease: false }))
      .toMatchObject({
        entry: path.join(current, 'runtime', 'executor', 'daemon.js'),
        releaseRef: null,
        upgradeId: null,
      });
    expect(resolveActiveRelease({ zylosDir, packageRoot: current }))
      .toMatchObject({
        entry: path.join(target, 'runtime', 'executor', 'daemon.js'),
        releaseRef: 'release-B',
        upgradeId: 'upgrade-B',
      });
    fs.rmSync(directory, { recursive: true, force: true });
  });
});
