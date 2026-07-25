#!/usr/bin/env node
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';

import { reconcileLegacyServicesForExecutorStart } from '../runtime/migration/installed-executor-upgrade.js';
import { assertExecutorStartFence } from '../runtime/executor/start-fence.js';

const MISSING_START_FENCE_MESSAGE = 'Executor startup requires a completed one-time runtime reconciliation.';

export function createDockerExecFileSync(exec = execFileSync) {
  return (command, args, options) => {
    if (command === 'pm2' && Array.isArray(args) && args[0] === 'jlist'
      && !args.includes('--silent')) {
      return exec(command, [...args, '--silent'], options);
    }
    return exec(command, args, options);
  };
}

function requireZylosDir(value) {
  const zylosDir = path.resolve(value || path.join(os.homedir(), 'zylos'));
  if (path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must not be a filesystem root');
  }
  return zylosDir;
}

export function prepareExecutorStart({
  zylosDir = process.argv[2] || process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos'),
  upgradeId = process.env.ZYLOS_DOCKER_START_FENCE_UPGRADE_ID || 'docker-entrypoint',
  assertStartFence = assertExecutorStartFence,
  reconcileLegacyServices = reconcileLegacyServicesForExecutorStart,
  execFileSyncFn = createDockerExecFileSync(),
} = {}) {
  const resolvedZylosDir = requireZylosDir(zylosDir);
  try {
    return Object.freeze({
      status: 'existing',
      fence: assertStartFence({ zylosDir: resolvedZylosDir }),
    });
  } catch (error) {
    if (error?.message !== MISSING_START_FENCE_MESSAGE) throw error;
  }

  const result = reconcileLegacyServices({
    zylosDir: resolvedZylosDir,
    upgradeId,
    execFileSyncFn,
  });
  return Object.freeze({ status: 'issued', ...result });
}

function main() {
  const result = prepareExecutorStart();
  if (result.status === 'existing') {
    console.log('[zylos] Executor start fence already present');
  } else {
    console.log(`[zylos] Executor start fence issued: ${result.executor_start_fence_path}`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(`[zylos] Failed to prepare executor start: ${error?.message ?? error}`);
    process.exit(1);
  }
}
