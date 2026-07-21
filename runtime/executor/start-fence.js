import fs from 'node:fs';
import path from 'node:path';

const START_FENCE_CONTRACT = 'zylos.executor-start-fence@1';

export function executorStartFencePath(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  return path.join(zylosDir, 'runtime', 'executor-start-fence.json');
}

export function assertExecutorStartFence({ zylosDir, readFileSync = fs.readFileSync } = {}) {
  let fence;
  try {
    fence = JSON.parse(readFileSync(executorStartFencePath(zylosDir), 'utf8'));
  } catch {
    throw new Error('Executor startup requires a completed one-time runtime reconciliation.');
  }
  if (fence?.contract !== START_FENCE_CONTRACT
    || fence.runtime_generation !== 'executor_only'
    || typeof fence.reconciled_at !== 'string') {
    throw new Error('Executor startup reconciliation fence is invalid.');
  }
  return Object.freeze(fence);
}
