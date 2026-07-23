import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import {
  createDockerExecFileSync,
  prepareExecutorStart,
} from '../docker/prepare-executor-start.js';

const MISSING_START_FENCE_MESSAGE = 'Executor startup requires a completed one-time runtime reconciliation.';

describe('Docker executor start preparation', () => {
  test('keeps an existing executor start fence', () => {
    let reconciled = false;
    const result = prepareExecutorStart({
      zylosDir: '/tmp/zylos-test',
      assertStartFence: () => ({ issuance_kind: 'committed_reconciliation' }),
      reconcileLegacyServices: () => {
        reconciled = true;
      },
    });

    expect(result).toMatchObject({
      status: 'existing',
      fence: { issuance_kind: 'committed_reconciliation' },
    });
    expect(reconciled).toBe(false);
  });

  test('issues the start fence through legacy reconciliation when it is missing', () => {
    const calls = [];
    const result = prepareExecutorStart({
      zylosDir: '/tmp/zylos-test',
      upgradeId: 'docker-test',
      assertStartFence: () => {
        throw new Error(MISSING_START_FENCE_MESSAGE);
      },
      reconcileLegacyServices: (input) => {
        calls.push(input);
        return { executor_start_fence_path: path.join(input.zylosDir, 'runtime', 'executor-start-fence.json') };
      },
    });

    expect(result).toEqual({
      status: 'issued',
      executor_start_fence_path: '/tmp/zylos-test/runtime/executor-start-fence.json',
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({ zylosDir: '/tmp/zylos-test', upgradeId: 'docker-test' });
    expect(typeof calls[0].execFileSyncFn).toBe('function');
  });

  test('does not overwrite an invalid executor start fence', () => {
    let reconciled = false;
    expect(() => prepareExecutorStart({
      zylosDir: '/tmp/zylos-test',
      assertStartFence: () => {
        throw new Error('Executor startup reconciliation fence is invalid.');
      },
      reconcileLegacyServices: () => {
        reconciled = true;
      },
    })).toThrow('Executor startup reconciliation fence is invalid');
    expect(reconciled).toBe(false);
  });

  test('reads PM2 inventory in silent mode so first-run banners do not corrupt JSON', () => {
    const calls = [];
    const execFileSyncFn = createDockerExecFileSync((command, args, options) => {
      calls.push({ command, args, options });
      return '[]';
    });

    expect(execFileSyncFn('pm2', ['jlist'], { encoding: 'utf8' })).toBe('[]');
    expect(calls).toEqual([
      { command: 'pm2', args: ['jlist', '--silent'], options: { encoding: 'utf8' } },
    ]);
  });
});
