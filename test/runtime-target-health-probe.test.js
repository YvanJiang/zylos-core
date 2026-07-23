import { describe, expect, jest, test } from '@jest/globals';

import { publishTargetHealthSnapshot } from '../runtime/executor/health-probe.js';

describe('target executor health probe', () => {
  test('retries transient SQLite write contention within a bounded deadline', async () => {
    const locked = Object.assign(new Error('database is locked'), { code: 'SQLITE_BUSY' });
    const proof = { service: { health: 'healthy' } };
    const publishObservabilitySnapshot = jest.fn()
      .mockImplementationOnce(() => { throw locked; })
      .mockImplementationOnce(() => { throw locked; })
      .mockReturnValueOnce(proof);
    const sleep = jest.fn().mockResolvedValue(undefined);

    await expect(publishTargetHealthSnapshot(
      { publishObservabilitySnapshot },
      { now: () => 1_000, sleep, retryMs: 25, timeoutMs: 100 },
    )).resolves.toBe(proof);
    expect(publishObservabilitySnapshot).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenNthCalledWith(1, 25);
    expect(sleep).toHaveBeenNthCalledWith(2, 25);
  });

  test('does not retry non-lock failures', async () => {
    const failure = Object.assign(new Error('disk I/O error'), { code: 'SQLITE_IOERR' });
    const publishObservabilitySnapshot = jest.fn(() => { throw failure; });
    const sleep = jest.fn();

    await expect(publishTargetHealthSnapshot(
      { publishObservabilitySnapshot },
      { now: () => 1_000, sleep },
    )).rejects.toBe(failure);
    expect(publishObservabilitySnapshot).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  test('stops retrying a lock after the deadline', async () => {
    const locked = Object.assign(new Error('database is locked'), { code: 'SQLITE_LOCKED' });
    const publishObservabilitySnapshot = jest.fn(() => { throw locked; });
    const sleep = jest.fn().mockResolvedValue(undefined);
    const now = jest.fn()
      .mockReturnValueOnce(1_000)
      .mockReturnValueOnce(1_050)
      .mockReturnValueOnce(1_100);

    await expect(publishTargetHealthSnapshot(
      { publishObservabilitySnapshot },
      { now, sleep, retryMs: 25, timeoutMs: 100 },
    )).rejects.toBe(locked);
    expect(publishObservabilitySnapshot).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledTimes(1);
  });
});
