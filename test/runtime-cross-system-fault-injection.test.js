import { describe, expect, test } from '@jest/globals';

import {
  runSchedulerBusyFaultProbe,
  runWalCheckpointBusyFaultProbe,
} from '../scripts/lib/core-storage-fault-probes.js';

describe('Global47 public storage fault seams', () => {
  test('retries scheduler admission after an injected SQLite writer lock without losing or duplicating work', () => {
    const evidence = runSchedulerBusyFaultProbe();

    expect(evidence).toEqual({
      classification: 'SQLITE_BUSY',
      rows_after_busy: {
        inbound: 0,
        turns: 0,
        scheduler_audit: 0,
        outbox: 0,
      },
      notification: {
        phase: 'received',
        terminal: false,
        user_action_required: false,
      },
      recovery: {
        first_status: 'accepted',
        replay_status: 'accepted',
        replay_deduplicated: true,
      },
      audit_metrics: {
        scheduler_audit_rows: 1,
        queue_rows: 1,
        outbox_rows: 1,
      },
      backlog: {
        queued: 1,
        duplicate_turns: 0,
      },
    });
  });

  test('reports a busy WAL checkpoint while a reader is pinned and drains after release', () => {
    const evidence = runWalCheckpointBusyFaultProbe();

    expect(evidence.classification).toBe('SQLITE_CHECKPOINT_BUSY');
    expect(evidence.pinned_checkpoint).toMatchObject({ busy: 1 });
    expect(evidence.pinned_checkpoint.log).toBeGreaterThan(0);
    expect(evidence.drained_checkpoint).toEqual({ busy: 0, log: 0, checkpointed: 0 });
    expect(evidence.notification).toEqual({
      phase: 'received',
      terminal: false,
      user_action_required: false,
    });
    expect(evidence.audit_metrics).toEqual({
      scheduler_audit_rows: 1,
      queue_rows: 1,
      outbox_rows: 1,
    });
    expect(evidence.backlog).toEqual({ queued: 1, duplicate_turns: 0 });
  });
});
