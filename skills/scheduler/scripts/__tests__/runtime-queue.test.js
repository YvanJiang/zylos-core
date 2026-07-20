import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import Database from 'better-sqlite3';

import { enqueueMissedScheduledTaskNotice, enqueueScheduledTask } from '../runtime.js';

test('the scheduler daemon admission seam persists one synthetic Core turn across a retry', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-scheduler-runtime-'));
  const database = new Database(path.join(directory, 'c4.db'));
  const task = {
    id: 'task-runtime-queue', prompt: 'Run the report.', next_run_at: 1_784_304_000,
  };
  const counts = new Map();
  const first = enqueueScheduledTask(database, task, {
    now: () => '2026-07-20T00:00:01.000Z',
    generateId(kind) {
      const next = (counts.get(kind) ?? 0) + 1;
      counts.set(kind, next);
      return `${kind}-first-${next}`;
    },
  });
  const replayed = enqueueScheduledTask(database, task, {
    now: () => '2026-07-20T00:00:02.000Z',
    generateId: () => { throw new Error('a scheduler retry must not allocate Core IDs'); },
  });
  assert.equal(first.status, 'accepted');
  assert.deepEqual(replayed, { ...first, deduplicated: true });
  assert.equal(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count, 1);
  assert.equal(database.prepare(`
    SELECT chat_id FROM runtime_conversations WHERE conversation_id = ?
  `).get(first.conversation_id).chat_id, 'scheduler:zylos:task-runtime-queue');
  const bound = enqueueScheduledTask(database, {
    id: 'task-bound-queue', prompt: 'Run the group report.', next_run_at: 1_784_304_001,
    bound_conversation_json: JSON.stringify({
      channel: 'telegram', chat_type: 'group', chat_id: 'group-runtime-queue',
      native_thread_or_topic_id: null, message_id: 'scheduler-anchor-runtime-queue',
    }),
  }, {
    now: () => '2026-07-20T00:00:03.000Z',
    generateId(kind) {
      const next = (counts.get(kind) ?? 0) + 1;
      counts.set(kind, next);
      return `${kind}-bound-${next}`;
    },
  });
  assert.deepEqual(database.prepare(`
    SELECT chat_type, chat_id FROM runtime_conversations WHERE conversation_id = ?
  `).get(bound.conversation_id), {
    chat_type: 'group',
    chat_id: 'group-runtime-queue',
  });
  database.close();
  fs.rmSync(directory, { recursive: true, force: true });
});

test('a missed occurrence persists one idempotent Core delivery notice instead of catch-up work', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-scheduler-runtime-'));
  const database = new Database(path.join(directory, 'c4.db'));
  const task = {
    id: 'task-missed-notice', name: 'Daily report', prompt: 'Run the report.',
    next_run_at: 1_784_304_000,
  };
  const notice = 'Scheduled task "Daily report" missed its occurrence and was skipped to avoid catch-up replay.';
  const ids = new Map();
  const first = enqueueMissedScheduledTaskNotice(database, task, notice, {
    now: () => '2026-07-20T00:00:01.000Z',
    generateId(kind) {
      const next = (ids.get(kind) ?? 0) + 1;
      ids.set(kind, next);
      return `${kind}-missed-notice-${next}`;
    },
  });
  const replayed = enqueueMissedScheduledTaskNotice(database, task, notice, {
    now: () => '2026-07-20T00:00:02.000Z',
    generateId: () => { throw new Error('a missed notice retry must not allocate Core IDs'); },
  });

  assert.equal(first.status, 'accepted');
  assert.deepEqual(replayed, { ...first, deduplicated: true });
  const command = JSON.parse(database.prepare(`
    SELECT command_json FROM runtime_outbox WHERE turn_id = ?
  `).get(first.turn_id).command_json);
  assert.equal(command.render_model.text, notice);
  assert.equal(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count, 1);
  database.close();
  fs.rmSync(directory, { recursive: true, force: true });
});

test('a queue-full missed notice converges on a new durable backoff attempt after capacity frees', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-scheduler-runtime-'));
  const database = new Database(path.join(directory, 'c4.db'));
  const baseTask = {
    id: 'task-missed-recovery', name: 'Recovery report', prompt: 'Run the report.',
  };
  for (let index = 0; index < 4; index += 1) {
    const admitted = enqueueScheduledTask(database, {
      ...baseTask, next_run_at: 1_784_304_000 + index,
    }, { maxQueuedTurns: 4 });
    assert.equal(admitted.status, 'accepted');
  }
  const notice = 'Recovery report missed its occurrence.';
  const firstAttemptTask = {
    ...baseTask, next_run_at: 1_784_304_100, missed_notice_attempt: 1,
  };
  const rejected = enqueueMissedScheduledTaskNotice(database, firstAttemptTask, notice, {
    maxQueuedTurns: 4,
  });
  assert.equal(rejected.status, 'rejected');
  assert.equal(rejected.error.code, 'queue_full');
  assert.equal(rejected.deduplicated, false);

  database.prepare(`
    UPDATE runtime_turn_queue SET status = 'completed'
    WHERE turn_id = (SELECT turn_id FROM runtime_turn_queue ORDER BY queue_sequence LIMIT 1)
  `).run();
  const secondAttemptTask = { ...firstAttemptTask, missed_notice_attempt: 2 };
  const accepted = enqueueMissedScheduledTaskNotice(database, secondAttemptTask, notice, {
    maxQueuedTurns: 4,
  });
  const replayed = enqueueMissedScheduledTaskNotice(database, secondAttemptTask, notice, {
    maxQueuedTurns: 4,
  });
  assert.equal(accepted.status, 'accepted');
  assert.deepEqual(replayed, { ...accepted, deduplicated: true });
  assert.equal(database.prepare(`
    SELECT COUNT(*) AS count FROM runtime_scheduler_occurrences
    WHERE occurrence_id LIKE 'task-missed-recovery:%:missed-notice:%'
  `).get().count, 2);
  database.close();
  fs.rmSync(directory, { recursive: true, force: true });
});
