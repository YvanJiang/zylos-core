import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { describe, it } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  updateNextRunTime,
  processCompletedTasks,
  recordMissedNoticeTerminalRejection,
  scheduleMissedNoticeRetry,
} from '../daemon-tasks.js';
import { now } from '../database.js';

async function withDb(fn) {
  const originalZylosDir = process.env.ZYLOS_DIR;
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-daemon-'));
  try {
    process.env.ZYLOS_DIR = tmpDir;
    const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const { getDb } = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
    const db = getDb();
    try {
      await fn(db);
    } finally {
      db.close();
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    if (originalZylosDir === undefined) {
      delete process.env.ZYLOS_DIR;
    } else {
      process.env.ZYLOS_DIR = originalZylosDir;
    }
  }
}

function insertTask(db, overrides = {}) {
  const currentTime = now();
  const defaults = {
    id: `task-test-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
    name: 'test task',
    prompt: 'test prompt',
    type: 'recurring',
    cron_expression: '0 9 * * *',
    interval_seconds: null,
    timezone: 'UTC',
    next_run_at: currentTime + 3600,
    priority: 3,
    status: 'pending',
    miss_threshold: 300,
    created_at: currentTime,
    updated_at: currentTime,
    last_error: null,
  };
  const task = { ...defaults, ...overrides };
  db.prepare(`
    INSERT INTO tasks (id, name, prompt, type, cron_expression, interval_seconds, timezone,
      next_run_at, priority, status, miss_threshold,
      created_at, updated_at, last_error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    task.id, task.name, task.prompt, task.type, task.cron_expression, task.interval_seconds,
    task.timezone, task.next_run_at, task.priority, task.status,
    task.miss_threshold,
    task.created_at, task.updated_at, task.last_error
  );
  return task;
}

// ---- updateNextRunTime ----

describe('updateNextRunTime', () => {
  it('schedules next cron run for recurring task', async () => {
    await withDb((db) => {
      const task = insertTask(db, { type: 'recurring', cron_expression: '0 9 * * *', timezone: 'UTC', status: 'completed' });
      updateNextRunTime(db, task);

      const updated = db.prepare('SELECT status, next_run_at FROM tasks WHERE id = ?').get(task.id);
      assert.equal(updated.status, 'pending');
      assert.ok(updated.next_run_at > now(), 'next_run_at should be in the future');
    });
  });

  it('schedules next interval run for interval task', async () => {
    await withDb((db) => {
      const task = insertTask(db, { type: 'interval', cron_expression: null, interval_seconds: 7200, status: 'completed' });
      updateNextRunTime(db, task);

      const updated = db.prepare('SELECT status, next_run_at FROM tasks WHERE id = ?').get(task.id);
      assert.equal(updated.status, 'pending');
      const expectedMin = now() + 7200 - 2;
      const expectedMax = now() + 7200 + 2;
      assert.ok(updated.next_run_at >= expectedMin && updated.next_run_at <= expectedMax,
        `expected next_run_at ~${now() + 7200}, got ${updated.next_run_at}`);
    });
  });

  it('does nothing for one-time task', async () => {
    await withDb((db) => {
      const originalNextRun = now() + 1000;
      const task = insertTask(db, { type: 'one-time', cron_expression: null, next_run_at: originalNextRun, status: 'completed' });
      updateNextRunTime(db, task);

      const updated = db.prepare('SELECT status, next_run_at FROM tasks WHERE id = ?').get(task.id);
      // Should remain unchanged
      assert.equal(updated.status, 'completed');
      assert.equal(updated.next_run_at, originalNextRun);
    });
  });

  for (const scenario of [
    {
      label: 'before Shanghai and UTC 09:00',
      reference: '2026-07-21T00:30:00.000Z',
      expected: { shanghai: 1784595600, utc: 1784624400 },
    },
    {
      label: 'after Shanghai but before UTC 09:00',
      reference: '2026-07-21T02:00:00.000Z',
      expected: { shanghai: 1784682000, utc: 1784624400 },
    },
  ]) {
    it(`uses task timezone for cron calculation ${scenario.label}`, async () => {
      await withDb((db) => {
        const task1 = insertTask(db, { id: 'task-utc', type: 'recurring', cron_expression: '0 9 * * *', timezone: 'UTC', status: 'completed' });
        const task2 = insertTask(db, { id: 'task-sh', type: 'recurring', cron_expression: '0 9 * * *', timezone: 'Asia/Shanghai', status: 'completed' });
        const fromDate = new Date(scenario.reference);

        updateNextRunTime(db, task1, { fromDate });
        updateNextRunTime(db, task2, { fromDate });

        const utcRow = db.prepare('SELECT next_run_at FROM tasks WHERE id = ?').get('task-utc');
        const shRow = db.prepare('SELECT next_run_at FROM tasks WHERE id = ?').get('task-sh');

        assert.deepEqual({ shanghai: shRow.next_run_at, utc: utcRow.next_run_at }, scenario.expected);
      });
    });
  }
});

// ---- processCompletedTasks ----

describe('processCompletedTasks', () => {
  it('reschedules completed recurring task', async () => {
    await withDb((db) => {
      insertTask(db, { type: 'recurring', cron_expression: '0 9 * * *', status: 'completed' });
      processCompletedTasks(db);

      const task = db.prepare('SELECT status FROM tasks LIMIT 1').get();
      assert.equal(task.status, 'pending');
    });
  });

  it('reschedules completed interval task', async () => {
    await withDb((db) => {
      insertTask(db, { type: 'interval', cron_expression: null, interval_seconds: 3600, status: 'completed' });
      processCompletedTasks(db);

      const task = db.prepare('SELECT status, next_run_at FROM tasks LIMIT 1').get();
      assert.equal(task.status, 'pending');
      assert.ok(task.next_run_at > now());
    });
  });

  it('leaves completed one-time task unchanged', async () => {
    await withDb((db) => {
      insertTask(db, { type: 'one-time', cron_expression: null, status: 'completed' });
      processCompletedTasks(db);

      const task = db.prepare('SELECT status FROM tasks LIMIT 1').get();
      assert.equal(task.status, 'completed');
    });
  });

  it('marks task as failed if rescheduling throws', async () => {
    await withDb((db) => {
      insertTask(db, { type: 'recurring', cron_expression: 'invalid cron expr', status: 'completed' });
      processCompletedTasks(db);

      const task = db.prepare('SELECT status, last_error FROM tasks LIMIT 1').get();
      assert.equal(task.status, 'failed');
      assert.ok(task.last_error.includes('Invalid cron'));
    });
  });

  it('handles multiple completed tasks', async () => {
    await withDb((db) => {
      insertTask(db, { id: 'task-a', type: 'recurring', cron_expression: '0 9 * * *', status: 'completed' });
      insertTask(db, { id: 'task-b', type: 'one-time', cron_expression: null, status: 'completed' });
      insertTask(db, { id: 'task-c', type: 'interval', cron_expression: null, interval_seconds: 1800, status: 'completed' });

      processCompletedTasks(db);

      const a = db.prepare('SELECT status FROM tasks WHERE id = ?').get('task-a');
      const b = db.prepare('SELECT status FROM tasks WHERE id = ?').get('task-b');
      const c = db.prepare('SELECT status FROM tasks WHERE id = ?').get('task-c');

      assert.equal(a.status, 'pending');
      assert.equal(b.status, 'completed');  // one-time stays completed
      assert.equal(c.status, 'pending');
    });
  });
});

describe('scheduler daemon failure backoff', () => {
  it('durably advances only queue-full notice attempts with exponential backoff', async () => {
    await withDb((database) => {
      const task = insertTask(database, { id: 'queue-full-backoff' });
      const first = scheduleMissedNoticeRetry(database, task, { code: 'queue_full' }, {
        now: () => 1000,
      });
      assert.deepEqual(first, {
        recorded: true, attempt: 2, retry_at: 1005, delay_seconds: 5,
      });
      assert.deepEqual(database.prepare(`
        SELECT missed_notice_attempt, missed_notice_retry_at, status
        FROM tasks WHERE id = ?
      `).get(task.id), {
        missed_notice_attempt: 2,
        missed_notice_retry_at: 1005,
        status: 'pending',
      });
      assert.equal(scheduleMissedNoticeRetry(database, task, { code: 'not_retryable' }), null);
    });
  });

  it('terminalizes a non-retryable missed-notice rejection without a polling loop', async () => {
    await withDb((database) => {
      const task = insertTask(database, {
        id: 'non-retryable-notice', next_run_at: 100, missed_notice_attempt: 1,
      });
      assert.equal(recordMissedNoticeTerminalRejection(database, task, {
        status: 'rejected', turn_id: null,
        error: { code: 'idempotency_conflict', user_message: 'Occurrence payload conflict.' },
      }, { now: () => 1000 }), true);
      assert.deepEqual(database.prepare(`
        SELECT status, current_occurrence_id, current_turn_id,
               last_core_state, last_error FROM tasks WHERE id = ?
      `).get(task.id), {
        status: 'failed',
        current_occurrence_id: 'non-retryable-notice:100:missed-notice:1',
        current_turn_id: null,
        last_core_state: 'failed',
        last_error: 'Occurrence payload conflict.',
      });
      assert.deepEqual(database.prepare(`
        SELECT occurrence_id, turn_id, status, error FROM task_history WHERE task_id = ?
      `).get(task.id), {
        occurrence_id: 'non-retryable-notice:100:missed-notice:1',
        turn_id: null, status: 'failed', error: 'Occurrence payload conflict.',
      });
      assert.equal(recordMissedNoticeTerminalRejection(database, task, {
        status: 'rejected', turn_id: null,
        error: { code: 'idempotency_conflict', user_message: 'Occurrence payload conflict.' },
      }, { now: () => 1001 }), false);
      assert.equal(
        database.prepare('SELECT COUNT(*) AS count FROM task_history WHERE task_id = ?')
          .get(task.id).count,
        1,
      );
    });
  });

  it('keeps a missed occurrence pending but sleeps after persistent Core notice failure', async () => {
    const originalZylosDir = process.env.ZYLOS_DIR;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-loop-'));
    let child;
    try {
      process.env.ZYLOS_DIR = tmpDir;
      const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const { getDb } = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
      const database = getDb();
      insertTask(database, {
        id: 'persistent-notice-failure',
        type: 'recurring',
        cron_expression: '0 9 * * *',
        next_run_at: now() - 600,
        miss_threshold: 1,
      });
      database.close();

      child = spawn(process.execPath, [path.resolve('skills/scheduler/scripts/daemon.js')], {
        cwd: path.resolve('.'),
        env: { ...process.env, ZYLOS_DIR: tmpDir, TZ: 'UTC' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let output = '';
      child.stdout.on('data', (chunk) => { output += chunk.toString(); });
      child.stderr.on('data', (chunk) => { output += chunk.toString(); });
      await new Promise((resolve) => setTimeout(resolve, 750));

      assert.equal(child.exitCode, null, output);
      const failures = output.match(/Failed to persist missed-task notice/g) ?? [];
      assert.equal(failures.length, 1, output);

      const reopened = new (await import('better-sqlite3')).default(
        path.join(tmpDir, 'scheduler', 'scheduler.db'),
      );
      assert.equal(reopened.prepare('SELECT status FROM tasks WHERE id = ?')
        .get('persistent-notice-failure').status, 'pending');
      reopened.close();
    } finally {
      if (child && child.exitCode === null) {
        child.kill('SIGKILL');
        await new Promise((resolve) => child.once('close', resolve));
      }
      fs.rmSync(tmpDir, { recursive: true, force: true });
      if (originalZylosDir === undefined) {
        delete process.env.ZYLOS_DIR;
      } else {
        process.env.ZYLOS_DIR = originalZylosDir;
      }
    }
  });
});
