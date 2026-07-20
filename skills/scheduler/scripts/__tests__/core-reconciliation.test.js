import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, test } from 'node:test';

import Database from 'better-sqlite3';

import {
  recordScheduledAdmission,
  reconcileRunningTasks,
} from '../daemon-tasks.js';

const fixtures = JSON.parse(fs.readFileSync(
  new URL('../../../../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

function database(filename = ':memory:') {
  const db = new Database(filename);
  db.exec(`
    CREATE TABLE tasks (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, prompt TEXT NOT NULL,
      type TEXT NOT NULL, cron_expression TEXT, interval_seconds INTEGER,
      timezone TEXT, next_run_at INTEGER NOT NULL, last_run_at INTEGER,
      priority INTEGER, status TEXT, require_idle INTEGER, miss_threshold INTEGER,
      bound_conversation_json TEXT, created_at INTEGER, updated_at INTEGER,
      last_error TEXT, current_occurrence_id TEXT, current_turn_id TEXT,
      last_core_state TEXT, core_wait_reason TEXT, requires_reconfiguration INTEGER DEFAULT 0
    );
    CREATE TABLE task_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
      occurrence_id TEXT, turn_id TEXT, executed_at INTEGER NOT NULL,
      completed_at INTEGER, status TEXT NOT NULL, duration_ms INTEGER, error TEXT
    );
  `);
  db.prepare(`
    INSERT INTO tasks (
      id, name, prompt, type, timezone, next_run_at, priority, status,
      require_idle, miss_threshold, created_at, updated_at
    ) VALUES ('task-A', 'Task A', 'Do A', 'one-time', 'UTC', 100, 3, 'pending', 0, 300, 1, 1)
  `).run();
  return db;
}

function snapshot(state, { maintenance = false, error = null } = {}) {
  const value = structuredClone(fixtures.cases.complete);
  value.service.maintenance = maintenance;
  value.turns.items[0] = {
    ...value.turns.items[0],
    state,
    phase: state,
    queue_position: state === 'queued' ? 1 : null,
    side_effect_status: error?.side_effect_status ?? 'none',
    error,
  };
  return value;
}

describe('scheduler Core admission and reconciliation', () => {
  test('records the accepted occurrence and turn atomically and replays without duplicate history', () => {
    const db = database();
    const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    const admission = { status: 'accepted', turn_id: 'turn-A', deduplicated: false };

    assert.equal(recordScheduledAdmission(db, task, admission, { now: () => 10 }), true);
    assert.deepEqual(db.prepare(`
      SELECT status, current_occurrence_id, current_turn_id, last_core_state, core_wait_reason
      FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'running',
      current_occurrence_id: 'task-A:100',
      current_turn_id: 'turn-A',
      last_core_state: 'queued',
      core_wait_reason: 'queue',
    });
    assert.deepEqual(db.prepare(`
      SELECT occurrence_id, turn_id, status FROM task_history
    `).get(), { occurrence_id: 'task-A:100', turn_id: 'turn-A', status: 'started' });

    const running = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    assert.equal(recordScheduledAdmission(db, running, { ...admission, deduplicated: true }, {
      now: () => 11,
    }), true);
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM task_history').get().count, 1);
    db.close();
  });

  test('does not abandon old work while Core reports maintenance or visibility loss', () => {
    const db = database();
    const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    recordScheduledAdmission(db, task, { status: 'accepted', turn_id: 'turn-A' }, {
      now: () => 10,
    });
    db.prepare("UPDATE tasks SET updated_at = 1 WHERE id = 'task-A'").run();

    assert.deepEqual(reconcileRunningTasks(db, snapshot('queued', { maintenance: true }), {
      now: () => 9_999,
    }), { pending: 1, terminal: 0, unavailable: 0 });
    assert.deepEqual(db.prepare(`
      SELECT status, last_core_state, core_wait_reason FROM tasks WHERE id = 'task-A'
    `).get(), { status: 'running', last_core_state: 'queued', core_wait_reason: 'maintenance' });

    const invisible = snapshot('running');
    invisible.turns.items = [];
    assert.deepEqual(reconcileRunningTasks(db, invisible, { now: () => 20_000 }), {
      pending: 0, terminal: 0, unavailable: 1,
    });
    assert.equal(db.prepare("SELECT status FROM tasks WHERE id = 'task-A'").get().status, 'running');
    assert.equal(db.prepare("SELECT status FROM task_history WHERE task_id = 'task-A'").get().status, 'started');
    db.close();
  });

  test('persists a terminal idempotency conflict without inventing a turn id', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-conflict-'));
    const filename = path.join(directory, 'scheduler.db');
    const db = database(filename);
    const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    assert.equal(recordScheduledAdmission(db, task, {
      status: 'rejected',
      turn_id: null,
      error: { user_message: 'The occurrence payload conflicts with its durable identity.' },
    }, { now: () => 10 }), false);
    assert.deepEqual(db.prepare(`
      SELECT status, current_occurrence_id, current_turn_id, last_core_state, last_error
      FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'failed',
      current_occurrence_id: 'task-A:100',
      current_turn_id: null,
      last_core_state: 'failed',
      last_error: 'The occurrence payload conflicts with its durable identity.',
    });
    assert.deepEqual(db.prepare(`
      SELECT occurrence_id, turn_id, status FROM task_history
    `).get(), { occurrence_id: 'task-A:100', turn_id: null, status: 'failed' });
    db.close();
    const restarted = new Database(filename);
    const terminalTask = restarted.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    assert.equal(recordScheduledAdmission(restarted, terminalTask, {
      status: 'rejected', turn_id: null,
      error: { user_message: 'The occurrence payload conflicts with its durable identity.' },
    }, { now: () => 11 }), false);
    assert.equal(restarted.prepare('SELECT COUNT(*) AS count FROM task_history').get().count, 1);
    restarted.close();
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('terminalizes once only when the canonical turn is terminal', () => {
    const db = database();
    const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    recordScheduledAdmission(db, task, { status: 'accepted', turn_id: 'turn-A' }, {
      now: () => 10,
    });

    assert.deepEqual(reconcileRunningTasks(db, snapshot('completed'), { now: () => 20 }), {
      pending: 0, terminal: 1, unavailable: 0,
    });
    assert.deepEqual(db.prepare(`
      SELECT status, last_run_at, last_core_state, core_wait_reason FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'completed', last_run_at: 20, last_core_state: 'completed', core_wait_reason: null,
    });
    assert.deepEqual(db.prepare(`
      SELECT status, completed_at, duration_ms FROM task_history WHERE task_id = 'task-A'
    `).get(), { status: 'success', completed_at: 20, duration_ms: 10_000 });

    assert.deepEqual(reconcileRunningTasks(db, snapshot('completed'), { now: () => 30 }), {
      pending: 0, terminal: 0, unavailable: 0,
    });
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM task_history').get().count, 1);
    db.close();
  });

  test('finishes an admitted migrated turn before pausing future occurrences', () => {
    const db = database();
    const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get('task-A');
    recordScheduledAdmission(db, task, { status: 'accepted', turn_id: 'turn-A' }, {
      now: () => 10,
    });
    db.prepare(`
      UPDATE tasks SET requires_reconfiguration = 1,
        last_error = 'Paused during migration: retired scheduler controls require explicit canonical reconfiguration.'
      WHERE id = 'task-A'
    `).run();

    assert.deepEqual(reconcileRunningTasks(db, snapshot('completed'), { now: () => 20 }), {
      pending: 0, terminal: 1, unavailable: 0,
    });
    assert.deepEqual(db.prepare(`
      SELECT status, requires_reconfiguration, last_core_state FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'paused', requires_reconfiguration: 1, last_core_state: 'completed',
    });
    assert.equal(
      db.prepare("SELECT status FROM task_history WHERE task_id = 'task-A'").get().status,
      'success',
    );
    db.close();
  });
});
