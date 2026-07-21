import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, test } from 'node:test';

import Database from 'better-sqlite3';

import {
  recordScheduledAdmission,
  recoverLegacyRunningTasks,
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
  test('adopts an exact durable Core occurrence after restart without replay or duplicate history', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-adopt-'));
    const schedulerPath = path.join(directory, 'scheduler.db');
    const corePath = path.join(directory, 'core.db');
    let scheduler = database(schedulerPath);
    scheduler.prepare(`
      UPDATE tasks SET status = 'running', requires_reconfiguration = 1
      WHERE id = 'task-A'
    `).run();
    const core = new Database(corePath);
    core.exec(`
      CREATE TABLE runtime_scheduler_occurrences (
        schedule_id TEXT NOT NULL, occurrence_id TEXT NOT NULL, task_id TEXT NOT NULL,
        turn_id TEXT NOT NULL, status TEXT NOT NULL, envelope_json TEXT NOT NULL,
        PRIMARY KEY (schedule_id, occurrence_id)
      );
      CREATE TABLE runtime_turns (turn_id TEXT PRIMARY KEY, state TEXT NOT NULL);
      CREATE TABLE runtime_turn_queue (turn_id TEXT PRIMARY KEY, wait_reason TEXT);
    `);
    core.prepare(`
      INSERT INTO runtime_turns VALUES ('turn-adopted', 'queued')
    `).run();
    core.prepare(`
      INSERT INTO runtime_turn_queue VALUES ('turn-adopted', 'queue')
    `).run();
    core.prepare(`
      INSERT INTO runtime_scheduler_occurrences VALUES (?, ?, ?, ?, 'accepted', ?)
    `).run(
      'task-A', 'task-A:100', 'task-A', 'turn-adopted',
      JSON.stringify({ schedule: {
        schedule_id: 'task-A', occurrence_id: 'task-A:100', task_id: 'task-A',
      } }),
    );

    assert.deepEqual(recoverLegacyRunningTasks(scheduler, core, { now: () => 10 }), {
      adopted: 1, paused: 0, terminal: 0,
    });
    assert.deepEqual(scheduler.prepare(`
      SELECT status, current_occurrence_id, current_turn_id, last_core_state,
             core_wait_reason, requires_reconfiguration
      FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'running', current_occurrence_id: 'task-A:100', current_turn_id: 'turn-adopted',
      last_core_state: 'queued', core_wait_reason: 'queue', requires_reconfiguration: 1,
    });
    assert.deepEqual(scheduler.prepare(`
      SELECT occurrence_id, turn_id, status FROM task_history
    `).get(), { occurrence_id: 'task-A:100', turn_id: 'turn-adopted', status: 'started' });
    scheduler.close();
    scheduler = new Database(schedulerPath);
    assert.deepEqual(recoverLegacyRunningTasks(scheduler, core, { now: () => 20 }), {
      adopted: 0, paused: 0, terminal: 0,
    });
    assert.equal(scheduler.prepare('SELECT COUNT(*) AS count FROM task_history').get().count, 1);

    const maintenance = snapshot('queued', { maintenance: true });
    maintenance.turns.items[0].turn_id = 'turn-adopted';
    assert.deepEqual(reconcileRunningTasks(scheduler, maintenance, { now: () => 30 }), {
      pending: 1, terminal: 0, unavailable: 0,
    });
    assert.equal(
      scheduler.prepare("SELECT status FROM tasks WHERE id = 'task-A'").get().status,
      'running',
    );

    const terminal = snapshot('completed');
    terminal.turns.items[0].turn_id = 'turn-adopted';
    assert.deepEqual(reconcileRunningTasks(scheduler, terminal, { now: () => 40 }), {
      pending: 0, terminal: 1, unavailable: 0,
    });
    assert.deepEqual(scheduler.prepare(`
      SELECT status, requires_reconfiguration, last_core_state
      FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'paused', requires_reconfiguration: 1, last_core_state: 'completed',
    });
    assert.equal(
      scheduler.prepare("SELECT status FROM task_history WHERE task_id = 'task-A'").get().status,
      'success',
    );
    scheduler.close();
    core.close();
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('settles an already-terminal durable Core occurrence during migration recovery', () => {
    const scheduler = database();
    scheduler.prepare(`
      UPDATE tasks SET status = 'running', requires_reconfiguration = 1
      WHERE id = 'task-A'
    `).run();
    const core = new Database(':memory:');
    core.exec(`
      CREATE TABLE runtime_scheduler_occurrences (
        schedule_id TEXT NOT NULL, occurrence_id TEXT NOT NULL, task_id TEXT NOT NULL,
        turn_id TEXT NOT NULL, status TEXT NOT NULL, envelope_json TEXT NOT NULL,
        PRIMARY KEY (schedule_id, occurrence_id)
      );
      CREATE TABLE runtime_turns (turn_id TEXT PRIMARY KEY, state TEXT NOT NULL);
      CREATE TABLE runtime_turn_queue (turn_id TEXT PRIMARY KEY, wait_reason TEXT);
      INSERT INTO runtime_turns VALUES ('turn-terminal', 'completed');
    `);
    core.prepare(`
      INSERT INTO runtime_scheduler_occurrences VALUES (?, ?, ?, ?, 'accepted', ?)
    `).run(
      'task-A', 'task-A:100', 'task-A', 'turn-terminal',
      JSON.stringify({ schedule: {
        schedule_id: 'task-A', occurrence_id: 'task-A:100', task_id: 'task-A',
      } }),
    );

    assert.deepEqual(recoverLegacyRunningTasks(scheduler, core, { now: () => 25 }), {
      adopted: 1, paused: 0, terminal: 1,
    });
    assert.deepEqual(scheduler.prepare(`
      SELECT status, current_turn_id, last_core_state, requires_reconfiguration
      FROM tasks WHERE id = 'task-A'
    `).get(), {
      status: 'paused', current_turn_id: 'turn-terminal', last_core_state: 'completed',
      requires_reconfiguration: 1,
    });
    assert.deepEqual(scheduler.prepare(`
      SELECT status, completed_at FROM task_history WHERE task_id = 'task-A'
    `).get(), { status: 'success', completed_at: 25 });
    scheduler.close();
    core.close();
  });

  for (const scenario of ['absent', 'mismatched', 'rejected']) {
    test(`pauses a legacy running task when the durable Core occurrence is ${scenario}`, () => {
      const scheduler = database();
      scheduler.prepare("UPDATE tasks SET status = 'running' WHERE id = 'task-A'").run();
      const core = new Database(':memory:');
      core.exec(`
        CREATE TABLE runtime_scheduler_occurrences (
          schedule_id TEXT NOT NULL, occurrence_id TEXT NOT NULL, task_id TEXT NOT NULL,
          turn_id TEXT, status TEXT NOT NULL, envelope_json TEXT NOT NULL,
          PRIMARY KEY (schedule_id, occurrence_id)
        );
        CREATE TABLE runtime_turns (turn_id TEXT PRIMARY KEY, state TEXT NOT NULL);
        CREATE TABLE runtime_turn_queue (turn_id TEXT PRIMARY KEY, wait_reason TEXT);
      `);
      if (scenario !== 'absent') {
        core.prepare(`INSERT INTO runtime_turns VALUES ('turn-core', 'queued')`).run();
        core.prepare(`
          INSERT INTO runtime_scheduler_occurrences VALUES (?, ?, ?, 'turn-core', ?, ?)
        `).run(
          'task-A', 'task-A:100', scenario === 'mismatched' ? 'other-task' : 'task-A',
          scenario === 'rejected' ? 'rejected' : 'accepted',
          JSON.stringify({ schedule: {
            schedule_id: 'task-A', occurrence_id: 'task-A:100',
            task_id: scenario === 'mismatched' ? 'other-task' : 'task-A',
          } }),
        );
      }
      assert.deepEqual(recoverLegacyRunningTasks(scheduler, core, { now: () => 10 }), {
        adopted: 0, paused: 1, terminal: 0,
      });
      const task = scheduler.prepare(`
        SELECT status, requires_reconfiguration, current_occurrence_id,
               current_turn_id, last_error FROM tasks WHERE id = 'task-A'
      `).get();
      assert.equal(task.status, 'paused');
      assert.equal(task.requires_reconfiguration, 1);
      assert.equal(task.current_occurrence_id, 'task-A:100');
      assert.equal(task.current_turn_id, null);
      assert.match(task.last_error, /exact admitted Core occurrence.*replay is forbidden/i);
      assert.equal(scheduler.prepare('SELECT COUNT(*) AS count FROM task_history').get().count, 0);
      scheduler.close();
      core.close();
    });
  }

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
