/**
 * Database Layer
 * SQLite-based persistence for tasks and execution history
 */

import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import os from 'os';

// Data goes to ~/zylos/scheduler/, code stays in skills directory
const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
const DATA_DIR = path.join(ZYLOS_DIR, 'scheduler');
const DB_PATH = path.join(DATA_DIR, 'scheduler.db');
const HISTORY_RETENTION_DAYS = 30;

let db = null;

export function getDb() {
  if (!db) {
    // Ensure data directory exists
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }

    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');  // Better concurrent access
    initSchema();
  }
  return db;
}

function initSchema() {
  // Create table if not exists
  db.exec(`
    -- Main tasks table
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      prompt TEXT NOT NULL,

      -- Scheduling
      type TEXT NOT NULL CHECK(type IN ('one-time', 'recurring', 'interval')),
      cron_expression TEXT,
      interval_seconds INTEGER,
      timezone TEXT DEFAULT 'UTC',

      -- Timing
      next_run_at INTEGER NOT NULL,
      last_run_at INTEGER,

      -- Priority & Status
      priority INTEGER DEFAULT 3 CHECK(priority BETWEEN 1 AND 3),
      status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed', 'paused')),

      miss_threshold INTEGER DEFAULT 300,       -- seconds: skip if overdue by more than this

      -- Complete durable Core conversation identity, or NULL for a synthetic schedule conversation
      bound_conversation_json TEXT DEFAULT NULL,

      -- Retry Logic (reserved, not currently used)
      -- Implicit retry is handled via miss_threshold: tasks stay pending
      -- until dispatched or overdue beyond miss_threshold window (default 300s).
      -- See daemon.js mainLoop + handleMissedTasks for details.
      retry_count INTEGER DEFAULT 0,
      max_retries INTEGER DEFAULT 3,

      -- Metadata
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,

      -- Error Tracking
      last_error TEXT,
      failed_at INTEGER,

      -- Durable Core occurrence projection
      current_occurrence_id TEXT,
      current_turn_id TEXT,
      last_core_state TEXT,
      core_wait_reason TEXT,
      missed_notice_attempt INTEGER NOT NULL DEFAULT 1
        CHECK(missed_notice_attempt > 0),
      missed_notice_retry_at INTEGER,
      requires_reconfiguration INTEGER NOT NULL DEFAULT 0
        CHECK(requires_reconfiguration IN (0, 1))
    );

    -- Critical indexes for performance
    CREATE INDEX IF NOT EXISTS idx_next_run ON tasks(next_run_at) WHERE status = 'pending';
    CREATE INDEX IF NOT EXISTS idx_status_priority ON tasks(status, priority);
    CREATE INDEX IF NOT EXISTS idx_type ON tasks(type);

    -- Execution history
    CREATE TABLE IF NOT EXISTS task_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id TEXT NOT NULL,
      occurrence_id TEXT,
      turn_id TEXT,
      executed_at INTEGER NOT NULL,
      completed_at INTEGER,
      status TEXT NOT NULL CHECK(status IN ('started', 'success', 'failed', 'timeout')),
      duration_ms INTEGER,
      error TEXT,

      FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_history_task ON task_history(task_id);
    CREATE INDEX IF NOT EXISTS idx_history_time ON task_history(executed_at);

    -- System state (for tracking scheduler status, etc.)
    CREATE TABLE IF NOT EXISTS system_state (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at INTEGER
    );
  `);

  const taskColumns = new Set(db.prepare('PRAGMA table_info(tasks)').all().map(({ name }) => name));
  for (const [name, definition] of [
    ['bound_conversation_json', 'TEXT DEFAULT NULL'],
    ['current_occurrence_id', 'TEXT DEFAULT NULL'],
    ['current_turn_id', 'TEXT DEFAULT NULL'],
    ['last_core_state', 'TEXT DEFAULT NULL'],
    ['core_wait_reason', 'TEXT DEFAULT NULL'],
    ['missed_notice_attempt', 'INTEGER NOT NULL DEFAULT 1'],
    ['missed_notice_retry_at', 'INTEGER DEFAULT NULL'],
    ['requires_reconfiguration', 'INTEGER NOT NULL DEFAULT 0'],
  ]) {
    if (!taskColumns.has(name)) db.exec(`ALTER TABLE tasks ADD COLUMN ${name} ${definition}`);
  }
  const legacyControls = ['require_idle', 'reply_channel', 'reply_endpoint']
    .filter((name) => taskColumns.has(name));
  if (legacyControls.length > 0) {
    const predicates = [];
    if (taskColumns.has('require_idle')) predicates.push('COALESCE(require_idle, 0) != 0');
    if (taskColumns.has('reply_channel')) predicates.push('reply_channel IS NOT NULL');
    if (taskColumns.has('reply_endpoint')) predicates.push('reply_endpoint IS NOT NULL');
    const assignments = [
      `status = CASE
        WHEN status = 'running' THEN 'running'
        WHEN status IN ('pending', 'paused') THEN 'paused'
        WHEN status = 'completed' AND type IN ('recurring', 'interval') THEN 'paused'
        ELSE status
      END`,
      'requires_reconfiguration = 1',
      "last_error = 'Paused during migration: retired scheduler controls require explicit canonical reconfiguration.'",
    ];
    if (taskColumns.has('require_idle')) assignments.push('require_idle = 0');
    if (taskColumns.has('reply_channel')) assignments.push('reply_channel = NULL');
    if (taskColumns.has('reply_endpoint')) assignments.push('reply_endpoint = NULL');
    db.prepare(`
      UPDATE tasks SET ${assignments.join(', ')}
      WHERE (${predicates.join(' OR ')})
    `).run();
  }
  const historyColumns = new Set(
    db.prepare('PRAGMA table_info(task_history)').all().map(({ name }) => name),
  );
  for (const name of ['occurrence_id', 'turn_id']) {
    if (!historyColumns.has(name)) {
      db.exec(`ALTER TABLE task_history ADD COLUMN ${name} TEXT DEFAULT NULL`);
    }
  }
}

// Clean up old history entries (older than HISTORY_RETENTION_DAYS)
export function cleanupHistory() {
  const cutoff = Math.floor(Date.now() / 1000) - (HISTORY_RETENTION_DAYS * 24 * 60 * 60);
  const result = db.prepare('DELETE FROM task_history WHERE executed_at < ?').run(cutoff);
  return result.changes;
}

// Generate a unique task ID
export function generateId() {
  return 'task-' + Date.now().toString(36) + '-' + Math.random().toString(36).substring(2, 8);
}

// Get current Unix timestamp
export function now() {
  return Math.floor(Date.now() / 1000);
}
