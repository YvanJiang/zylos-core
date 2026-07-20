import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import Database from 'better-sqlite3';

import { generateId, now } from '../database.js';

describe('generateId', () => {
  it('starts with task- prefix', () => {
    const id = generateId();
    assert.ok(id.startsWith('task-'), `expected task- prefix: ${id}`);
  });

  it('generates unique IDs', () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateId()));
    assert.equal(ids.size, 100);
  });

  it('contains only alphanumeric and hyphens', () => {
    const id = generateId();
    assert.match(id, /^task-[a-z0-9]+-[a-z0-9]+$/);
  });
});

describe('now', () => {
  it('returns current Unix timestamp in seconds', () => {
    const timestamp = now();
    const expected = Math.floor(Date.now() / 1000);
    assert.ok(Math.abs(timestamp - expected) <= 1, `expected ~${expected}, got ${timestamp}`);
  });

  it('returns an integer', () => {
    assert.equal(Number.isInteger(now()), true);
  });
});

describe('getDb', () => {
  it('creates data directory and initializes schema', async () => {
    const originalZylosDir = process.env.ZYLOS_DIR;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-db-'));
    const dbPath = path.join(tmpDir, 'scheduler', 'scheduler.db');
    try {
      process.env.ZYLOS_DIR = tmpDir;

      // Dynamic import to pick up new ZYLOS_DIR
      const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const { getDb } = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
      const db = getDb();

      // Verify directory and file were created
      assert.ok(fs.existsSync(dbPath));

      // Verify schema: tasks table
      const tables = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
      ).all().map(t => t.name);
      assert.ok(tables.includes('tasks'));
      assert.ok(tables.includes('task_history'));
      assert.ok(tables.includes('system_state'));

      // Verify tasks table has timezone column
      const cols = db.prepare('PRAGMA table_info(tasks)').all().map(c => c.name);
      assert.ok(cols.includes('timezone'));
      assert.ok(cols.includes('next_run_at'));
      assert.ok(cols.includes('priority'));
      assert.ok(cols.includes('bound_conversation_json'));
      assert.ok(cols.includes('requires_reconfiguration'));
      assert.ok(!cols.includes('require_idle'));
      assert.ok(!cols.includes('reply_channel'));
      assert.ok(!cols.includes('reply_endpoint'));

      db.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      if (originalZylosDir === undefined) {
        delete process.env.ZYLOS_DIR;
      } else {
        process.env.ZYLOS_DIR = originalZylosDir;
      }
    }
  });

  it('pauses and clears active tasks carrying retired idle or primitive reply controls', async () => {
    const originalZylosDir = process.env.ZYLOS_DIR;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-db-legacy-controls-'));
    const schedulerDir = path.join(tmpDir, 'scheduler');
    fs.mkdirSync(schedulerDir, { recursive: true });
    const dbPath = path.join(schedulerDir, 'scheduler.db');
    const legacy = new Database(dbPath);
    legacy.exec(`
      CREATE TABLE tasks (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, prompt TEXT NOT NULL,
        type TEXT NOT NULL, cron_expression TEXT, interval_seconds INTEGER, timezone TEXT,
        next_run_at INTEGER NOT NULL, last_run_at INTEGER, priority INTEGER, status TEXT,
        require_idle INTEGER DEFAULT 0, miss_threshold INTEGER DEFAULT 300,
        reply_channel TEXT, reply_endpoint TEXT, bound_conversation_json TEXT,
        retry_count INTEGER DEFAULT 0, max_retries INTEGER DEFAULT 3,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_error TEXT, failed_at INTEGER
      );
      CREATE TABLE task_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, executed_at INTEGER NOT NULL,
        completed_at INTEGER, status TEXT NOT NULL, duration_ms INTEGER, error TEXT
      );
      CREATE TABLE system_state (key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER);
      INSERT INTO tasks (
        id, name, prompt, type, timezone, next_run_at, priority, status,
        require_idle, reply_channel, reply_endpoint, created_at, updated_at
      ) VALUES
        ('legacy-idle', 'legacy idle', 'work', 'recurring', 'UTC', 100, 3, 'pending', 1, NULL, NULL, 1, 1),
        ('legacy-reply', 'legacy reply', 'work', 'recurring', 'UTC', 100, 3, 'running', 0, 'lark', 'chat-only', 1, 1),
        ('canonical', 'canonical', 'work', 'recurring', 'UTC', 100, 3, 'pending', 0, NULL, NULL, 1, 1);
    `);
    legacy.close();

    try {
      process.env.ZYLOS_DIR = tmpDir;
      const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const { getDb } = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
      const db = getDb();
      const rows = db.prepare(`
        SELECT id, status, require_idle, reply_channel, reply_endpoint,
               requires_reconfiguration, last_error
        FROM tasks ORDER BY id
      `).all();
      assert.deepEqual(rows, [
        {
          id: 'canonical', status: 'pending', require_idle: 0,
          reply_channel: null, reply_endpoint: null,
          requires_reconfiguration: 0, last_error: null,
        },
        {
          id: 'legacy-idle', status: 'paused', require_idle: 0,
          reply_channel: null, reply_endpoint: null,
          requires_reconfiguration: 1,
          last_error: 'Paused during migration: retired scheduler controls require explicit canonical reconfiguration.',
        },
        {
          id: 'legacy-reply', status: 'running', require_idle: 0,
          reply_channel: null, reply_endpoint: null,
          requires_reconfiguration: 1,
          last_error: 'Paused during migration: retired scheduler controls require explicit canonical reconfiguration.',
        },
      ]);
      db.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      if (originalZylosDir === undefined) delete process.env.ZYLOS_DIR;
      else process.env.ZYLOS_DIR = originalZylosDir;
    }
  });
});

describe('cleanupHistory', () => {
  it('removes entries older than retention period', async () => {
    const originalZylosDir = process.env.ZYLOS_DIR;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-cleanup-'));
    try {
      process.env.ZYLOS_DIR = tmpDir;

      const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const mod = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
      const db = mod.getDb();

      // Insert a task first (foreign key constraint)
      const taskId = 'task-cleanup-test';
      const currentTime = mod.now();
      db.prepare(`
        INSERT INTO tasks (id, name, prompt, type, next_run_at, created_at, updated_at)
        VALUES (?, 'cleanup test', 'test', 'one-time', ?, ?, ?)
      `).run(taskId, currentTime, currentTime, currentTime);

      // Insert old history entry (60 days ago)
      const oldTime = currentTime - (60 * 24 * 60 * 60);
      db.prepare(`
        INSERT INTO task_history (task_id, executed_at, status)
        VALUES (?, ?, 'success')
      `).run(taskId, oldTime);

      // Insert recent history entry
      db.prepare(`
        INSERT INTO task_history (task_id, executed_at, status)
        VALUES (?, ?, 'success')
      `).run(taskId, currentTime);

      const deleted = mod.cleanupHistory();
      assert.equal(deleted, 1);

      const remaining = db.prepare('SELECT COUNT(*) as count FROM task_history').get();
      assert.equal(remaining.count, 1);

      db.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      if (originalZylosDir === undefined) {
        delete process.env.ZYLOS_DIR;
      } else {
        process.env.ZYLOS_DIR = originalZylosDir;
      }
    }
  });

  it('returns 0 when nothing to clean', async () => {
    const originalZylosDir = process.env.ZYLOS_DIR;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-cleanup-empty-'));
    try {
      process.env.ZYLOS_DIR = tmpDir;

      const cacheBuster = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const mod = await import(new URL(`../database.js?${cacheBuster}`, import.meta.url));
      const db = mod.getDb();

      const deleted = mod.cleanupHistory();
      assert.equal(deleted, 0);

      db.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      if (originalZylosDir === undefined) {
        delete process.env.ZYLOS_DIR;
      } else {
        process.env.ZYLOS_DIR = originalZylosDir;
      }
    }
  });
});
