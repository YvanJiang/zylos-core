import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import Database from 'better-sqlite3';

import { enqueueScheduledTask } from '../runtime.js';

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
