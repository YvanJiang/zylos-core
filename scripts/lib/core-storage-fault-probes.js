import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import Database from '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { initializeRuntimePersistence } from '../../runtime/persistence/schema.js';
import { acceptScheduledOccurrence } from '../../runtime/scheduler/scheduler-queue.js';

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function occurrence(suffix) {
  return {
    schedule_id: 'schedule-global47',
    task_id: 'task-global47',
    occurrence_id: `occurrence-${suffix}`,
    prompt: 'Run isolated Global47 storage acceptance work.',
    occurred_at: '2026-07-22T12:00:00.000Z',
    received_at: '2026-07-22T12:00:01.000Z',
    region: 'global',
    tenant_id: 'tenant-fixture',
    bot_id: 'bot-fixture',
    bound_conversation: {
      channel: 'telegram',
      chat_type: 'group',
      chat_id: 'chat-fixture',
      native_thread_or_topic_id: null,
      message_id: 'message-fixture',
      root_message_id: null,
    },
  };
}

function count(database, table, where = '') {
  return database.prepare(`SELECT COUNT(*) AS count FROM ${table} ${where}`).get().count;
}

function notification(database, turnId) {
  const row = database.prepare(`
    SELECT command_json
    FROM runtime_outbox
    WHERE turn_id = ?
    ORDER BY rowid ASC
    LIMIT 1
  `).get(turnId);
  const renderModel = JSON.parse(row.command_json).render_model;
  return Object.freeze({
    phase: renderModel.phase,
    terminal: renderModel.terminal,
    user_action_required: renderModel.user_action_required,
  });
}

function durableEvidence(database, accepted) {
  const turnCount = count(database, 'runtime_turns');
  return Object.freeze({
    notification: notification(database, accepted.turn_id),
    audit_metrics: Object.freeze({
      scheduler_audit_rows: count(database, 'runtime_scheduler_occurrences'),
      queue_rows: count(database, 'runtime_turn_queue'),
      outbox_rows: count(database, 'runtime_outbox'),
    }),
    backlog: Object.freeze({
      queued: count(database, 'runtime_turn_queue', "WHERE status = 'queued'"),
      duplicate_turns: Math.max(0, turnCount - 1),
    }),
  });
}

function withTemporaryDatabase(prefix, operation) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const databasePath = path.join(directory, 'core.db');
  let primary;
  let secondary;
  try {
    primary = new Database(databasePath);
    initializeRuntimePersistence(primary);
    secondary = new Database(databasePath);
    return operation({ primary, secondary });
  } finally {
    try { secondary?.exec('ROLLBACK'); } catch {}
    secondary?.close();
    primary?.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

export function runSchedulerBusyFaultProbe() {
  return withTemporaryDatabase('zylos-global47-scheduler-busy-', ({ primary, secondary }) => {
    primary.pragma('busy_timeout = 1');
    secondary.exec('BEGIN IMMEDIATE');
    let classification = null;
    try {
      acceptScheduledOccurrence(primary, occurrence('scheduler-busy'), {
        now: () => '2026-07-22T12:00:02.000Z',
        generateId: deterministicIds('scheduler-busy-blocked'),
      });
    } catch (error) {
      classification = error?.code ?? null;
    }
    const rowsAfterBusy = Object.freeze({
      inbound: count(primary, 'runtime_inbound_events'),
      turns: count(primary, 'runtime_turns'),
      scheduler_audit: count(primary, 'runtime_scheduler_occurrences'),
      outbox: count(primary, 'runtime_outbox'),
    });
    secondary.exec('COMMIT');

    const accepted = acceptScheduledOccurrence(primary, occurrence('scheduler-busy'), {
      now: () => '2026-07-22T12:00:03.000Z',
      generateId: deterministicIds('scheduler-busy-accepted'),
    });
    const replayed = acceptScheduledOccurrence(primary, occurrence('scheduler-busy'), {
      now: () => { throw new Error('a scheduler replay must not allocate a new timestamp'); },
      generateId: () => { throw new Error('a scheduler replay must not allocate identifiers'); },
    });
    const durable = durableEvidence(primary, accepted);
    return Object.freeze({
      classification,
      rows_after_busy: rowsAfterBusy,
      notification: durable.notification,
      recovery: Object.freeze({
        first_status: accepted.status,
        replay_status: replayed.status,
        replay_deduplicated: replayed.deduplicated,
      }),
      audit_metrics: durable.audit_metrics,
      backlog: durable.backlog,
    });
  });
}

export function runWalCheckpointBusyFaultProbe() {
  return withTemporaryDatabase('zylos-global47-checkpoint-busy-', ({ primary, secondary }) => {
    primary.pragma('busy_timeout = 1');
    secondary.exec('BEGIN');
    secondary.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get();

    const accepted = acceptScheduledOccurrence(primary, occurrence('checkpoint-busy'), {
      now: () => '2026-07-22T12:01:00.000Z',
      generateId: deterministicIds('checkpoint-busy'),
    });
    const pinnedCheckpoint = primary.pragma('wal_checkpoint(TRUNCATE)')[0];
    secondary.exec('COMMIT');
    const drainedCheckpoint = primary.pragma('wal_checkpoint(TRUNCATE)')[0];
    const durable = durableEvidence(primary, accepted);
    return Object.freeze({
      classification: pinnedCheckpoint.busy === 1
        ? 'SQLITE_CHECKPOINT_BUSY'
        : 'SQLITE_CHECKPOINT_UNEXPECTED',
      pinned_checkpoint: Object.freeze({ ...pinnedCheckpoint }),
      drained_checkpoint: Object.freeze({ ...drainedCheckpoint }),
      notification: durable.notification,
      audit_metrics: durable.audit_metrics,
      backlog: durable.backlog,
    });
  });
}
