/**
 * Daemon task processing logic
 * Extracted from daemon.js for testability
 */

import { now } from './database.js';
import { getNextRun } from './cron-utils.js';
import { formatTime } from './time-utils.js';
import { projectScheduledTurn } from '../../../runtime/scheduler/scheduler-observability.js';

function occurrenceId(task) {
  return `${task.id}:${task.next_run_at}`;
}

function exactCoreOccurrence(coreDb, task, occurrence) {
  let row;
  try {
    row = coreDb.prepare(`
      SELECT occurrence.schedule_id, occurrence.occurrence_id, occurrence.task_id,
             occurrence.turn_id, occurrence.status, occurrence.envelope_json,
             turn.state AS turn_state, queue.wait_reason
      FROM runtime_scheduler_occurrences AS occurrence
      JOIN runtime_turns AS turn ON turn.turn_id = occurrence.turn_id
      LEFT JOIN runtime_turn_queue AS queue ON queue.turn_id = occurrence.turn_id
      WHERE occurrence.schedule_id = ? AND occurrence.occurrence_id = ?
    `).get(task.id, occurrence);
  } catch (error) {
    if (error?.code === 'SQLITE_ERROR') return null;
    throw error;
  }
  if (!row || row.status !== 'accepted' || row.task_id !== task.id) return null;
  try {
    const schedule = JSON.parse(row.envelope_json)?.schedule;
    if (schedule?.schedule_id !== task.id
      || schedule?.task_id !== task.id
      || schedule?.occurrence_id !== occurrence) return null;
  } catch {
    return null;
  }
  return row;
}

const LEGACY_RECOVERY_BARRIER =
  'Paused during migration: no exact admitted Core occurrence; replay is forbidden.';

function pauseLegacyRecovery(schedulerDb, task, occurrence, recordedAt, reason) {
  return schedulerDb.transaction(() => {
    const update = schedulerDb.prepare(`
      UPDATE tasks
      SET status = 'paused', requires_reconfiguration = 1,
          requires_occurrence_advance = 1,
          current_occurrence_id = ?, current_turn_id = NULL,
          last_core_state = 'unknown', core_wait_reason = NULL,
          last_error = ?, updated_at = ?
      WHERE id = ? AND status = 'running' AND current_turn_id IS NULL
    `).run(occurrence, reason, recordedAt, task.id);
    if (update.changes === 0) return false;
    schedulerDb.prepare(`
      UPDATE task_history
      SET status = 'failed', completed_at = ?,
          duration_ms = (? - executed_at) * 1000, error = ?
      WHERE task_id = ? AND status = 'started'
    `).run(recordedAt, recordedAt, reason, task.id);
    return true;
  })();
}

/**
 * Recover a pre-migration local `running` row only by adopting the exact
 * durable Core occurrence. Absence or mismatch is a replay barrier: the task
 * is paused for explicit operator reconfiguration and is never redispatched.
 */
export function recoverLegacyRunningTasks(schedulerDb, coreDb, {
  now: clock = now,
} = {}) {
  const result = { adopted: 0, paused: 0, terminal: 0 };
  const tasks = schedulerDb.prepare(`
    SELECT * FROM tasks
    WHERE status = 'running' AND current_turn_id IS NULL
    ORDER BY updated_at ASC, id ASC
  `).all();
  for (const task of tasks) {
    const occurrence = occurrenceId(task);
    const core = exactCoreOccurrence(coreDb, task, occurrence);
    const recordedAt = clock();
    if (!core) {
      if (pauseLegacyRecovery(
        schedulerDb, task, occurrence, recordedAt, LEGACY_RECOVERY_BARRIER,
      )) result.paused += 1;
      continue;
    }
    const recovery = schedulerDb.transaction(() => {
      const startedRows = schedulerDb.prepare(`
        SELECT id, occurrence_id, turn_id
        FROM task_history
        WHERE task_id = ? AND status = 'started'
        ORDER BY id ASC
      `).all(task.id);
      const exactStarted = startedRows.length === 1
        && startedRows[0].occurrence_id === occurrence
        && startedRows[0].turn_id === core.turn_id;
      const legacyStarted = startedRows.length === 1
        && startedRows[0].occurrence_id === null
        && startedRows[0].turn_id === null;
      if (startedRows.length > 1 || (startedRows.length === 1 && !exactStarted && !legacyStarted)) {
        const reason = 'Paused during migration: ambiguous legacy started history; replay is forbidden.';
        const paused = schedulerDb.prepare(`
          UPDATE tasks
          SET status = 'paused', requires_reconfiguration = 1,
              requires_occurrence_advance = 1,
              current_occurrence_id = ?, current_turn_id = NULL,
              last_core_state = 'unknown', core_wait_reason = NULL,
              last_error = ?, updated_at = ?
          WHERE id = ? AND status = 'running' AND current_turn_id IS NULL
        `).run(occurrence, reason, recordedAt, task.id);
        if (paused.changes === 0) return null;
        schedulerDb.prepare(`
          UPDATE task_history
          SET status = 'failed', completed_at = ?,
              duration_ms = (? - executed_at) * 1000, error = ?
          WHERE task_id = ? AND status = 'started'
        `).run(recordedAt, recordedAt, reason, task.id);
        return { paused: true, terminal: false };
      }
      const update = schedulerDb.prepare(`
        UPDATE tasks
        SET current_occurrence_id = ?, current_turn_id = ?,
            last_core_state = ?, core_wait_reason = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND current_turn_id IS NULL
      `).run(
        occurrence,
        core.turn_id,
        core.turn_state,
        core.wait_reason ?? null,
        recordedAt,
        task.id,
      );
      if (update.changes === 0) return null;
      if (legacyStarted) {
        schedulerDb.prepare(`
          UPDATE task_history SET occurrence_id = ?, turn_id = ? WHERE id = ?
        `).run(occurrence, core.turn_id, startedRows[0].id);
      } else if (!exactStarted) {
        schedulerDb.prepare(`
          INSERT INTO task_history (
            task_id, occurrence_id, turn_id, executed_at, status
          ) VALUES (?, ?, ?, ?, 'started')
        `).run(task.id, occurrence, core.turn_id, recordedAt);
      }
      const terminal = [
        'completed', 'stopped', 'cancelled', 'interrupted', 'failed', 'timed_out',
      ].includes(core.turn_state);
      if (terminal) {
        const taskStatus = task.requires_reconfiguration === 1
          ? 'paused'
          : (core.turn_state === 'completed'
            ? 'completed'
            : (task.type === 'one-time' ? 'failed' : 'completed'));
        const historyStatus = core.turn_state === 'completed'
          ? 'success'
          : (core.turn_state === 'timed_out' ? 'timeout' : 'failed');
        const terminalError = core.turn_state === 'completed'
          ? task.last_error
          : `Scheduled turn ${core.turn_state}.`;
        schedulerDb.prepare(`
          UPDATE tasks
          SET status = ?, last_run_at = ?, last_core_state = ?,
              core_wait_reason = NULL, last_error = ?, updated_at = ?
          WHERE id = ? AND status = 'running' AND current_turn_id = ?
        `).run(
          taskStatus,
          recordedAt,
          core.turn_state,
          terminalError,
          recordedAt,
          task.id,
          core.turn_id,
        );
        schedulerDb.prepare(`
          UPDATE task_history
          SET status = ?, completed_at = ?,
              duration_ms = (? - executed_at) * 1000, error = ?
          WHERE task_id = ? AND occurrence_id = ? AND turn_id = ? AND status = 'started'
        `).run(
          historyStatus,
          recordedAt,
          recordedAt,
          core.turn_state === 'completed' ? null : terminalError,
          task.id,
          occurrence,
          core.turn_id,
        );
      }
      return { paused: false, terminal };
    })();
    if (recovery) {
      if (recovery.paused) result.paused += 1;
      else {
        result.adopted += 1;
        if (recovery.terminal) result.terminal += 1;
      }
    }
  }
  return result;
}

export function scheduleMissedNoticeRetry(db, task, error, {
  now: clock = now,
} = {}) {
  if (error?.code !== 'queue_full') return null;
  const attempt = task.missed_notice_attempt ?? 1;
  if (!Number.isSafeInteger(attempt) || attempt < 1) {
    throw new TypeError('task.missed_notice_attempt must be a positive safe integer');
  }
  const delaySeconds = Math.min(300, 5 * (2 ** Math.min(attempt - 1, 6)));
  const recordedAt = clock();
  const retryAt = recordedAt + delaySeconds;
  const update = db.prepare(`
    UPDATE tasks
    SET missed_notice_attempt = ?, missed_notice_retry_at = ?,
        last_error = ?, updated_at = ?
    WHERE id = ? AND status = 'pending' AND missed_notice_attempt = ?
  `).run(
    attempt + 1,
    retryAt,
    `Core queue full; missed notice retry ${attempt + 1} after ${delaySeconds}s.`,
    recordedAt,
    task.id,
    attempt,
  );
  return Object.freeze({
    recorded: update.changes === 1,
    attempt: attempt + 1,
    retry_at: retryAt,
    delay_seconds: delaySeconds,
  });
}

export function recordMissedNoticeTerminalRejection(db, task, admission, {
  now: clock = now,
} = {}) {
  if (admission?.status !== 'rejected' || admission.error?.code === 'queue_full') {
    throw new TypeError('admission must be a non-retryable rejected Core result');
  }
  const attempt = task.missed_notice_attempt ?? 1;
  if (!Number.isSafeInteger(attempt) || attempt < 1) {
    throw new TypeError('task.missed_notice_attempt must be a positive safe integer');
  }
  const occurrence = `${occurrenceId(task)}:missed-notice:${attempt}`;
  const recordedAt = clock();
  const error = admission.error?.user_message ?? 'Core rejected the missed-occurrence notice.';
  return db.transaction(() => {
    const update = db.prepare(`
      UPDATE tasks
      SET status = 'failed', current_occurrence_id = ?, current_turn_id = ?,
          last_core_state = 'failed', core_wait_reason = NULL,
          last_error = ?, updated_at = ?
      WHERE id = ? AND status = 'pending' AND missed_notice_attempt = ?
    `).run(occurrence, admission.turn_id ?? null, error, recordedAt, task.id, attempt);
    if (update.changes === 0) return false;
    db.prepare(`
      INSERT INTO task_history (
        task_id, occurrence_id, turn_id, executed_at, completed_at, status, error
      ) VALUES (?, ?, ?, ?, ?, 'failed', ?)
    `).run(task.id, occurrence, admission.turn_id ?? null, recordedAt, recordedAt, error);
    return true;
  })();
}

/**
 * Persist the durable Core admission in the local scheduling projection.
 * Core admission happens first, so a crash before this transaction is safe:
 * the stable occurrence ID replays against Core and this transaction resumes.
 */
export function recordScheduledAdmission(db, task, admission, {
  now: clock = now,
} = {}) {
  if (!admission || !['accepted', 'rejected'].includes(admission.status)) {
    throw new TypeError('admission must be an accepted or rejected Core result');
  }
  if (admission.status === 'accepted'
    && (typeof admission.turn_id !== 'string' || admission.turn_id.length === 0)) {
    throw new TypeError('an accepted admission.turn_id must be a non-empty string');
  }
  if (admission.status === 'rejected'
    && admission.turn_id !== null
    && (typeof admission.turn_id !== 'string' || admission.turn_id.length === 0)) {
    throw new TypeError('a rejected admission.turn_id must be a non-empty string or null');
  }
  const occurrence = occurrenceId(task);
  if (task.status === 'running'
    && task.current_occurrence_id === occurrence
    && task.current_turn_id === admission.turn_id) {
    return true;
  }
  const recordedAt = clock();
  return db.transaction(() => {
    const terminalStatus = task.type === 'one-time' ? 'failed' : 'completed';
    const taskStatus = admission.status === 'accepted' ? 'running' : terminalStatus;
    const error = admission.status === 'rejected'
      ? (admission.error?.user_message ?? 'Core rejected the scheduled occurrence.')
      : null;
    const update = db.prepare(`
      UPDATE tasks
      SET status = ?, current_occurrence_id = ?, current_turn_id = ?,
          last_core_state = ?, core_wait_reason = ?, last_error = ?, updated_at = ?
      WHERE id = ? AND status = 'pending'
    `).run(
      taskStatus,
      occurrence,
      admission.turn_id,
      admission.status === 'accepted' ? 'queued' : 'failed',
      admission.status === 'accepted' ? 'queue' : null,
      error,
      recordedAt,
      task.id,
    );
    if (update.changes === 0) return false;
    db.prepare(`
      INSERT INTO task_history (
        task_id, occurrence_id, turn_id, executed_at, completed_at, status, error
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      task.id,
      occurrence,
      admission.turn_id,
      recordedAt,
      admission.status === 'accepted' ? null : recordedAt,
      admission.status === 'accepted' ? 'started' : 'failed',
      error,
    );
    return admission.status === 'accepted';
  })();
}

/**
 * Reconcile the scheduler projection from a complete provider-neutral Core
 * snapshot. Missing/degraded facts never become local timeout authority.
 */
export function reconcileRunningTasks(db, snapshot, {
  now: clock = now,
} = {}) {
  const result = { pending: 0, terminal: 0, unavailable: 0 };
  const tasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status = 'running' AND current_turn_id IS NOT NULL
    ORDER BY updated_at ASC, id ASC
  `).all();
  for (const task of tasks) {
    const projection = projectScheduledTurn(snapshot, task.current_turn_id);
    if (projection.disposition === 'unavailable') {
      db.prepare(`
        UPDATE tasks SET core_wait_reason = ? WHERE id = ? AND status = 'running'
      `).run(projection.wait_reason, task.id);
      result.unavailable += 1;
      continue;
    }
    if (projection.disposition === 'pending') {
      db.prepare(`
        UPDATE tasks
        SET last_core_state = ?, core_wait_reason = ?
        WHERE id = ? AND status = 'running' AND current_turn_id = ?
      `).run(projection.core_state, projection.wait_reason, task.id, task.current_turn_id);
      result.pending += 1;
      continue;
    }

    const completedAt = clock();
    const taskStatus = task.requires_reconfiguration === 1
      ? 'paused'
      : (projection.disposition === 'succeeded'
        ? 'completed'
        : (task.type === 'one-time' ? 'failed' : 'completed'));
    const historyStatus = projection.disposition === 'succeeded'
      ? 'success'
      : (projection.core_state === 'timed_out' ? 'timeout' : 'failed');
    const changed = db.transaction(() => {
      const update = db.prepare(`
        UPDATE tasks
        SET status = ?, last_run_at = ?, updated_at = ?, last_core_state = ?,
            core_wait_reason = NULL, last_error = ?
        WHERE id = ? AND status = 'running' AND current_turn_id = ?
      `).run(
        taskStatus,
        completedAt,
        completedAt,
        projection.core_state,
        projection.terminal_error ?? task.last_error,
        task.id,
        task.current_turn_id,
      );
      if (update.changes === 0) return false;
      db.prepare(`
        UPDATE task_history
        SET status = ?, completed_at = ?,
            duration_ms = (? - executed_at) * 1000, error = ?
        WHERE task_id = ? AND occurrence_id = ? AND turn_id = ? AND status = 'started'
      `).run(
        historyStatus,
        completedAt,
        completedAt,
        projection.terminal_error,
        task.id,
        task.current_occurrence_id,
        task.current_turn_id,
      );
      return true;
    })();
    if (changed) result.terminal += 1;
  }
  return result;
}

/**
 * Update next_run_at for recurring/interval tasks after completion
 */
export function updateNextRunTime(db, task, { fromDate = new Date() } = {}) {
  let nextRun;

  if (task.type === 'recurring' && task.cron_expression) {
    nextRun = getNextRun(task.cron_expression, task.timezone, fromDate);
  } else if (task.type === 'interval' && task.interval_seconds) {
    nextRun = now() + task.interval_seconds;
  } else {
    return; // One-time task, no update needed
  }

  db.prepare(`
    UPDATE tasks
    SET next_run_at = ?, status = 'pending', last_run_at = ?, updated_at = ?,
        current_occurrence_id = NULL, current_turn_id = NULL,
        last_core_state = NULL, core_wait_reason = NULL,
        missed_notice_attempt = 1, missed_notice_retry_at = NULL
    WHERE id = ?
  `).run(nextRun, now(), now(), task.id);

  console.log(`[${new Date().toISOString()}] Updated next run for ${task.id}: ${formatTime(nextRun)}`);
}

/**
 * Handle completed tasks - update recurring ones, finalize one-time
 */
export function processCompletedTasks(db) {
  const completedTasks = db.prepare(`
    SELECT * FROM tasks WHERE status = 'completed'
  `).all();

  for (const task of completedTasks) {
    if (task.type === 'one-time') {
      continue;
    }

    try {
      updateNextRunTime(db, task);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Failed to reschedule task ${task.id}: ${error.message}`);
      db.prepare(`
        UPDATE tasks SET status = 'failed', last_error = ?, updated_at = ? WHERE id = ?
      `).run(error.message, now(), task.id);
    }
  }
}
