#!/usr/bin/env node
/**
 * Scheduler Daemon
 * Main orchestrator for autonomous task execution
 */

import { getDb, cleanupHistory, now } from './database.js';
import { getNextRun } from './cron-utils.js';
import { dispatchMissedScheduledTaskNotice, dispatchScheduledTask } from './runtime.js';
import { decideScheduledOccurrence } from '../../../runtime/scheduler/scheduler-queue.js';
import { formatTime } from './time-utils.js';
import { loadTimezone } from './tz.js';
import { updateNextRunTime as _updateNextRunTime, processCompletedTasks as _processCompletedTasks, handleStaleRunningTasks as _handleStaleRunningTasks, TASK_TIMEOUT } from './daemon-tasks.js';

const CHECK_INTERVAL = 5000;  // 5 seconds
const CLEANUP_INTERVAL = 3600000;  // 1 hour

let db;
let running = true;

try {
  process.env.TZ = loadTimezone();
} catch (error) {
  const code = error.code || 'UNKNOWN_TZ_ERROR';
  console.error(`[${new Date().toISOString()}] Fatal timezone config error [${code}]: ${error.message}`);
  process.exit(1);
}

/**
 * Get the next pending task that's due
 */
function getNextPendingTask() {
  const currentTime = now();

  return db.prepare(`
    SELECT * FROM tasks
    WHERE status = 'pending'
    AND next_run_at <= ?
    ORDER BY priority ASC, next_run_at ASC
    LIMIT 1
  `).get(currentTime);
}

/**
 * Admit a task into the durable Core conversation queue.
 */
function dispatchTask(task) {
  console.log(`[${new Date().toISOString()}] Dispatching task: ${task.id} (${task.name})`);

  // Atomically claim the task (only if still pending)
  const claim = db.prepare(`
    UPDATE tasks
    SET status = 'running', updated_at = ?
    WHERE id = ? AND status = 'pending'
  `).run(now(), task.id);

  if (claim.changes === 0) {
    console.log(`[${new Date().toISOString()}] Task ${task.id} already claimed/modified, skipping`);
    return false;
  }

  // Create history entry
  db.prepare(`
    INSERT INTO task_history (task_id, executed_at, status)
    VALUES (?, ?, 'started')
  `).run(task.id, now());

  let admission;
  try {
    admission = dispatchScheduledTask(task);
  } catch (error) {
    console.error(`Failed to admit task ${task.id}:`, error.message);
    console.error(`Failed to dispatch task ${task.id}`);

    // Revert to pending
    db.prepare(`
      UPDATE tasks
      SET status = 'pending', last_error = 'Failed to dispatch message', updated_at = ?
      WHERE id = ?
    `).run(now(), task.id);

    // Mark task_history as failed (latest entry only)
    const historyEntry = db.prepare(`
      SELECT id FROM task_history
      WHERE task_id = ? AND status = 'started'
      ORDER BY executed_at DESC LIMIT 1
    `).get(task.id);

    if (historyEntry) {
      db.prepare(`
        UPDATE task_history
        SET status = 'failed', completed_at = ?
        WHERE id = ?
      `).run(now(), historyEntry.id);
    }
    return false;
  }
  if (admission.status === 'rejected') {
    console.error(`Task ${task.id} was rejected by the durable queue: ${admission.error.user_message}`);
    db.prepare(`
      UPDATE tasks
      SET status = 'failed', last_error = ?, updated_at = ?
      WHERE id = ?
    `).run(admission.error.user_message, now(), task.id);
    return false;
  }
  return true;
}

function updateNextRunTime(task) {
  _updateNextRunTime(db, task);
}

function processCompletedTasks() {
  _processCompletedTasks(db);
}

function missedTaskNotice(task) {
  return `Scheduled task "${task.name}" missed its occurrence and was skipped to avoid catch-up replay.`;
}

function persistMissedTaskNotice(task) {
  const notice = missedTaskNotice(task);
  try {
    const admission = dispatchMissedScheduledTaskNotice(task, notice);
    if (admission.status === 'rejected') {
      console.error(`Missed-task notice for ${task.id} was rejected by the durable queue: ${admission.error.user_message}`);
      return false;
    }
    return true;
  } catch (error) {
    console.error(`Failed to persist missed-task notice for ${task.id}: ${error.message}`);
    return false;
  }
}

/**
 * Check for missed tasks (past due but still pending)
 * - Recurring and interval occurrences beyond miss_threshold are skipped once.
 * - All other work enters the durable Core queue without a runtime-alive gate.
 */
function handleMissedTasks() {
  const currentTime = now();
  const recentMissedThreshold = currentTime - 300;   // 5 minutes

  // Find recurring/interval tasks that are past due (>5 min)
  const missedTasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status = 'pending'
    AND type IN ('recurring', 'interval')
    AND next_run_at < ?
  `).all(recentMissedThreshold);

  for (const task of missedTasks) {
    const decision = decideScheduledOccurrence({
      schedule_type: task.type,
      scheduled_for: new Date(task.next_run_at * 1_000).toISOString(),
      now: new Date(currentTime * 1_000).toISOString(),
      miss_threshold_ms: (task.miss_threshold || 300) * 1_000,
    });

    if (decision.status === 'skipped') {
      // The occurrence is stale; move to the next schedule without catch-up replay.
      console.log(`[${new Date().toISOString()}] Task ${task.id} (${task.name}) missed its occurrence; recording only the next schedule.`);
      if (!persistMissedTaskNotice(task)) continue;
      db.prepare(`
        UPDATE tasks
        SET last_error = 'Missed scheduled occurrence was skipped to avoid catch-up replay.', updated_at = ?
        WHERE id = ?
      `).run(currentTime, task.id);
      updateNextRunTime({
        ...task,
        status: 'completed'
      });
    } else {
      dispatchTask(task);
    }
  }
}

function handleStaleRunningTasks() {
  _handleStaleRunningTasks(db);
}

/**
 * Main scheduler loop
 */
async function mainLoop() {
  console.log(`[${new Date().toISOString()}] Scheduler V2 started (TZ: ${process.env.TZ})`);
  console.log(`Check interval: ${CHECK_INTERVAL}ms`);

  // Clean up stale running tasks on startup
  console.log(`[${new Date().toISOString()}] Checking for stale running tasks...`);
  handleStaleRunningTasks();

  let lastCleanup = Date.now();

  while (running) {
    try {
      // The durable Core queue accepts work during maintenance or overload; execution claims later.
      const task = getNextPendingTask();

      // Dispatch if task is due and runtime is alive
      if (task) {
        const currentTime = now();
        const decision = decideScheduledOccurrence({
          schedule_type: task.type,
          scheduled_for: new Date(task.next_run_at * 1_000).toISOString(),
          now: new Date(currentTime * 1_000).toISOString(),
          miss_threshold_ms: (task.miss_threshold || 300) * 1_000,
        });

        if (decision.status === 'skipped') {
          // Skip this task
          console.log(`[${new Date().toISOString()}] Task ${task.id} (${task.name}) missed its occurrence; avoiding catch-up replay.`);
          if (!persistMissedTaskNotice(task)) continue;

          if (task.type === 'one-time') {
            // One-time tasks: mark as failed
            db.prepare(`
              UPDATE tasks
              SET status = 'failed', last_error = 'Missed execution window', updated_at = ?
              WHERE id = ?
            `).run(currentTime, task.id);
          } else {
            // Recurring/interval tasks: schedule next run
            updateNextRunTime(task);
          }
        } else {
          // Within threshold: dispatch normally
          dispatchTask(task);
        }
      }

      // Process completed tasks (update recurring schedules)
      processCompletedTasks();

      // Handle missed tasks
      handleMissedTasks();

      // Handle stale running tasks (orphaned due to compaction/crash)
      handleStaleRunningTasks();

      // Periodic cleanup of old history
      if (Date.now() - lastCleanup > CLEANUP_INTERVAL) {
        const deleted = cleanupHistory();
        if (deleted > 0) {
          console.log(`[${new Date().toISOString()}] Cleaned up ${deleted} old history entries`);
        }
        lastCleanup = Date.now();
      }

    } catch (error) {
      console.error(`[${new Date().toISOString()}] Scheduler error:`, error.message);
    }

    await sleep(CHECK_INTERVAL);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down scheduler...');
  running = false;
});

process.on('SIGTERM', () => {
  console.log('\nShutting down scheduler...');
  running = false;
});

// Start the scheduler
db = getDb();
mainLoop().then(() => {
  console.log('Scheduler stopped');
  if (db) db.close();
  process.exit(0);
});
