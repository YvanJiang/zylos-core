#!/usr/bin/env node
/**
 * Scheduler Daemon
 * Main orchestrator for autonomous task execution
 */

import { getDb, cleanupHistory, migrateLegacyTaskScopes, now } from './database.js';
import {
  dispatchMissedScheduledTaskNotice,
  dispatchScheduledTask,
  recoverLegacyRunningTasksFromCore,
} from './runtime.js';
import { decideScheduledOccurrence } from '../../../runtime/scheduler/scheduler-queue.js';
import { readExecutorObservability } from '../../../runtime/scheduler/scheduler-observability.js';
import { loadTimezone } from './tz.js';
import {
  processCompletedTasks as _processCompletedTasks,
  reconcileRunningTasks,
  recordMissedNoticeTerminalRejection,
  recordScheduledAdmission,
  scheduleMissedNoticeRetry,
  updateNextRunTime as _updateNextRunTime,
} from './daemon-tasks.js';
import { homedir } from 'node:os';
import { join } from 'node:path';

const CHECK_INTERVAL = 5000;  // 5 seconds
const CLEANUP_INTERVAL = 3600000;  // 1 hour
const ZYLOS_DIR = process.env.ZYLOS_DIR || join(homedir(), 'zylos');

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
    AND (missed_notice_retry_at IS NULL OR missed_notice_retry_at <= ?)
    ORDER BY priority ASC, next_run_at ASC
    LIMIT 1
  `).get(currentTime, currentTime);
}

/**
 * Admit a task into the durable Core conversation queue.
 */
function dispatchTask(task) {
  console.log(`[${new Date().toISOString()}] Dispatching task: ${task.id} (${task.name})`);

  let admission;
  try {
    admission = dispatchScheduledTask(task);
  } catch (error) {
    console.error(`Failed to admit task ${task.id}:`, error.message);
    // Leave the local occurrence pending. A later attempt uses the same Core
    // occurrence ID and exact envelope, so acceptance is safely replayable.
    return false;
  }
  const recorded = recordScheduledAdmission(db, task, admission);
  if (admission.status === 'rejected') {
    console.error(`Task ${task.id} was rejected by the durable queue: ${admission.error.user_message}`);
    return false;
  }
  if (!recorded) {
    console.log(`[${new Date().toISOString()}] Task ${task.id} already claimed/modified, skipping`);
  }
  return recorded;
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
      if (admission.error.code === 'queue_full') {
        scheduleMissedNoticeRetry(db, task, admission.error);
      } else {
        recordMissedNoticeTerminalRejection(db, task, admission);
      }
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
    AND (missed_notice_retry_at IS NULL OR missed_notice_retry_at <= ?)
  `).all(recentMissedThreshold, currentTime);

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

async function reconcileCoreState() {
  const snapshot = await readExecutorObservability({ zylosDir: ZYLOS_DIR });
  return reconcileRunningTasks(db, snapshot);
}

/**
 * Main scheduler loop
 */
async function mainLoop() {
  console.log(`[${new Date().toISOString()}] Scheduler V2 started (TZ: ${process.env.TZ})`);
  console.log(`Check interval: ${CHECK_INTERVAL}ms`);

  let lastCleanup = Date.now();

  while (running) {
    try {
      // The durable Core queue accepts work during maintenance or overload; execution claims later.
      const task = getNextPendingTask();

      // Persist a due occurrence regardless of transient executor availability.
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
          if (persistMissedTaskNotice(task)) {
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
          }
        } else {
          // Within threshold: dispatch normally
          dispatchTask(task);
        }
      }

      // Only authoritative Core terminal states complete local occurrences.
      await reconcileCoreState();

      // Process completed tasks (update recurring schedules)
      processCompletedTasks();

      // Handle missed tasks
      handleMissedTasks();

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
migrateLegacyTaskScopes(db);
recoverLegacyRunningTasksFromCore(db);
mainLoop().then(() => {
  console.log('Scheduler stopped');
  if (db) db.close();
  process.exit(0);
});
