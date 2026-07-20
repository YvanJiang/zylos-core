import { homedir } from 'os';
import { join } from 'path';
import Database from 'better-sqlite3';

import { acceptScheduledOccurrence } from '../../../runtime/scheduler/scheduler-queue.js';

const ZYLOS_DIR = process.env.ZYLOS_DIR || join(homedir(), 'zylos');
const CORE_DATABASE_PATH = join(ZYLOS_DIR, 'comm-bridge', 'c4.db');

function timestampFromSeconds(seconds) {
  return new Date(seconds * 1_000).toISOString();
}

function taskOccurrence(task, { notificationText = null } = {}) {
  const scheduledAt = timestampFromSeconds(task.next_run_at);
  let boundConversation = null;
  if (task.bound_conversation_json !== null && task.bound_conversation_json !== undefined) {
    try {
      boundConversation = JSON.parse(task.bound_conversation_json);
    } catch {
      throw new TypeError(`Task ${task.id} has invalid bound_conversation_json`);
    }
  }
  const missedNoticeAttempt = task.missed_notice_attempt ?? 1;
  if (!Number.isSafeInteger(missedNoticeAttempt) || missedNoticeAttempt < 1) {
    throw new TypeError(`Task ${task.id} has invalid missed_notice_attempt`);
  }
  return {
    schedule_id: task.id,
    task_id: task.id,
    occurrence_id: notificationText === null
      ? `${task.id}:${task.next_run_at}`
      : `${task.id}:${task.next_run_at}:missed-notice:${missedNoticeAttempt}`,
    prompt: notificationText ?? `[Scheduled Task: ${task.id}] ${task.prompt}`,
    occurred_at: scheduledAt,
    // A retry after a scheduler crash must reproduce the exact same envelope.
    received_at: scheduledAt,
    region: process.env.ZYLOS_REGION ?? 'global',
    tenant_id: process.env.ZYLOS_TENANT_ID ?? 'default',
    bot_id: process.env.ZYLOS_BOT_ID ?? 'zylos',
    bound_conversation: boundConversation,
    ...(notificationText === null ? {} : { notification_text: notificationText }),
  };
}

export function enqueueScheduledTask(database, task, {
  now = () => new Date().toISOString(),
  generateId,
  maxQueuedTurns,
} = {}) {
  const options = { now, ...(generateId ? { generateId } : {}), ...(maxQueuedTurns ? { maxQueuedTurns } : {}) };
  return acceptScheduledOccurrence(database, taskOccurrence(task), options);
}

export function enqueueMissedScheduledTaskNotice(database, task, notificationText, {
  now = () => new Date().toISOString(),
  generateId,
  maxQueuedTurns,
} = {}) {
  const options = { now, ...(generateId ? { generateId } : {}), ...(maxQueuedTurns ? { maxQueuedTurns } : {}) };
  return acceptScheduledOccurrence(database, taskOccurrence(task, { notificationText }), options);
}

export function dispatchScheduledTask(task, options = {}) {
  const database = new Database(options.databasePath ?? CORE_DATABASE_PATH);
  try {
    return enqueueScheduledTask(database, task, options);
  } finally {
    database.close();
  }
}

export function dispatchMissedScheduledTaskNotice(task, notificationText, options = {}) {
  const database = new Database(options.databasePath ?? CORE_DATABASE_PATH);
  try {
    return enqueueMissedScheduledTaskNotice(database, task, notificationText, options);
  } finally {
    database.close();
  }
}
