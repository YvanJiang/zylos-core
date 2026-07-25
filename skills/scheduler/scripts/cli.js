#!/usr/bin/env node
/**
 * Task Management CLI
 * Command-line interface for task creation, monitoring, and control
 */

import { getDb, generateId, getSchedulerScope, now } from './database.js';
import { getNextRun, isValidCron, describeCron, getDefaultTimezone } from './cron-utils.js';
import { parseTime, parseDuration, formatTime, getRelativeTime } from './time-utils.js';
import { loadTimezone } from './tz.js';
import { createBoundConversationIdentity } from '../../../runtime/scheduler/scheduler-queue.js';

const db = getDb();

/** Escape special LIKE pattern characters in user input */
function escapeLike(str) {
  return str.replace(/[%_!]/g, '!$&');
}

const ALLOWED_UPDATE_COLUMNS = new Set([
  'name', 'prompt', 'priority', 'bound_conversation_json',
  'miss_threshold', 'type', 'cron_expression', 'interval_seconds', 'next_run_at', 'timezone',
  'requires_reconfiguration', 'requires_occurrence_advance', 'last_error', 'updated_at'
]);

const HELP = `
Task CLI - Scheduler V2

Usage: ~/zylos/.claude/skills/scheduler/scripts/cli.js <command> [options]

Commands:
  list                    List all tasks
  add <prompt> [options]  Add a new task
  update <task-id> [options]  Update an existing task
  remove <task-id>        Remove a task
  pause <task-id>         Pause a task
  resume <task-id>        Resume a paused task
  history [task-id]       Show execution history
  next                    Show upcoming tasks
  running                 Show currently running tasks

Add Options:
  --in "<duration>"       One-time: run in X time (e.g., "30 minutes")
  --at "<time>"           One-time: run at specific time (e.g., "tomorrow 9am")
  --cron "<expression>"   Recurring: cron expression (e.g., "0 8 * * *")
  --every "<interval>"    Interval: repeat every X time (e.g., "2 hours")
  --priority <1-3>        Priority level (1=urgent, 2=high, 3=normal, default=3)
  --name "<name>"         Task name (optional)
  --bound-conversation-json "<json>"  Full Core conversation identity for a chat-bound occurrence
  --miss-threshold <seconds>  Skip if overdue by more than this (default=300)

Update Options (same as Add, plus):
  --prompt "<prompt>"     Update task content
  --use-synthetic-conversation  Explicitly use a scheduler-owned synthetic conversation

Examples:
  ~/zylos/.claude/skills/scheduler/scripts/cli.js add "Say hello" --in "30 minutes"
  ~/zylos/.claude/skills/scheduler/scripts/cli.js add "Health check" --cron "0 8 * * *"
  ~/zylos/.claude/skills/scheduler/scripts/cli.js add "Check updates" --every "1 hour"
  ~/zylos/.claude/skills/scheduler/scripts/cli.js update task-abc --priority 1
`;

const BOOLEAN_OPTIONS = new Set(['use-synthetic-conversation']);
const VALUE_OPTIONS = new Set([
  'at',
  'bound-conversation-json',
  'cron',
  'every',
  'in',
  'miss-threshold',
  'name',
  'priority',
  'prompt',
]);

function parseArgs(args) {
  const result = { command: null, args: [], options: {} };

  if (args.length === 0) {
    return result;
  }

  result.command = args[0];

  let i = 1;
  while (i < args.length) {
    const arg = args[i];

    if (arg.startsWith('--')) {
      const key = arg.slice(2);

      if (BOOLEAN_OPTIONS.has(key)) {
        result.options[key] = true;
        i++;
      } else if (VALUE_OPTIONS.has(key)) {
        const value = args[i + 1];
        if (value === undefined || value.startsWith('--')) {
          result.error = `Option --${key} requires a value.`;
          return result;
        }
        result.options[key] = value;
        i += 2;
      } else {
        result.error = `Unknown option: --${key}`;
        return result;
      }
    } else {
      result.args.push(arg);
      i++;
    }
  }

  return result;
}

// ===== Commands =====

function cmdList() {
  // Show all active tasks including failed ones (so user can see what timed out)
  const tasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status != 'completed' OR type != 'one-time'
    ORDER BY priority ASC, next_run_at ASC
  `).all();

  if (tasks.length === 0) {
    console.log('No tasks scheduled.');
    return;
  }

  console.log(`\n  Tasks (TZ: ${getDefaultTimezone()}):\n`);
  console.log('  ID              | Pri | Type      | Status  | Next Run           | Name');
  console.log('  ' + '-'.repeat(85));

  for (const task of tasks) {
    const id = task.id.substring(0, 14).padEnd(14);
    const pri = task.priority.toString().padEnd(3);
    const type = task.type.padEnd(9);
    const status = task.status.padEnd(7);
    const nextRun = task.status === 'completed' ? 'done'.padEnd(18) :
                    formatTime(task.next_run_at).padEnd(18);
    const name = task.name || task.prompt.substring(0, 30);

    console.log(`  ${id} | ${pri} | ${type} | ${status} | ${nextRun} | ${name}`);

    // Show prompt (truncated to 80 chars)
    const promptPreview = task.prompt.substring(0, 80).replace(/\n/g, ' ');
    console.log(`                    └─ ${promptPreview}${task.prompt.length > 80 ? '...' : ''}`);
  }
  console.log();
}

function cmdAdd(args, options) {
  const prompt = args.join(' ');

  if (!prompt) {
    console.error('Error: Prompt is required');
    console.log('Usage: cli.js add "<prompt>" [options]');
    return;
  }

  let type, nextRunAt, cronExpression, intervalSeconds;

  // Determine task type from options
  if (options.in) {
    type = 'one-time';
    const seconds = parseDuration(options.in);
    if (!seconds) {
      console.error(`Error: Invalid duration "${options.in}"`);
      return;
    }
    nextRunAt = now() + seconds;
  } else if (options.at) {
    type = 'one-time';
    nextRunAt = parseTime(options.at);
    if (!nextRunAt) {
      console.error(`Error: Could not parse time "${options.at}"`);
      return;
    }
  } else if (options.cron) {
    type = 'recurring';
    cronExpression = options.cron;
    if (!isValidCron(cronExpression)) {
      console.error(`Error: Invalid cron expression "${cronExpression}"`);
      return;
    }
    nextRunAt = getNextRun(cronExpression);
  } else if (options.every) {
    type = 'interval';
    intervalSeconds = parseDuration(options.every);
    if (!intervalSeconds) {
      console.error(`Error: Invalid interval "${options.every}"`);
      return;
    }
    nextRunAt = now() + intervalSeconds;
  } else {
    console.error('Error: Must specify timing (--in, --at, --cron, or --every)');
    console.log(HELP);
    return;
  }

  const priority = options.priority ? parseInt(options.priority, 10) : 3;
  if (!Number.isInteger(priority) || priority < 1 || priority > 3) {
    console.error('Error: Priority must be 1-3 (1=urgent, 2=high, 3=normal)');
    return;
  }

  let boundConversationJson = null;
  if (options['bound-conversation-json']) {
    try {
      const parsed = JSON.parse(options['bound-conversation-json']);
      const canonical = createBoundConversationIdentity({ bound_conversation: parsed });
      boundConversationJson = JSON.stringify(canonical);
    } catch (error) {
      console.error(`Error: ${error.message}`);
      process.exitCode = 2;
      return;
    }
  }

  // Parse miss-threshold
  const missThreshold = options['miss-threshold']
    ? parseInt(options['miss-threshold'], 10)
    : 300;  // Default 5 minutes
  if (!Number.isInteger(missThreshold) || missThreshold < 0) {
    console.error('Error: miss-threshold must be a positive integer');
    return;
  }

  const taskId = generateId();
  const currentTime = now();
  const scope = getSchedulerScope();

  db.prepare(`
    INSERT INTO tasks (
      id, name, prompt, type,
      cron_expression, interval_seconds,
      next_run_at, priority, status,
      miss_threshold, bound_conversation_json,
      scope_region, scope_tenant_id, scope_bot_id,
      created_at, updated_at, timezone
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    taskId,
    options.name || prompt.substring(0, 40),  // Default name to truncated prompt
    prompt,
    type,
    cronExpression || null,
    intervalSeconds || null,
    nextRunAt,
    priority,
    missThreshold,
    boundConversationJson,
    scope.region,
    scope.tenant_id,
    scope.bot_id,
    currentTime,
    currentTime,
    getDefaultTimezone()
  );

  console.log(`\nTask created: ${taskId}`);
  console.log(`  Type: ${type}`);
  console.log(`  Priority: ${priority}`);
  console.log(`  Next run: ${formatTime(nextRunAt)} (${getRelativeTime(nextRunAt)})`);

  if (cronExpression) {
    console.log(`  Schedule: ${describeCron(cronExpression)}`);
  }
  console.log();
}

function cmdRemove(taskId) {
  if (!taskId) {
    console.error('Error: Task ID is required');
    return;
  }

  // Support partial ID match
  const tasks = db.prepare(`
    SELECT id FROM tasks WHERE id LIKE ? ESCAPE '!'
  `).all(escapeLike(taskId) + '%');

  if (tasks.length === 0) {
    console.error(`Error: Task not found: ${taskId}`);
    return;
  }

  if (tasks.length > 1) {
    console.error(`Error: Ambiguous task ID prefix '${taskId}' matches multiple tasks:`);
    tasks.forEach(t => console.error(`  - ${t.id}`));
    console.error('Please provide a more specific prefix.');
    return;
  }

  db.prepare('DELETE FROM tasks WHERE id = ?').run(tasks[0].id);
  console.log(`Removed task: ${tasks[0].id}`);
}

function cmdPause(taskId) {
  if (!taskId) {
    console.error('Error: Task ID is required');
    return;
  }

  const tasks = db.prepare(`
    SELECT id FROM tasks WHERE id LIKE ? ESCAPE '!' AND status = 'pending'
  `).all(escapeLike(taskId) + '%');

  if (tasks.length === 0) {
    console.error(`Error: Pending task not found: ${taskId}`);
    return;
  }

  if (tasks.length > 1) {
    console.error(`Error: Ambiguous task ID prefix '${taskId}' matches multiple pending tasks:`);
    tasks.forEach(t => console.error(`  - ${t.id}`));
    console.error('Please provide a more specific prefix.');
    return;
  }

  db.prepare(`
    UPDATE tasks SET status = 'paused', updated_at = ? WHERE id = ?
  `).run(now(), tasks[0].id);

  console.log(`Paused task: ${tasks[0].id}`);
}

function cmdResume(taskId) {
  if (!taskId) {
    console.error('Error: Task ID is required');
    return;
  }

  const tasks = db.prepare(`
    SELECT id, requires_reconfiguration, requires_occurrence_advance FROM tasks
    WHERE id LIKE ? ESCAPE '!' AND status = 'paused'
  `).all(escapeLike(taskId) + '%');

  if (tasks.length === 0) {
    console.error(`Error: Paused task not found: ${taskId}`);
    return;
  }

  if (tasks.length > 1) {
    console.error(`Error: Ambiguous task ID prefix '${taskId}' matches multiple paused tasks:`);
    tasks.forEach(t => console.error(`  - ${t.id}`));
    console.error('Please provide a more specific prefix.');
    return;
  }

  if (tasks[0].requires_reconfiguration === 1) {
    console.error(
      'Error: Scheduler migration reconfiguration is required before resume; '
      + 'run update with --bound-conversation-json or --use-synthetic-conversation.',
    );
    process.exitCode = 2;
    return;
  }
  if (tasks[0].requires_occurrence_advance === 1) {
    console.error(
      'Error: Advance the task schedule before resume; the previous occurrence is a replay barrier.',
    );
    process.exitCode = 2;
    return;
  }

  db.prepare(`
    UPDATE tasks
    SET status = 'pending', last_error = NULL, updated_at = ?
    WHERE id = ?
  `).run(now(), tasks[0].id);

  console.log(`Resumed task: ${tasks[0].id}`);
}

function cmdHistory(taskId) {
  let query = `
    SELECT h.*, t.name, t.prompt
    FROM task_history h
    JOIN tasks t ON h.task_id = t.id
  `;
  let params = [];

  if (taskId) {
    query += ` WHERE h.task_id LIKE ? ESCAPE '!'`;
    params.push(escapeLike(taskId) + '%');
  }

  // Check for ambiguous task prefix before querying history
  if (taskId) {
    const matchingTasks = db.prepare(`
      SELECT id FROM tasks WHERE id LIKE ? ESCAPE '!'
    `).all(escapeLike(taskId) + '%');

    if (matchingTasks.length > 1) {
      console.log(`\n  ⚠ Warning: Prefix '${taskId}' matches ${matchingTasks.length} tasks:`);
      matchingTasks.forEach(t => console.log(`    - ${t.id}`));
      console.log();
    }
  }

  query += ' ORDER BY h.executed_at DESC LIMIT 20';

  const history = db.prepare(query).all(...params);

  if (history.length === 0) {
    console.log('No execution history.');
    return;
  }

  console.log('\n  Execution History:\n');
  console.log('  Time                | Task ID        | Status  | Duration');
  console.log('  ' + '-'.repeat(65));

  for (const entry of history) {
    const time = formatTime(entry.executed_at).padEnd(18);
    const id = entry.task_id.substring(0, 14).padEnd(14);
    const status = entry.status.padEnd(7);
    const duration = entry.duration_ms ? `${Math.round(entry.duration_ms / 1000)}s` : '-';

    console.log(`  ${time} | ${id} | ${status} | ${duration}`);
  }
  console.log();
}

function cmdNext() {
  const tasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status = 'pending'
    ORDER BY next_run_at ASC
    LIMIT 5
  `).all();

  if (tasks.length === 0) {
    console.log('No pending tasks.');
    return;
  }

  console.log('\n  Upcoming Tasks:\n');

  for (const task of tasks) {
    console.log(`  ${getRelativeTime(task.next_run_at).padEnd(12)} | P${task.priority} | ${task.name || task.prompt.substring(0, 40)}`);
  }
  console.log();
}

function cmdRunning() {
  const tasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status = 'running'
    ORDER BY updated_at ASC
  `).all();

  if (tasks.length === 0) {
    console.log('\n  No running tasks. Safe to compact.\n');
    return;
  }

  console.log('\n  Running Tasks (authoritative state from Core):\n');
  console.log('  ID              | Core state    | Wait reason       | Name');
  console.log('  ' + '-'.repeat(82));

  for (const task of tasks) {
    const id = task.id.substring(0, 14).padEnd(14);
    const coreState = (task.last_core_state || 'unknown').padEnd(13);
    const waitReason = (task.core_wait_reason || '-').padEnd(17);
    const name = task.name || task.prompt.substring(0, 30);

    console.log(`  ${id} | ${coreState} | ${waitReason} | ${name}`);
  }
  console.log();
}

function cmdUpdate(taskId, options) {
  if (!taskId) {
    console.error('Error: Task ID is required');
    return;
  }

  // Support partial ID match
  const tasks = db.prepare(`
    SELECT * FROM tasks WHERE id LIKE ? ESCAPE '!'
  `).all(escapeLike(taskId) + '%');

  if (tasks.length === 0) {
    console.error(`Error: Task not found: ${taskId}`);
    return;
  }

  if (tasks.length > 1) {
    console.error(`Error: Ambiguous task ID prefix '${taskId}' matches multiple tasks:`);
    tasks.forEach(t => console.error(`  - ${t.id}`));
    console.error('Please provide a more specific prefix.');
    return;
  }

  const task = tasks[0];
  const updates = {};
  const updatedFields = [];

  // Update name
  if (options.name) {
    updates.name = options.name;
    updatedFields.push('name');
  }

  // Update prompt
  if (options.prompt) {
    updates.prompt = options.prompt;
    updatedFields.push('prompt');
  }

  // Update priority
  if (options.priority) {
    const priority = parseInt(options.priority, 10);
    if (!Number.isInteger(priority) || priority < 1 || priority > 3) {
      console.error('Error: Priority must be 1-3');
      return;
    }
    updates.priority = priority;
    updatedFields.push('priority');
  }

  if (options['bound-conversation-json'] && options['use-synthetic-conversation']) {
    console.error(
      'Error: Choose either --bound-conversation-json or --use-synthetic-conversation.',
    );
    process.exitCode = 2;
    return;
  }

  if ((options['bound-conversation-json'] || options['use-synthetic-conversation'])
    && task.status === 'running' && task.requires_reconfiguration === 1) {
    console.error(
      'Error: The admitted legacy turn is still running; wait for its Core terminal state, '
      + 'then reconfigure and explicitly resume the paused task.',
    );
    process.exitCode = 2;
    return;
  }
  if ((options.in || options.at || options.cron || options.every) && task.status === 'running') {
    console.error('Error: A running task schedule cannot change before its Core turn is terminal.');
    process.exitCode = 2;
    return;
  }

  if (options['bound-conversation-json']) {
    try {
      const parsed = JSON.parse(options['bound-conversation-json']);
      updates.bound_conversation_json = JSON.stringify(
        createBoundConversationIdentity({ bound_conversation: parsed }),
      );
      updates.requires_reconfiguration = 0;
      if (task.requires_reconfiguration === 1) updates.last_error = null;
      updatedFields.push('bound_conversation_json');
    } catch (error) {
      console.error(`Error: ${error.message}`);
      process.exitCode = 2;
      return;
    }
  }

  if (options['use-synthetic-conversation']) {
    updates.bound_conversation_json = null;
    updates.requires_reconfiguration = 0;
    if (task.requires_reconfiguration === 1) updates.last_error = null;
    updatedFields.push('synthetic_conversation');
  }

  // Update miss_threshold
  if (options['miss-threshold']) {
    const threshold = parseInt(options['miss-threshold'], 10);
    if (!Number.isInteger(threshold) || threshold < 0) {
      console.error('Error: miss-threshold must be a positive integer');
      return;
    }
    updates.miss_threshold = threshold;
    updatedFields.push('miss_threshold');
  }

  // Update schedule (type and next_run_at)
  let scheduleUpdated = false;
  if (options.in) {
    const seconds = parseDuration(options.in);
    if (!seconds) {
      console.error(`Error: Invalid duration "${options.in}"`);
      return;
    }
    updates.type = 'one-time';
    updates.cron_expression = null;
    updates.interval_seconds = null;
    updates.next_run_at = now() + seconds;
    scheduleUpdated = true;
  } else if (options.at) {
    const nextRunAt = parseTime(options.at);
    if (!nextRunAt) {
      console.error(`Error: Could not parse time "${options.at}"`);
      return;
    }
    updates.type = 'one-time';
    updates.cron_expression = null;
    updates.interval_seconds = null;
    updates.next_run_at = nextRunAt;
    scheduleUpdated = true;
  } else if (options.cron) {
    const cronExpression = options.cron;
    if (!isValidCron(cronExpression)) {
      console.error(`Error: Invalid cron expression "${cronExpression}"`);
      return;
    }
    updates.type = 'recurring';
    updates.cron_expression = cronExpression;
    updates.interval_seconds = null;
    updates.next_run_at = getNextRun(cronExpression);
    scheduleUpdated = true;
  } else if (options.every) {
    const intervalSeconds = parseDuration(options.every);
    if (!intervalSeconds) {
      console.error(`Error: Invalid interval "${options.every}"`);
      return;
    }
    updates.type = 'interval';
    updates.cron_expression = null;
    updates.interval_seconds = intervalSeconds;
    updates.next_run_at = now() + intervalSeconds;
    scheduleUpdated = true;
  }

  if (scheduleUpdated) {
    const nextOccurrenceId = `${task.id}:${updates.next_run_at}`;
    if (task.requires_occurrence_advance === 1
      && (!Number.isSafeInteger(updates.next_run_at)
        || updates.next_run_at <= task.next_run_at
        || nextOccurrenceId === task.current_occurrence_id)) {
      console.error(
        'Error: The new schedule must be strictly after the fenced occurrence.',
      );
      process.exitCode = 2;
      return;
    }
    updates.timezone = getDefaultTimezone();
    updates.requires_occurrence_advance = 0;
    updatedFields.push('type', 'schedule');
  }

  // Check if any updates were provided
  if (Object.keys(updates).length === 0) {
    console.error('Error: No updates provided');
    console.log('Use --help to see available options');
    return;
  }

  // Build UPDATE query (validate column names against whitelist)
  updates.updated_at = now();
  for (const key of Object.keys(updates)) {
    if (!ALLOWED_UPDATE_COLUMNS.has(key)) {
      console.error(`Error: Invalid update field: ${key}`);
      return;
    }
  }
  const setClauses = Object.keys(updates).map(key => `${key} = ?`).join(', ');
  const values = Object.values(updates);

  db.prepare(`
    UPDATE tasks SET ${setClauses} WHERE id = ?
  `).run(...values, task.id);

  console.log(`\nTask updated: ${task.id}`);
  console.log(`  Updated fields: ${updatedFields.join(', ')}`);

  if (scheduleUpdated) {
    console.log(`  Type: ${updates.type}`);
    console.log(`  Next run: ${formatTime(updates.next_run_at)} (${getRelativeTime(updates.next_run_at)})`);
  }
  console.log();
}

// ===== Main =====

function main() {
  try {
    process.env.TZ = loadTimezone();
  } catch (error) {
    const code = error.code || 'UNKNOWN_TZ_ERROR';
    console.error(`Error [${code}]: ${error.message}`);
    process.exit(1);
  }

  const { command, args, options, error } = parseArgs(process.argv.slice(2));
  if (error) {
    console.error(`Error: ${error}`);
    process.exitCode = 2;
    return;
  }
  if (options['use-synthetic-conversation'] && command !== 'update') {
    console.error('Error: --use-synthetic-conversation is only valid for update.');
    process.exitCode = 2;
    return;
  }

  switch (command) {
    case 'list':
      cmdList();
      break;
    case 'add':
      cmdAdd(args, options);
      break;
    case 'update':
      cmdUpdate(args[0], options);
      break;
    case 'remove':
    case 'rm':
    case 'delete':
      cmdRemove(args[0]);
      break;
    case 'pause':
      cmdPause(args[0]);
      break;
    case 'resume':
      cmdResume(args[0]);
      break;
    case 'history':
      cmdHistory(args[0]);
      break;
    case 'next':
      cmdNext();
      break;
    case 'running':
      cmdRunning();
      break;
    case 'help':
    case '--help':
    case '-h':
      console.log(HELP);
      break;
    default:
      if (command) {
        console.error(`Unknown command: ${command}`);
      }
      console.log(HELP);
  }
}

main();
