const ACTIVE_TASK_STATES = Object.freeze([
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
]);

const MAX_VISIBLE_RUNNING_TASKS = 20;
const MAX_TASK_SUMMARY_CODE_POINTS = 120;

const QUEUE_REASON_MESSAGES = Object.freeze({
  background_task_queued: 'Your task is queued.',
  executor_capacity: 'Waiting for executor capacity.',
  maintenance: 'Zylos is in maintenance; your task is durably queued.',
  provider_retry: 'Your task is queued for a provider retry.',
  workspace_lease: 'Waiting for another conversation to finish using this workspace.',
});

const TASK_STATE_LABELS = Object.freeze({
  starting: 'starting',
  running: 'running',
  waiting_user: 'waiting for input',
  redirecting: 'redirecting',
  recovering: 'recovering',
});

function decodeXmlEntities(value) {
  return value
    .replaceAll('&quot;', '"')
    .replaceAll('&apos;', "'")
    .replaceAll('&gt;', '>')
    .replaceAll('&lt;', '<')
    .replaceAll('&amp;', '&');
}

function extractCurrentMessage(value) {
  const match = /<current-message>\s*([\s\S]*?)\s*<\/current-message>/u.exec(value);
  return match?.[1] ?? value;
}

function escapeMarkdown(value) {
  return value.replace(/[\\`*_[\]()~<>#>|]/gu, '\\$&');
}

function truncateCodePoints(value, maximum) {
  const codePoints = Array.from(value);
  if (codePoints.length <= maximum) return value;
  return `${codePoints.slice(0, maximum - 1).join('')}…`;
}

function taskSummary(envelopeJson) {
  try {
    const envelope = JSON.parse(envelopeJson);
    const preferred = envelope?.content?.task_summary;
    const source = typeof preferred === 'string' && preferred.trim().length > 0
      ? preferred
      : envelope?.content?.text;
    if (typeof source !== 'string' || source.trim().length === 0) {
      return 'Task without a text description';
    }
    const normalized = decodeXmlEntities(extractCurrentMessage(source))
      .toWellFormed()
      .replace(/[\u0000-\u001f\u007f]+/gu, ' ')
      .replace(/\s+/gu, ' ')
      .trim();
    if (normalized.length === 0) return 'Task without a text description';
    return escapeMarkdown(truncateCodePoints(normalized, MAX_TASK_SUMMARY_CODE_POINTS));
  } catch {
    return 'Task without a text description';
  }
}

function hasCurrentExecutionFence(task, observedAtMs) {
  const leaseExpiresAtMs = Date.parse(task.lease_expires_at);
  return task.turn_state === task.state
    && task.attempt_state === task.state
    && task.turn_attempt_id === task.attempt_id
    && task.turn_attempt_no === task.attempt_no
    && task.turn_lease_epoch === task.attempt_lease_epoch
    && task.lease_turn_id === task.execution_turn_id
    && task.lease_attempt_id === task.attempt_id
    && task.lease_attempt_no === task.attempt_no
    && task.lease_epoch === task.attempt_lease_epoch
    && task.lease_owner === task.service_instance_id
    && task.registered_service_instance_id === task.service_instance_id
    && task.service_revoked_at === null
    && Number.isFinite(leaseExpiresAtMs)
    && leaseExpiresAtMs > observedAtMs;
}

export function readQueuedTaskStatus(database, executionTurnId, observedAt) {
  const observedAtMs = Date.parse(observedAt);
  if (!Number.isFinite(observedAtMs)) {
    throw new TypeError('observedAt must be an RFC3339 timestamp');
  }
  const current = database.prepare(`
    SELECT origin_conversation_id
    FROM runtime_background_tasks
    WHERE execution_turn_id = ?
  `).get(executionTurnId);
  if (!current) return null;

  const activePlaceholders = ACTIVE_TASK_STATES.map(() => '?').join(', ');
  const running = database.prepare(`
    SELECT task.state, task.execution_turn_id, inbound.envelope_json,
      turn.state AS turn_state,
      turn.attempt_id AS turn_attempt_id,
      turn.attempt_no AS turn_attempt_no,
      turn.lease_epoch AS turn_lease_epoch,
      lease.lease_owner, lease.lease_epoch,
      lease.turn_id AS lease_turn_id,
      lease.attempt_id AS lease_attempt_id,
      lease.attempt_no AS lease_attempt_no,
      lease.lease_expires_at,
      attempt.attempt_id, attempt.attempt_no,
      attempt.lease_epoch AS attempt_lease_epoch,
      attempt.state AS attempt_state,
      attempt.service_instance_id,
      service.service_instance_id AS registered_service_instance_id,
      service.revoked_at AS service_revoked_at
    FROM runtime_background_tasks AS task
    JOIN runtime_turns AS turn
      ON turn.turn_id = task.execution_turn_id
    JOIN runtime_inbound_events AS inbound
      ON inbound.inbound_event_id = turn.inbound_event_id
    LEFT JOIN runtime_executor_leases AS lease
      ON lease.conversation_id = task.execution_conversation_id
    LEFT JOIN runtime_provider_attempts AS attempt
      ON attempt.turn_id = task.execution_turn_id
     AND attempt.attempt_id = turn.attempt_id
    LEFT JOIN runtime_executor_service_instances AS service
      ON service.service_instance_id = attempt.service_instance_id
    WHERE task.origin_conversation_id = ?
      AND task.state IN (${activePlaceholders})
    ORDER BY COALESCE(task.started_at, task.created_at), task.background_task_id
  `).all(current.origin_conversation_id, ...ACTIVE_TASK_STATES)
    .filter((task) => hasCurrentExecutionFence(task, observedAtMs));
  const queuedCount = database.prepare(`
    SELECT COUNT(*) AS count
    FROM runtime_background_tasks
    WHERE origin_conversation_id = ? AND state = 'queued'
  `).get(current.origin_conversation_id).count;

  return Object.freeze({
    running_tasks: Object.freeze(running.map((task) => Object.freeze({
      state: task.state,
      summary: taskSummary(task.envelope_json),
    }))),
    queued_count: queuedCount,
  });
}

export function buildQueuedTaskStatusText(
  database,
  executionTurnId,
  reasonCode,
  observedAt,
) {
  const status = readQueuedTaskStatus(database, executionTurnId, observedAt);
  if (status === null) return null;

  const visibleTasks = status.running_tasks.slice(0, MAX_VISIBLE_RUNNING_TASKS);
  const lines = [
    QUEUE_REASON_MESSAGES[reasonCode] ?? 'Your task is queued.',
    '',
    'Running tasks:',
  ];
  if (visibleTasks.length === 0) {
    lines.push('- None');
  } else {
    for (const task of visibleTasks) {
      lines.push(`- [${TASK_STATE_LABELS[task.state] ?? task.state}] ${task.summary}`);
    }
    const hiddenCount = status.running_tasks.length - visibleTasks.length;
    if (hiddenCount > 0) lines.push(`- …and ${hiddenCount} more`);
  }
  lines.push(
    '',
    `Queued tasks: ${status.queued_count} (including this task).`,
  );
  return lines.join('\n');
}
