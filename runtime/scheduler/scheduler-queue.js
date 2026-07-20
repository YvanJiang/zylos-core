import {
  createIdempotencyKey,
  validateInboundEnvelope,
} from '../../contracts/public/index.js';
import { acceptNormalInbound } from '../persistence/inbound-acceptance.js';

function requireNonEmptyString(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

function requireTimestamp(name, value) {
  requireNonEmptyString(name, value);
  if (Number.isNaN(Date.parse(value))) throw new TypeError(`${name} must be an RFC3339 timestamp`);
  return value;
}

export function createBoundConversationIdentity(occurrence) {
  const bound = occurrence.bound_conversation;
  if (bound === null || bound === undefined) return null;
  if (!bound || typeof bound !== 'object' || Array.isArray(bound)) {
    throw new TypeError('bound_conversation must be an object or null');
  }
  const fields = [
    'channel', 'chat_type', 'chat_id', 'native_thread_or_topic_id', 'message_id',
  ];
  for (const field of fields) {
    if (!Object.hasOwn(bound, field)) {
      throw new TypeError(`bound_conversation.${field} is required`);
    }
  }
  if (bound.native_thread_or_topic_id !== null
    && (typeof bound.native_thread_or_topic_id !== 'string'
      || bound.native_thread_or_topic_id.length === 0)) {
    throw new TypeError('bound_conversation.native_thread_or_topic_id must be a string or null');
  }
  if (bound.chat_type === 'thread') {
    requireNonEmptyString('bound_conversation.native_thread_or_topic_id', bound.native_thread_or_topic_id);
    requireNonEmptyString('bound_conversation.root_message_id', bound.root_message_id);
  } else if (bound.root_message_id !== null && bound.root_message_id !== undefined) {
    throw new TypeError('bound_conversation.root_message_id must be null outside a thread');
  }
  return {
    channel: requireNonEmptyString('bound_conversation.channel', bound.channel),
    chat_type: requireNonEmptyString('bound_conversation.chat_type', bound.chat_type),
    chat_id: requireNonEmptyString('bound_conversation.chat_id', bound.chat_id),
    native_thread_or_topic_id: bound.native_thread_or_topic_id,
    message_id: requireNonEmptyString('bound_conversation.message_id', bound.message_id),
    root_message_id: bound.root_message_id ?? null,
  };
}

export function createScheduledOccurrenceEnvelope(occurrence) {
  if (!occurrence || typeof occurrence !== 'object' || Array.isArray(occurrence)) {
    throw new TypeError('occurrence must be an object');
  }
  const scheduleId = requireNonEmptyString('schedule_id', occurrence.schedule_id);
  const taskId = requireNonEmptyString('task_id', occurrence.task_id);
  const occurrenceId = requireNonEmptyString('occurrence_id', occurrence.occurrence_id);
  const notificationText = occurrence.notification_text === undefined
    ? null
    : requireNonEmptyString('notification_text', occurrence.notification_text);
  const bound = createBoundConversationIdentity(occurrence);
  const identity = bound ?? {
    channel: 'scheduler',
    chat_type: 'synthetic',
    chat_id: `scheduler:${requireNonEmptyString('bot_id', occurrence.bot_id)}:${taskId}`,
    native_thread_or_topic_id: null,
    message_id: `scheduler:${taskId}:${occurrenceId}`,
  };
  const envelope = {
    contract: 'zylos.inbound-envelope',
    contract_version: '1.0',
    inbound_event_id: occurrenceId,
    idempotency_key: createIdempotencyKey('scheduler', {
      region: requireNonEmptyString('region', occurrence.region),
      tenant_id: requireNonEmptyString('tenant_id', occurrence.tenant_id),
      bot_id: requireNonEmptyString('bot_id', occurrence.bot_id),
      schedule_id: scheduleId,
      occurrence_id: occurrenceId,
    }),
    trace_id: `scheduler:${scheduleId}:${occurrenceId}`,
    occurred_at: requireTimestamp('occurred_at', occurrence.occurred_at),
    received_at: requireTimestamp('received_at', occurrence.received_at),
    region: occurrence.region,
    tenant_id: occurrence.tenant_id,
    channel: identity.channel,
    bot_id: occurrence.bot_id,
    chat_type: identity.chat_type,
    chat_id: identity.chat_id,
    native_thread_or_topic_id: identity.native_thread_or_topic_id,
    message_id: identity.message_id,
    actor: { type: 'scheduler', actor_id: scheduleId, authenticated: true, roles: [] },
    content: {
      kind: 'text',
      text: requireNonEmptyString('prompt', occurrence.prompt),
      attachments: [],
    },
    reply: {
      root_message_id: bound?.root_message_id ?? null,
      parent_message_id: null,
      reply_to_message_id: null,
    },
    source: { kind: 'scheduler', source_ref: `schedule:${scheduleId}:${occurrenceId}` },
    schedule: {
      schedule_id: scheduleId,
      task_id: taskId,
      occurrence_id: occurrenceId,
      bound_conversation: bound !== null,
      ...(notificationText === null ? {} : { notification_text: notificationText }),
    },
  };
  return validateInboundEnvelope(envelope).forwarded;
}

export function acceptScheduledOccurrence(database, occurrence, options = {}) {
  return acceptNormalInbound(database, createScheduledOccurrenceEnvelope(occurrence), options);
}

export function decideScheduledOccurrence({
  schedule_type: scheduleType,
  scheduled_for: scheduledFor,
  now,
  miss_threshold_ms: missThresholdMs,
}) {
  if (!['one-time', 'recurring', 'interval'].includes(scheduleType)) {
    throw new TypeError('schedule_type must be one-time, recurring, or interval');
  }
  const scheduledAt = Date.parse(requireTimestamp('scheduled_for', scheduledFor));
  const observedAt = Date.parse(requireTimestamp('now', now));
  if (!Number.isSafeInteger(missThresholdMs) || missThresholdMs < 0) {
    throw new TypeError('miss_threshold_ms must be a non-negative safe integer');
  }
  if (['recurring', 'interval'].includes(scheduleType) && observedAt - scheduledAt > missThresholdMs) {
    return Object.freeze({ status: 'skipped', reason: 'missed_occurrence' });
  }
  return Object.freeze({ status: 'enqueue', reason: null });
}
