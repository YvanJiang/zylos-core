import { createLegacyC4IdempotencyKey, verifyIdempotencyKey } from './idempotency.js';
import {
  partitionContractDocument,
  rejectContract,
  requireArray,
  requireBoolean,
  requireCriticalEnum,
  requireDisplayString,
  requireNullableOpaqueId,
  requireOwnFields,
  requirePlainObject,
  requirePositiveInteger,
  requireTimestamp,
} from './contract-utils.js';
import {
  validateContractError,
  validateOpaqueId,
  validatePublicFixtureSafety,
} from './validation.js';

export const INBOUND_ENVELOPE_CONTRACT = 'zylos.inbound-envelope';
export const INBOUND_RESULT_CONTRACT = 'zylos.inbound-result';

export const CHAT_TYPES = Object.freeze(['dm', 'group', 'thread', 'synthetic']);
export const INBOUND_ACTOR_TYPES = Object.freeze(['user', 'service', 'scheduler', 'system']);
export const INBOUND_ACTOR_ROLES = Object.freeze([
  'member',
  'group_owner',
  'tenant_admin',
  'bot_owner',
  'bot_admin',
]);
export const INBOUND_CONTENT_KINDS = Object.freeze(['text', 'rich_text', 'file', 'mixed']);
export const INBOUND_SOURCE_KINDS = Object.freeze([
  'platform_original',
  'scheduler',
  'legacy_compat',
]);
export const INBOUND_RESULT_STATUSES = Object.freeze(['accepted', 'rejected']);
export const LINEAGE_RESOLUTION_STATES = Object.freeze([
  'bound',
  'pending_recovery',
  'not_applicable',
]);

const ENVELOPE_FIELDS = Object.freeze([
  'inbound_event_id',
  'idempotency_key',
  'trace_id',
  'occurred_at',
  'received_at',
  'region',
  'tenant_id',
  'channel',
  'bot_id',
  'chat_type',
  'chat_id',
  'native_thread_or_topic_id',
  'message_id',
  'actor',
  'content',
  'reply',
  'source',
  'schedule',
  'legacy',
]);

const RESULT_FIELDS = Object.freeze([
  'trace_id',
  'inbound_event_id',
  'idempotency_key',
  'status',
  'conversation_id',
  'turn_id',
  'lineage_id',
  'control_id',
  'turn_version',
  'lineage_resolution_state',
  'deduplicated',
  'error',
  'committed_at',
]);

function validateActor(actor, occurredAt) {
  requirePlainObject('actor', actor, { occurredAt });
  requireOwnFields('actor', actor, ['type', 'actor_id', 'authenticated', 'roles'], { occurredAt });
  requireCriticalEnum('actor.type', actor.type, INBOUND_ACTOR_TYPES, { occurredAt });
  validateOpaqueId('actor.actor_id', actor.actor_id, { occurredAt });
  requireBoolean('actor.authenticated', actor.authenticated, { occurredAt });
  requireArray('actor.roles', actor.roles, { occurredAt });
  const uniqueRoles = new Set(actor.roles);
  if (uniqueRoles.size !== actor.roles.length) {
    rejectContract('validation_error', 'actor.roles must not contain duplicates.', { occurredAt });
  }
  for (const role of actor.roles) {
    requireCriticalEnum('actor.roles[]', role, INBOUND_ACTOR_ROLES, { occurredAt });
  }
  if (actor.type === 'service' && actor.roles.length > 0) {
    rejectContract(
      'validation_error',
      'A service actor cannot inherit human authorization roles.',
      { occurredAt },
    );
  }
}

function validateContent(content, occurredAt) {
  requirePlainObject('content', content, { occurredAt });
  requireOwnFields('content', content, ['kind', 'text', 'attachments'], { occurredAt });
  requireCriticalEnum('content.kind', content.kind, INBOUND_CONTENT_KINDS, { occurredAt });
  requireDisplayString('content.text', content.text, { nullable: true, occurredAt });
  requireArray('content.attachments', content.attachments, { occurredAt });

  if (['text', 'rich_text', 'mixed'].includes(content.kind) && content.text === null) {
    rejectContract('validation_error', `content.text is required for ${content.kind}.`, { occurredAt });
  }
  if (['file', 'mixed'].includes(content.kind) && content.attachments.length === 0) {
    rejectContract(
      'validation_error',
      `content.attachments must not be empty for ${content.kind}.`,
      { occurredAt },
    );
  }

  content.attachments.forEach((attachment, index) => {
    const fieldName = `content.attachments[${index}]`;
    requirePlainObject(fieldName, attachment, { occurredAt });
    requireOwnFields(
      fieldName,
      attachment,
      ['attachment_id', 'media_type', 'name', 'content_ref'],
      { occurredAt },
    );
    validateOpaqueId(`${fieldName}.attachment_id`, attachment.attachment_id, { occurredAt });
    requireDisplayString(`${fieldName}.media_type`, attachment.media_type, { occurredAt });
    requireDisplayString(`${fieldName}.name`, attachment.name, { occurredAt });
    validateOpaqueId(`${fieldName}.content_ref`, attachment.content_ref, { occurredAt });
  });
}

function validateReply(reply, occurredAt) {
  requirePlainObject('reply', reply, { occurredAt });
  const fields = ['root_message_id', 'parent_message_id', 'reply_to_message_id'];
  requireOwnFields('reply', reply, fields, { occurredAt });
  for (const fieldName of fields) {
    requireNullableOpaqueId(`reply.${fieldName}`, reply[fieldName], { occurredAt });
  }
}

function validateSource(source, occurredAt) {
  requirePlainObject('source', source, { occurredAt });
  requireOwnFields('source', source, ['kind', 'source_ref'], { occurredAt });
  requireCriticalEnum('source.kind', source.kind, INBOUND_SOURCE_KINDS, { occurredAt });
  requireNullableOpaqueId('source.source_ref', source.source_ref, { occurredAt });
}

function validateSchedule(schedule, occurredAt) {
  requirePlainObject('schedule', schedule, { occurredAt });
  requireOwnFields(
    'schedule',
    schedule,
    ['schedule_id', 'task_id', 'occurrence_id', 'bound_conversation'],
    { occurredAt },
  );
  validateOpaqueId('schedule.schedule_id', schedule.schedule_id, { occurredAt });
  validateOpaqueId('schedule.task_id', schedule.task_id, { occurredAt });
  validateOpaqueId('schedule.occurrence_id', schedule.occurrence_id, { occurredAt });
  requireBoolean('schedule.bound_conversation', schedule.bound_conversation, { occurredAt });
  if (Object.hasOwn(schedule, 'notification_text')) {
    requireDisplayString('schedule.notification_text', schedule.notification_text, { occurredAt });
  }
}

function validateLegacy(legacy, occurredAt) {
  requirePlainObject('legacy', legacy, { occurredAt });
  requireOwnFields(
    'legacy',
    legacy,
    ['legacy_record_id', 'legacy_state', 'migration_batch_id'],
    { occurredAt },
  );
  validateOpaqueId('legacy.legacy_record_id', legacy.legacy_record_id, { occurredAt });
  validateOpaqueId('legacy.legacy_state', legacy.legacy_state, { occurredAt });
  validateOpaqueId('legacy.migration_batch_id', legacy.migration_batch_id, { occurredAt });
}

function validateEnvelopeIdentity(value, occurredAt) {
  requireCriticalEnum('chat_type', value.chat_type, CHAT_TYPES, { occurredAt });
  if (value.chat_type === 'thread') {
    if (value.native_thread_or_topic_id === null) {
      rejectContract(
        'validation_error',
        'native_thread_or_topic_id is required for a real thread/topic conversation.',
        { occurredAt },
      );
    }
  } else if (value.native_thread_or_topic_id !== null) {
    rejectContract(
      'validation_error',
      'native_thread_or_topic_id must be null outside a real thread/topic conversation.',
      { occurredAt },
    );
  }

  if (value.chat_type === 'synthetic' && (value.source.kind !== 'scheduler' || !value.schedule)) {
    rejectContract('validation_error', 'synthetic conversations require scheduler source fields.', {
      occurredAt,
    });
  }
  if (value.source.kind === 'scheduler') {
    if (!value.schedule) {
      rejectContract(
        'validation_error',
        'scheduler source requires schedule fields.',
        { occurredAt },
      );
    }
    if (value.chat_type !== 'synthetic') {
      if (!value.schedule.bound_conversation) {
        rejectContract(
          'validation_error',
          'a non-synthetic scheduler occurrence must be bound to a conversation.',
          { occurredAt },
        );
      }
      return;
    }
    const expectedChatId = `scheduler:${value.bot_id}:${value.schedule.task_id}`;
    if (value.chat_id !== expectedChatId) {
      rejectContract(
        'validation_error',
        'scheduler synthetic chat_id must use scheduler:<bot_id>:<task_id>.',
        { occurredAt },
      );
    }
  }
}

function validateEnvelopeSource(value, occurredAt) {
  if (value.source.kind === 'scheduler') {
    if (value.actor.type !== 'scheduler') {
      rejectContract(
        'validation_error',
        'scheduler source requires the scheduler actor type.',
        { occurredAt },
      );
    }
    if (!Object.hasOwn(value, 'schedule')) {
      rejectContract('validation_error', 'schedule is required for scheduler source.', { occurredAt });
    }
    if (Object.hasOwn(value, 'legacy')) {
      rejectContract('validation_error', 'legacy must be omitted for scheduler source.', { occurredAt });
    }
    validateSchedule(value.schedule, occurredAt);
    if (value.chat_type === 'synthetic' && value.channel !== 'scheduler') {
      rejectContract(
        'validation_error',
        'a synthetic scheduler occurrence requires the scheduler channel.',
        { occurredAt },
      );
    }
    if (value.chat_type !== 'synthetic' && value.channel === 'scheduler') {
      rejectContract(
        'validation_error',
        'a conversation-bound scheduler occurrence requires its real channel identity.',
        { occurredAt },
      );
    }
    verifyIdempotencyKey('scheduler', {
      region: value.region,
      tenant_id: value.tenant_id,
      bot_id: value.bot_id,
      schedule_id: value.schedule.schedule_id,
      occurrence_id: value.schedule.occurrence_id,
    }, value.idempotency_key, { occurredAt });
    return;
  }

  if (value.source.kind === 'legacy_compat') {
    if (value.actor.type !== 'system') {
      rejectContract('validation_error', 'legacy_compat source requires a system actor.', { occurredAt });
    }
    if (!Object.hasOwn(value, 'legacy')) {
      rejectContract('validation_error', 'legacy is required for legacy_compat source.', { occurredAt });
    }
    if (Object.hasOwn(value, 'schedule')) {
      rejectContract('validation_error', 'schedule must be omitted for legacy_compat source.', {
        occurredAt,
      });
    }
    validateLegacy(value.legacy, occurredAt);
    const expectedKey = createLegacyC4IdempotencyKey(value.legacy.legacy_record_id);
    if (value.idempotency_key !== expectedKey) {
      rejectContract(
        'idempotency_key_mismatch',
        'The legacy C4 idempotency key does not match legacy_record_id.',
        { category: 'conflict', occurredAt },
      );
    }
    return;
  }

  if (
    value.channel === 'scheduler'
    || !['user', 'service'].includes(value.actor.type)
  ) {
    rejectContract(
      'validation_error',
      'platform_original source requires a non-scheduler channel and user or service actor.',
      { occurredAt },
    );
  }
  if (Object.hasOwn(value, 'schedule') || Object.hasOwn(value, 'legacy')) {
    rejectContract(
      'validation_error',
      'schedule and legacy fields must be omitted for platform_original source.',
      { occurredAt },
    );
  }
  verifyIdempotencyKey('inbound', {
    region: value.region,
    tenant_id: value.tenant_id,
    channel: value.channel,
    bot_id: value.bot_id,
    inbound_event_id: value.inbound_event_id,
  }, value.idempotency_key, { occurredAt });
}

export function validateInboundEnvelope(value, { occurredAt } = {}) {
  const result = partitionContractDocument(
    value,
    INBOUND_ENVELOPE_CONTRACT,
    ENVELOPE_FIELDS,
    { occurredAt },
  );
  const requiredFields = ENVELOPE_FIELDS.filter(
    (fieldName) => fieldName !== 'schedule' && fieldName !== 'legacy',
  );
  requireOwnFields('inbound-envelope', value, requiredFields, { occurredAt });

  for (const fieldName of [
    'inbound_event_id',
    'idempotency_key',
    'trace_id',
    'region',
    'tenant_id',
    'channel',
    'bot_id',
    'chat_id',
    'message_id',
  ]) {
    validateOpaqueId(fieldName, value[fieldName], { occurredAt });
  }
  requireTimestamp('occurred_at', value.occurred_at, { occurredAt });
  requireTimestamp('received_at', value.received_at, { occurredAt });
  requireNullableOpaqueId(
    'native_thread_or_topic_id',
    value.native_thread_or_topic_id,
    { occurredAt },
  );
  validateActor(value.actor, occurredAt);
  validateContent(value.content, occurredAt);
  validateReply(value.reply, occurredAt);
  validateSource(value.source, occurredAt);
  validateEnvelopeIdentity(value, occurredAt);
  validateEnvelopeSource(value, occurredAt);
  validatePublicFixtureSafety(value, { occurredAt });
  return result;
}

function requireNonNull(fieldName, value, occurredAt) {
  if (value === null) {
    rejectContract('validation_error', `${fieldName} must not be null for this result.`, {
      occurredAt,
    });
  }
}

function requireNull(fieldName, value, occurredAt) {
  if (value !== null) {
    rejectContract('validation_error', `${fieldName} must be null for this result.`, { occurredAt });
  }
}

function validateAcceptedResult(value, occurredAt) {
  requireNull('error', value.error, occurredAt);
  requireNonNull('conversation_id', value.conversation_id, occurredAt);
  requireNonNull('committed_at', value.committed_at, occurredAt);

  if (value.lineage_resolution_state === 'bound') {
    requireNonNull('turn_id', value.turn_id, occurredAt);
    requireNonNull('lineage_id', value.lineage_id, occurredAt);
    requireNonNull('turn_version', value.turn_version, occurredAt);
    requireNull('control_id', value.control_id, occurredAt);
    return;
  }
  if (value.lineage_resolution_state === 'pending_recovery') {
    requireNonNull('turn_id', value.turn_id, occurredAt);
    requireNull('lineage_id', value.lineage_id, occurredAt);
    requireNonNull('turn_version', value.turn_version, occurredAt);
    requireNull('control_id', value.control_id, occurredAt);
    return;
  }

  requireNull('turn_id', value.turn_id, occurredAt);
  requireNull('lineage_id', value.lineage_id, occurredAt);
  requireNull('turn_version', value.turn_version, occurredAt);
  requireNonNull('control_id', value.control_id, occurredAt);
}

function validateRejectedResult(value, occurredAt) {
  requireNonNull('error', value.error, occurredAt);
  if (value.error.code === 'queue_full') {
    requireNonNull('conversation_id', value.conversation_id, occurredAt);
    requireNonNull('turn_id', value.turn_id, occurredAt);
    requireNonNull('lineage_id', value.lineage_id, occurredAt);
    requireNonNull('turn_version', value.turn_version, occurredAt);
    requireNull('control_id', value.control_id, occurredAt);
    requireNonNull('committed_at', value.committed_at, occurredAt);
    if (value.lineage_resolution_state !== 'bound') {
      rejectContract(
        'validation_error',
        'queue_full rejection must return the bound failed turn lineage.',
        { occurredAt },
      );
    }
    return;
  }

  if (value.control_id !== null) {
    requireNonNull('conversation_id', value.conversation_id, occurredAt);
    requireNull('turn_id', value.turn_id, occurredAt);
    requireNull('lineage_id', value.lineage_id, occurredAt);
    requireNull('turn_version', value.turn_version, occurredAt);
    requireNonNull('committed_at', value.committed_at, occurredAt);
    if (value.lineage_resolution_state !== 'not_applicable') {
      rejectContract(
        'validation_error',
        'rejected control results must use lineage_resolution_state=not_applicable.',
        { occurredAt },
      );
    }
    return;
  }

  if (value.turn_id !== null) {
    requireNonNull('conversation_id', value.conversation_id, occurredAt);
    requireNonNull('turn_version', value.turn_version, occurredAt);
    requireNonNull('committed_at', value.committed_at, occurredAt);
    if (value.lineage_resolution_state === 'bound') {
      requireNonNull('lineage_id', value.lineage_id, occurredAt);
      return;
    }
    if (value.lineage_resolution_state === 'pending_recovery') {
      requireNull('lineage_id', value.lineage_id, occurredAt);
      return;
    }
    rejectContract(
      'validation_error',
      'rejected turns must return bound or pending_recovery lineage state.',
      { occurredAt },
    );
  }

  requireNull('lineage_id', value.lineage_id, occurredAt);
  requireNull('turn_version', value.turn_version, occurredAt);
  if (value.lineage_resolution_state !== 'not_applicable') {
    rejectContract(
      'validation_error',
      'rejection without a turn must use lineage_resolution_state=not_applicable.',
      { occurredAt },
    );
  }
}

export function validateInboundResult(value, { occurredAt } = {}) {
  const result = partitionContractDocument(
    value,
    INBOUND_RESULT_CONTRACT,
    RESULT_FIELDS,
    { occurredAt },
  );
  requireOwnFields('inbound-result', value, RESULT_FIELDS, { occurredAt });
  for (const fieldName of ['trace_id', 'inbound_event_id', 'idempotency_key']) {
    validateOpaqueId(fieldName, value[fieldName], { occurredAt });
  }
  for (const fieldName of ['conversation_id', 'turn_id', 'lineage_id', 'control_id']) {
    requireNullableOpaqueId(fieldName, value[fieldName], { occurredAt });
  }
  if (value.turn_version !== null) {
    requirePositiveInteger('turn_version', value.turn_version, { occurredAt });
  }
  requireCriticalEnum('status', value.status, INBOUND_RESULT_STATUSES, { occurredAt });
  requireCriticalEnum(
    'lineage_resolution_state',
    value.lineage_resolution_state,
    LINEAGE_RESOLUTION_STATES,
    { occurredAt },
  );
  requireBoolean('deduplicated', value.deduplicated, { occurredAt });
  if (value.error !== null) validateContractError(value.error, { occurredAt });
  if (value.committed_at !== null) requireTimestamp('committed_at', value.committed_at, { occurredAt });

  if (value.status === 'accepted') validateAcceptedResult(value, occurredAt);
  else validateRejectedResult(value, occurredAt);
  validatePublicFixtureSafety(value, { occurredAt });
  return result;
}
