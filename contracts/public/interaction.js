import { ContractKernelError, createContractError } from './errors.js';
import { verifyIdempotencyKey } from './idempotency.js';
import { canonicalizeJson } from './jcs.js';
import { SIDE_EFFECT_STATUSES } from './constants.js';
import {
  validateContractError,
  validateContractHeader,
  validateOpaqueId,
  validatePublicNumber,
  validateRfc3339Timestamp,
  validateSafetyCriticalEnum,
} from './validation.js';

function deepFreeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}

const REQUEST_REQUIRED_FIELDS = [
  'contract',
  'contract_version',
  'trace_id',
  'interaction_id',
  'conversation_id',
  'turn_id',
  'lineage_id',
  'control_id',
  'parent_type',
  'tool_use_id',
  'ordinal',
  'kind',
  'prompt',
  'choices',
  'authorized_subjects',
  'allowed_sources',
  'runtime_fence',
  'state',
  'version',
  'handoff_state',
  'created_at',
  'expires_at',
  'card_delivery_id',
];

const ANSWER_REQUIRED_FIELDS = [
  'contract',
  'contract_version',
  'trace_id',
  'interaction_id',
  'interaction_version',
  'answer_id',
  'source_event_or_action_id',
  'actor',
  'source_context',
  'source',
  'value',
  'answered_at',
  'idempotency_key',
];

const ANSWER_RESULT_REQUIRED_FIELDS = [
  'contract',
  'contract_version',
  'trace_id',
  'interaction_id',
  'answer_id',
  'idempotency_key',
  'status',
  'interaction_state',
  'interaction_version',
  'handoff_state',
  'handoff_id',
  'turn_id',
  'turn_version',
  'control_id',
  'error',
  'received_at',
  'committed_at',
];

const HANDOFF_REQUIRED_FIELDS = [
  'handoff_id',
  'interaction_id',
  'answer_id',
  'parent_type',
  'state',
  'provider_attempt_id',
  'handoff_attempt_id',
  'handoff_attempt_no',
  'lease_epoch',
  'claimed_by',
  'claimed_at',
  'last_send_started_at',
  'provider_acked_at',
  'handoff_deadline_at',
  'reason_code',
  'error',
  'side_effect_status',
];

const REQUEST_SCOPE_FIELDS = [
  'region',
  'tenant_id',
  'channel',
  'bot_id',
  'chat_id',
  'native_thread_or_topic_id',
];

export const INTERACTION_REQUEST_SCHEMA_V1 = deepFreeze({
  contract: 'zylos.interaction-request',
  contractVersion: '1.0',
  requiredFields: REQUEST_REQUIRED_FIELDS,
  parentTypes: ['provider_turn', 'security_control', 'recovery_control'],
  kinds: [
    'question',
    'choice',
    'tool_approval',
    'permission_approval',
    'recovery_decision',
  ],
  states: [
    'pending',
    'answer_committed',
    'answer_delivering',
    'delivery_unknown',
    'answered',
    'rejected',
    'expired',
    'cancelled',
  ],
  handoffStates: [
    'not_started',
    'pending',
    'delivering',
    'retry_wait',
    'accepted',
    'delivery_unknown',
    'rejected',
    'cancelled',
  ],
  allowedSources: [
    'main_card_reply',
    'card_action',
    'magic_command_repeat',
    'operations_control',
  ],
  stateHandoffStates: {
    pending: ['not_started'],
    answer_committed: ['pending', 'retry_wait'],
    answer_delivering: ['delivering'],
    delivery_unknown: ['delivery_unknown'],
    answered: ['accepted'],
    rejected: ['rejected'],
    expired: ['not_started'],
    cancelled: ['not_started', 'cancelled'],
  },
});

export const INTERACTION_ANSWER_SCHEMA_V1 = deepFreeze({
  contract: 'zylos.interaction-answer',
  contractVersion: '1.0',
  requiredFields: ANSWER_REQUIRED_FIELDS,
  allowedSources: INTERACTION_REQUEST_SCHEMA_V1.allowedSources,
  valueKinds: ['choice', 'text', 'decision'],
  decisions: ['approve', 'deny'],
  actorTypes: ['user', 'service'],
  userRoles: ['member', 'group_owner', 'tenant_admin', 'bot_owner', 'bot_admin'],
  requestScopeFields: REQUEST_SCOPE_FIELDS,
});

export const INTERACTION_ANSWER_RESULT_SCHEMA_V1 = deepFreeze({
  contract: 'zylos.interaction-answer-result',
  contractVersion: '1.0',
  requiredFields: ANSWER_RESULT_REQUIRED_FIELDS,
  statuses: ['accepted', 'duplicate', 'rejected', 'conflict'],
  acceptedInteractionState: 'answer_committed',
  acceptedHandoffState: 'pending',
  unacceptedHandoffState: 'not_applicable',
});

export const INTERACTION_HANDOFF_SCHEMA_V1 = deepFreeze({
  record: 'zylos.interaction-handoff',
  recordVersion: '1.0',
  requiredFields: HANDOFF_REQUIRED_FIELDS,
  parentTypes: INTERACTION_REQUEST_SCHEMA_V1.parentTypes,
  states: [
    'pending',
    'delivering',
    'retry_wait',
    'accepted',
    'delivery_unknown',
    'rejected',
    'cancelled',
  ],
  terminalStates: ['accepted', 'rejected', 'cancelled'],
});

export const INTERACTION_TRANSITIONS_V1 = deepFreeze({
  pending: ['answer_committed', 'expired', 'cancelled'],
  answer_committed: ['answer_delivering', 'cancelled'],
  answer_delivering: [
    'answered',
    'rejected',
    'delivery_unknown',
    'answer_committed',
    'cancelled',
  ],
  delivery_unknown: ['answered', 'rejected', 'cancelled'],
  answered: [],
  rejected: [],
  expired: [],
  cancelled: [],
});

export const INTERACTION_HANDOFF_TRANSITIONS_V1 = deepFreeze({
  pending: ['delivering', 'cancelled'],
  delivering: ['accepted', 'rejected', 'retry_wait', 'delivery_unknown', 'cancelled'],
  retry_wait: ['delivering', 'cancelled'],
  delivery_unknown: ['accepted', 'rejected', 'cancelled'],
  accepted: [],
  rejected: [],
  cancelled: [],
});

function reject(userMessage, occurredAt) {
  throw new ContractKernelError(createContractError({
    code: 'validation_error',
    userMessage,
    occurredAt,
  }));
}

function rejectWithCode(code, userMessage, occurredAt, category = 'validation') {
  throw new ContractKernelError(createContractError({
    code,
    category,
    userMessage,
    occurredAt,
  }));
}

function assertPlainObject(fieldName, value, occurredAt) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    reject(`${fieldName} must be a JSON object.`, occurredAt);
  }
  return value;
}

function assertExactKeys(fieldName, value, expectedKeys, occurredAt) {
  assertPlainObject(fieldName, value, occurredAt);
  const actual = Object.keys(value).sort();
  const expected = [...expectedKeys].sort();
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    reject(`${fieldName} must contain exactly ${expected.join(', ')}.`, occurredAt);
  }
}

function assertAllFieldsPresent(value, fieldNames, occurredAt) {
  for (const fieldName of fieldNames) {
    if (!Object.hasOwn(value, fieldName)) reject(`${fieldName} is required.`, occurredAt);
  }
}

function validateNullableOpaqueId(fieldName, value, occurredAt) {
  if (value === null) return null;
  return validateOpaqueId(fieldName, value, { occurredAt });
}

function validatePositiveInteger(fieldName, value, occurredAt) {
  validatePublicNumber(fieldName, value, { occurredAt });
  if (value < 1) reject(`${fieldName} must be a positive integer.`, occurredAt);
  return value;
}

function validateDisplayText(fieldName, value, occurredAt) {
  validateOpaqueId(fieldName, value, { occurredAt });
  if (value.trim().length === 0) reject(`${fieldName} must not be blank.`, occurredAt);
  return value;
}

function validateChoices(value, occurredAt) {
  if (!Array.isArray(value)) reject('choices must be an array.', occurredAt);
  const choiceIds = new Set();
  for (const [index, choice] of value.entries()) {
    const fieldName = `choices[${index}]`;
    assertExactKeys(fieldName, choice, ['choice_id', 'label'], occurredAt);
    const choiceId = validateOpaqueId(`${fieldName}.choice_id`, choice.choice_id, { occurredAt });
    validateDisplayText(`${fieldName}.label`, choice.label, occurredAt);
    if (choiceIds.has(choiceId)) reject('choices must use unique choice_id values.', occurredAt);
    choiceIds.add(choiceId);
  }
}

function validateAuthorizedSubjects(value, occurredAt) {
  if (!Array.isArray(value) || value.length === 0) {
    reject('authorized_subjects must be a non-empty array.', occurredAt);
  }
  for (const [index, subject] of value.entries()) {
    const fieldName = `authorized_subjects[${index}]`;
    assertPlainObject(fieldName, subject, occurredAt);
    validateSafetyCriticalEnum(
      `${fieldName}.type`,
      subject.type,
      ['actor', 'capability'],
      { occurredAt },
    );
    if (subject.type === 'actor') {
      assertExactKeys(fieldName, subject, ['type', 'actor_id'], occurredAt);
      validateOpaqueId(`${fieldName}.actor_id`, subject.actor_id, { occurredAt });
      continue;
    }
    assertExactKeys(fieldName, subject, ['type', 'capability', 'scope'], occurredAt);
    validateOpaqueId(`${fieldName}.capability`, subject.capability, { occurredAt });
    validateCapabilityScope(`${fieldName}.scope`, subject.scope, occurredAt);
  }
}

function validateCapabilityScope(fieldName, scope, occurredAt) {
  const scopeFields = [
    'scope_type',
    'region',
    'tenant_id',
    'bot_id',
    'conversation_id',
    'service_instance_id',
    'recovery_id',
  ];
  assertExactKeys(fieldName, scope, scopeFields, occurredAt);
  validateSafetyCriticalEnum(
    `${fieldName}.scope_type`,
    scope.scope_type,
    ['tenant', 'bot', 'conversation', 'service', 'recovery'],
    { occurredAt },
  );
  validateOpaqueId(`${fieldName}.region`, scope.region, { occurredAt });
  validateOpaqueId(`${fieldName}.tenant_id`, scope.tenant_id, { occurredAt });
  for (const idField of [
    'bot_id',
    'conversation_id',
    'service_instance_id',
    'recovery_id',
  ]) {
    validateNullableOpaqueId(`${fieldName}.${idField}`, scope[idField], occurredAt);
  }

  const requiredIds = {
    tenant: [],
    bot: ['bot_id'],
    conversation: ['bot_id', 'conversation_id'],
    service: ['service_instance_id'],
    recovery: ['bot_id', 'conversation_id', 'recovery_id'],
  }[scope.scope_type];
  const nullIds = ['bot_id', 'conversation_id', 'service_instance_id', 'recovery_id']
    .filter((field) => !requiredIds.includes(field));
  if (requiredIds.some((field) => scope[field] === null)) {
    reject(`${fieldName} is missing an ID required by ${scope.scope_type} scope.`, occurredAt);
  }
  if (nullIds.some((field) => scope[field] !== null)) {
    reject(`${fieldName} contains an ID outside ${scope.scope_type} scope.`, occurredAt);
  }
}

function validateAllowedSources(value, occurredAt) {
  if (!Array.isArray(value) || value.length === 0) {
    reject('allowed_sources must be a non-empty array.', occurredAt);
  }
  if (new Set(value).size !== value.length) {
    reject('allowed_sources must not contain duplicates.', occurredAt);
  }
  for (const source of value) {
    validateSafetyCriticalEnum(
      'allowed_sources[]',
      source,
      INTERACTION_REQUEST_SCHEMA_V1.allowedSources,
      { occurredAt },
    );
  }
}

function validateRuntimeFence(parentType, value, occurredAt) {
  if (parentType !== 'provider_turn') {
    if (value !== null) reject('Control interaction runtime_fence must be null.', occurredAt);
    return;
  }
  assertExactKeys(
    'runtime_fence',
    value,
    ['provider_attempt_id', 'lease_epoch', 'provider_interaction_ref'],
    occurredAt,
  );
  validateOpaqueId('runtime_fence.provider_attempt_id', value.provider_attempt_id, { occurredAt });
  validatePositiveInteger('runtime_fence.lease_epoch', value.lease_epoch, occurredAt);
  validateOpaqueId(
    'runtime_fence.provider_interaction_ref',
    value.provider_interaction_ref,
    { occurredAt },
  );
}

function validateParentIdentity(value, occurredAt) {
  const parentType = value.parent_type;
  validateOpaqueId('conversation_id', value.conversation_id, { occurredAt });
  if (parentType === 'provider_turn') {
    validateOpaqueId('turn_id', value.turn_id, { occurredAt });
    validateOpaqueId('lineage_id', value.lineage_id, { occurredAt });
    if (value.control_id !== null) reject('provider_turn control_id must be null.', occurredAt);
    return;
  }
  validateOpaqueId('control_id', value.control_id, { occurredAt });
  if (parentType === 'security_control') {
    if (value.turn_id !== null || value.lineage_id !== null) {
      reject('security_control turn_id and lineage_id must be null.', occurredAt);
    }
    return;
  }
  validateOpaqueId('turn_id', value.turn_id, { occurredAt });
  validateNullableOpaqueId('lineage_id', value.lineage_id, occurredAt);
}

function partitionDocument(value, schema, header) {
  const knownNames = new Set(schema.requiredFields);
  return {
    header,
    known: Object.fromEntries(schema.requiredFields.map(
      (fieldName) => [fieldName, structuredClone(value[fieldName])],
    )),
    extensions: Object.fromEntries(Object.entries(value)
      .filter(([fieldName]) => !knownNames.has(fieldName))
      .map(([fieldName, fieldValue]) => [fieldName, structuredClone(fieldValue)])),
    forwarded: structuredClone(value),
  };
}

function validateAnswerActor(actor, occurredAt) {
  assertExactKeys('actor', actor, ['type', 'actor_id', 'authenticated', 'roles'], occurredAt);
  validateSafetyCriticalEnum(
    'actor.type',
    actor.type,
    INTERACTION_ANSWER_SCHEMA_V1.actorTypes,
    { occurredAt },
  );
  validateOpaqueId('actor.actor_id', actor.actor_id, { occurredAt });
  if (actor.authenticated !== true) reject('actor.authenticated must be true.', occurredAt);
  if (!Array.isArray(actor.roles)) reject('actor.roles must be an array.', occurredAt);
  if (new Set(actor.roles).size !== actor.roles.length) {
    reject('actor.roles must not contain duplicates.', occurredAt);
  }
  for (const role of actor.roles) {
    if (actor.type === 'user') {
      validateSafetyCriticalEnum(
        'actor.roles[]',
        role,
        INTERACTION_ANSWER_SCHEMA_V1.userRoles,
        { occurredAt },
      );
    } else {
      validateOpaqueId('actor.roles[]', role, { occurredAt });
    }
  }
}

function validateSourceContext(source, context, occurredAt) {
  const fields = [
    'region',
    'tenant_id',
    'channel',
    'bot_id',
    'chat_id',
    'native_thread_or_topic_id',
    'platform_message_or_action_id',
  ];
  assertExactKeys('source_context', context, fields, occurredAt);
  for (const fieldName of ['region', 'tenant_id', 'channel', 'bot_id']) {
    validateOpaqueId(`source_context.${fieldName}`, context[fieldName], { occurredAt });
  }
  validateNullableOpaqueId('source_context.chat_id', context.chat_id, occurredAt);
  validateNullableOpaqueId(
    'source_context.native_thread_or_topic_id',
    context.native_thread_or_topic_id,
    occurredAt,
  );
  validateOpaqueId(
    'source_context.platform_message_or_action_id',
    context.platform_message_or_action_id,
    { occurredAt },
  );
  if (source === 'operations_control') {
    if (
      context.channel !== 'operations'
      || context.chat_id !== null
      || context.native_thread_or_topic_id !== null
    ) {
      reject('operations_control requires the operations channel and null chat/thread IDs.', occurredAt);
    }
  } else if (context.channel === 'operations') {
    reject('Only operations_control may use the operations channel.', occurredAt);
  }
}

function validateRequestScope(scope, occurredAt) {
  assertExactKeys('requestScope', scope, REQUEST_SCOPE_FIELDS, occurredAt);
  for (const fieldName of ['region', 'tenant_id', 'channel', 'bot_id', 'chat_id']) {
    validateOpaqueId(`requestScope.${fieldName}`, scope[fieldName], { occurredAt });
  }
  validateNullableOpaqueId(
    'requestScope.native_thread_or_topic_id',
    scope.native_thread_or_topic_id,
    occurredAt,
  );
}

function validateActorCapabilities(value, occurredAt) {
  if (!Array.isArray(value)) reject('actorCapabilities must be an array.', occurredAt);
  for (const [index, subject] of value.entries()) {
    const fieldName = `actorCapabilities[${index}]`;
    assertExactKeys(fieldName, subject, ['type', 'capability', 'scope'], occurredAt);
    if (subject.type !== 'capability') {
      reject(`${fieldName}.type must be capability.`, occurredAt);
    }
    validateOpaqueId(`${fieldName}.capability`, subject.capability, { occurredAt });
    validateCapabilityScope(`${fieldName}.scope`, subject.scope, occurredAt);
  }
}

function validateAnswerAuthorization(answer, request, actorCapabilities, occurredAt) {
  validateActorCapabilities(actorCapabilities, occurredAt);
  const directActorAuthorized = request.authorized_subjects.some((subject) => (
    subject.type === 'actor' && subject.actor_id === answer.actor.actor_id
  ));
  const capabilityAuthorized = request.authorized_subjects.some((subject) => (
    subject.type === 'capability'
    && actorCapabilities.some((capability) => (
      canonicalizeJson(capability) === canonicalizeJson(subject)
    ))
  ));
  if (!directActorAuthorized && !capabilityAuthorized) {
    rejectWithCode(
      'interaction_actor_forbidden',
      'answer actor is not authorized by the interaction request.',
      occurredAt,
      'authorization',
    );
  }
}

function validateAnswerScope(answer, requestScope, occurredAt) {
  validateRequestScope(requestScope, occurredAt);
  const commonFields = ['region', 'tenant_id', 'bot_id'];
  const channelFields = ['channel', 'chat_id', 'native_thread_or_topic_id'];
  const fields = answer.source === 'operations_control'
    ? commonFields
    : [...commonFields, ...channelFields];
  if (fields.some((fieldName) => (
    answer.source_context[fieldName] !== requestScope[fieldName]
  ))) {
    reject('answer source_context does not match the interaction request scope.', occurredAt);
  }
}

function validateInteractionHandoffProjection(state, handoffState, occurredAt) {
  if (!INTERACTION_REQUEST_SCHEMA_V1.stateHandoffStates[state].includes(handoffState)) {
    reject(`Interaction state ${state} cannot project handoff state ${handoffState}.`, occurredAt);
  }
}

function validateAnswerValue(value, occurredAt) {
  assertPlainObject('value', value, occurredAt);
  validateSafetyCriticalEnum(
    'value.kind',
    value.kind,
    INTERACTION_ANSWER_SCHEMA_V1.valueKinds,
    { occurredAt },
  );
  if (value.kind === 'choice') {
    assertExactKeys('value', value, ['kind', 'choice_id'], occurredAt);
    validateOpaqueId('value.choice_id', value.choice_id, { occurredAt });
    return;
  }
  if (value.kind === 'text') {
    assertExactKeys('value', value, ['kind', 'text'], occurredAt);
    validateDisplayText('value.text', value.text, occurredAt);
    return;
  }
  assertExactKeys('value', value, ['kind', 'decision'], occurredAt);
  validateSafetyCriticalEnum(
    'value.decision',
    value.decision,
    INTERACTION_ANSWER_SCHEMA_V1.decisions,
    { occurredAt },
  );
}

export function validateInteractionRequest(value, { occurredAt } = {}) {
  const header = validateContractHeader(value, { occurredAt });
  if (header.contract !== INTERACTION_REQUEST_SCHEMA_V1.contract) {
    reject(`Expected contract ${INTERACTION_REQUEST_SCHEMA_V1.contract}.`, occurredAt);
  }
  assertAllFieldsPresent(value, INTERACTION_REQUEST_SCHEMA_V1.requiredFields, occurredAt);
  validateOpaqueId('trace_id', value.trace_id, { occurredAt });
  validateOpaqueId('interaction_id', value.interaction_id, { occurredAt });
  validateSafetyCriticalEnum(
    'parent_type',
    value.parent_type,
    INTERACTION_REQUEST_SCHEMA_V1.parentTypes,
    { occurredAt },
  );
  validateParentIdentity(value, occurredAt);
  validateNullableOpaqueId('tool_use_id', value.tool_use_id, occurredAt);
  validatePositiveInteger('ordinal', value.ordinal, occurredAt);
  validateSafetyCriticalEnum(
    'kind',
    value.kind,
    INTERACTION_REQUEST_SCHEMA_V1.kinds,
    { occurredAt },
  );
  validateDisplayText('prompt', value.prompt, occurredAt);
  validateChoices(value.choices, occurredAt);
  if (value.kind === 'choice' && value.choices.length === 0) {
    reject('choice interactions require at least one choice.', occurredAt);
  }
  validateAuthorizedSubjects(value.authorized_subjects, occurredAt);
  validateAllowedSources(value.allowed_sources, occurredAt);
  validateRuntimeFence(value.parent_type, value.runtime_fence, occurredAt);
  validateSafetyCriticalEnum(
    'state',
    value.state,
    INTERACTION_REQUEST_SCHEMA_V1.states,
    { occurredAt },
  );
  validatePositiveInteger('version', value.version, occurredAt);
  validateSafetyCriticalEnum(
    'handoff_state',
    value.handoff_state,
    INTERACTION_REQUEST_SCHEMA_V1.handoffStates,
    { occurredAt },
  );
  validateInteractionHandoffProjection(value.state, value.handoff_state, occurredAt);
  validateRfc3339Timestamp('created_at', value.created_at, { occurredAt });
  validateRfc3339Timestamp('expires_at', value.expires_at, { occurredAt });
  if (Date.parse(value.expires_at) <= Date.parse(value.created_at)) {
    reject('expires_at must be later than created_at.', occurredAt);
  }
  validateNullableOpaqueId('card_delivery_id', value.card_delivery_id, occurredAt);
  return partitionDocument(value, INTERACTION_REQUEST_SCHEMA_V1, header);
}

function interactionParentKey(value) {
  const parentId = value.parent_type === 'provider_turn' ? value.turn_id : value.control_id;
  return `${value.parent_type}\u0000${value.conversation_id}\u0000${parentId}`;
}

export function validateInteractionRequestSequence(value, { occurredAt } = {}) {
  if (!Array.isArray(value) || value.length === 0) {
    reject('interaction sequence must be a non-empty array.', occurredAt);
  }

  const groups = new Map();
  const interactionIds = new Set();
  for (const interaction of value) {
    validateInteractionRequest(interaction, { occurredAt });
    if (interactionIds.has(interaction.interaction_id)) {
      reject('interaction sequence must use unique interaction_id values.', occurredAt);
    }
    interactionIds.add(interaction.interaction_id);
    const key = interactionParentKey(interaction);
    const group = groups.get(key) ?? [];
    group.push(interaction.ordinal);
    groups.set(key, group);
  }

  for (const ordinals of groups.values()) {
    ordinals.sort((left, right) => left - right);
    if (ordinals.some((ordinal, index) => ordinal !== index + 1)) {
      reject('ordinals must be unique and contiguous from 1 within each parent.', occurredAt);
    }
  }
  return true;
}

export function validateInteractionAnswer(value, { occurredAt } = {}) {
  const header = validateContractHeader(value, { occurredAt });
  if (header.contract !== INTERACTION_ANSWER_SCHEMA_V1.contract) {
    reject(`Expected contract ${INTERACTION_ANSWER_SCHEMA_V1.contract}.`, occurredAt);
  }
  assertAllFieldsPresent(value, INTERACTION_ANSWER_SCHEMA_V1.requiredFields, occurredAt);
  validateOpaqueId('trace_id', value.trace_id, { occurredAt });
  validateOpaqueId('interaction_id', value.interaction_id, { occurredAt });
  validatePositiveInteger('interaction_version', value.interaction_version, occurredAt);
  validateOpaqueId('answer_id', value.answer_id, { occurredAt });
  validateOpaqueId(
    'source_event_or_action_id',
    value.source_event_or_action_id,
    { occurredAt },
  );
  validateAnswerActor(value.actor, occurredAt);
  validateSafetyCriticalEnum(
    'source',
    value.source,
    INTERACTION_ANSWER_SCHEMA_V1.allowedSources,
    { occurredAt },
  );
  validateSourceContext(value.source, value.source_context, occurredAt);
  if (
    value.source_context.platform_message_or_action_id
    !== value.source_event_or_action_id
  ) {
    reject('source_context platform ID must match source_event_or_action_id.', occurredAt);
  }
  validateAnswerValue(value.value, occurredAt);
  validateRfc3339Timestamp('answered_at', value.answered_at, { occurredAt });
  verifyIdempotencyKey('interaction', {
    interaction_id: value.interaction_id,
    source_event_or_action_id: value.source_event_or_action_id,
  }, value.idempotency_key, { occurredAt });
  return partitionDocument(value, INTERACTION_ANSWER_SCHEMA_V1, header);
}

export function validateInteractionAnswerAgainstRequest(
  answer,
  request,
  {
    interactions = [request],
    requestScope,
    actorCapabilities = [],
    occurredAt,
  } = {},
) {
  validateInteractionAnswer(answer, { occurredAt });
  validateInteractionRequest(request, { occurredAt });

  if (answer.interaction_id !== request.interaction_id) {
    reject('answer interaction_id must match the request.', occurredAt);
  }
  if (answer.interaction_version !== request.version) {
    rejectWithCode('version_conflict', 'answer interaction_version must match the request.', occurredAt);
  }
  if (request.state === 'expired') {
    rejectWithCode('interaction_expired', 'the interaction has expired.', occurredAt);
  }
  if (request.state !== 'pending') {
    rejectWithCode(
      'interaction_already_answered',
      'only a pending interaction can accept an answer.',
      occurredAt,
    );
  }
  if (!request.allowed_sources.includes(answer.source)) {
    reject('answer source is not allowed by the interaction request.', occurredAt);
  }
  if (answer.source === 'magic_command_repeat' && request.kind !== 'permission_approval') {
    reject('magic_command_repeat requires a permission confirmation request.', occurredAt);
  }
  validateAnswerAuthorization(answer, request, actorCapabilities, occurredAt);
  validateAnswerScope(answer, requestScope, occurredAt);

  const sequence = [
    ...interactions.filter(
      (interaction) => interaction.interaction_id !== request.interaction_id,
    ),
    request,
  ];
  validateInteractionRequestSequence(sequence, { occurredAt });
  const blockingStates = new Set([
    'pending',
    'answer_committed',
    'answer_delivering',
    'delivery_unknown',
  ]);
  const parentKey = interactionParentKey(request);
  const blockingOrdinals = sequence
    .filter((interaction) => (
      interactionParentKey(interaction) === parentKey
      && blockingStates.has(interaction.state)
    ))
    .map((interaction) => interaction.ordinal);
  if (blockingOrdinals.length > 0 && request.ordinal !== Math.min(...blockingOrdinals)) {
    rejectWithCode(
      'interaction_out_of_order',
      'only the smallest blocking interaction ordinal can accept an answer.',
      occurredAt,
    );
  }
  return true;
}

function validateResultParent(value, occurredAt) {
  validateNullableOpaqueId('turn_id', value.turn_id, occurredAt);
  validateNullableOpaqueId('control_id', value.control_id, occurredAt);
  if (value.turn_id === null) {
    if (value.turn_version !== null) reject('turn_version must be null without turn_id.', occurredAt);
  } else {
    validatePositiveInteger('turn_version', value.turn_version, occurredAt);
  }
}

export function validateInteractionAnswerResult(value, { occurredAt } = {}) {
  const header = validateContractHeader(value, { occurredAt });
  if (header.contract !== INTERACTION_ANSWER_RESULT_SCHEMA_V1.contract) {
    reject(`Expected contract ${INTERACTION_ANSWER_RESULT_SCHEMA_V1.contract}.`, occurredAt);
  }
  assertAllFieldsPresent(value, INTERACTION_ANSWER_RESULT_SCHEMA_V1.requiredFields, occurredAt);
  validateOpaqueId('trace_id', value.trace_id, { occurredAt });
  validateOpaqueId('interaction_id', value.interaction_id, { occurredAt });
  validateOpaqueId('answer_id', value.answer_id, { occurredAt });
  validateOpaqueId('idempotency_key', value.idempotency_key, { occurredAt });
  if (!/^zid:v1:interaction:[0-9a-f]{64}$/.test(value.idempotency_key)) {
    reject('idempotency_key must use the v1 interaction scope.', occurredAt);
  }
  validateSafetyCriticalEnum(
    'status',
    value.status,
    INTERACTION_ANSWER_RESULT_SCHEMA_V1.statuses,
    { occurredAt },
  );
  validateResultParent(value, occurredAt);

  if (value.status === 'accepted' || value.status === 'duplicate') {
    if (value.interaction_state !== INTERACTION_ANSWER_RESULT_SCHEMA_V1.acceptedInteractionState) {
      reject('accepted/duplicate only confirms interaction state answer_committed.', occurredAt);
    }
    validatePositiveInteger('interaction_version', value.interaction_version, occurredAt);
    if (value.handoff_state !== INTERACTION_ANSWER_RESULT_SCHEMA_V1.acceptedHandoffState) {
      reject('accepted/duplicate requires a pending durable handoff.', occurredAt);
    }
    validateOpaqueId('handoff_id', value.handoff_id, { occurredAt });
    if (value.turn_id === null && value.control_id === null) {
      reject('accepted/duplicate must identify a turn or control parent.', occurredAt);
    }
    if (value.error !== null) reject('accepted/duplicate error must be null.', occurredAt);
    validateRfc3339Timestamp('received_at', value.received_at, { occurredAt });
    validateRfc3339Timestamp('committed_at', value.committed_at, { occurredAt });
    return partitionDocument(value, INTERACTION_ANSWER_RESULT_SCHEMA_V1, header);
  }

  if (value.interaction_state === null || value.interaction_version === null) {
    if (value.interaction_state !== null || value.interaction_version !== null) {
      reject('interaction_state and interaction_version must be null together.', occurredAt);
    }
  } else {
    validateSafetyCriticalEnum(
      'interaction_state',
      value.interaction_state,
      INTERACTION_REQUEST_SCHEMA_V1.states,
      { occurredAt },
    );
    validatePositiveInteger('interaction_version', value.interaction_version, occurredAt);
  }
  if (value.handoff_state !== INTERACTION_ANSWER_RESULT_SCHEMA_V1.unacceptedHandoffState) {
    reject('rejected/conflict handoff_state must be not_applicable.', occurredAt);
  }
  if (value.handoff_id !== null) reject('rejected/conflict handoff_id must be null.', occurredAt);
  if (value.error === null) reject('rejected/conflict requires an error.', occurredAt);
  validateContractError(value.error, { occurredAt });
  if (value.received_at !== null || value.committed_at !== null) {
    reject('rejected/conflict received_at and committed_at must be null.', occurredAt);
  }
  return partitionDocument(value, INTERACTION_ANSWER_RESULT_SCHEMA_V1, header);
}

const ANSWER_RESULT_REPLAY_FIELDS = [
  'interaction_id',
  'answer_id',
  'idempotency_key',
  'interaction_state',
  'interaction_version',
  'handoff_state',
  'handoff_id',
  'turn_id',
  'turn_version',
  'control_id',
  'error',
  'received_at',
  'committed_at',
];

export function validateInteractionAnswerResultReplay(firstResult, replayResult, options = {}) {
  validateInteractionAnswerResult(firstResult, options);
  validateInteractionAnswerResult(replayResult, options);
  if (firstResult.status !== 'accepted' || replayResult.status !== 'duplicate') {
    reject('Answer result replay requires an accepted first result and duplicate replay.');
  }
  const project = (result) => Object.fromEntries(
    ANSWER_RESULT_REPLAY_FIELDS.map((fieldName) => [fieldName, result[fieldName]]),
  );
  if (canonicalizeJson(project(firstResult)) !== canonicalizeJson(project(replayResult))) {
    reject('Duplicate answer result must preserve the first immutable business result.');
  }
  return true;
}

function validateHandoffClaim(record, occurredAt) {
  const claimValues = [
    record.handoff_attempt_id,
    record.handoff_attempt_no,
    record.claimed_by,
    record.claimed_at,
  ];
  const nullCount = claimValues.filter((value) => value === null).length;
  if (nullCount !== 0 && nullCount !== claimValues.length) {
    reject('Handoff attempt and claim fields must be null or non-null together.', occurredAt);
  }
  if (nullCount === claimValues.length) return false;
  validateOpaqueId('handoff_attempt_id', record.handoff_attempt_id, { occurredAt });
  validatePositiveInteger('handoff_attempt_no', record.handoff_attempt_no, occurredAt);
  validateOpaqueId('claimed_by', record.claimed_by, { occurredAt });
  validateRfc3339Timestamp('claimed_at', record.claimed_at, { occurredAt });
  return true;
}

function validateNullableTimestamp(fieldName, value, occurredAt) {
  if (value === null) return;
  validateRfc3339Timestamp(fieldName, value, { occurredAt });
}

export function validateInteractionHandoff(value, { occurredAt } = {}) {
  assertExactKeys('interaction handoff', value, INTERACTION_HANDOFF_SCHEMA_V1.requiredFields, occurredAt);
  for (const fieldName of ['handoff_id', 'interaction_id', 'answer_id']) {
    validateOpaqueId(fieldName, value[fieldName], { occurredAt });
  }
  validateSafetyCriticalEnum(
    'parent_type',
    value.parent_type,
    INTERACTION_HANDOFF_SCHEMA_V1.parentTypes,
    { occurredAt },
  );
  validateSafetyCriticalEnum(
    'state',
    value.state,
    INTERACTION_HANDOFF_SCHEMA_V1.states,
    { occurredAt },
  );
  if (value.parent_type === 'provider_turn') {
    validateOpaqueId('provider_attempt_id', value.provider_attempt_id, { occurredAt });
    validatePositiveInteger('lease_epoch', value.lease_epoch, occurredAt);
  } else if (value.provider_attempt_id !== null || value.lease_epoch !== null) {
    reject('Control handoff provider_attempt_id and lease_epoch must be null.', occurredAt);
  }

  const hasClaim = validateHandoffClaim(value, occurredAt);
  validateNullableTimestamp('last_send_started_at', value.last_send_started_at, occurredAt);
  validateNullableTimestamp('provider_acked_at', value.provider_acked_at, occurredAt);
  validateRfc3339Timestamp('handoff_deadline_at', value.handoff_deadline_at, { occurredAt });
  validateNullableOpaqueId('reason_code', value.reason_code, occurredAt);
  if (value.error !== null) validateContractError(value.error, { occurredAt });
  validateSafetyCriticalEnum(
    'side_effect_status',
    value.side_effect_status,
    SIDE_EFFECT_STATUSES,
    { occurredAt },
  );

  if (value.state === 'pending') {
    if (
      hasClaim
      || value.last_send_started_at !== null
      || value.provider_acked_at !== null
      || value.error !== null
    ) {
      reject('Pending handoff cannot contain claim, send, ack, or error fields.', occurredAt);
    }
  } else if (!hasClaim) {
    if (value.state !== 'cancelled') {
      reject(`${value.state} handoff requires attempt and claim fields.`, occurredAt);
    }
  }

  if (value.state === 'delivering') {
    if (value.provider_acked_at !== null || value.error !== null) {
      reject('Delivering handoff cannot contain provider ack or error.', occurredAt);
    }
  } else if (value.state === 'retry_wait') {
    if (value.provider_acked_at !== null || value.error === null) {
      reject('retry_wait requires an error and no provider ack.', occurredAt);
    }
  } else if (value.state === 'accepted') {
    if (
      value.last_send_started_at === null
      || value.provider_acked_at === null
      || value.error !== null
      || value.side_effect_status !== 'known'
    ) {
      reject('Accepted handoff requires send/ack evidence, known effects, and no error.', occurredAt);
    }
  } else if (value.state === 'delivery_unknown') {
    if (
      value.last_send_started_at === null
      || value.provider_acked_at !== null
      || value.reason_code === null
      || value.error === null
      || value.side_effect_status !== 'unknown'
    ) {
      reject('delivery_unknown requires send evidence, reason/error, unknown effects, and no ack.', occurredAt);
    }
  } else if (value.state === 'rejected') {
    if (
      value.last_send_started_at === null
      || value.provider_acked_at === null
      || value.reason_code === null
      || value.error === null
    ) {
      reject('Rejected handoff requires send/ack evidence plus reason and error.', occurredAt);
    }
  } else if (value.state === 'cancelled') {
    if (
      value.reason_code === null
      || value.last_send_started_at !== null
      || value.provider_acked_at !== null
    ) {
      reject('Cancelled handoff requires a reason and cannot contain send/ack evidence.', occurredAt);
    }
  }
  return structuredClone(value);
}

function validateTransitionInput(
  from,
  to,
  states,
  transitions,
  transitionName,
  occurredAt,
) {
  validateSafetyCriticalEnum(`${transitionName}.from`, from, states, { occurredAt });
  validateSafetyCriticalEnum(`${transitionName}.to`, to, states, { occurredAt });
  if (!transitions[from].includes(to)) {
    reject(`${transitionName} ${from} -> ${to} is prohibited.`, occurredAt);
  }
}

function validateTransitionBoolean(fieldName, value, occurredAt) {
  if (typeof value !== 'boolean') reject(`${fieldName} must be a boolean.`, occurredAt);
}

export function validateInteractionTransition({
  from,
  to,
  sendStarted = false,
  acknowledgementProven = false,
  occurredAt,
} = {}) {
  validateTransitionBoolean('sendStarted', sendStarted, occurredAt);
  validateTransitionBoolean('acknowledgementProven', acknowledgementProven, occurredAt);
  validateTransitionInput(
    from,
    to,
    INTERACTION_REQUEST_SCHEMA_V1.states,
    INTERACTION_TRANSITIONS_V1,
    'Interaction transition',
    occurredAt,
  );
  if (from === 'answer_delivering') {
    const postSend = ['answered', 'rejected', 'delivery_unknown'].includes(to);
    if (postSend !== sendStarted) {
      reject(
        postSend
          ? `${to} requires evidence that answer delivery started.`
          : `${to} is only permitted before answer delivery starts.`,
        occurredAt,
      );
    }
  }
  if (from === 'delivery_unknown' && to === 'answered' && !acknowledgementProven) {
    reject('delivery_unknown can become answered only after proven idempotent acknowledgement.', occurredAt);
  }
  return true;
}

export function validateInteractionHandoffTransition({
  from,
  to,
  sendStarted = false,
  safeToRetry = false,
  acknowledgementProven = false,
  occurredAt,
} = {}) {
  validateTransitionBoolean('sendStarted', sendStarted, occurredAt);
  validateTransitionBoolean('safeToRetry', safeToRetry, occurredAt);
  validateTransitionBoolean('acknowledgementProven', acknowledgementProven, occurredAt);
  validateTransitionInput(
    from,
    to,
    INTERACTION_HANDOFF_SCHEMA_V1.states,
    INTERACTION_HANDOFF_TRANSITIONS_V1,
    'Interaction handoff transition',
    occurredAt,
  );
  if (from === 'delivering') {
    const postSend = ['accepted', 'rejected', 'delivery_unknown'].includes(to);
    if (postSend && !sendStarted) {
      reject(`${to} requires evidence that handoff delivery started.`, occurredAt);
    }
    if (to === 'cancelled' && sendStarted) {
      reject('A handoff can be cancelled from delivering only before send starts.', occurredAt);
    }
    if (to === 'retry_wait' && !safeToRetry) {
      reject('retry_wait requires proof that retry cannot duplicate provider effects.', occurredAt);
    }
  }
  if (from === 'delivery_unknown' && to === 'accepted' && !acknowledgementProven) {
    reject('delivery_unknown can become accepted only after proven idempotent acknowledgement.', occurredAt);
  }
  return true;
}
