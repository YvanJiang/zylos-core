import {
  partitionContractDocument,
  rejectContract,
  requireCriticalEnum,
  requireDisplayString,
  requireNonNegativeInteger,
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
import { SIDE_EFFECT_STATUSES } from './constants.js';

export const NORMALIZED_EVENT_CONTRACT = 'zylos.normalized-event';

export const TURN_STATES = Object.freeze([
  'received',
  'queued',
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);

export const TERMINAL_TURN_STATES = Object.freeze([
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);

export const NORMALIZED_EVENT_PHASES = Object.freeze([
  'received',
  'queued',
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
  'retrying',
  ...TERMINAL_TURN_STATES,
]);

export const INTERACTION_EVENT_KINDS = Object.freeze([
  'interaction_requested',
  'interaction_answer_committed',
  'interaction_answer_handoff_started',
  'interaction_answered',
  'interaction_answer_delivery_unknown',
  'interaction_rejected',
  'interaction_expired',
  'interaction_cancelled',
]);

export const RETRY_EVENT_KINDS = Object.freeze([
  'retry_scheduled',
  'retry_attempt_started',
  'retry_exhausted',
]);

export const RECOVERY_EVENT_KINDS = Object.freeze([
  'recovery_started',
  'recovery_waiting_decision',
  'recovery_finished',
]);

export const NORMALIZED_EVENT_KINDS = Object.freeze([
  'turn_state_changed',
  'text_delta',
  'text_snapshot',
  'tool_started',
  'tool_progress',
  'tool_finished',
  ...INTERACTION_EVENT_KINDS,
  ...RETRY_EVENT_KINDS,
  ...RECOVERY_EVENT_KINDS,
  'permission_changed',
  'permission_rejected',
  'permission_expired',
  'delivery_degraded',
]);

const PROVIDERS = Object.freeze(['claude', 'codex']);
const EVENT_FIELDS = Object.freeze([
  'event_id',
  'trace_id',
  'conversation_id',
  'turn_id',
  'lineage_id',
  'event_sequence',
  'turn_version',
  'attempt_id',
  'attempt_no',
  'lease_epoch',
  'kind',
  'phase',
  'occurred_at',
  'persisted_at',
  'provider',
  'provider_native_id',
  'payload',
  'causation_event_id',
  'error',
]);
const KIND_PATTERN = /^[a-z][a-z0-9_]*$/;
const RESERVED_UNKNOWN_KIND_PATTERN = /^(?:turn_|tool_|interaction_|retry_|recovery_|permission_|control_|delivery_|security_|terminal_)/;
const PROVIDER_PROGRESS_EVENT_KINDS = new Set([
  'text_delta',
  'text_snapshot',
  'tool_started',
  'tool_progress',
  'tool_finished',
]);
const PROVIDER_OUTPUT_EVENT_KINDS = new Set([
  ...PROVIDER_PROGRESS_EVENT_KINDS,
  'interaction_requested',
]);
const UNKNOWN_PROGRESS_FORBIDDEN_FIELDS = new Set([
  'state',
  'from_state',
  'to_state',
  'terminal',
  'permission',
  'authorized_subjects',
  'interaction_id',
  'control_id',
  'side_effect_status',
]);
const ERROR_REQUIRED_KINDS = new Set([
  'interaction_answer_delivery_unknown',
  'interaction_rejected',
  'interaction_expired',
  ...RETRY_EVENT_KINDS,
  'recovery_started',
  'recovery_waiting_decision',
  'permission_rejected',
  'delivery_degraded',
]);
const ERROR_TURN_STATES = new Set(['interrupted', 'failed', 'timed_out']);

function validateUnknownProgressKinds(unknownProgressKinds) {
  if (
    !Array.isArray(unknownProgressKinds)
    || unknownProgressKinds.some(
      (kind) => typeof kind !== 'string' || !KIND_PATTERN.test(kind),
    )
    || new Set(unknownProgressKinds).size !== unknownProgressKinds.length
  ) {
    throw new TypeError('unknownProgressKinds must be a unique array of normalized kind names');
  }
  return new Set(unknownProgressKinds);
}

function findForbiddenUnknownProgressField(value) {
  if (!value || typeof value !== 'object') return null;
  if (Array.isArray(value)) {
    for (const entry of value) {
      const fieldName = findForbiddenUnknownProgressField(entry);
      if (fieldName) return fieldName;
    }
    return null;
  }
  for (const [fieldName, fieldValue] of Object.entries(value)) {
    if (UNKNOWN_PROGRESS_FORBIDDEN_FIELDS.has(fieldName)) return fieldName;
    const nestedName = findForbiddenUnknownProgressField(fieldValue);
    if (nestedName) return nestedName;
  }
  return null;
}

function classifyEventKind(value, unknownProgressKinds, occurredAt) {
  if (typeof value.kind !== 'string' || !KIND_PATTERN.test(value.kind)) {
    rejectContract('validation_error', 'kind must be a stable lowercase machine name.', {
      occurredAt,
    });
  }
  if (NORMALIZED_EVENT_KINDS.includes(value.kind)) return 'known';

  if (
    RESERVED_UNKNOWN_KIND_PATTERN.test(value.kind)
    || !unknownProgressKinds.has(value.kind)
  ) {
    rejectContract(
      'unsupported_capability',
      `Normalized event kind ${value.kind} is not an allowed opaque progress capability.`,
      { occurredAt },
    );
  }
  if (!['starting', 'running'].includes(value.phase) || value.error !== null) {
    rejectContract(
      'unsupported_capability',
      'Opaque progress events must remain error-free starting or running progress.',
      { occurredAt },
    );
  }
  const forbiddenField = findForbiddenUnknownProgressField(value.payload);
  if (forbiddenField) {
    rejectContract(
      'unsupported_capability',
      `Opaque progress payload cannot carry lifecycle field ${forbiddenField}.`,
      { occurredAt },
    );
  }
  return 'opaque_progress';
}

function validateTurnStatePayload(value, occurredAt) {
  requireOwnFields('payload', value.payload, ['from_state', 'to_state', 'reason_code'], {
    occurredAt,
  });
  if (value.payload.from_state !== null) {
    requireCriticalEnum('payload.from_state', value.payload.from_state, TURN_STATES, { occurredAt });
  }
  requireCriticalEnum('payload.to_state', value.payload.to_state, TURN_STATES, { occurredAt });
  validateOpaqueId('payload.reason_code', value.payload.reason_code, { occurredAt });
  if (value.phase !== value.payload.to_state) {
    rejectContract('validation_error', 'turn_state_changed phase must equal payload.to_state.', {
      occurredAt,
    });
  }
  if (
    ['interrupted', 'failed', 'timed_out'].includes(value.payload.to_state)
    && value.error === null
  ) {
    rejectContract(
      'validation_error',
      `${value.payload.to_state} state event requires the public error shape.`,
      { occurredAt },
    );
  }
}

function validateTextPayload(value, occurredAt) {
  requireDisplayString('payload.text', value.payload.text, { occurredAt });
  if (value.kind === 'text_delta') {
    requireOwnFields('payload', value.payload, ['text', 'start_offset', 'end_offset'], { occurredAt });
    requireNonNegativeInteger('payload.start_offset', value.payload.start_offset, { occurredAt });
    requireNonNegativeInteger('payload.end_offset', value.payload.end_offset, { occurredAt });
    if (value.payload.end_offset < value.payload.start_offset) {
      rejectContract('validation_error', 'payload.end_offset must not precede start_offset.', {
        occurredAt,
      });
    }
    return;
  }
  requireOwnFields('payload', value.payload, ['text', 'end_offset'], { occurredAt });
  requireNonNegativeInteger('payload.end_offset', value.payload.end_offset, { occurredAt });
}

function validateToolPayload(value, occurredAt) {
  requireOwnFields(
    'payload',
    value.payload,
    ['tool_use_id', 'tool_name', 'summary', 'side_effect_status'],
    { occurredAt },
  );
  validateOpaqueId('payload.tool_use_id', value.payload.tool_use_id, { occurredAt });
  requireDisplayString('payload.tool_name', value.payload.tool_name, { occurredAt });
  requireDisplayString('payload.summary', value.payload.summary, { occurredAt });
  requireCriticalEnum(
    'payload.side_effect_status',
    value.payload.side_effect_status,
    SIDE_EFFECT_STATUSES,
    { occurredAt },
  );
}

function validateInteractionPayload(value, occurredAt) {
  requireOwnFields(
    'payload',
    value.payload,
    ['interaction_id', 'ordinal', 'interaction_version', 'handoff_version'],
    { occurredAt },
  );
  validateOpaqueId('payload.interaction_id', value.payload.interaction_id, { occurredAt });
  requirePositiveInteger('payload.ordinal', value.payload.ordinal, { occurredAt });
  requirePositiveInteger('payload.interaction_version', value.payload.interaction_version, {
    occurredAt,
  });
  if (value.payload.handoff_version !== null) {
    requirePositiveInteger('payload.handoff_version', value.payload.handoff_version, { occurredAt });
  }
}

function validateRetryPayload(value, occurredAt) {
  requireOwnFields(
    'payload',
    value.payload,
    ['retry_no', 'attempt_no', 'backoff_ms', 'reason_code'],
    { occurredAt },
  );
  requirePositiveInteger('payload.retry_no', value.payload.retry_no, { occurredAt });
  requirePositiveInteger('payload.attempt_no', value.payload.attempt_no, { occurredAt });
  requireNonNegativeInteger('payload.backoff_ms', value.payload.backoff_ms, { occurredAt });
  validateOpaqueId('payload.reason_code', value.payload.reason_code, { occurredAt });
  if (
    value.kind === 'retry_attempt_started'
    && value.payload.attempt_no !== value.attempt_no
  ) {
    rejectContract(
      'validation_error',
      'retry_attempt_started payload.attempt_no must match the current attempt fence.',
      { occurredAt },
    );
  }
}

function validateRecoveryPayload(value, occurredAt) {
  requireOwnFields(
    'payload',
    value.payload,
    [
      'recovery_id',
      'recovery_of_turn_id',
      'recovery_of_lineage_id',
      'side_effect_status',
    ],
    { occurredAt },
  );
  validateOpaqueId('payload.recovery_id', value.payload.recovery_id, { occurredAt });
  requireNullableOpaqueId(
    'payload.recovery_of_turn_id',
    value.payload.recovery_of_turn_id,
    { occurredAt },
  );
  requireNullableOpaqueId(
    'payload.recovery_of_lineage_id',
    value.payload.recovery_of_lineage_id,
    { occurredAt },
  );
  requireCriticalEnum(
    'payload.side_effect_status',
    value.payload.side_effect_status,
    SIDE_EFFECT_STATUSES,
    { occurredAt },
  );
}

function validatePermissionPayload(value, occurredAt) {
  requireOwnFields('payload', value.payload, ['scope', 'actor_id', 'audit_id'], { occurredAt });
  requirePlainObject('payload.scope', value.payload.scope, { occurredAt });
  validateOpaqueId('payload.actor_id', value.payload.actor_id, { occurredAt });
  validateOpaqueId('payload.audit_id', value.payload.audit_id, { occurredAt });
}

function validateDeliveryPayload(value, occurredAt) {
  requireOwnFields('payload', value.payload, ['delivery_id', 'reason_code'], { occurredAt });
  validateOpaqueId('payload.delivery_id', value.payload.delivery_id, { occurredAt });
  validateOpaqueId('payload.reason_code', value.payload.reason_code, { occurredAt });
}

function validateKnownPayload(value, occurredAt) {
  if (value.kind === 'turn_state_changed') validateTurnStatePayload(value, occurredAt);
  else if (['text_delta', 'text_snapshot'].includes(value.kind)) validateTextPayload(value, occurredAt);
  else if (value.kind.startsWith('tool_')) validateToolPayload(value, occurredAt);
  else if (INTERACTION_EVENT_KINDS.includes(value.kind)) validateInteractionPayload(value, occurredAt);
  else if (RETRY_EVENT_KINDS.includes(value.kind)) validateRetryPayload(value, occurredAt);
  else if (RECOVERY_EVENT_KINDS.includes(value.kind)) validateRecoveryPayload(value, occurredAt);
  else if (value.kind.startsWith('permission_')) validatePermissionPayload(value, occurredAt);
  else if (value.kind === 'delivery_degraded') validateDeliveryPayload(value, occurredAt);
}

export function validateNormalizedEvent(
  value,
  { occurredAt, unknownProgressKinds = [] } = {},
) {
  const result = partitionContractDocument(
    value,
    NORMALIZED_EVENT_CONTRACT,
    EVENT_FIELDS,
    { occurredAt },
  );
  requireOwnFields('normalized-event', value, EVENT_FIELDS, { occurredAt });
  for (const fieldName of ['event_id', 'trace_id', 'conversation_id', 'turn_id']) {
    validateOpaqueId(fieldName, value[fieldName], { occurredAt });
  }
  requireNullableOpaqueId('lineage_id', value.lineage_id, { occurredAt });
  requirePositiveInteger('event_sequence', value.event_sequence, { occurredAt });
  requirePositiveInteger('turn_version', value.turn_version, { occurredAt });
  requireNullableOpaqueId('attempt_id', value.attempt_id, { occurredAt });
  if (value.attempt_no !== null) {
    requirePositiveInteger('attempt_no', value.attempt_no, { occurredAt });
  }
  if (value.lease_epoch !== null) {
    requirePositiveInteger('lease_epoch', value.lease_epoch, { occurredAt });
  }
  if ((value.attempt_id === null) !== (value.attempt_no === null)) {
    rejectContract(
      'validation_error',
      'attempt_id and attempt_no must be both null or both non-null.',
      { occurredAt },
    );
  }
  requireCriticalEnum('phase', value.phase, NORMALIZED_EVENT_PHASES, { occurredAt });
  requireTimestamp('occurred_at', value.occurred_at, { occurredAt });
  requireTimestamp('persisted_at', value.persisted_at, { occurredAt });
  if (value.provider !== null) {
    requireCriticalEnum('provider', value.provider, PROVIDERS, { occurredAt });
  }
  requireNullableOpaqueId('provider_native_id', value.provider_native_id, { occurredAt });
  requireNullableOpaqueId('causation_event_id', value.causation_event_id, { occurredAt });
  requirePlainObject('payload', value.payload, { occurredAt });
  if (value.error !== null) validateContractError(value.error, { occurredAt });
  if (value.attempt_id !== null && (value.provider === null || value.lease_epoch === null)) {
    rejectContract(
      'validation_error',
      'provider attempt events require a provider and lease_epoch.',
      { occurredAt },
    );
  }
  if (value.provider_native_id !== null && value.provider === null) {
    rejectContract('validation_error', 'provider_native_id requires a provider.', { occurredAt });
  }
  if (value.provider !== null && (value.attempt_id === null || value.lease_epoch === null)) {
    rejectContract(
      'validation_error',
      'provider events require the current attempt and lease fence.',
      { occurredAt },
    );
  }
  if (PROVIDER_OUTPUT_EVENT_KINDS.has(value.kind) && value.provider === null) {
    rejectContract(
      'validation_error',
      `${value.kind} must be attributed to a fenced provider attempt.`,
      { occurredAt },
    );
  }
  if (
    value.lineage_id === null
    && (
      value.provider !== null
      || value.attempt_id !== null
      || !['turn_state_changed', ...RECOVERY_EVENT_KINDS].includes(value.kind)
      || ![
        'received',
        'queued',
        'starting',
        'recovering',
        ...(value.kind === 'turn_state_changed' ? ['stopped', 'cancelled'] : []),
      ].includes(value.phase)
    )
  ) {
    rejectContract(
      'validation_error',
      'lineage_id may be null only for attemptless pending-recovery lifecycle events.',
      { occurredAt },
    );
  }

  const kindClassification = classifyEventKind(
    value,
    validateUnknownProgressKinds(unknownProgressKinds),
    occurredAt,
  );
  if (kindClassification === 'opaque_progress' && value.provider === null) {
    rejectContract(
      'validation_error',
      'Opaque provider progress requires the current provider attempt and lease fence.',
      { occurredAt },
    );
  }
  if (kindClassification === 'known') {
    if (
      TERMINAL_TURN_STATES.includes(value.phase)
      && value.kind !== 'turn_state_changed'
    ) {
      rejectContract(
        'validation_error',
        'Only turn_state_changed can publish a terminal phase.',
        { occurredAt },
      );
    }
    if (value.phase === 'retrying' && !RETRY_EVENT_KINDS.includes(value.kind)) {
      rejectContract('validation_error', 'retrying phase is reserved for retry events.', {
        occurredAt,
      });
    }
    const turnStateError = value.kind === 'turn_state_changed'
      && ERROR_TURN_STATES.has(value.payload.to_state);
    if (
      value.error !== null
      && !turnStateError
      && !ERROR_REQUIRED_KINDS.has(value.kind)
    ) {
      rejectContract(
        'validation_error',
        `${value.kind} is a success or progress event and must not carry an error.`,
        { occurredAt },
      );
    }
    if (ERROR_REQUIRED_KINDS.has(value.kind) && value.error === null) {
      rejectContract('validation_error', `${value.kind} requires the public error shape.`, {
        occurredAt,
      });
    }
    validateKnownPayload(value, occurredAt);
  }
  validatePublicFixtureSafety(value, { occurredAt });
  return { ...result, kindClassification };
}

export function createNormalizedEventStreamState() {
  return Object.freeze({
    turn_id: null,
    last_event_sequence: 0,
    last_turn_version: 0,
    current_state: null,
    current_attempt_id: null,
    current_attempt_no: null,
    lease_epoch: null,
    terminal: false,
  });
}

function rejectStream(code, userMessage, occurredAt) {
  rejectContract(code, userMessage, { category: 'conflict', occurredAt });
}

function validateAttemptFence(state, event, occurredAt) {
  if (state.current_attempt_id === null) {
    if (event.attempt_id !== null && event.attempt_no !== 1) {
      rejectStream('stale_attempt', 'The first provider attempt must use attempt_no=1.', occurredAt);
    }
    if (
      state.lease_epoch !== null
      && (event.lease_epoch === null || event.lease_epoch < state.lease_epoch)
    ) {
      rejectStream('stale_attempt', 'The normalized event carries a stale lease fence.', occurredAt);
    }
    return {
      current_attempt_id: event.attempt_id,
      current_attempt_no: event.attempt_no,
      lease_epoch: event.lease_epoch,
    };
  }

  const exactCurrentFence = event.attempt_id === state.current_attempt_id
    && event.attempt_no === state.current_attempt_no
    && event.lease_epoch === state.lease_epoch;
  if (exactCurrentFence) {
    return {
      current_attempt_id: state.current_attempt_id,
      current_attempt_no: state.current_attempt_no,
      lease_epoch: state.lease_epoch,
    };
  }

  const validRetryFence = event.kind === 'retry_attempt_started'
    && event.attempt_id !== null
    && event.attempt_id !== state.current_attempt_id
    && event.attempt_no === state.current_attempt_no + 1
    && event.lease_epoch > state.lease_epoch;
  if (!validRetryFence) {
    rejectStream(
      'stale_attempt',
      'The normalized event does not match the current provider attempt and lease fence.',
      occurredAt,
    );
  }
  return {
    current_attempt_id: event.attempt_id,
    current_attempt_no: event.attempt_no,
    lease_epoch: event.lease_epoch,
  };
}

export function admitNormalizedEvent(
  state,
  event,
  { occurredAt, unknownProgressKinds = [] } = {},
) {
  requirePlainObject('normalized event stream state', state, { occurredAt });
  if (state.terminal) {
    rejectStream('turn_terminal', 'Terminal turns reject late normalized events.', occurredAt);
  }

  const validatedEvent = validateNormalizedEvent(event, { occurredAt, unknownProgressKinds });
  if (state.turn_id !== null && event.turn_id !== state.turn_id) {
    rejectStream('version_conflict', 'A normalized event stream cannot change turn_id.', occurredAt);
  }
  if (event.event_sequence !== state.last_event_sequence + 1) {
    rejectStream(
      'version_conflict',
      'event_sequence must be continuous and increase by exactly one.',
      occurredAt,
    );
  }
  if (event.turn_version <= state.last_turn_version) {
    rejectStream('version_conflict', 'turn_version must increase monotonically.', occurredAt);
  }
  if (state.current_state === null && event.kind !== 'turn_state_changed') {
    rejectStream(
      'version_conflict',
      'The first normalized event must establish canonical lifecycle state.',
      occurredAt,
    );
  }

  const attemptFence = validateAttemptFence(state, event, occurredAt);
  const providerProgress = PROVIDER_PROGRESS_EVENT_KINDS.has(event.kind)
    || validatedEvent.kindClassification === 'opaque_progress';
  if (
    providerProgress
    && (
      !['starting', 'running'].includes(state.current_state)
      || !['starting', 'running'].includes(event.phase)
    )
  ) {
    rejectStream(
      'version_conflict',
      'Provider progress requires an admitted starting or running lifecycle state.',
      occurredAt,
    );
  }
  let currentState = state.current_state;
  if (event.kind === 'turn_state_changed') {
    if (event.payload.from_state !== currentState) {
      rejectStream(
        'version_conflict',
        'turn_state_changed.from_state does not match the admitted stream state.',
        occurredAt,
      );
    }
    currentState = event.payload.to_state;
  }

  return Object.freeze({
    turn_id: state.turn_id ?? event.turn_id,
    last_event_sequence: event.event_sequence,
    last_turn_version: event.turn_version,
    current_state: currentState,
    current_attempt_id: attemptFence.current_attempt_id,
    current_attempt_no: attemptFence.current_attempt_no,
    lease_epoch: attemptFence.lease_epoch,
    terminal: TERMINAL_TURN_STATES.includes(currentState),
  });
}
