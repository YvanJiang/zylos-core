import { SIDE_EFFECT_STATUSES } from './constants.js';
import { isPlainJsonObject } from './scalars.js';
import { validatePublicFixtureSafety } from './validation.js';
import {
  deepFreeze,
  finishRuntimeContract,
  rejectRuntimeContract,
  runtimeContractsEqual,
  requireArray,
  requireBoolean,
  requireEnum,
  requireErrorOrNull,
  requireFields,
  requireInteger,
  requireNullableEnum,
  requireNullableOpaqueId,
  requireNullableText,
  requireOpaqueId,
  requireRecord,
  requireText,
  requireTimestamp,
} from './runtime-contract-validation.js';
import {
  CANONICAL_TURN_STATES,
  PROVIDERS,
  RUNTIME_HEALTH_STATES,
} from './runtime-vocabulary.js';

const PROJECTION_FIELDS = Object.freeze([
  'projection_id',
  'dashboard_instance_id',
  'projection_sequence',
  'generated_at',
  'source_core_service_instance_id',
  'source_snapshot_version',
  'service',
  'runtimes',
  'capabilities',
  'complete',
  'error',
]);

const PROJECTION_SUPPORTED_FIELDS = Object.freeze([
  'service',
  'runtimes',
  'queue_length',
  'wait_reason',
  'side_effect_status',
]);

const CAPABILITY_FIELDS = Object.freeze([
  'supported_fields',
  'supported_states',
  'control',
  'core_direct_access',
]);

const CAPABILITY_ACCESS_FIELD_PATTERN = new RegExp(
  '(?:^|_)(?:admin|command|control|core|direct_access|endpoint|mutation|write)(?:_|$)',
);
const PRESENTATION_FIELD_PATTERN = new RegExp(
  '(?:^|_)(?:color|display|hint|icon|label|mode|presentation|rendering)(?:_|$)',
);

const PROJECTION_FIELD_RULES = deepFreeze({
  projection_id: { required: true, kind: 'opaque_id' },
  dashboard_instance_id: { required: true, kind: 'opaque_id' },
  projection_sequence: { required: true, kind: 'number' },
  generated_at: { required: true, kind: 'rfc3339' },
  source_core_service_instance_id: { required: true, kind: 'opaque_id' },
  source_snapshot_version: { required: true, kind: 'number' },
  service: { required: true, kind: 'prevalidated' },
  runtimes: { required: true, kind: 'prevalidated' },
  capabilities: { required: true, kind: 'prevalidated' },
  complete: { required: true, kind: 'boolean' },
  error: { required: true, kind: 'prevalidated' },
});

export const DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA = deepFreeze({
  contract: 'zylos.dashboard-runtime-projection',
  contract_version: '1.0',
  required: ['contract', 'contract_version', ...PROJECTION_FIELDS],
  canonical_turn_states: CANONICAL_TURN_STATES,
  executor_health_states: RUNTIME_HEALTH_STATES,
  supported_fields: PROJECTION_SUPPORTED_FIELDS,
  ordering: 'dashboard_instance_id_projection_sequence',
  first_event: 'complete_projection',
  consumer_boundary: {
    producer: 'dashboard',
    consumer: 'luna',
    transport_event: 'runtime_projection',
    control: false,
    core_direct_access: false,
  },
});

function validateService(value, options) {
  requireFields('service', value, [
    'health',
    'maintenance',
    'reconciling',
    'last_update_at',
  ], options);
  requireEnum('service.health', value.health, RUNTIME_HEALTH_STATES, options);
  requireBoolean('service.maintenance', value.maintenance, options);
  requireBoolean('service.reconciling', value.reconciling, options);
  requireTimestamp('service.last_update_at', value.last_update_at, options);
}

function validateRuntime(path, value, options) {
  requireFields(path, value, [
    'runtime_id',
    'conversation_id',
    'display_label',
    'provider',
    'executor_health',
    'active_turn_state',
    'queue_length',
    'wait_reason',
    'side_effect_status',
    'last_changed_at',
  ], options);
  requireOpaqueId(`${path}.runtime_id`, value.runtime_id, options);
  requireNullableOpaqueId(`${path}.conversation_id`, value.conversation_id, options);
  requireText(`${path}.display_label`, value.display_label, options);
  requireNullableEnum(`${path}.provider`, value.provider, PROVIDERS, options);
  requireEnum(`${path}.executor_health`, value.executor_health, RUNTIME_HEALTH_STATES, options);
  requireNullableEnum(
    `${path}.active_turn_state`,
    value.active_turn_state,
    CANONICAL_TURN_STATES,
    options,
  );
  requireInteger(`${path}.queue_length`, value.queue_length, { min: 0, ...options });
  requireNullableText(`${path}.wait_reason`, value.wait_reason, options);
  requireNullableEnum(
    `${path}.side_effect_status`,
    value.side_effect_status,
    SIDE_EFFECT_STATUSES,
    options,
  );
  requireTimestamp(`${path}.last_changed_at`, value.last_changed_at, options);
}

function requireUniqueStrings(path, value, options) {
  requireArray(path, value, options);
  const seen = new Set();
  value.forEach((entry, index) => {
    requireText(`${path}[${index}]`, entry, options);
    if (seen.has(entry)) {
      rejectRuntimeContract('validation_error', `${path} must not contain duplicates.`, options);
    }
    seen.add(entry);
  });
}

function normalizeCapabilityFieldName(fieldName) {
  return fieldName
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^A-Za-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

function rejectCapabilityAccessField(path, fieldName, options) {
  const normalized = normalizeCapabilityFieldName(fieldName);
  if (CAPABILITY_ACCESS_FIELD_PATTERN.test(normalized)) {
    rejectRuntimeContract(
      'unsupported_capability',
      `${path}.${fieldName} cannot advertise control or direct Core access.`,
      options,
    );
  }
  return normalized;
}

function validatePresentationValue(path, value, options) {
  if (isPlainJsonObject(value)) {
    validatePresentationMetadata(path, value, options);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) => (
      validatePresentationValue(`${path}[${index}]`, entry, options)
    ));
    return;
  }
  if (value !== null && typeof value === 'object') {
    rejectRuntimeContract(
      'validation_error',
      `${path} must contain only JSON presentation metadata.`,
      options,
    );
  }
}

function validatePresentationMetadata(path, value, options) {
  requireRecord(path, value, options);
  for (const [fieldName, fieldValue] of Object.entries(value)) {
    const childPath = `${path}.${fieldName}`;
    const normalized = rejectCapabilityAccessField(path, fieldName, options);
    if (!PRESENTATION_FIELD_PATTERN.test(normalized)) {
      rejectRuntimeContract(
        'unsupported_capability',
        `${childPath} is not presentation-only capability metadata.`,
        options,
      );
    }
    validatePresentationValue(childPath, fieldValue, options);
  }
}

function validateCapabilityExtensions(value, options) {
  const knownFields = new Set(CAPABILITY_FIELDS);
  for (const [fieldName, fieldValue] of Object.entries(value)) {
    if (knownFields.has(fieldName)) continue;
    rejectCapabilityAccessField('capabilities', fieldName, options);
    validatePresentationMetadata(`capabilities.${fieldName}`, fieldValue, options);
  }
}

function validateCapabilities(value, options) {
  requireFields('capabilities', value, CAPABILITY_FIELDS, options);
  validateCapabilityExtensions(value, options);
  requireUniqueStrings('capabilities.supported_fields', value.supported_fields, options);
  value.supported_fields.forEach((field, index) => requireEnum(
    `capabilities.supported_fields[${index}]`,
    field,
    PROJECTION_SUPPORTED_FIELDS,
    options,
  ));
  requireArray('capabilities.supported_states', value.supported_states, options);
  const seenStates = new Set();
  value.supported_states.forEach((state, index) => {
    requireEnum(
      `capabilities.supported_states[${index}]`,
      state,
      CANONICAL_TURN_STATES,
      options,
    );
    if (seenStates.has(state)) {
      rejectRuntimeContract(
        'validation_error',
        'capabilities.supported_states must not contain duplicates.',
        options,
      );
    }
    seenStates.add(state);
  });
  if (value.control !== false || value.core_direct_access !== false) {
    rejectRuntimeContract(
      'unsupported_capability',
      'The Luna projection cannot grant control or direct Core access.',
      options,
    );
  }
}

export function validateDashboardRuntimeProjection(value, { occurredAt } = {}) {
  const options = { occurredAt };
  requireRecord('runtime projection', value, options);
  validatePublicFixtureSafety(value, options);
  requireFields(
    'runtime projection',
    value,
    ['contract', 'contract_version', ...PROJECTION_FIELDS],
    options,
  );
  requireOpaqueId('projection_id', value.projection_id, options);
  requireOpaqueId('dashboard_instance_id', value.dashboard_instance_id, options);
  requireInteger('projection_sequence', value.projection_sequence, { min: 1, ...options });
  requireTimestamp('generated_at', value.generated_at, options);
  requireOpaqueId(
    'source_core_service_instance_id',
    value.source_core_service_instance_id,
    options,
  );
  requireInteger('source_snapshot_version', value.source_snapshot_version, {
    min: 1,
    ...options,
  });
  validateService(value.service, options);
  requireArray('runtimes', value.runtimes, options);
  value.runtimes.forEach((runtime, index) => validateRuntime(`runtimes[${index}]`, runtime, options));
  validateCapabilities(value.capabilities, options);
  requireBoolean('complete', value.complete, options);
  requireErrorOrNull('error', value.error, options);
  if (value.complete && value.error !== null) {
    rejectRuntimeContract('validation_error', 'A complete projection must have error=null.', options);
  }
  if (!value.complete && value.error === null) {
    rejectRuntimeContract('validation_error', 'A degraded projection must include an error.', options);
  }

  return finishRuntimeContract(value, {
    contract: DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA.contract,
    fieldRules: PROJECTION_FIELD_RULES,
    occurredAt,
  });
}

export function resolveDashboardRuntimeProjectionUpdate(
  currentValue,
  nextValue,
  { requiresFull = false } = {},
) {
  const next = validateDashboardRuntimeProjection(nextValue).forwarded;
  if (currentValue === null || currentValue === undefined) {
    return next.complete
      ? { status: 'initial', apply: true, requires_full: false }
      : { status: 'initial_degraded', apply: false, requires_full: true };
  }

  const current = validateDashboardRuntimeProjection(currentValue).forwarded;
  if (current.dashboard_instance_id !== next.dashboard_instance_id) {
    return next.complete
      ? { status: 'replace_instance', apply: true, requires_full: false }
      : { status: 'replace_instance_degraded', apply: false, requires_full: true };
  }

  if (next.projection_sequence === current.projection_sequence) {
    if (runtimeContractsEqual(current, next)) {
      return { status: 'duplicate', apply: false, requires_full: requiresFull };
    }
    rejectRuntimeContract(
      'version_conflict',
      'The same Dashboard instance and projection sequence contain different payloads.',
      { category: 'conflict', occurredAt: next.generated_at },
    );
  }

  if (next.projection_sequence < current.projection_sequence) {
    return { status: 'obsolete', apply: false, requires_full: requiresFull };
  }
  if (requiresFull) {
    return next.complete
      ? { status: 'resync', apply: true, requires_full: false }
      : { status: 'degraded', apply: false, requires_full: true };
  }
  if (next.projection_sequence > current.projection_sequence + 1) {
    return { status: 'gap', apply: false, requires_full: true };
  }
  return { status: 'replace', apply: true, requires_full: false };
}
