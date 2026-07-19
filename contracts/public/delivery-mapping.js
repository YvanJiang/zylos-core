import {
  DELIVERY_OPERATIONS,
  DELIVERY_RESULT_STATUSES,
  MAPPING_BINDING_AUTHORITIES,
  MAPPING_BINDING_STATES,
  MAPPING_RECOVERY_REASONS,
} from './constants.js';
import { ContractKernelError, createContractError } from './errors.js';
import { verifyIdempotencyKey } from './idempotency.js';
import {
  validateContractError,
  validateContractDocument,
  validateOpaqueId,
  validatePublicNumber,
  validateRfc3339Timestamp,
  validateSafetyCriticalEnum,
} from './validation.js';

const DELIVERY_AGGREGATE_TYPES = Object.freeze([
  'turn_main',
  'interaction',
  'text_notice',
  'security_notice',
  'control_notice',
]);
const DELIVERY_CHAT_TYPES = Object.freeze(['dm', 'group', 'thread', 'synthetic']);
const DELIVERY_COMMAND_FIELDS = Object.freeze([
  'contract',
  'contract_version',
  'outbox_id',
  'delivery_id',
  'trace_id',
  'delivery_attempt_id',
  'delivery_attempt_no',
  'outbox_lease_epoch',
  'target',
  'aggregate_type',
  'aggregate_id',
  'operation',
  'aggregate_version',
  'event_sequence_through',
  'idempotency_key',
  'render_model',
  'mapping',
  'target_platform_message_id',
  'predecessor_delivery_id',
  'expected_platform_version',
  'priority',
  'not_before',
  'created_at',
]);
const DELIVERY_RESULT_FIELDS = Object.freeze([
  'contract',
  'contract_version',
  'trace_id',
  'outbox_id',
  'delivery_id',
  'idempotency_key',
  'delivery_attempt_id',
  'delivery_attempt_no',
  'outbox_lease_epoch',
  'mapping_id',
  'operation',
  'aggregate_version',
  'status',
  'platform_message_id',
  'applied_platform_version',
  'delivered_at',
  'error',
  'renderer_capabilities',
  'result_at',
]);
const RENDERER_CAPABILITY_FIELDS = Object.freeze([
  'supports_update',
  'supports_actions',
  'supports_platform_idempotency',
  'supports_platform_version',
]);

function defineContractFields(fieldNames, rules) {
  return Object.freeze(Object.fromEntries(
    fieldNames
      .filter((fieldName) => fieldName !== 'contract' && fieldName !== 'contract_version')
      .map((fieldName) => [
        fieldName,
        { required: true, kind: 'presence', ...rules[fieldName] },
      ]),
  ));
}

const DELIVERY_COMMAND_FIELD_RULES = defineContractFields(DELIVERY_COMMAND_FIELDS, {
  outbox_id: { kind: 'opaque_id' },
  delivery_id: { kind: 'opaque_id' },
  trace_id: { kind: 'opaque_id' },
  delivery_attempt_id: { kind: 'opaque_id' },
  delivery_attempt_no: { kind: 'number' },
  outbox_lease_epoch: { kind: 'number' },
  aggregate_type: { kind: 'critical_enum', values: DELIVERY_AGGREGATE_TYPES },
  aggregate_id: { kind: 'opaque_id' },
  operation: { kind: 'critical_enum', values: DELIVERY_OPERATIONS },
  aggregate_version: { kind: 'number' },
  idempotency_key: { kind: 'opaque_id' },
  priority: { kind: 'number' },
  not_before: { kind: 'rfc3339' },
  created_at: { kind: 'rfc3339' },
});

const DELIVERY_RESULT_FIELD_RULES = defineContractFields(DELIVERY_RESULT_FIELDS, {
  trace_id: { kind: 'opaque_id' },
  outbox_id: { kind: 'opaque_id' },
  delivery_id: { kind: 'opaque_id' },
  idempotency_key: { kind: 'opaque_id' },
  delivery_attempt_id: { kind: 'opaque_id' },
  delivery_attempt_no: { kind: 'number' },
  outbox_lease_epoch: { kind: 'number' },
  mapping_id: { kind: 'opaque_id' },
  operation: { kind: 'critical_enum', values: DELIVERY_OPERATIONS },
  aggregate_version: { kind: 'number' },
  status: { kind: 'critical_enum', values: DELIVERY_RESULT_STATUSES },
  result_at: { kind: 'rfc3339' },
});

function reject(code, userMessage, occurredAt, category = 'validation') {
  throw new ContractKernelError(createContractError({
    code,
    category,
    userMessage,
    occurredAt,
  }));
}

function requireObject(fieldName, value, occurredAt) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    reject('validation_error', `${fieldName} must be a JSON object.`, occurredAt);
  }
  return value;
}

function requireFields(fieldName, value, fieldNames, occurredAt) {
  for (const requiredField of fieldNames) {
    if (!Object.hasOwn(value, requiredField)) {
      reject(
        'validation_error',
        `${fieldName}.${requiredField} is required.`,
        occurredAt,
      );
    }
  }
}

function validateNullableOpaqueId(fieldName, value, occurredAt) {
  if (value === null) return null;
  return validateOpaqueId(fieldName, value, { occurredAt });
}

function validatePositiveInteger(fieldName, value, occurredAt) {
  validatePublicNumber(fieldName, value, { occurredAt });
  if (value < 1) {
    reject('validation_error', `${fieldName} must be a positive integer.`, occurredAt);
  }
  return value;
}

function validateNullablePositiveInteger(fieldName, value, occurredAt) {
  if (value === null) return null;
  return validatePositiveInteger(fieldName, value, occurredAt);
}

function validateNullablePlatformVersion(fieldName, value, occurredAt) {
  if (value === null) return null;
  if (typeof value === 'string') {
    return validateOpaqueId(fieldName, value, { occurredAt });
  }
  validatePublicNumber(fieldName, value, { occurredAt });
  if (value < 0) {
    reject('validation_error', `${fieldName} must not be negative.`, occurredAt);
  }
  return value;
}

function validateTarget(value, occurredAt) {
  requireObject('target', value, occurredAt);
  requireFields('target', value, [
    'region',
    'tenant_id',
    'channel',
    'bot_id',
    'chat_type',
    'chat_id',
    'native_thread_or_topic_id',
  ], occurredAt);
  for (const fieldName of ['region', 'tenant_id', 'channel', 'bot_id', 'chat_id']) {
    validateOpaqueId(`target.${fieldName}`, value[fieldName], { occurredAt });
  }
  validateSafetyCriticalEnum(
    'target.chat_type',
    value.chat_type,
    DELIVERY_CHAT_TYPES,
    { occurredAt },
  );
  validateNullableOpaqueId(
    'target.native_thread_or_topic_id',
    value.native_thread_or_topic_id,
    occurredAt,
  );
}

function validateNullableText(fieldName, value, occurredAt) {
  if (value === null) return;
  if (typeof value !== 'string') {
    reject('validation_error', `${fieldName} must be a string or null.`, occurredAt);
  }
}

function validateRenderModel(value, occurredAt) {
  requireObject('render_model', value, occurredAt);
  requireFields('render_model', value, [
    'title',
    'phase',
    'text',
    'error',
    'tools',
    'interactions',
    'terminal',
    'user_action_required',
  ], occurredAt);
  for (const fieldName of ['title', 'phase', 'text']) {
    validateNullableText(`render_model.${fieldName}`, value[fieldName], occurredAt);
  }
  if (value.error !== null) validateContractError(value.error, { occurredAt });
  for (const fieldName of ['tools', 'interactions']) {
    if (!Array.isArray(value[fieldName])) {
      reject('validation_error', `render_model.${fieldName} must be an array.`, occurredAt);
    }
  }
  for (const fieldName of ['terminal', 'user_action_required']) {
    if (typeof value[fieldName] !== 'boolean') {
      reject('validation_error', `render_model.${fieldName} must be a boolean.`, occurredAt);
    }
  }
}

function validateRendererCapabilities(value, occurredAt) {
  requireObject('renderer_capabilities', value, occurredAt);
  requireFields(
    'renderer_capabilities',
    value,
    RENDERER_CAPABILITY_FIELDS,
    occurredAt,
  );
  for (const fieldName of RENDERER_CAPABILITY_FIELDS) {
    if (typeof value[fieldName] !== 'boolean') {
      reject(
        'validation_error',
        `renderer_capabilities.${fieldName} must be a boolean.`,
        occurredAt,
      );
    }
  }
}

export function validateDeliveryMapping(value, { occurredAt } = {}) {
  requireObject('mapping', value, occurredAt);
  requireFields('mapping', value, [
    'mapping_id',
    'conversation_id',
    'turn_id',
    'lineage_id',
    'binding_state',
    'mapping_version',
  ], occurredAt);

  validateOpaqueId('mapping.mapping_id', value.mapping_id, { occurredAt });
  validateNullableOpaqueId('mapping.conversation_id', value.conversation_id, occurredAt);
  validateNullableOpaqueId('mapping.turn_id', value.turn_id, occurredAt);
  validateNullableOpaqueId('mapping.lineage_id', value.lineage_id, occurredAt);
  validateSafetyCriticalEnum(
    'mapping.binding_state',
    value.binding_state,
    MAPPING_BINDING_STATES,
    { occurredAt },
  );
  validatePublicNumber('mapping.mapping_version', value.mapping_version, { occurredAt });
  if (value.mapping_version < 1) {
    reject('validation_error', 'mapping.mapping_version must be a positive integer.', occurredAt);
  }
  const hasReason = Object.hasOwn(value, 'reason');
  if (hasReason && value.reason !== null) {
    validateSafetyCriticalEnum(
      'mapping.reason',
      value.reason,
      MAPPING_RECOVERY_REASONS,
      { occurredAt },
    );
  }

  if (value.binding_state === 'pending') {
    if (
      value.conversation_id === null
      || value.turn_id === null
      || value.lineage_id !== null
      || !hasReason
      || value.reason === null
    ) {
      reject(
        'validation_error',
        'A pending mapping requires conversation_id, turn_id and recovery reason, with lineage_id null.',
        occurredAt,
      );
    }
    if (value.mapping_version !== 1) {
      reject(
        'validation_error',
        'A pending provisional mapping must have mapping_version 1.',
        occurredAt,
      );
    }
  } else if (value.binding_state === 'bound') {
    if (value.conversation_id === null || value.turn_id === null || value.lineage_id === null) {
      reject(
        'validation_error',
        'A bound mapping requires non-null conversation_id, turn_id and lineage_id.',
        occurredAt,
      );
    }
    if (hasReason && value.reason !== null && value.mapping_version < 2) {
      reject(
        'validation_error',
        'A mapping bound from provisional recovery must retain reason and version 2 or later.',
        occurredAt,
      );
    }
  } else if (
    (hasReason && value.reason !== null)
    || (
      value.lineage_id !== null
      && (value.conversation_id === null || value.turn_id === null)
    )
  ) {
    reject(
      'validation_error',
      'A non-reply mapping requires a null recovery reason and cannot expose an orphan lineage.',
      occurredAt,
    );
  }

  return structuredClone(value);
}

export function validateDeliveryCommand(value, { occurredAt } = {}) {
  const document = validateContractDocument(value, {
    contract: 'zylos.delivery-command',
    fields: DELIVERY_COMMAND_FIELD_RULES,
    occurredAt,
  });

  validatePositiveInteger('delivery_attempt_no', value.delivery_attempt_no, occurredAt);
  validatePositiveInteger('outbox_lease_epoch', value.outbox_lease_epoch, occurredAt);
  validateTarget(value.target, occurredAt);
  validatePositiveInteger('aggregate_version', value.aggregate_version, occurredAt);
  validateNullablePositiveInteger(
    'event_sequence_through',
    value.event_sequence_through,
    occurredAt,
  );
  validateRenderModel(value.render_model, occurredAt);
  validateDeliveryMapping(value.mapping, { occurredAt });
  validateNullableOpaqueId(
    'target_platform_message_id',
    value.target_platform_message_id,
    occurredAt,
  );
  validateNullableOpaqueId(
    'predecessor_delivery_id',
    value.predecessor_delivery_id,
    occurredAt,
  );
  validateNullablePlatformVersion(
    'expected_platform_version',
    value.expected_platform_version,
    occurredAt,
  );
  verifyIdempotencyKey('delivery', {
    channel: value.target.channel,
    target: value.target,
    delivery_id: value.delivery_id,
  }, value.idempotency_key, { occurredAt });

  const isMainOperation = value.operation === 'create_main'
    || value.operation === 'update_main'
    || value.operation === 'send_fallback';
  if (isMainOperation) {
    if (
      value.aggregate_type !== 'turn_main'
      || value.mapping.binding_state === 'not_applicable'
      || value.mapping.turn_id !== value.aggregate_id
    ) {
      reject(
        'validation_error',
        `${value.operation} requires a reply-capable mapping for the same turn_main aggregate.`,
        occurredAt,
      );
    }
  }

  if (value.operation === 'create_main') {
    if (
      value.target_platform_message_id !== null
      || value.predecessor_delivery_id !== null
      || value.expected_platform_version !== null
    ) {
      reject(
        'validation_error',
        'create_main requires null target, predecessor and expected platform version.',
        occurredAt,
      );
    }
  } else if (value.operation === 'update_main') {
    if (
      value.target_platform_message_id === null
      || value.predecessor_delivery_id === null
    ) {
      reject(
        'validation_error',
        'update_main requires a target platform message and predecessor delivery.',
        occurredAt,
      );
    }
  } else if (value.operation === 'send_text') {
    if (
      value.target_platform_message_id !== null
      || value.predecessor_delivery_id !== null
      || value.expected_platform_version !== null
    ) {
      reject(
        'validation_error',
        'send_text requires null target, predecessor and expected platform version.',
        occurredAt,
      );
    }
  } else if (
    value.target_platform_message_id !== null
    || value.predecessor_delivery_id === null
    || value.expected_platform_version !== null
  ) {
    reject(
      'validation_error',
      'send_fallback requires a predecessor and null target/platform version.',
      occurredAt,
    );
  }

  return document;
}

export function validateDeliveryResult(value, { command, occurredAt } = {}) {
  const document = validateContractDocument(value, {
    contract: 'zylos.delivery-result',
    fields: DELIVERY_RESULT_FIELD_RULES,
    occurredAt,
  });

  validatePositiveInteger('delivery_attempt_no', value.delivery_attempt_no, occurredAt);
  validatePositiveInteger('outbox_lease_epoch', value.outbox_lease_epoch, occurredAt);
  validatePositiveInteger('aggregate_version', value.aggregate_version, occurredAt);
  validateNullableOpaqueId('platform_message_id', value.platform_message_id, occurredAt);
  validateNullablePlatformVersion(
    'applied_platform_version',
    value.applied_platform_version,
    occurredAt,
  );
  if (value.delivered_at !== null) {
    validateRfc3339Timestamp('delivered_at', value.delivered_at, { occurredAt });
  }
  if (value.error !== null) validateContractError(value.error, { occurredAt });
  validateRendererCapabilities(value.renderer_capabilities, occurredAt);

  if (value.status === 'delivered') {
    if (
      value.platform_message_id === null
      || value.delivered_at === null
      || value.error !== null
    ) {
      reject(
        'validation_error',
        'A delivered result requires platform_message_id and delivered_at, with error set to null.',
        occurredAt,
      );
    }
    if (
      value.renderer_capabilities.supports_platform_version
      !== (value.applied_platform_version !== null)
    ) {
      reject(
        'validation_error',
        'A delivered result must reflect platform version support in applied_platform_version.',
        occurredAt,
      );
    }
  } else {
    if (value.delivered_at !== null || value.error === null) {
      reject(
        'validation_error',
        'A failed or obsolete result requires error and a null delivered_at.',
        occurredAt,
      );
    }

    if (value.status === 'retryable_failure') {
      if (!value.error.retryable || value.applied_platform_version !== null) {
        reject(
          'validation_error',
          'retryable_failure requires retryable=true and a null applied platform version.',
          occurredAt,
        );
      }
    } else if (value.status === 'permanent_failure') {
      if (
        value.error.retryable
        || value.applied_platform_version !== null
        || (
          value.operation !== 'update_main'
          && value.error.side_effect_status === 'unknown'
        )
      ) {
        reject(
          'validation_error',
          'permanent_failure must be non-retryable, confirmed and have no applied platform version.',
          occurredAt,
        );
      }
    } else if (
      value.operation !== 'update_main'
      || value.platform_message_id === null
      || value.error.code !== 'obsolete'
      || value.error.retryable
      || value.error.side_effect_status !== 'none'
    ) {
      reject(
        'validation_error',
        'obsolete is update-only and requires the target ID plus a non-retryable obsolete error.',
        occurredAt,
      );
    }
  }

  if (command === undefined) {
    reject(
      'validation_error',
      'validateDeliveryResult requires the current fenced delivery command.',
      occurredAt,
    );
  }
  validateDeliveryCommand(command, { occurredAt });
  const echoedFields = [
    ['trace_id', 'trace_id'],
    ['outbox_id', 'outbox_id'],
    ['delivery_id', 'delivery_id'],
    ['idempotency_key', 'idempotency_key'],
    ['delivery_attempt_id', 'delivery_attempt_id'],
    ['delivery_attempt_no', 'delivery_attempt_no'],
    ['outbox_lease_epoch', 'outbox_lease_epoch'],
    ['mapping_id', 'mapping.mapping_id'],
    ['operation', 'operation'],
    ['aggregate_version', 'aggregate_version'],
  ];
  for (const [resultField, commandPath] of echoedFields) {
    const expected = commandPath === 'mapping.mapping_id'
      ? command.mapping.mapping_id
      : command[commandPath];
    if (value[resultField] !== expected) {
      reject(
        'stale_attempt',
        `Delivery result ${resultField} does not match the current fenced command.`,
        occurredAt,
        'conflict',
      );
    }
  }

  if (command.operation === 'update_main') {
    if (value.platform_message_id !== command.target_platform_message_id) {
      reject(
        'stale_attempt',
        'Delivery result platform_message_id does not match the update target.',
        occurredAt,
        'conflict',
      );
    }
  } else if (value.status !== 'delivered' && value.platform_message_id !== null) {
    reject(
      'validation_error',
      'An unconfirmed create, text or fallback failure must have a null platform_message_id.',
      occurredAt,
    );
  }

  return document;
}

function mappingBindingError(code, category, userMessage, occurredAt) {
  return createContractError({
    code,
    category,
    userMessage,
    occurredAt,
  });
}

export function resolveProvisionalMappingBinding(
  currentMapping,
  request,
  { occurredAt } = {},
) {
  const current = validateDeliveryMapping(currentMapping, { occurredAt });
  requireObject('mapping binding request', request, occurredAt);
  requireFields('mapping binding request', request, [
    'authority',
    'mapping_id',
    'expected_mapping_version',
    'lineage_id',
  ], occurredAt);
  validateSafetyCriticalEnum(
    'mapping binding request.authority',
    request.authority,
    MAPPING_BINDING_AUTHORITIES,
    { occurredAt },
  );
  validateOpaqueId('mapping binding request.mapping_id', request.mapping_id, { occurredAt });
  validatePositiveInteger(
    'mapping binding request.expected_mapping_version',
    request.expected_mapping_version,
    occurredAt,
  );
  validateOpaqueId('mapping binding request.lineage_id', request.lineage_id, { occurredAt });

  if (request.authority !== 'core') {
    return {
      status: 'forbidden',
      mapping: current,
      error: mappingBindingError(
        'forbidden',
        'authorization',
        'Only Core may bind a provisional delivery mapping.',
        occurredAt,
      ),
    };
  }

  if (request.mapping_id !== current.mapping_id) {
    return {
      status: 'conflict',
      mapping: current,
      error: mappingBindingError(
        'version_conflict',
        'conflict',
        'The mapping binding request targets a different mapping.',
        occurredAt,
      ),
    };
  }

  if (current.binding_state === 'bound') {
    if (current.lineage_id === request.lineage_id) {
      return { status: 'duplicate', mapping: current, error: null };
    }
    return {
      status: 'conflict',
      mapping: current,
      error: mappingBindingError(
        'version_conflict',
        'conflict',
        'A bound delivery mapping cannot change lineage.',
        occurredAt,
      ),
    };
  }

  if (
    current.binding_state !== 'pending'
    || request.expected_mapping_version !== current.mapping_version
  ) {
    return {
      status: 'conflict',
      mapping: current,
      error: mappingBindingError(
        'version_conflict',
        'conflict',
        'The provisional mapping state or version no longer matches the binding request.',
        occurredAt,
      ),
    };
  }

  return {
    status: 'bound',
    mapping: {
      ...current,
      lineage_id: request.lineage_id,
      binding_state: 'bound',
      mapping_version: current.mapping_version + 1,
    },
    error: null,
  };
}
