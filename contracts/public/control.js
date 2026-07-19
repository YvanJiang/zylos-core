import { verifyIdempotencyKey } from './idempotency.js';
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
  requireExactFields,
  requireFields,
  requireInteger,
  requireNullableOpaqueId,
  requireNullableTimestamp,
  requireOpaqueId,
  requireRecord,
  requireText,
  requireTimestamp,
} from './runtime-contract-validation.js';

const CALLER_NAMESPACE_PATTERN = /^[a-z0-9][a-z0-9._:-]*$/;

const CONTROL_STATUSES = Object.freeze([
  'accepted',
  'completed',
  'noop',
  'conflict',
  'forbidden',
  'not_found',
  'failed',
]);

const REQUEST_FIELDS = Object.freeze([
  'trace_id',
  'caller_namespace',
  'control_id',
  'action',
  'target',
  'expected_version',
  'actor',
  'auth_context',
  'reason',
  'idempotency_key',
  'created_at',
]);

const RESULT_FIELDS = Object.freeze([
  'trace_id',
  'caller_namespace',
  'control_id',
  'control_result_version',
  'status',
  'target',
  'previous_target_version',
  'target_version',
  'audit_id',
  'result',
  'error',
  'accepted_at',
  'completed_at',
]);

const SCOPE_FIELDS = Object.freeze([
  'scope_type',
  'region',
  'tenant_id',
  'bot_id',
  'conversation_id',
  'service_instance_id',
  'recovery_id',
]);

const ACTION_DEFINITIONS = deepFreeze({
  inspect: {
    capability: 'runtime.inspect',
    mutable: false,
    targets: {
      service: { fields: ['service_instance_id'], identity_field: 'service_instance_id' },
      conversation: { fields: ['conversation_id'], identity_field: 'conversation_id' },
      turn: { fields: ['turn_id'], identity_field: 'turn_id' },
      executor: { fields: ['executor_instance_id'], identity_field: 'executor_instance_id' },
      queue: { fields: ['conversation_id'], identity_field: 'conversation_id' },
      recovery: { fields: ['recovery_id'], identity_field: 'recovery_id' },
    },
    result: {
      snapshot: { kind: 'record' },
    },
  },
  stop_active_turn: {
    capability: 'turn.stop',
    mutable: true,
    targets: {
      turn: {
        fields: ['conversation_id', 'turn_id'],
        identity_field: 'turn_id',
      },
    },
    result: {
      winner: { kind: 'enum', values: ['stop', 'steer'] },
      active_turn_id: { kind: 'opaque_id' },
      active_turn_version: { kind: 'integer', min: 1 },
      priority_turn_created: { kind: 'boolean' },
      priority_turn_cancelled: { kind: 'boolean' },
    },
  },
  clear_unstarted_queue: {
    capability: 'queue.clear',
    mutable: true,
    targets: {
      queue: {
        fields: ['conversation_id', 'through_queue_sequence'],
        identity_field: 'conversation_id',
      },
    },
    result: {
      cleared_turn_ids: { kind: 'opaque_id_array' },
      through_queue_sequence: { kind: 'integer', min: 1 },
    },
  },
  reconcile: {
    capability: 'service.reconcile',
    mutable: true,
    targets: {
      service: { fields: ['service_instance_id'], identity_field: 'service_instance_id' },
    },
    result: {
      intent_id: { kind: 'opaque_id' },
      state: { kind: 'enum', values: ['pending', 'completed'] },
    },
  },
  evict_idle_executor: {
    capability: 'executor.evict',
    mutable: true,
    targets: {
      executor: {
        fields: ['conversation_id', 'executor_instance_id'],
        identity_field: 'executor_instance_id',
      },
    },
    result: {
      evicted: { kind: 'boolean' },
      executor_instance_id: { kind: 'opaque_id' },
    },
  },
  confirm_recovery: {
    capability: 'recovery.decide',
    mutable: true,
    targets: {
      recovery: {
        fields: ['recovery_id', 'conversation_id', 'turn_id'],
        identity_field: 'recovery_id',
      },
    },
    result: {
      decision: { kind: 'enum', values: ['confirmed'] },
      recovery_turn_id: { kind: 'nullable_opaque_id' },
    },
  },
  reject_recovery: {
    capability: 'recovery.decide',
    mutable: true,
    targets: {
      recovery: {
        fields: ['recovery_id', 'conversation_id', 'turn_id'],
        identity_field: 'recovery_id',
      },
    },
    result: {
      decision: { kind: 'enum', values: ['rejected'] },
      recovery_turn_id: { kind: 'nullable_opaque_id' },
    },
  },
});

export const CONTROL_ACTIONS = Object.freeze(Object.keys(ACTION_DEFINITIONS));

export const CONTROL_CAPABILITIES = Object.freeze([
  ...new Set(Object.values(ACTION_DEFINITIONS).map(({ capability }) => capability)),
]);

const REQUEST_FIELD_RULES = deepFreeze({
  trace_id: { required: true, kind: 'opaque_id' },
  caller_namespace: { required: true, kind: 'text' },
  control_id: { required: true, kind: 'opaque_id' },
  action: { required: true, kind: 'critical_enum', values: CONTROL_ACTIONS },
  target: { required: true, kind: 'prevalidated' },
  expected_version: { required: true, kind: 'prevalidated' },
  actor: { required: true, kind: 'prevalidated' },
  auth_context: { required: true, kind: 'prevalidated' },
  reason: { required: true, kind: 'text' },
  idempotency_key: { required: true, kind: 'opaque_id' },
  created_at: { required: true, kind: 'rfc3339' },
});

const RESULT_FIELD_RULES = deepFreeze({
  trace_id: { required: true, kind: 'opaque_id' },
  caller_namespace: { required: true, kind: 'text' },
  control_id: { required: true, kind: 'opaque_id' },
  control_result_version: { required: true, kind: 'number' },
  status: { required: true, kind: 'critical_enum', values: CONTROL_STATUSES },
  target: { required: true, kind: 'prevalidated' },
  previous_target_version: { required: true, kind: 'nullable_number' },
  target_version: { required: true, kind: 'nullable_number' },
  audit_id: { required: true, kind: 'nullable_opaque_id' },
  result: { required: true, kind: 'prevalidated' },
  error: { required: true, kind: 'prevalidated' },
  accepted_at: { required: true, kind: 'nullable_rfc3339' },
  completed_at: { required: true, kind: 'nullable_rfc3339' },
});

export const CONTROL_REQUEST_V1_SCHEMA = deepFreeze({
  contract: 'zylos.control-request',
  contract_version: '1.0',
  required: ['contract', 'contract_version', ...REQUEST_FIELDS],
  caller_namespace_pattern: CALLER_NAMESPACE_PATTERN.source,
  actions: ACTION_DEFINITIONS,
  authorization: 'subject_capability_scope_policy_version',
  mutation_precondition: 'expected_version_compare_and_swap',
});

export const CONTROL_RESULT_V1_SCHEMA = deepFreeze({
  contract: 'zylos.control-result',
  contract_version: '1.0',
  required: ['contract', 'contract_version', ...RESULT_FIELDS],
  statuses: CONTROL_STATUSES,
  ordering: 'caller_namespace_control_id_control_result_version',
  validation_context: 'request_action_required_when_target_and_result_do_not_discriminate',
});

function validateCallerNamespace(value, options) {
  if (
    typeof value !== 'string'
    || value.length < 1
    || value.length > 128
    || !CALLER_NAMESPACE_PATTERN.test(value)
  ) {
    rejectRuntimeContract(
      'validation_error',
      'caller_namespace must match [a-z0-9][a-z0-9._:-]* and be at most 128 characters.',
      options,
    );
  }
}

function validateNullableId(path, value, required, options) {
  if (required) return requireOpaqueId(path, value, options);
  if (value !== null) {
    rejectRuntimeContract('validation_error', `${path} must be null for this scope.`, options);
  }
  return null;
}

function validateScope(path, scope, options) {
  requireExactFields(path, scope, SCOPE_FIELDS, options);
  requireEnum(`${path}.scope_type`, scope.scope_type, [
    'tenant',
    'bot',
    'conversation',
    'service',
    'recovery',
  ], options);
  requireOpaqueId(`${path}.region`, scope.region, options);
  requireOpaqueId(`${path}.tenant_id`, scope.tenant_id, options);

  const requiresBot = ['bot', 'conversation', 'recovery'].includes(scope.scope_type);
  const requiresConversation = ['conversation', 'recovery'].includes(scope.scope_type);
  validateNullableId(`${path}.bot_id`, scope.bot_id, requiresBot, options);
  validateNullableId(
    `${path}.conversation_id`,
    scope.conversation_id,
    requiresConversation,
    options,
  );
  validateNullableId(
    `${path}.service_instance_id`,
    scope.service_instance_id,
    scope.scope_type === 'service',
    options,
  );
  validateNullableId(
    `${path}.recovery_id`,
    scope.recovery_id,
    scope.scope_type === 'recovery',
    options,
  );
}

function validateCapability(path, value, authContext, options) {
  requireExactFields(path, value, [
    'capability',
    'scope',
    'policy_id',
    'policy_version',
    'grant_id',
    'expires_at',
  ], options);
  requireEnum(`${path}.capability`, value.capability, CONTROL_CAPABILITIES, options);
  validateScope(`${path}.scope`, value.scope, options);
  requireOpaqueId(`${path}.policy_id`, value.policy_id, options);
  requireInteger(`${path}.policy_version`, value.policy_version, { min: 1, ...options });
  requireOpaqueId(`${path}.grant_id`, value.grant_id, options);
  requireNullableTimestamp(`${path}.expires_at`, value.expires_at, options);
  if (
    value.policy_id !== authContext.authorization_policy_id
    || value.policy_version !== authContext.authorization_policy_version
  ) {
    rejectRuntimeContract(
      'validation_error',
      `${path} must use the auth_context authorization policy ID and version.`,
      options,
    );
  }
}

function validateAuthContext(value, options) {
  requireExactFields('auth_context', value, [
    'source',
    'auth_subject_id',
    'tenant_id',
    'bot_id',
    'authorization_policy_id',
    'authorization_policy_version',
    'authenticated_at',
  ], options);
  requireEnum('auth_context.source', value.source, [
    'dashboard_session',
    'service_credential',
    'platform_admin_bridge',
  ], options);
  requireOpaqueId('auth_context.auth_subject_id', value.auth_subject_id, options);
  requireNullableOpaqueId('auth_context.tenant_id', value.tenant_id, options);
  requireNullableOpaqueId('auth_context.bot_id', value.bot_id, options);
  requireOpaqueId(
    'auth_context.authorization_policy_id',
    value.authorization_policy_id,
    options,
  );
  requireInteger(
    'auth_context.authorization_policy_version',
    value.authorization_policy_version,
    { min: 1, ...options },
  );
  requireTimestamp('auth_context.authenticated_at', value.authenticated_at, options);
}

function validateActor(value, authContext, options) {
  requireExactFields('actor', value, [
    'type',
    'actor_id',
    'authenticated',
    'roles',
    'capabilities',
  ], options);
  requireEnum('actor.type', value.type, ['user', 'service'], options);
  requireOpaqueId('actor.actor_id', value.actor_id, options);
  requireBoolean('actor.authenticated', value.authenticated, options);
  if (value.authenticated !== true) {
    rejectRuntimeContract('unauthenticated', 'The control actor must be authenticated.', {
      category: 'authentication',
      ...options,
    });
  }
  requireArray('actor.roles', value.roles, options);
  value.roles.forEach((role, index) => requireText(`actor.roles[${index}]`, role, options));
  requireArray('actor.capabilities', value.capabilities, options);
  value.capabilities.forEach((capability, index) => {
    validateCapability(`actor.capabilities[${index}]`, capability, authContext, options);
  });
}

function scopeCoversControlTarget(scope, target, authContext) {
  if (scope.tenant_id !== authContext.tenant_id) return false;

  switch (scope.scope_type) {
    case 'tenant':
      return true;
    case 'bot':
      return authContext.bot_id !== null
        && scope.bot_id === authContext.bot_id
        && target.aggregate_type !== 'service';
    case 'conversation':
      return scope.bot_id === authContext.bot_id
        && target.conversation_id === scope.conversation_id;
    case 'service':
      return target.aggregate_type === 'service'
        && target.service_instance_id === scope.service_instance_id;
    case 'recovery':
      return scope.bot_id === authContext.bot_id
        && target.aggregate_type === 'recovery'
        && (
          !Object.hasOwn(target, 'conversation_id')
          || target.conversation_id === scope.conversation_id
        )
        && target.recovery_id === scope.recovery_id;
    default:
      return false;
  }
}

function requireActionCapabilityGrant(actor, action, target, authContext, options) {
  const requiredCapability = ACTION_DEFINITIONS[action].capability;
  const hasCoveringGrant = actor.capabilities.some(
    (grant) => grant.capability === requiredCapability
      && scopeCoversControlTarget(grant.scope, target, authContext),
  );
  if (!hasCoveringGrant) {
    rejectRuntimeContract(
      'unsupported_capability',
      `${action} requires a ${requiredCapability} grant whose scope covers the auth context and target.`,
      { category: 'authorization', ...options },
    );
  }
}

function validateTargetFields(target, aggregateType, definition, options) {
  const fields = ['aggregate_type', ...definition.fields];
  requireExactFields('target', target, fields, options);
  if (target.aggregate_type !== aggregateType) {
    rejectRuntimeContract(
      'validation_error',
      `target.aggregate_type must be ${aggregateType}.`,
      options,
    );
  }
  for (const field of definition.fields) {
    if (field === 'through_queue_sequence') {
      requireInteger(`target.${field}`, target[field], { min: 1, ...options });
    } else {
      requireOpaqueId(`target.${field}`, target[field], options);
    }
  }
  return {
    aggregateType,
    aggregateId: target[definition.identity_field],
  };
}

function validateTarget(action, target, options) {
  requireRecord('target', target, options);
  requireFields('target', target, ['aggregate_type'], options);
  const definition = ACTION_DEFINITIONS[action].targets[target.aggregate_type];
  if (!definition) {
    rejectRuntimeContract(
      'unsupported_capability',
      `${action} does not support target type ${String(target.aggregate_type)}.`,
      options,
    );
  }
  return validateTargetFields(target, target.aggregate_type, definition, options);
}

function validateExpectedVersion(value, targetIdentity, required, options) {
  if (value === null) {
    if (required) {
      rejectRuntimeContract(
        'validation_error',
        'expected_version is required for every mutation.',
        options,
      );
    }
    return;
  }
  requireExactFields('expected_version', value, [
    'aggregate_type',
    'aggregate_id',
    'version',
  ], options);
  requireOpaqueId('expected_version.aggregate_type', value.aggregate_type, options);
  requireOpaqueId('expected_version.aggregate_id', value.aggregate_id, options);
  requireInteger('expected_version.version', value.version, { min: 1, ...options });
  if (
    value.aggregate_type !== targetIdentity.aggregateType
    || value.aggregate_id !== targetIdentity.aggregateId
  ) {
    rejectRuntimeContract(
      'validation_error',
      'expected_version must identify the target aggregate used by the action.',
      options,
    );
  }
}

export function validateControlRequest(value, { occurredAt } = {}) {
  const options = { occurredAt };
  requireRecord('control request', value, options);
  validatePublicFixtureSafety(value, options);
  requireFields('control request', value, ['contract', 'contract_version', ...REQUEST_FIELDS], options);
  requireOpaqueId('trace_id', value.trace_id, options);
  validateCallerNamespace(value.caller_namespace, options);
  requireOpaqueId('control_id', value.control_id, options);
  requireEnum('action', value.action, CONTROL_ACTIONS, options);
  const targetIdentity = validateTarget(value.action, value.target, options);
  validateExpectedVersion(
    value.expected_version,
    targetIdentity,
    ACTION_DEFINITIONS[value.action].mutable,
    options,
  );
  validateAuthContext(value.auth_context, options);
  validateActor(value.actor, value.auth_context, options);
  requireActionCapabilityGrant(
    value.actor,
    value.action,
    value.target,
    value.auth_context,
    options,
  );
  requireText('reason', value.reason, options);
  requireOpaqueId('idempotency_key', value.idempotency_key, options);
  requireTimestamp('created_at', value.created_at, options);
  verifyIdempotencyKey('control', {
    caller_namespace: value.caller_namespace,
    control_id: value.control_id,
  }, value.idempotency_key, { occurredAt });

  return finishRuntimeContract(value, {
    contract: CONTROL_REQUEST_V1_SCHEMA.contract,
    fieldRules: REQUEST_FIELD_RULES,
    occurredAt,
  });
}

function targetMatches(target, aggregateType, definition) {
  const fields = ['aggregate_type', ...definition.fields];
  return target.aggregate_type === aggregateType
    && Object.keys(target).length === fields.length
    && fields.every((field) => Object.hasOwn(target, field));
}

function validateGenericTarget(target, options) {
  requireRecord('target', target, options);
  const candidates = Object.entries(ACTION_DEFINITIONS).flatMap(([action, actionDefinition]) => (
    Object.entries(actionDefinition.targets)
      .filter(([aggregateType, definition]) => targetMatches(target, aggregateType, definition))
      .map(([aggregateType, definition]) => ({ action, aggregateType, definition }))
  ));
  if (candidates.length === 0) {
    rejectRuntimeContract('validation_error', 'target does not match a control target shape.', options);
  }
  const [{ aggregateType, definition }] = candidates;
  validateTargetFields(target, aggregateType, definition, options);
  return [...new Set(candidates.map(({ action }) => action))];
}

function validateResultField(path, value, rule, options) {
  switch (rule.kind) {
    case 'record':
      requireRecord(path, value, options);
      break;
    case 'opaque_id':
      requireOpaqueId(path, value, options);
      break;
    case 'nullable_opaque_id':
      requireNullableOpaqueId(path, value, options);
      break;
    case 'integer':
      requireInteger(path, value, { min: rule.min, ...options });
      break;
    case 'boolean':
      requireBoolean(path, value, options);
      break;
    case 'enum':
      requireEnum(path, value, rule.values, options);
      break;
    case 'opaque_id_array':
      requireArray(path, value, options);
      value.forEach((entry, index) => requireOpaqueId(`${path}[${index}]`, entry, options));
      break;
    default:
      throw new TypeError(`Unsupported control result field kind ${String(rule.kind)}.`);
  }
}

function validateActionResult(value, candidateActions, options) {
  requireRecord('result', value, options);
  validatePublicFixtureSafety(value, options);
  const shapeActions = candidateActions.filter((action) => {
    const fields = Object.keys(ACTION_DEFINITIONS[action].result);
    return Object.keys(value).length === fields.length
      && fields.every((field) => Object.hasOwn(value, field));
  });
  const matchingActions = shapeActions.filter((action) => (
    Object.entries(ACTION_DEFINITIONS[action].result).every(([field, rule]) => (
      rule.kind !== 'enum' || rule.values.includes(value[field])
    ))
  ));
  if (matchingActions.length !== 1) {
    if (matchingActions.length === 0 && shapeActions.length > 0) {
      for (const field of Object.keys(value)) {
        const enumValues = [...new Set(shapeActions.flatMap((action) => {
          const rule = ACTION_DEFINITIONS[action].result[field];
          return rule?.kind === 'enum' ? rule.values : [];
        }))];
        if (enumValues.length > 0) requireEnum(`result.${field}`, value[field], enumValues, options);
      }
    }
    rejectRuntimeContract(
      'validation_error',
      'result must match exactly one action schema for the target.',
      options,
    );
  }
  const [action] = matchingActions;
  const definition = ACTION_DEFINITIONS[action].result;
  requireExactFields('result', value, Object.keys(definition), options);
  for (const [field, rule] of Object.entries(definition)) {
    validateResultField(`result.${field}`, value[field], rule, options);
  }
  return action;
}

function resolveResultAction(
  candidateActions,
  resultAction,
  requestedAction,
  status,
  options,
) {
  if (requestedAction !== undefined) {
    requireEnum('control result action context', requestedAction, CONTROL_ACTIONS, options);
    if (!candidateActions.includes(requestedAction)) {
      rejectRuntimeContract(
        'validation_error',
        'The correlated request action does not support this control result target.',
        options,
      );
    }
    if (resultAction !== null && resultAction !== requestedAction) {
      rejectRuntimeContract(
        'validation_error',
        'The control result schema does not match the correlated request action.',
        options,
      );
    }
    return requestedAction;
  }
  if (resultAction !== null) return resultAction;
  if (candidateActions.length === 1) return candidateActions[0];
  if (['conflict', 'forbidden'].includes(status)) return null;
  rejectRuntimeContract(
    'validation_error',
    'The correlated request action is required for an ambiguous control result.',
    options,
  );
}

function requireResultObject(path, value, options) {
  if (value === null) {
    rejectRuntimeContract('validation_error', `${path} must be a JSON object for this status.`, options);
  }
  requireRecord(path, value, options);
}

export function validateControlResult(value, { occurredAt, action: requestedAction } = {}) {
  const options = { occurredAt };
  requireRecord('control result', value, options);
  requireFields('control result', value, ['contract', 'contract_version', ...RESULT_FIELDS], options);
  requireOpaqueId('trace_id', value.trace_id, options);
  validateCallerNamespace(value.caller_namespace, options);
  requireOpaqueId('control_id', value.control_id, options);
  requireInteger('control_result_version', value.control_result_version, { min: 1, ...options });
  requireEnum('status', value.status, CONTROL_STATUSES, options);
  const candidateActions = validateGenericTarget(value.target, options);
  requireInteger('previous_target_version', value.previous_target_version, {
    min: 1,
    nullable: true,
    ...options,
  });
  requireInteger('target_version', value.target_version, {
    min: 1,
    nullable: true,
    ...options,
  });
  requireNullableOpaqueId('audit_id', value.audit_id, options);
  const resultAction = value.result === null
    ? null
    : validateActionResult(value.result, candidateActions, options);
  const action = resolveResultAction(
    candidateActions,
    resultAction,
    requestedAction,
    value.status,
    options,
  );
  requireErrorOrNull('error', value.error, options);
  requireNullableTimestamp('accepted_at', value.accepted_at, options);
  requireNullableTimestamp('completed_at', value.completed_at, options);

  const success = ['accepted', 'completed', 'noop'].includes(value.status);
  if (success) {
    if (value.error !== null) {
      rejectRuntimeContract('validation_error', 'Successful control results must have error=null.', options);
    }
    requireResultObject('result', value.result, options);
    requireTimestamp('accepted_at', value.accepted_at, options);
    requireInteger('previous_target_version', value.previous_target_version, { min: 1, ...options });
    requireInteger('target_version', value.target_version, { min: 1, ...options });
    if (value.target_version < value.previous_target_version) {
      rejectRuntimeContract(
        'validation_error',
        'target_version must not precede previous_target_version.',
        options,
      );
    }
  } else if (value.error === null) {
    rejectRuntimeContract('validation_error', 'Failed control results must include an error.', options);
  }

  if (value.status === 'accepted' && value.completed_at !== null) {
    rejectRuntimeContract('validation_error', 'accepted control results require completed_at=null.', options);
  }
  if (value.status !== 'accepted' && value.completed_at === null) {
    rejectRuntimeContract(
      'validation_error',
      `${value.status} control results require completed_at.`,
      options,
    );
  }
  if (['conflict', 'forbidden'].includes(value.status) && value.audit_id === null) {
    rejectRuntimeContract(
      'validation_error',
      `${value.status} control results require audit_id.`,
      options,
    );
  }
  if (action && ACTION_DEFINITIONS[action].mutable && value.audit_id === null) {
    rejectRuntimeContract(
      'validation_error',
      `${action} control results require audit_id.`,
      options,
    );
  }

  const validated = finishRuntimeContract(value, {
    contract: CONTROL_RESULT_V1_SCHEMA.contract,
    fieldRules: RESULT_FIELD_RULES,
    occurredAt,
  });
  return { ...validated, metadata: { action } };
}

export function resolveControlResultUpdate(currentValue, nextValue, { action } = {}) {
  const currentValidation = validateControlResult(currentValue, { action });
  const nextValidation = validateControlResult(nextValue, { action });
  const current = currentValidation.forwarded;
  const next = nextValidation.forwarded;
  if (
    current.caller_namespace !== next.caller_namespace
    || current.control_id !== next.control_id
    || current.trace_id !== next.trace_id
    || !runtimeContractsEqual(current.target, next.target)
    || current.previous_target_version !== next.previous_target_version
    || current.accepted_at !== next.accepted_at
    || currentValidation.metadata.action !== nextValidation.metadata.action
  ) {
    rejectRuntimeContract(
      'validation_error',
      'Control result updates must preserve the original control intent identity.',
      { occurredAt: next.completed_at ?? next.accepted_at },
    );
  }
  if (next.control_result_version > current.control_result_version) {
    if (current.status !== 'accepted' || next.status === 'accepted') {
      rejectRuntimeContract(
        'version_conflict',
        'Control result versions can advance only once from accepted to a terminal status.',
        {
          category: 'conflict',
          occurredAt: next.completed_at ?? next.accepted_at,
        },
      );
    }
    return { status: 'replace', apply: true };
  }
  if (next.control_result_version < current.control_result_version) {
    return { status: 'obsolete', apply: false };
  }
  if (runtimeContractsEqual(current, next)) {
    return { status: 'duplicate', apply: false };
  }
  rejectRuntimeContract(
    'version_conflict',
    'The same control result version contains different payloads.',
    { category: 'conflict', occurredAt: next.completed_at ?? next.accepted_at },
  );
}
