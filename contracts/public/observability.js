import { SIDE_EFFECT_STATUSES } from './constants.js';
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
  requireNullableOpaqueId,
  requireNullableText,
  requireNullableTimestamp,
  requireOpaqueId,
  requireRecord,
  requireText,
  requireTimestamp,
} from './runtime-contract-validation.js';
import {
  CANONICAL_TURN_STATES,
  HANDOFF_STATES,
  INTERACTION_STATES,
  PROVIDERS,
  RUNTIME_HEALTH_STATES,
  TURN_PHASES,
} from './runtime-vocabulary.js';

const SNAPSHOT_FIELDS = Object.freeze([
  'snapshot_id',
  'core_service_instance_id',
  'generated_at',
  'snapshot_version',
  'service',
  'executors',
  'turns',
  'interactions',
  'workspace_leases',
  'outbox',
  'audit_summary',
  'error',
]);

const SNAPSHOT_FIELD_RULES = deepFreeze({
  snapshot_id: { required: true, kind: 'opaque_id' },
  core_service_instance_id: { required: true, kind: 'opaque_id' },
  generated_at: { required: true, kind: 'rfc3339' },
  snapshot_version: { required: true, kind: 'number' },
  service: { required: true, kind: 'prevalidated' },
  executors: { required: true, kind: 'prevalidated' },
  turns: { required: true, kind: 'prevalidated' },
  interactions: { required: true, kind: 'prevalidated' },
  workspace_leases: { required: true, kind: 'prevalidated' },
  outbox: { required: true, kind: 'prevalidated' },
  audit_summary: { required: true, kind: 'prevalidated' },
  error: { required: true, kind: 'prevalidated' },
});

const INTERACTION_KINDS = Object.freeze([
  'question',
  'choice',
  'tool_approval',
  'permission_approval',
  'recovery_decision',
]);

const OUTBOX_STATUSES = Object.freeze([
  'pending',
  'delivering',
  'retry_wait',
  'delivered',
  'superseded',
  'dead_letter',
  'delivery_unknown',
]);
const OUTBOX_RECONCILIATION_STATES = Object.freeze([
  'required',
  'claimed',
  'confirmed',
  'replacement_authorized',
  'replacement_fenced',
  'failed',
  'not_reconcilable',
  'not_applicable',
  'multiple',
]);

export const OBSERVABILITY_SNAPSHOT_V1_SCHEMA = deepFreeze({
  contract: 'zylos.observability-snapshot',
  contract_version: '1.0',
  required: ['contract', 'contract_version', ...SNAPSHOT_FIELDS],
  collections: ['executors', 'turns', 'interactions', 'workspace_leases', 'outbox', 'audit_summary'],
  canonical_turn_states: CANONICAL_TURN_STATES,
  turn_phases: TURN_PHASES,
  interaction_states: INTERACTION_STATES,
  handoff_states: HANDOFF_STATES,
  diagnostic_only_runtime_identity: true,
  update_semantics: 'full_replace_by_core_service_instance_and_snapshot_version',
});

function validateCompleteness(path, value, options) {
  requireFields(path, value, ['complete', 'error'], options);
  requireBoolean(`${path}.complete`, value.complete, options);
  requireErrorOrNull(`${path}.error`, value.error, options);
  if (value.complete && value.error !== null) {
    rejectRuntimeContract('validation_error', `${path}.error must be null when complete is true.`, options);
  }
  if (!value.complete && value.error === null) {
    rejectRuntimeContract('validation_error', `${path}.error is required when complete is false.`, options);
  }
}

function validateCollection(path, value, itemValidator, options) {
  validateCompleteness(path, value, options);
  requireFields(path, value, ['items'], options);
  requireArray(`${path}.items`, value.items, options);
  value.items.forEach((item, index) => itemValidator(`${path}.items[${index}]`, item, options));
}

function validateService(service, coreServiceInstanceId, options) {
  const path = 'service';
  requireFields(path, service, [
    'complete',
    'service_version',
    'health',
    'maintenance',
    'draining',
    'reconciling',
    'host_id',
    'service_instance_id',
    'started_at',
    'last_reconciliation_at',
    'error',
  ], options);
  validateCompleteness(path, service, options);
  requireInteger(`${path}.service_version`, service.service_version, { min: 1, ...options });
  requireEnum(`${path}.health`, service.health, RUNTIME_HEALTH_STATES, options);
  requireBoolean(`${path}.maintenance`, service.maintenance, options);
  requireBoolean(`${path}.draining`, service.draining, options);
  requireBoolean(`${path}.reconciling`, service.reconciling, options);
  requireOpaqueId(`${path}.host_id`, service.host_id, options);
  requireOpaqueId(`${path}.service_instance_id`, service.service_instance_id, options);
  requireTimestamp(`${path}.started_at`, service.started_at, options);
  requireNullableTimestamp(
    `${path}.last_reconciliation_at`,
    service.last_reconciliation_at,
    options,
  );
  if (service.service_instance_id !== coreServiceInstanceId) {
    rejectRuntimeContract(
      'validation_error',
      'service.service_instance_id must match core_service_instance_id.',
      options,
    );
  }
}

function validateConversationKey(path, value, options) {
  requireFields(path, value, [
    'region',
    'tenant_id',
    'bot_id',
    'chat_type',
    'chat_id',
    'native_thread_or_topic_id',
  ], options);
  requireOpaqueId(`${path}.region`, value.region, options);
  requireOpaqueId(`${path}.tenant_id`, value.tenant_id, options);
  requireOpaqueId(`${path}.bot_id`, value.bot_id, options);
  requireEnum(`${path}.chat_type`, value.chat_type, ['dm', 'group', 'thread', 'synthetic'], options);
  requireOpaqueId(`${path}.chat_id`, value.chat_id, options);
  requireNullableOpaqueId(
    `${path}.native_thread_or_topic_id`,
    value.native_thread_or_topic_id,
    options,
  );
}

function validateExecutor(path, value, options) {
  requireFields(path, value, [
    'conversation_key',
    'conversation_id',
    'executor_version',
    'provider',
    'lineage_id',
    'provider_native_id',
    'executor_instance_id',
    'health',
    'resident',
    'evictable',
    'active_turn_id',
    'queue_length',
    'wait_reason',
    'last_event_id',
    'last_event_at',
    'lease',
    'runtime_identity',
  ], options);
  validateConversationKey(`${path}.conversation_key`, value.conversation_key, options);
  requireOpaqueId(`${path}.conversation_id`, value.conversation_id, options);
  requireInteger(`${path}.executor_version`, value.executor_version, { min: 1, ...options });
  requireEnum(`${path}.provider`, value.provider, PROVIDERS, options);
  requireOpaqueId(`${path}.lineage_id`, value.lineage_id, options);
  requireNullableOpaqueId(`${path}.provider_native_id`, value.provider_native_id, options);
  requireOpaqueId(`${path}.executor_instance_id`, value.executor_instance_id, options);
  requireEnum(`${path}.health`, value.health, RUNTIME_HEALTH_STATES, options);
  requireBoolean(`${path}.resident`, value.resident, options);
  requireBoolean(`${path}.evictable`, value.evictable, options);
  requireNullableOpaqueId(`${path}.active_turn_id`, value.active_turn_id, options);
  requireInteger(`${path}.queue_length`, value.queue_length, { min: 0, ...options });
  requireNullableText(`${path}.wait_reason`, value.wait_reason, options);
  requireNullableOpaqueId(`${path}.last_event_id`, value.last_event_id, options);
  requireNullableTimestamp(`${path}.last_event_at`, value.last_event_at, options);

  requireFields(`${path}.lease`, value.lease, ['owner', 'expires_at', 'epoch'], options);
  requireNullableOpaqueId(`${path}.lease.owner`, value.lease.owner, options);
  requireNullableTimestamp(`${path}.lease.expires_at`, value.lease.expires_at, options);
  requireInteger(`${path}.lease.epoch`, value.lease.epoch, { min: 0, ...options });

  requireFields(`${path}.runtime_identity`, value.runtime_identity, [
    'diagnostic_only',
    'pid',
    'pgid',
    'process_start_time',
  ], options);
  if (value.runtime_identity.diagnostic_only !== true) {
    rejectRuntimeContract(
      'validation_error',
      `${path}.runtime_identity.diagnostic_only must be true.`,
      options,
    );
  }
  requireInteger(`${path}.runtime_identity.pid`, value.runtime_identity.pid, {
    min: 1,
    nullable: true,
    ...options,
  });
  requireInteger(`${path}.runtime_identity.pgid`, value.runtime_identity.pgid, {
    min: 1,
    nullable: true,
    ...options,
  });
  requireNullableTimestamp(
    `${path}.runtime_identity.process_start_time`,
    value.runtime_identity.process_start_time,
    options,
  );
}

function validateTurn(path, value, options) {
  requireFields(path, value, [
    'turn_id',
    'conversation_id',
    'lineage_id',
    'turn_version',
    'state',
    'phase',
    'attempt_count',
    'retry_count',
    'queue_position',
    'recovery_of_turn_id',
    'side_effect_status',
    'error',
  ], options);
  requireOpaqueId(`${path}.turn_id`, value.turn_id, options);
  requireOpaqueId(`${path}.conversation_id`, value.conversation_id, options);
  requireNullableOpaqueId(`${path}.lineage_id`, value.lineage_id, options);
  requireInteger(`${path}.turn_version`, value.turn_version, { min: 1, ...options });
  requireEnum(`${path}.state`, value.state, CANONICAL_TURN_STATES, options);
  requireEnum(`${path}.phase`, value.phase, TURN_PHASES, options);
  requireInteger(`${path}.attempt_count`, value.attempt_count, { min: 0, ...options });
  requireInteger(`${path}.retry_count`, value.retry_count, { min: 0, ...options });
  requireInteger(`${path}.queue_position`, value.queue_position, {
    min: 1,
    nullable: true,
    ...options,
  });
  requireNullableOpaqueId(`${path}.recovery_of_turn_id`, value.recovery_of_turn_id, options);
  requireEnum(`${path}.side_effect_status`, value.side_effect_status, SIDE_EFFECT_STATUSES, options);
  requireErrorOrNull(`${path}.error`, value.error, options);
  const inputGroupFields = [
    'input_group_id',
    'input_group_state',
    'input_group_member_count',
    'input_group_supplement_count',
    'input_group_collect_until',
  ];
  const presentInputGroupFields = inputGroupFields.filter((field) => (
    Object.hasOwn(value, field)
  ));
  if (presentInputGroupFields.length > 0) {
    requireFields(path, value, inputGroupFields, options);
    requireOpaqueId(`${path}.input_group_id`, value.input_group_id, options);
    requireEnum(
      `${path}.input_group_state`,
      value.input_group_state,
      ['input_settling', 'sealed', 'cancelled'],
      options,
    );
    requireInteger(`${path}.input_group_member_count`, value.input_group_member_count, {
      min: 1,
      ...options,
    });
    requireInteger(
      `${path}.input_group_supplement_count`,
      value.input_group_supplement_count,
      { min: 0, ...options },
    );
    requireTimestamp(
      `${path}.input_group_collect_until`,
      value.input_group_collect_until,
      options,
    );
    if (value.input_group_supplement_count !== value.input_group_member_count - 1) {
      rejectRuntimeContract(
        'validation_error',
        `${path} input group counts are inconsistent.`,
        options,
      );
    }
  }
}

function validateInteraction(path, value, options) {
  requireFields(path, value, [
    'interaction_id',
    'conversation_id',
    'turn_id',
    'blocking_ordinal',
    'kind',
    'state',
    'interaction_version',
    'handoff_state',
    'handoff_deadline_at',
    'authorized_subject_summary',
    'created_at',
    'expires_at',
  ], options);
  requireOpaqueId(`${path}.interaction_id`, value.interaction_id, options);
  requireOpaqueId(`${path}.conversation_id`, value.conversation_id, options);
  requireNullableOpaqueId(`${path}.turn_id`, value.turn_id, options);
  requireInteger(`${path}.blocking_ordinal`, value.blocking_ordinal, { min: 1, ...options });
  requireEnum(`${path}.kind`, value.kind, INTERACTION_KINDS, options);
  requireEnum(`${path}.state`, value.state, INTERACTION_STATES, options);
  requireInteger(`${path}.interaction_version`, value.interaction_version, { min: 1, ...options });
  requireEnum(`${path}.handoff_state`, value.handoff_state, HANDOFF_STATES, options);
  requireNullableTimestamp(`${path}.handoff_deadline_at`, value.handoff_deadline_at, options);
  requireFields(`${path}.authorized_subject_summary`, value.authorized_subject_summary, [
    'actor_count',
    'capability_count',
  ], options);
  requireInteger(
    `${path}.authorized_subject_summary.actor_count`,
    value.authorized_subject_summary.actor_count,
    { min: 0, ...options },
  );
  requireInteger(
    `${path}.authorized_subject_summary.capability_count`,
    value.authorized_subject_summary.capability_count,
    { min: 0, ...options },
  );
  requireTimestamp(`${path}.created_at`, value.created_at, options);
  requireTimestamp(`${path}.expires_at`, value.expires_at, options);
}

function validateWorkspaceLease(path, value, options) {
  requireFields(path, value, [
    'workspace_root',
    'mode',
    'holder_conversation_id',
    'holder_turn_id',
    'holder_background_work_id',
    'expires_at',
    'epoch',
    'waiter_count',
  ], options);
  requireText(`${path}.workspace_root`, value.workspace_root, options);
  requireEnum(`${path}.mode`, value.mode, ['read', 'write'], options);
  requireOpaqueId(`${path}.holder_conversation_id`, value.holder_conversation_id, options);
  requireNullableOpaqueId(`${path}.holder_turn_id`, value.holder_turn_id, options);
  requireNullableOpaqueId(
    `${path}.holder_background_work_id`,
    value.holder_background_work_id,
    options,
  );
  requireTimestamp(`${path}.expires_at`, value.expires_at, options);
  requireInteger(`${path}.epoch`, value.epoch, { min: 1, ...options });
  requireInteger(`${path}.waiter_count`, value.waiter_count, { min: 0, ...options });
}

function validateOutbox(path, value, options) {
  validateCollection(path, value, (itemPath, item) => {
    requireFields(itemPath, item, ['channel', 'status', 'count', 'oldest_age_seconds'], options);
    requireOpaqueId(`${itemPath}.channel`, item.channel, options);
    requireEnum(`${itemPath}.status`, item.status, OUTBOX_STATUSES, options);
    requireInteger(`${itemPath}.count`, item.count, { min: 0, ...options });
    requireInteger(`${itemPath}.oldest_age_seconds`, item.oldest_age_seconds, {
      min: 0,
      ...options,
    });
    const diagnosticFields = [
      'stale_delivering_age_seconds',
      'reconciliation_state',
      'error_code',
    ];
    if (diagnosticFields.some((fieldName) => Object.hasOwn(item, fieldName))) {
      requireFields(itemPath, item, diagnosticFields, options);
      if (item.status !== 'delivery_unknown') {
        rejectRuntimeContract(
          'validation_error',
          `${itemPath} reconciliation diagnostics require delivery_unknown status.`,
          options,
        );
      }
      requireInteger(
        `${itemPath}.stale_delivering_age_seconds`,
        item.stale_delivering_age_seconds,
        { min: 0, ...options },
      );
      requireEnum(
        `${itemPath}.reconciliation_state`,
        item.reconciliation_state,
        OUTBOX_RECONCILIATION_STATES,
        options,
      );
      requireOpaqueId(`${itemPath}.error_code`, item.error_code, options);
    }
  }, options);
  requireFields(path, value, ['retry_count', 'dead_letter_count'], options);
  requireInteger(`${path}.retry_count`, value.retry_count, { min: 0, ...options });
  requireInteger(`${path}.dead_letter_count`, value.dead_letter_count, { min: 0, ...options });
}

function validateAuditSummary(path, value, options) {
  validateCollection(path, value, (itemPath, item) => {
    requireFields(itemPath, item, ['category', 'count', 'last_committed_at'], options);
    requireOpaqueId(`${itemPath}.category`, item.category, options);
    requireInteger(`${itemPath}.count`, item.count, { min: 0, ...options });
    requireNullableTimestamp(`${itemPath}.last_committed_at`, item.last_committed_at, options);
  }, options);
}

export function validateObservabilitySnapshot(value, { occurredAt } = {}) {
  const options = { occurredAt };
  requireRecord('snapshot', value, options);
  validatePublicFixtureSafety(value, options);
  requireFields('snapshot', value, ['contract', 'contract_version', ...SNAPSHOT_FIELDS], options);
  requireOpaqueId('snapshot_id', value.snapshot_id, options);
  requireOpaqueId('core_service_instance_id', value.core_service_instance_id, options);
  requireTimestamp('generated_at', value.generated_at, options);
  requireInteger('snapshot_version', value.snapshot_version, { min: 1, ...options });
  validateService(value.service, value.core_service_instance_id, options);
  validateCollection('executors', value.executors, validateExecutor, options);
  validateCollection('turns', value.turns, validateTurn, options);
  validateCollection('interactions', value.interactions, validateInteraction, options);
  validateCollection('workspace_leases', value.workspace_leases, validateWorkspaceLease, options);
  validateOutbox('outbox', value.outbox, options);
  validateAuditSummary('audit_summary', value.audit_summary, options);
  requireErrorOrNull('error', value.error, options);

  const sections = [
    value.service,
    value.executors,
    value.turns,
    value.interactions,
    value.workspace_leases,
    value.outbox,
    value.audit_summary,
  ];
  const degraded = sections.some((section) => !section.complete);
  if (degraded && value.error === null) {
    rejectRuntimeContract(
      'validation_error',
      'A degraded observability snapshot must include a top-level error.',
      options,
    );
  }
  if (!degraded && value.error !== null) {
    rejectRuntimeContract(
      'validation_error',
      'A complete observability snapshot must have error=null.',
      options,
    );
  }

  return finishRuntimeContract(value, {
    contract: OBSERVABILITY_SNAPSHOT_V1_SCHEMA.contract,
    fieldRules: SNAPSHOT_FIELD_RULES,
    occurredAt,
  });
}

export function resolveObservabilitySnapshotUpdate(currentValue, nextValue) {
  const current = validateObservabilitySnapshot(currentValue).forwarded;
  const next = validateObservabilitySnapshot(nextValue).forwarded;

  if (current.core_service_instance_id !== next.core_service_instance_id) {
    return next.error === null
      ? { status: 'replace_instance', apply: true }
      : { status: 'replace_instance_degraded', apply: false, requires_full: true };
  }
  if (next.snapshot_version > current.snapshot_version) {
    return { status: 'replace', apply: true };
  }
  if (next.snapshot_version < current.snapshot_version) {
    return { status: 'obsolete', apply: false };
  }
  if (runtimeContractsEqual(current, next)) {
    return { status: 'duplicate', apply: false };
  }

  rejectRuntimeContract(
    'version_conflict',
    'The same Core service instance and snapshot version contain different payloads.',
    { category: 'conflict', occurredAt: next.generated_at },
  );
}
