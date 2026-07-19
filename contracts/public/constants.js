export const PUBLIC_CONTRACTS = Object.freeze([
  'zylos.inbound-envelope',
  'zylos.inbound-result',
  'zylos.normalized-event',
  'zylos.interaction-request',
  'zylos.interaction-answer',
  'zylos.interaction-answer-result',
  'zylos.delivery-command',
  'zylos.delivery-result',
  'zylos.observability-snapshot',
  'zylos.dashboard-runtime-projection',
  'zylos.control-request',
  'zylos.control-result',
]);

export const PUBLIC_CONTRACT_MAJOR = 1;

export const ERROR_CATEGORIES = Object.freeze([
  'validation',
  'authentication',
  'authorization',
  'conflict',
  'capacity',
  'provider',
  'channel',
  'storage',
  'internal',
]);

export const SIDE_EFFECT_STATUSES = Object.freeze(['none', 'known', 'unknown']);

export const DELIVERY_OPERATIONS = Object.freeze([
  'create_main',
  'update_main',
  'send_text',
  'send_fallback',
]);

export const DELIVERY_COMMAND_VERSIONS = Object.freeze(['1.0', '1.1']);
export const DELIVERY_COMMAND_CURRENT_VERSION = '1.1';

export const DELIVERY_TARGET_FIELDS_V1_0 = Object.freeze([
  'region',
  'tenant_id',
  'channel',
  'bot_id',
  'chat_type',
  'chat_id',
  'native_thread_or_topic_id',
]);

export const DELIVERY_TARGET_FIELDS_V1_1 = Object.freeze([
  ...DELIVERY_TARGET_FIELDS_V1_0,
  'native_thread_root_message_id',
  'native_thread_reply_target_message_id',
]);

export const DELIVERY_RESULT_STATUSES = Object.freeze([
  'delivered',
  'retryable_failure',
  'permanent_failure',
  'obsolete',
]);

export const MAPPING_BINDING_STATES = Object.freeze([
  'pending',
  'bound',
  'not_applicable',
]);

export const MAPPING_BINDING_AUTHORITIES = Object.freeze(['core', 'channel']);

export const MAPPING_RECOVERY_REASONS = Object.freeze([
  'mapping_missing',
  'mapping_corrupt',
  'mapping_unbound',
  'provider_lineage_invalid',
]);

export const PUBLIC_ERROR_CODES = Object.freeze([
  'unsupported_contract_version',
  'unsupported_capability',
  'validation_error',
  'unauthenticated',
  'forbidden',
  'not_found',
  'idempotency_key_mismatch',
  'idempotency_conflict',
  'version_conflict',
  'obsolete',
  'queue_full',
  'lineage_resolution_pending',
  'mapping_missing',
  'mapping_corrupt',
  'interaction_out_of_order',
  'interaction_expired',
  'interaction_already_answered',
  'interaction_actor_forbidden',
  'interaction_answer_delivery_unknown',
  'permission_duration_exceeds_policy',
  'steer_precondition_failed',
  'turn_terminal',
  'stale_attempt',
  'lease_conflict',
  'provider_auth_failed',
  'provider_context_invalid',
  'delivery_transient',
  'delivery_permanent',
  'side_effect_unknown',
]);
