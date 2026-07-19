import crypto from 'node:crypto';

import { ContractKernelError, createContractError } from './errors.js';
import { canonicalizeJsonBytes } from './jcs.js';
import { validateOpaqueId } from './validation.js';

const KEY_PREFIX = 'zylos-idempotency-v1';
const OMITTED_TRANSIENT_FIELDS = new Set([
  'idempotency_key',
  'trace_id',
  'received_at',
  'headers',
  'http_headers',
  'rpc_headers',
  'signature',
  'credential',
  'credentials',
  'authorization',
  'cookie',
  'detail_ref',
  'source_ref',
]);
const OMITTED_DELIVERY_ATTEMPT_FIELDS = new Set([
  'delivery_attempt_id',
  'delivery_attempt_no',
  'outbox_lease_epoch',
  'not_before',
  'claimed_at',
  'claim_expires_at',
  'dispatch_started_at',
  'dispatched_at',
]);
const PAYLOAD_HASH_PATTERN = /^[0-9a-f]{64}$/;
export const IDEMPOTENCY_SCOPES = Object.freeze([
  'inbound',
  'scheduler',
  'interaction',
  'control',
  'delivery',
]);

function sha256(bytes) {
  return crypto.createHash('sha256').update(bytes).digest('hex');
}

function constantTimeTextEqual(left, right) {
  const leftDigest = crypto.createHash('sha256').update(String(left), 'utf8').digest();
  const rightDigest = crypto.createHash('sha256').update(String(right), 'utf8').digest();
  return crypto.timingSafeEqual(leftDigest, rightDigest);
}

function rejectPayload(message) {
  throw new ContractKernelError(createContractError({
    code: 'validation_error',
    userMessage: message,
  }));
}

function isTransientField(scope, fieldName) {
  const normalizedName = fieldName.toLowerCase();
  return OMITTED_TRANSIENT_FIELDS.has(normalizedName)
    || (scope === 'delivery' && OMITTED_DELIVERY_ATTEMPT_FIELDS.has(normalizedName));
}

function projectValue(value, { scope, path, decimalPaths }) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || Object.is(value, -0)) {
      rejectPayload(`${path || 'payload'} must be finite and must not be negative zero.`);
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        rejectPayload(`${path || 'payload'} must be a JSON safe integer.`);
      }
    } else if (!decimalPaths.has(path)) {
      rejectPayload(`${path || 'payload'} is a decimal not declared by the contract schema.`);
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => projectValue(entry, {
      scope,
      path: `${path}[]`,
      decimalPaths,
    }));
  }
  if (!value || typeof value !== 'object' || Object.getPrototypeOf(value) !== Object.prototype) {
    rejectPayload(`${path || 'payload'} must contain JSON values only.`);
  }

  const entries = [];
  for (const [fieldName, fieldValue] of Object.entries(value)) {
    if (isTransientField(scope, fieldName)) continue;
    const childPath = path ? `${path}.${fieldName}` : fieldName;
    entries.push([fieldName, projectValue(fieldValue, {
      scope,
      path: childPath,
      decimalPaths,
    })]);
  }
  return Object.fromEntries(entries);
}

export function buildIdempotencyKeyInput(scope, fields) {
  fields ??= {};
  if (scope === 'inbound') {
    return [
      KEY_PREFIX,
      scope,
      validateOpaqueId('region', fields.region),
      validateOpaqueId('tenant_id', fields.tenant_id),
      validateOpaqueId('channel', fields.channel),
      validateOpaqueId('bot_id', fields.bot_id),
      validateOpaqueId('inbound_event_id', fields.inbound_event_id),
    ];
  }
  if (scope === 'scheduler') {
    return [
      KEY_PREFIX,
      scope,
      validateOpaqueId('region', fields.region),
      validateOpaqueId('tenant_id', fields.tenant_id),
      validateOpaqueId('bot_id', fields.bot_id),
      validateOpaqueId('schedule_id', fields.schedule_id),
      validateOpaqueId('occurrence_id', fields.occurrence_id),
    ];
  }
  if (scope === 'interaction') {
    return [
      KEY_PREFIX,
      scope,
      validateOpaqueId('interaction_id', fields.interaction_id),
      validateOpaqueId('source_event_or_action_id', fields.source_event_or_action_id),
    ];
  }
  if (scope === 'control') {
    return [
      KEY_PREFIX,
      scope,
      validateOpaqueId('caller_namespace', fields.caller_namespace),
      validateOpaqueId('control_id', fields.control_id),
    ];
  }
  if (scope === 'delivery') {
    const target = fields.target ?? {};
    const nativeThread = target.native_thread_or_topic_id === null
      ? null
      : validateOpaqueId('native_thread_or_topic_id', target.native_thread_or_topic_id);
    return [
      KEY_PREFIX,
      scope,
      validateOpaqueId('channel', fields.channel),
      [
        validateOpaqueId('region', target.region),
        validateOpaqueId('tenant_id', target.tenant_id),
        validateOpaqueId('bot_id', target.bot_id),
        validateOpaqueId('chat_type', target.chat_type),
        validateOpaqueId('chat_id', target.chat_id),
        nativeThread,
      ],
      validateOpaqueId('delivery_id', fields.delivery_id),
    ];
  }
  throw new TypeError(`unsupported idempotency scope: ${String(scope)}`);
}

export function createIdempotencyKey(scope, fields) {
  const digest = sha256(canonicalizeJsonBytes(buildIdempotencyKeyInput(scope, fields)));
  return `zid:v1:${scope}:${digest}`;
}

export function createLegacyC4IdempotencyKey(legacyRecordId) {
  return `legacy-c4:${validateOpaqueId('legacy_record_id', legacyRecordId)}`;
}

export function verifyIdempotencyKey(
  scope,
  fields,
  suppliedKey,
  { occurredAt } = {},
) {
  const expectedKey = createIdempotencyKey(scope, fields);
  if (typeof suppliedKey === 'string' && constantTimeTextEqual(expectedKey, suppliedKey)) {
    return true;
  }
  throw new ContractKernelError(createContractError({
    code: 'idempotency_key_mismatch',
    category: 'conflict',
    userMessage: 'The idempotency key does not match the validated request fields.',
    occurredAt,
  }));
}

export function projectPayloadForHash(
  payload,
  { scope, knownFields, extensionFields = [], decimalPaths = [] } = {},
) {
  if (!IDEMPOTENCY_SCOPES.includes(scope) && scope !== 'legacy-c4') {
    throw new TypeError(`unsupported idempotency scope: ${String(scope)}`);
  }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    rejectPayload('Idempotency payload must be a JSON object.');
  }
  if (!Array.isArray(knownFields) || knownFields.length === 0) {
    throw new TypeError('knownFields must list the current contract fields');
  }

  function toFieldSet(fieldNames, optionName) {
    if (
      !Array.isArray(fieldNames)
      || fieldNames.some((fieldName) => typeof fieldName !== 'string' || fieldName.length === 0)
    ) {
      throw new TypeError(`${optionName} must be an array of non-empty field names`);
    }
    const result = new Set(fieldNames);
    if (result.size !== fieldNames.length) {
      throw new TypeError(`${optionName} must not contain duplicate field names`);
    }
    return result;
  }

  const known = toFieldSet(knownFields, 'knownFields');
  const extensions = toFieldSet(extensionFields, 'extensionFields');
  for (const fieldName of known) {
    if (extensions.has(fieldName)) {
      throw new TypeError(`${fieldName} cannot be both known and an optional extension`);
    }
    if (!Object.hasOwn(payload, fieldName)) {
      throw new TypeError(`known field ${fieldName} is absent from the payload`);
    }
  }
  for (const fieldName of extensions) {
    if (!Object.hasOwn(payload, fieldName)) {
      throw new TypeError(`optional extension ${fieldName} is absent from the payload`);
    }
  }
  for (const fieldName of Object.keys(payload)) {
    if (!known.has(fieldName) && !extensions.has(fieldName) && !isTransientField(scope, fieldName)) {
      rejectPayload(
        `Payload field ${fieldName} is neither a known business field nor an explicit optional extension.`,
      );
    }
  }
  const stableEntries = Object.entries(payload).filter(([fieldName]) => known.has(fieldName));
  return projectValue(Object.fromEntries(stableEntries), {
    scope,
    path: '',
    decimalPaths: new Set(decimalPaths),
  });
}

export function createPayloadHash(payload, options) {
  return sha256(canonicalizeJsonBytes(projectPayloadForHash(payload, options)));
}

export function resolveIdempotencyReplay(existing, candidate, { occurredAt } = {}) {
  if (!existing) return { status: 'new', error: null };
  for (const record of [existing, candidate]) {
    if (
      !record
      || typeof record.idempotency_key !== 'string'
      || !PAYLOAD_HASH_PATTERN.test(record.payload_hash)
    ) {
      rejectPayload('Idempotency records require a string key and lowercase SHA-256 payload hash.');
    }
  }

  if (!constantTimeTextEqual(existing.idempotency_key, candidate.idempotency_key)) {
    return { status: 'new', error: null };
  }
  if (constantTimeTextEqual(existing.payload_hash, candidate.payload_hash)) {
    return { status: 'duplicate', error: null };
  }
  return {
    status: 'conflict',
    error: createContractError({
      code: 'idempotency_conflict',
      category: 'conflict',
      userMessage: 'The idempotency key was already used with a different payload.',
      occurredAt,
    }),
  };
}
