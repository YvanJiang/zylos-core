import { ContractKernelError, createContractError } from './errors.js';
import { canonicalizeJson } from './jcs.js';
import { isWellFormedUnicode } from './scalars.js';
import {
  validateContractError,
  validateContractDocument,
  validateOpaqueId,
  validatePublicNumber,
  validateRfc3339Timestamp,
  validateSafetyCriticalEnum,
} from './validation.js';

export function rejectRuntimeContract(
  code,
  userMessage,
  { category = 'validation', occurredAt } = {},
) {
  throw new ContractKernelError(createContractError({
    code,
    category,
    userMessage,
    occurredAt,
  }));
}

export function requireRecord(path, value, options) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    rejectRuntimeContract('validation_error', `${path} must be a JSON object.`, options);
  }
  return value;
}

export function requireArray(path, value, options) {
  if (!Array.isArray(value)) {
    rejectRuntimeContract('validation_error', `${path} must be an array.`, options);
  }
  return value;
}

export function requireFields(path, value, fields, options) {
  requireRecord(path, value, options);
  for (const field of fields) {
    if (!Object.hasOwn(value, field)) {
      rejectRuntimeContract('validation_error', `${path}.${field} is required.`, options);
    }
  }
}

export function requireExactFields(path, value, fields, options) {
  requireFields(path, value, fields, options);
  const allowed = new Set(fields);
  for (const field of Object.keys(value)) {
    if (!allowed.has(field)) {
      rejectRuntimeContract('validation_error', `${path}.${field} is not allowed.`, options);
    }
  }
}

export function requireBoolean(path, value, options) {
  if (typeof value !== 'boolean') {
    rejectRuntimeContract('validation_error', `${path} must be a boolean.`, options);
  }
  return value;
}

export function requireText(path, value, { nonEmpty = true, ...options } = {}) {
  if (
    typeof value !== 'string'
    || (nonEmpty && value.trim().length === 0)
    || !isWellFormedUnicode(value)
  ) {
    rejectRuntimeContract('validation_error', `${path} must be a displayable string.`, options);
  }
  return value;
}

export function requireNullableText(path, value, options) {
  return value === null ? null : requireText(path, value, options);
}

export function requireOpaqueId(path, value, options) {
  return validateOpaqueId(path, value, options);
}

export function requireNullableOpaqueId(path, value, options) {
  return value === null ? null : requireOpaqueId(path, value, options);
}

export function requireTimestamp(path, value, options) {
  return validateRfc3339Timestamp(path, value, options);
}

export function requireNullableTimestamp(path, value, options) {
  return value === null ? null : requireTimestamp(path, value, options);
}

export function requireInteger(path, value, { min, nullable = false, ...options } = {}) {
  if (nullable && value === null) return null;
  validatePublicNumber(path, value, options);
  if (min !== undefined && value < min) {
    rejectRuntimeContract('validation_error', `${path} must be at least ${min}.`, options);
  }
  return value;
}

export function requireEnum(path, value, values, options) {
  return validateSafetyCriticalEnum(path, value, values, options);
}

export function requireNullableEnum(path, value, values, options) {
  return value === null ? null : requireEnum(path, value, values, options);
}

export function requireErrorOrNull(path, value, options) {
  if (value === null) return null;
  requireRecord(path, value, options);
  return validateContractError(value, options);
}

export function finishRuntimeContract(
  value,
  { contract, fieldRules, occurredAt } = {},
) {
  return validateContractDocument(value, {
    contract,
    fields: fieldRules,
    occurredAt,
  });
}

export function runtimeContractsEqual(left, right) {
  return canonicalizeJson(left) === canonicalizeJson(right);
}

export function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) deepFreeze(child);
  return Object.freeze(value);
}
