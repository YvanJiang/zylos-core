import { canonicalizeJson } from './jcs.js';
import {
  rejectContract as rejectRuntimeContract,
  requireArray,
  requireBoolean,
  requireCriticalEnum as requireEnum,
  requireDisplayString,
  requireExactFields,
  requireNullableOpaqueId,
  requireNullableTimestamp,
  requireOpaqueId,
  requireOwnFields,
  requirePlainObject as requireRecord,
  requireTimestamp,
} from './contract-utils.js';
import {
  validateContractError,
  validateContractDocument,
  validatePublicNumber,
} from './validation.js';

export {
  rejectRuntimeContract,
  requireArray,
  requireBoolean,
  requireEnum,
  requireExactFields,
  requireNullableOpaqueId,
  requireNullableTimestamp,
  requireOpaqueId,
  requireRecord,
  requireTimestamp,
};

export function requireFields(path, value, fields, options) {
  requireRecord(path, value, options);
  requireOwnFields(path, value, fields, options);
}

export function requireText(path, value, { nonEmpty = true, ...options } = {}) {
  return requireDisplayString(path, value, { nonEmpty, ...options });
}

export function requireNullableText(path, value, options) {
  return value === null ? null : requireText(path, value, options);
}

export function requireInteger(path, value, { min, nullable = false, ...options } = {}) {
  if (nullable && value === null) return null;
  validatePublicNumber(path, value, options);
  if (min !== undefined && value < min) {
    rejectRuntimeContract('validation_error', `${path} must be at least ${min}.`, options);
  }
  return value;
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
