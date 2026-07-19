import { ContractKernelError, createContractError } from './errors.js';
import { isPlainJsonObject, isWellFormedUnicode } from './scalars.js';
import {
  validateContractHeader,
  validateOpaqueId,
  validatePublicNumber,
  validateRfc3339Timestamp,
  validateSafetyCriticalEnum,
} from './validation.js';

export function rejectContract(
  code,
  userMessage,
  {
    category = 'validation',
    retryable = false,
    sideEffectStatus = 'none',
    occurredAt,
  } = {},
) {
  throw new ContractKernelError(createContractError({
    code,
    category,
    retryable,
    sideEffectStatus,
    userMessage,
    occurredAt,
  }));
}

export function requirePlainObject(fieldName, value, { occurredAt } = {}) {
  if (!isPlainJsonObject(value)) {
    rejectContract('validation_error', `${fieldName} must be a JSON object.`, { occurredAt });
  }
  return value;
}

export function requireArray(fieldName, value, { occurredAt } = {}) {
  if (!Array.isArray(value)) {
    rejectContract('validation_error', `${fieldName} must be an array.`, { occurredAt });
  }
  return value;
}

export function requireBoolean(fieldName, value, { occurredAt } = {}) {
  if (typeof value !== 'boolean') {
    rejectContract('validation_error', `${fieldName} must be a boolean.`, { occurredAt });
  }
  return value;
}

export function requireDisplayString(
  fieldName,
  value,
  { nullable = false, nonEmpty = true, occurredAt } = {},
) {
  if (nullable && value === null) return value;
  if (
    typeof value !== 'string'
    || (nonEmpty && value.trim().length === 0)
    || !isWellFormedUnicode(value)
  ) {
    rejectContract('validation_error', `${fieldName} must be a displayable string.`, { occurredAt });
  }
  return value;
}

export function requireOpaqueId(fieldName, value, { occurredAt } = {}) {
  return validateOpaqueId(fieldName, value, { occurredAt });
}

export function requireNullableOpaqueId(fieldName, value, { occurredAt } = {}) {
  if (value === null) return value;
  return requireOpaqueId(fieldName, value, { occurredAt });
}

export function requirePositiveInteger(fieldName, value, { occurredAt } = {}) {
  validatePublicNumber(fieldName, value, { occurredAt });
  if (value < 1) {
    rejectContract('validation_error', `${fieldName} must be a positive integer.`, { occurredAt });
  }
  return value;
}

export function requireNonNegativeInteger(fieldName, value, { occurredAt } = {}) {
  validatePublicNumber(fieldName, value, { occurredAt });
  if (value < 0) {
    rejectContract('validation_error', `${fieldName} must be a non-negative integer.`, { occurredAt });
  }
  return value;
}

export function requireCriticalEnum(fieldName, value, allowedValues, { occurredAt } = {}) {
  return validateSafetyCriticalEnum(fieldName, value, allowedValues, { occurredAt });
}

export function requireTimestamp(fieldName, value, { occurredAt } = {}) {
  return validateRfc3339Timestamp(fieldName, value, { occurredAt });
}

export function requireNullableTimestamp(fieldName, value, { occurredAt } = {}) {
  if (value === null) return value;
  return requireTimestamp(fieldName, value, { occurredAt });
}

export function requireOwnFields(fieldName, value, fieldNames, { occurredAt } = {}) {
  for (const childName of fieldNames) {
    if (!Object.hasOwn(value, childName)) {
      rejectContract('validation_error', `${fieldName}.${childName} is required.`, { occurredAt });
    }
  }
}

export function requireExactFields(fieldName, value, fieldNames, { occurredAt } = {}) {
  requirePlainObject(fieldName, value, { occurredAt });
  requireOwnFields(fieldName, value, fieldNames, { occurredAt });
  const allowed = new Set(fieldNames);
  for (const childName of Object.keys(value)) {
    if (!allowed.has(childName)) {
      rejectContract(
        'validation_error',
        `${fieldName}.${childName} is not allowed.`,
        { occurredAt },
      );
    }
  }
}

export function partitionContractDocument(
  value,
  contract,
  knownFieldNames,
  { occurredAt } = {},
) {
  const header = validateContractHeader(value, { occurredAt });
  if (header.contract !== contract) {
    rejectContract(
      'unsupported_capability',
      `Expected contract ${contract} but received ${header.contract}.`,
      { occurredAt },
    );
  }

  const knownNames = new Set(['contract', 'contract_version', ...knownFieldNames]);
  const knownEntries = Object.entries(value)
    .filter(([fieldName]) => knownNames.has(fieldName))
    .map(([fieldName, fieldValue]) => [fieldName, structuredClone(fieldValue)]);
  const extensionEntries = Object.entries(value)
    .filter(([fieldName]) => !knownNames.has(fieldName))
    .map(([fieldName, fieldValue]) => [fieldName, structuredClone(fieldValue)]);

  return {
    header,
    known: Object.fromEntries(knownEntries),
    extensions: Object.fromEntries(extensionEntries),
    forwarded: structuredClone(value),
  };
}
