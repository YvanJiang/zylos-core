import { PUBLIC_CONTRACT_MAJOR, PUBLIC_CONTRACTS } from './constants.js';
import {
  ContractKernelError,
  createContractError,
  inspectContractError,
} from './errors.js';
import { isValidRfc3339Timestamp, isWellFormedUnicode } from './scalars.js';

const VERSION_PATTERN = /^(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const CONTROL_CHARACTER_PATTERN = /[\u0000-\u001f\u007f]/;
const SECRET_FIELD_PATTERN = /(?:^|_)(?:secret|token|password|credential|credentials|authorization|cookie|signature)(?:$|_)/i;
const EXPLICIT_SECRET_FIELD_SUFFIXES = Object.freeze([
  'api_key',
  'private_key',
  'access_key',
]);
const PRIVATE_PAYLOAD_FIELD_PATTERN = /^(?:raw_provider|raw_channel|provider_payload|channel_payload|provider_private|channel_private)(?:_|$)/i;
const SECRET_VALUE_PATTERNS = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /\bBearer\s+\S+/i,
  /\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{8,}/,
  /\bxox[baprs]-[A-Za-z0-9-]{8,}/,
];

function reject(code, userMessage, occurredAt) {
  throw new ContractKernelError(createContractError({ code, userMessage, occurredAt }));
}

export function validateOpaqueId(fieldName, value, { occurredAt } = {}) {
  if (typeof value !== 'string' || value.length === 0) {
    reject('validation_error', `${fieldName} must be a non-empty opaque string.`, occurredAt);
  }
  if (CONTROL_CHARACTER_PATTERN.test(value) || !isWellFormedUnicode(value)) {
    reject(
      'validation_error',
      `${fieldName} must not contain control characters or invalid Unicode.`,
      occurredAt,
    );
  }
  return value;
}

export function validateRfc3339Timestamp(fieldName, value, { occurredAt } = {}) {
  if (!isValidRfc3339Timestamp(value)) {
    reject(
      'validation_error',
      `${fieldName} must be an RFC 3339 timestamp with an explicit timezone.`,
      occurredAt,
    );
  }
  return value;
}

export function validatePublicNumber(
  fieldName,
  value,
  { allowDecimal = false, occurredAt } = {},
) {
  if (typeof value !== 'number' || !Number.isFinite(value) || Object.is(value, -0)) {
    reject('validation_error', `${fieldName} must be a finite JSON number and not negative zero.`, occurredAt);
  }
  if (Number.isInteger(value)) {
    if (!Number.isSafeInteger(value)) {
      reject('validation_error', `${fieldName} must be a JSON safe integer.`, occurredAt);
    }
    return value;
  }
  if (!allowDecimal) {
    reject('validation_error', `${fieldName} must be a JSON safe integer.`, occurredAt);
  }
  return value;
}

export function validateSafetyCriticalEnum(
  fieldName,
  value,
  allowedValues,
  { occurredAt } = {},
) {
  if (!allowedValues.includes(value)) {
    reject(
      'unsupported_capability',
      `${fieldName} contains an unsupported security-critical value.`,
      occurredAt,
    );
  }
  return value;
}

export function validateContractError(value, { occurredAt } = {}) {
  const violation = inspectContractError(value);
  if (violation) reject(violation.code, violation.message, occurredAt);
  return { ...value };
}

function normalizeFixtureFieldName(fieldName) {
  return fieldName
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^A-Za-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

function isSecretFixtureField(fieldName) {
  const normalized = normalizeFixtureFieldName(fieldName);
  return SECRET_FIELD_PATTERN.test(normalized)
    || EXPLICIT_SECRET_FIELD_SUFFIXES.some(
      (secretName) => normalized === secretName || normalized.endsWith(`_${secretName}`),
    );
}

export function validatePublicFixtureSafety(value, { occurredAt } = {}) {
  const ancestors = new Set();

  function inspect(current, path) {
    if (typeof current === 'string') {
      if (SECRET_VALUE_PATTERNS.some((pattern) => pattern.test(current))) {
        reject('validation_error', `Fixture ${path} contains secret-shaped content.`, occurredAt);
      }
      return;
    }
    if (current === null || typeof current !== 'object') return;
    if (ancestors.has(current)) {
      reject('validation_error', `Fixture ${path} contains a cycle.`, occurredAt);
    }
    ancestors.add(current);
    if (Array.isArray(current)) {
      current.forEach((entry, index) => inspect(entry, `${path}[${index}]`));
    } else {
      for (const [fieldName, fieldValue] of Object.entries(current)) {
        if (isSecretFixtureField(fieldName)) {
          reject('validation_error', `Fixture ${path}.${fieldName} contains a secret field.`, occurredAt);
        }
        if (PRIVATE_PAYLOAD_FIELD_PATTERN.test(fieldName)) {
          reject(
            'validation_error',
            `Fixture ${path}.${fieldName} contains a provider/channel private object.`,
            occurredAt,
          );
        }
        inspect(fieldValue, `${path}.${fieldName}`);
      }
    }
    ancestors.delete(current);
  }

  inspect(value, '$');
  return true;
}

export function validateContractHeader(value, { occurredAt } = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    reject('validation_error', 'Contract payload must be a JSON object.', occurredAt);
  }
  if (!PUBLIC_CONTRACTS.includes(value.contract)) {
    reject(
      'unsupported_capability',
      `Public contract ${String(value.contract)} is not supported.`,
      occurredAt,
    );
  }
  if (typeof value.contract_version !== 'string') {
    reject('validation_error', 'contract_version must be a string.', occurredAt);
  }

  const match = VERSION_PATTERN.exec(value.contract_version);
  if (!match) {
    reject('validation_error', 'contract_version must use <major>.<minor>.', occurredAt);
  }

  const major = Number(match[1]);
  const minor = Number(match[2]);
  if (!Number.isSafeInteger(major) || !Number.isSafeInteger(minor)) {
    reject('validation_error', 'contract_version components must be safe integers.', occurredAt);
  }
  if (major !== PUBLIC_CONTRACT_MAJOR) {
    reject(
      'unsupported_contract_version',
      `Contract ${value.contract} major version ${major} is not supported.`,
      occurredAt,
    );
  }

  return {
    contract: value.contract,
    contractVersion: value.contract_version,
    major,
    minor,
  };
}

function validateField(fieldName, value, rule, occurredAt) {
  switch (rule.kind) {
    case 'presence':
      return value;
    case 'opaque_id':
      return validateOpaqueId(fieldName, value, { occurredAt });
    case 'rfc3339':
      return validateRfc3339Timestamp(fieldName, value, { occurredAt });
    case 'number':
      return validatePublicNumber(fieldName, value, {
        allowDecimal: Boolean(rule.allowDecimal),
        occurredAt,
      });
    case 'critical_enum':
      return validateSafetyCriticalEnum(fieldName, value, rule.values ?? [], { occurredAt });
    default:
      throw new TypeError(`unsupported contract field rule: ${String(rule.kind)}`);
  }
}

export function validateContractDocument(
  value,
  { contract, fields = {}, occurredAt } = {},
) {
  const header = validateContractHeader(value, { occurredAt });
  if (header.contract !== contract) {
    reject(
      'unsupported_capability',
      `Expected contract ${String(contract)} but received ${header.contract}.`,
      occurredAt,
    );
  }

  const knownEntries = [
    ['contract', value.contract],
    ['contract_version', value.contract_version],
  ];
  for (const [fieldName, rule] of Object.entries(fields)) {
    const present = Object.hasOwn(value, fieldName);
    if (!present && rule.required) {
      reject('validation_error', `${fieldName} is required.`, occurredAt);
    }
    if (!present) continue;
    validateField(fieldName, value[fieldName], rule, occurredAt);
    knownEntries.push([fieldName, structuredClone(value[fieldName])]);
  }

  const knownNames = new Set(['contract', 'contract_version', ...Object.keys(fields)]);
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
