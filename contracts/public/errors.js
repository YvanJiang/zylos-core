import {
  ERROR_CATEGORIES,
  PUBLIC_ERROR_CODES,
  SIDE_EFFECT_STATUSES,
} from './constants.js';
import { isValidRfc3339Timestamp, isWellFormedUnicode } from './scalars.js';

const ERROR_CODE_PATTERN = /^[a-z][a-z0-9_]*$/;

export function inspectContractError(value, { requireKnownCode = false } = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return { code: 'validation_error', message: 'error must be a JSON object.' };
  }
  if (
    typeof value.code !== 'string'
    || !ERROR_CODE_PATTERN.test(value.code)
    || (requireKnownCode && !PUBLIC_ERROR_CODES.includes(value.code))
  ) {
    return { code: 'validation_error', message: 'error.code must be a known stable machine code.' };
  }
  if (!ERROR_CATEGORIES.includes(value.category)) {
    return {
      code: 'unsupported_capability',
      message: 'error.category contains an unsupported security-critical value.',
    };
  }
  if (typeof value.retryable !== 'boolean') {
    return { code: 'validation_error', message: 'error.retryable must be a boolean.' };
  }
  if (!SIDE_EFFECT_STATUSES.includes(value.side_effect_status)) {
    return {
      code: 'unsupported_capability',
      message: 'error.side_effect_status contains an unsupported security-critical value.',
    };
  }
  if (
    typeof value.user_message !== 'string'
    || value.user_message.trim().length === 0
    || !isWellFormedUnicode(value.user_message)
  ) {
    return { code: 'validation_error', message: 'error.user_message must be a displayable string.' };
  }
  if (
    value.detail_ref !== undefined
    && (
      typeof value.detail_ref !== 'string'
      || value.detail_ref.length === 0
      || !isWellFormedUnicode(value.detail_ref)
    )
  ) {
    return { code: 'validation_error', message: 'error.detail_ref must be a non-empty reference.' };
  }
  if (!isValidRfc3339Timestamp(value.occurred_at)) {
    return {
      code: 'validation_error',
      message: 'error.occurred_at must be a valid RFC 3339 timestamp with an explicit timezone.',
    };
  }
  return null;
}

export class ContractKernelError extends Error {
  constructor(contractError) {
    super(contractError.user_message);
    this.name = 'ContractKernelError';
    this.contractError = Object.freeze({ ...contractError });
  }
}

export function createContractError({
  code,
  category = 'validation',
  retryable = false,
  sideEffectStatus = 'none',
  userMessage,
  detailRef,
  occurredAt = new Date().toISOString(),
}) {
  const error = {
    code,
    category,
    retryable,
    side_effect_status: sideEffectStatus,
    user_message: userMessage,
  };
  if (detailRef !== undefined) error.detail_ref = detailRef;
  error.occurred_at = occurredAt;
  const violation = inspectContractError(error, { requireKnownCode: true });
  if (violation) throw new TypeError(`Invalid contract error: ${violation.message}`);
  return Object.freeze(error);
}
