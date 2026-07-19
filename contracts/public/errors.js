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
  return Object.freeze(error);
}
