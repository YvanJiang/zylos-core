export function deliveredResult(command, resultAt) {
  return {
    contract: 'zylos.delivery-result',
    contract_version: '1.0',
    trace_id: command.trace_id,
    outbox_id: command.outbox_id,
    delivery_id: command.delivery_id,
    idempotency_key: command.idempotency_key,
    delivery_attempt_id: command.delivery_attempt_id,
    delivery_attempt_no: command.delivery_attempt_no,
    outbox_lease_epoch: command.outbox_lease_epoch,
    mapping_id: command.mapping.mapping_id,
    operation: command.operation,
    aggregate_version: command.aggregate_version,
    status: 'delivered',
    platform_message_id: command.operation === 'update_main'
      ? command.target_platform_message_id
      : `platform-${command.delivery_attempt_id}`,
    applied_platform_version: null,
    delivered_at: resultAt,
    error: null,
    renderer_capabilities: {
      supports_update: true,
      supports_actions: true,
      supports_platform_idempotency: true,
      supports_platform_version: false,
    },
    result_at: resultAt,
  };
}
