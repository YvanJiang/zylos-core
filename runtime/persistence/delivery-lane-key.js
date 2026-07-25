import { canonicalizeJson } from '../../contracts/public/index.js';

export function createDeliveryLaneKeyFromIdentity({ target, turnId, aggregateType }) {
  const identity = [
    target.channel,
    target.region,
    target.tenant_id,
    target.bot_id,
    target.chat_type,
    target.chat_id,
    turnId,
    aggregateType,
  ];
  if (target.chat_type === 'thread') {
    identity.push(target.native_thread_or_topic_id);
    if (
      Object.hasOwn(target, 'native_thread_root_message_id')
      && Object.hasOwn(target, 'native_thread_reply_target_message_id')
    ) {
      identity.push(
        target.native_thread_root_message_id,
        target.native_thread_reply_target_message_id,
      );
    }
  }
  return canonicalizeJson(identity);
}

export function createDeliveryLaneKey(command) {
  return createDeliveryLaneKeyFromIdentity({
    target: command.target,
    turnId: command.mapping.turn_id,
    aggregateType: command.aggregate_type,
  });
}
