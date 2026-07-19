import { canonicalizeJson } from '../../contracts/public/index.js';

export function createDeliveryLaneKey(command) {
  return canonicalizeJson([
    command.target.channel,
    command.target.tenant_id,
    command.target.bot_id,
    command.target.chat_id,
    command.mapping.turn_id,
    command.aggregate_type,
  ]);
}
