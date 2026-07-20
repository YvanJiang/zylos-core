import { createChannelNeutralTextRenderer } from '../../../runtime/compatibility/c4-channel-fallback.js';
import { createOutboxService } from '../../../runtime/delivery/outbox-service.js';

export function createWebConsoleOutboxOwner({
  database,
  clients,
  broadcast,
  onDeliveredMessage = () => {},
  serviceInstanceId = `web-console-${process.pid}`,
  now = () => new Date().toISOString(),
}) {
  if (!(clients instanceof Set)) throw new TypeError('clients must be a Set');
  if (typeof broadcast !== 'function') throw new TypeError('broadcast must be a function');
  if (typeof onDeliveredMessage !== 'function') {
    throw new TypeError('onDeliveredMessage must be a function');
  }

  const owner = createOutboxService({
    database,
    channel: 'web-console',
    serviceInstanceId,
    now,
    renderer: createChannelNeutralTextRenderer({
      now,
      async sendText(delivery) {
        const outboxRow = database.prepare(`
          SELECT event.rowid * 2 + 1 AS id
          FROM runtime_outbox AS outbox
          JOIN runtime_normalized_events AS event
            ON event.turn_id = outbox.turn_id
           AND event.event_sequence = CAST(
             json_extract(outbox.command_json, '$.event_sequence_through') AS INTEGER
           )
          WHERE outbox.delivery_id = ?
        `
        ).get(delivery.delivery_id);
        if (!outboxRow) throw new Error('Web Console outbox row is unavailable.');
        const message = Object.freeze({
          id: outboxRow.id,
          direction: 'out',
          channel: 'web-console',
          endpoint_id: delivery.target.chat_id,
          content: delivery.text,
          timestamp: now(),
        });
        const delivered = broadcast('messages', [message]);
        if (!Number.isSafeInteger(delivered) || delivered < 1) {
          throw new Error('No Web Console client accepted the delivery.');
        }
        onDeliveredMessage(message);
        return { platform_message_id: `web-console:${delivery.delivery_id}` };
      },
    }),
  });

  async function drain({ limit = 20 } = {}) {
    if (clients.size === 0) return Object.freeze({ status: 'no_consumers', delivered: 0 });
    let delivered = 0;
    for (let count = 0; count < limit; count += 1) {
      const result = await owner.dispatchNext();
      if (result.status === 'idle') break;
      delivered += 1;
    }
    return Object.freeze({ status: delivered === 0 ? 'idle' : 'delivered', delivered });
  }

  return Object.freeze({ drain });
}
