import crypto from 'node:crypto';

import { createChannelNeutralTextRenderer } from '../../../runtime/compatibility/c4-channel-fallback.js';
import { createOutboxService } from '../../../runtime/delivery/outbox-service.js';

export function createWebConsoleOutboxOwner({
  database,
  projectInbound,
  deliverMessage,
  region,
  tenantId,
  botId,
  serviceInstanceId = `web-console-${crypto.randomUUID()}`,
  now = () => new Date().toISOString(),
}) {
  if (typeof projectInbound !== 'function') throw new TypeError('projectInbound must be a function');
  if (typeof deliverMessage !== 'function') throw new TypeError('deliverMessage must be a function');
  for (const [fieldName, value] of [
    ['region', region], ['tenantId', tenantId], ['botId', botId],
  ]) {
    if (typeof value !== 'string' || value.length === 0) {
      throw new TypeError(`${fieldName} must be a non-empty string`);
    }
  }

  const textRenderer = createChannelNeutralTextRenderer({
    now,
    async sendText(delivery) {
      const message = Object.freeze({
        delivery_id: delivery.delivery_id,
        direction: 'out',
        channel: 'web-console',
        endpoint_id: delivery.target.chat_id,
        content: delivery.text,
        timestamp: now(),
      });
      const effect = await deliverMessage(message, delivery);
      if (!effect || typeof effect.platform_message_id !== 'string'
        || effect.platform_message_id.length === 0) {
        throw new Error('Web Console mailbox did not confirm its durable delivery effect.');
      }
      return { platform_message_id: effect.platform_message_id };
    },
  });
  const owner = createOutboxService({
    database,
    channel: 'web-console',
    targetChatId: 'console',
    targetRegion: region,
    targetTenantId: tenantId,
    targetBotId: botId,
    serviceInstanceId,
    now,
    renderer: {
      async deliver(command) {
        await projectInbound(command);
        return textRenderer.deliver(command);
      },
    },
  });

  async function drain({ limit = 20 } = {}) {
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

export function createDrainBarrier({ drain }) {
  if (typeof drain !== 'function') throw new TypeError('drain must be a function');
  let accepting = true;
  const active = new Set();

  function run(options) {
    if (!accepting) return Promise.resolve(Object.freeze({ status: 'stopped', delivered: 0 }));
    const operation = Promise.resolve().then(() => drain(options));
    active.add(operation);
    operation.then(
      () => active.delete(operation),
      () => active.delete(operation),
    );
    return operation;
  }

  async function stop() {
    accepting = false;
    await Promise.allSettled([...active]);
  }

  return Object.freeze({ run, stop });
}
