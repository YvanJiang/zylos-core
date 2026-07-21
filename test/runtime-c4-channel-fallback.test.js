import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createContractError,
  createIdempotencyKey,
  validateDeliveryCommand,
  validateDeliveryResult,
  validateInboundEnvelope,
} from '../contracts/public/index.js';
import {
  acceptCompatibilityInbound,
  createChannelNeutralTextRenderer,
} from '../runtime/compatibility/c4-channel-fallback.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-c4-fallback-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function compatibilityMessage(suffix = 'initial') {
  return {
    inbound_event_id: `telegram-event-${suffix}`,
    trace_id: `trace-${suffix}`,
    occurred_at: '2026-07-19T08:00:00Z',
    received_at: '2026-07-19T08:00:01Z',
    region: 'global',
    tenant_id: 'tenant-c4',
    channel: 'telegram',
    bot_id: 'bot-c4',
    chat_type: 'dm',
    chat_id: 'chat-user-42',
    native_thread_or_topic_id: null,
    message_id: `telegram-message-${suffix}`,
    actor: {
      type: 'user',
      actor_id: 'user-42',
      authenticated: true,
      roles: ['member'],
    },
    content: {
      kind: 'text',
      text: `message ${suffix}`,
      attachments: [],
    },
    reply: {
      root_message_id: null,
      parent_message_id: null,
      reply_to_message_id: null,
    },
    source_ref: null,
  };
}

function lifecycleCommand(base, suffix, renderModel) {
  const command = {
    ...structuredClone(base),
    outbox_id: `outbox-${suffix}`,
    delivery_id: `delivery-${suffix}`,
    trace_id: `delivery-trace-${suffix}`,
    delivery_attempt_id: `delivery-attempt-${suffix}`,
    aggregate_version: base.aggregate_version + 1,
    event_sequence_through: base.event_sequence_through + 1,
    mapping: {
      ...structuredClone(base.mapping),
      mapping_id: `mapping-${suffix}`,
    },
    render_model: renderModel,
  };
  command.idempotency_key = createIdempotencyKey('delivery', {
    channel: command.target.channel,
    target: command.target,
    delivery_id: command.delivery_id,
  });
  return command;
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('C4 channel-neutral fallback', () => {
  test('requires an explicit current-claim fence before any text side effect', () => {
    expect(() => createChannelNeutralTextRenderer({
      async sendText() {
        return { platform_message_id: 'must-not-run' };
      },
    })).toThrow('beforeSend must be a function');
  });

  test('turns a compatibility message into the canonical durable inbound flow', () => {
    const database = openTestDatabase();
    const originalMessage = compatibilityMessage();
    const accepted = acceptCompatibilityInbound(database, originalMessage, {
      now: () => '2026-07-19T08:00:02Z',
      generateId: deterministicIds('compatibility'),
    });

    expect(accepted).toMatchObject({
      status: 'accepted',
      lineage_resolution_state: 'bound',
      deduplicated: false,
      error: null,
    });
    const storedEnvelope = JSON.parse(database.prepare(`
      SELECT envelope_json
      FROM runtime_inbound_events
      WHERE inbound_event_id = ?
    `).get('telegram-event-initial').envelope_json);
    expect(validateInboundEnvelope(storedEnvelope).forwarded).toEqual(storedEnvelope);
    expect(storedEnvelope).toMatchObject({
      contract: 'zylos.inbound-envelope',
      contract_version: '1.0',
      channel: 'telegram',
      chat_id: 'chat-user-42',
      source: { kind: 'platform_original', source_ref: null },
    });
    expect(storedEnvelope).not.toHaveProperty('legacy');

    expect(database.prepare(`
      SELECT state, queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'queued', queue_sequence: 1 });
    const command = JSON.parse(database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ? AND status = 'pending'
    `).get(accepted.turn_id).command_json);
    expect(validateDeliveryCommand(command).forwarded).toEqual(command);
    expect(command).toMatchObject({
      aggregate_type: 'turn_main',
      aggregate_id: accepted.turn_id,
      operation: 'send_text',
      mapping: {
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
        lineage_id: accepted.lineage_id,
        binding_state: 'bound',
      },
      render_model: { phase: 'received', terminal: false },
    });

    const duplicateMessage = structuredClone(originalMessage);
    duplicateMessage.trace_id = 'trace-initial-retry';
    const replayed = acceptCompatibilityInbound(database, duplicateMessage, {
      now: () => '2026-07-19T08:00:03Z',
      generateId() {
        throw new Error('a duplicate compatibility event must not allocate Core IDs');
      },
    });
    expect(replayed).toEqual({
      ...accepted,
      trace_id: 'trace-initial-retry',
      deduplicated: true,
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count).toBe(1);

    database.close();
  });

  test('renders received, completed, failed and action-required delivery commands as text', async () => {
    const database = openTestDatabase();
    const accepted = acceptCompatibilityInbound(database, compatibilityMessage('render'), {
      now: () => '2026-07-19T08:10:00Z',
      generateId: deterministicIds('render'),
    });
    const received = JSON.parse(database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ?
    `).get(accepted.turn_id).command_json);
    const completed = lifecycleCommand(received, 'completed', {
      ...received.render_model,
      phase: 'completed',
      text: 'The report is ready.',
      terminal: true,
    });
    const failed = lifecycleCommand(received, 'failed', {
      ...received.render_model,
      phase: 'failed',
      text: null,
      error: createContractError({
        code: 'provider_context_invalid',
        category: 'provider',
        userMessage: 'The provider could not complete this request.',
        occurredAt: '2026-07-19T08:10:01Z',
      }),
      terminal: true,
    });
    const actionRequired = lifecycleCommand(received, 'action', {
      ...received.render_model,
      phase: 'waiting_user',
      text: 'Choose a deployment target.',
      interactions: [{ interaction_id: 'interaction-action-1', ordinal: 1 }],
      terminal: false,
      user_action_required: true,
    });
    const sent = [];
    const renderer = createChannelNeutralTextRenderer({
      beforeSend() {},
      async sendText(delivery) {
        sent.push(delivery);
        return { platform_message_id: `telegram-out-${sent.length}` };
      },
      now: () => '2026-07-19T08:10:02Z',
    });

    const commands = [received, completed, failed, actionRequired];
    const results = [];
    for (const command of commands) {
      expect(validateDeliveryCommand(command).forwarded).toEqual(command);
      const result = await renderer.deliver(command);
      expect(validateDeliveryResult(result, { command }).forwarded).toEqual(result);
      results.push(result);
    }

    expect(sent.map(({ text }) => text)).toEqual([
      'Received\nMessage received.',
      'Completed\nThe report is ready.',
      'Failed\nThe provider could not complete this request.',
      'Action required\nChoose a deployment target.\nReply to this message so Zylos can route your response.',
    ]);
    expect(sent).toEqual(sent.map((delivery, index) => expect.objectContaining({
      target: received.target,
      delivery_id: commands[index].delivery_id,
      idempotency_key: commands[index].idempotency_key,
      reply_to_platform_message_id: null,
    })));
    expect(results.every((result) => result.status === 'delivered')).toBe(true);
    expect(results.map((result) => result.platform_message_id)).toEqual([
      'telegram-out-1',
      'telegram-out-2',
      'telegram-out-3',
      'telegram-out-4',
    ]);

    database.close();
  });

  test('delivers the completed turn as text and maps replies to the original lineage', async () => {
    const database = openTestDatabase();
    const accepted = acceptCompatibilityInbound(database, compatibilityMessage('round-trip'), {
      now: () => '2026-07-19T08:20:00Z',
      generateId: deterministicIds('round-trip-inbound'),
    });
    const executor = createExecutorService({
      database,
      adapter: {
        async *execute() {
          yield {
            kind: 'text_snapshot',
            payload: { text: 'provider-neutral result', end_offset: 23 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-round-trip',
      now: () => '2026-07-19T08:20:01Z',
      generateId: deterministicIds('round-trip-executor'),
    });
    await expect(executor.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });

    const sent = [];
    const renderer = createChannelNeutralTextRenderer({
      beforeSend() {},
      async sendText(delivery) {
        sent.push(delivery);
        return { platform_message_id: `telegram-round-trip-${sent.length}` };
      },
      now: () => '2026-07-19T08:20:02Z',
    });
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'outbox-service-round-trip',
      now: () => '2026-07-19T08:20:02Z',
      generateId: deterministicIds('round-trip-outbox'),
      throttleMs: 0,
    });
    for (let attempt = 0; attempt < 10; attempt += 1) {
      const outcome = await outbox.dispatchNext();
      if (outcome.status === 'idle') break;
    }

    expect(sent.map(({ text }) => text)).toEqual([
      'Received\nMessage received.',
      'Completed\nprovider-neutral result',
    ]);
    const commands = database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
      ORDER BY aggregate_version ASC
    `).all(accepted.turn_id).map(({ command_json: commandJson }) => JSON.parse(commandJson));
    expect(commands).toHaveLength(2);
    expect(commands.every((command) => command.operation === 'send_text')).toBe(true);
    expect(new Set(commands.map((command) => command.mapping.mapping_id)).size).toBe(2);
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_message_mappings
      WHERE conversation_id = ? AND lineage_id = ?
    `).get(accepted.conversation_id, accepted.lineage_id).count).toBe(2);

    const reply = compatibilityMessage('reply');
    reply.reply = {
      root_message_id: 'telegram-round-trip-1',
      parent_message_id: 'telegram-round-trip-1',
      reply_to_message_id: 'telegram-round-trip-1',
    };
    const acceptedReply = acceptCompatibilityInbound(database, reply, {
      now: () => '2026-07-19T08:20:03Z',
      generateId: deterministicIds('round-trip-reply'),
    });
    expect(acceptedReply).toMatchObject({
      status: 'accepted',
      conversation_id: accepted.conversation_id,
      lineage_id: accepted.lineage_id,
    });
    expect(acceptedReply.turn_id).not.toBe(accepted.turn_id);

    database.close();
  });

  test('keeps executor progress and durable text delivery across a channel restart', async () => {
    const database = openTestDatabase();
    const accepted = acceptCompatibilityInbound(database, compatibilityMessage('restart'), {
      now: () => '2026-07-19T08:30:00Z',
      generateId: deterministicIds('restart-inbound'),
    });
    const unavailableRenderer = createChannelNeutralTextRenderer({
      beforeSend() {},
      async sendText() {
        throw new Error('channel process stopped during delivery');
      },
      now: () => '2026-07-19T08:30:00Z',
    });
    const stoppedChannel = createOutboxService({
      database,
      renderer: unavailableRenderer,
      serviceInstanceId: 'outbox-service-before-channel-restart',
      now: () => '2026-07-19T08:30:00Z',
      generateId: deterministicIds('restart-outbox-before'),
      leaseDurationMs: 10_000,
      throttleMs: 0,
    });
    await expect(stoppedChannel.dispatchNext())
      .rejects.toThrow('channel process stopped during delivery');
    expect(database.prepare(`
      SELECT status
      FROM runtime_outbox
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'delivering' });

    const executor = createExecutorService({
      database,
      adapter: {
        async *execute() {
          yield {
            kind: 'text_snapshot',
            payload: { text: 'completed while the channel was down', end_offset: 36 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-during-channel-restart',
      now: () => '2026-07-19T08:30:05Z',
      generateId: deterministicIds('restart-executor'),
    });
    await expect(executor.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'completed' });

    const recoveredDeliveries = [];
    const restartedChannel = createOutboxService({
      database,
      renderer: createChannelNeutralTextRenderer({
        beforeSend() {},
        async sendText(delivery) {
          recoveredDeliveries.push(delivery);
          return {
            platform_message_id: `telegram-after-restart-${recoveredDeliveries.length}`,
          };
        },
        now: () => '2026-07-19T08:30:11Z',
      }),
      serviceInstanceId: 'outbox-service-after-channel-restart',
      now: () => '2026-07-19T08:30:11Z',
      generateId: deterministicIds('restart-outbox-after'),
      leaseDurationMs: 10_000,
      throttleMs: 0,
    });
    await expect(restartedChannel.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });
    await expect(restartedChannel.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });
    await expect(restartedChannel.dispatchNext()).resolves.toEqual({ status: 'idle' });

    expect(recoveredDeliveries.map(({ text }) => text)).toEqual([
      'Received\nMessage received.',
      'Completed\ncompleted while the channel was down',
    ]);
    expect(database.prepare(`
      SELECT status, COUNT(*) AS count
      FROM runtime_outbox
      WHERE turn_id = ?
      GROUP BY status
    `).all(accepted.turn_id)).toEqual([{ status: 'delivered', count: 2 }]);
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_message_mappings
      WHERE turn_id = ? AND lineage_id = ?
    `).get(accepted.turn_id, accepted.lineage_id).count).toBe(2);

    database.close();
  });

  test('delivers an issue-15 interaction prompt and routes its reply to the same lineage', async () => {
    const database = openTestDatabase();
    const accepted = acceptCompatibilityInbound(database, compatibilityMessage('interaction'), {
      now: () => '2026-07-19T08:40:00Z',
      generateId: deterministicIds('interaction-inbound'),
    });
    const executor = createExecutorService({
      database,
      adapter: {
        async *execute() {
          yield {
            kind: 'interaction_requested',
            payload: {
              provider_interaction_ref: 'provider-question-text-fallback',
              tool_use_id: 'tool-use-text-fallback',
              kind: 'tool_approval',
              prompt: 'Allow the requested workspace write?',
              choices: [],
              authorized_subjects: [{ type: 'actor', actor_id: 'user-42' }],
              allowed_sources: ['main_card_reply', 'card_action'],
            },
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-text-interaction',
      now: () => '2026-07-19T08:40:01Z',
      generateId: deterministicIds('interaction-executor'),
    });
    await expect(executor.runNext()).resolves.toMatchObject({
      status: 'waiting_user',
      turn_id: accepted.turn_id,
      request: {
        kind: 'tool_approval',
        prompt: 'Allow the requested workspace write?',
      },
    });

    const sent = [];
    const outbox = createOutboxService({
      database,
      renderer: createChannelNeutralTextRenderer({
        beforeSend() {},
        async sendText(delivery) {
          sent.push(delivery);
          return { platform_message_id: `telegram-interaction-${sent.length}` };
        },
        now: () => '2026-07-19T08:40:02Z',
      }),
      serviceInstanceId: 'outbox-service-text-interaction',
      now: () => '2026-07-19T08:40:02Z',
      generateId: deterministicIds('interaction-outbox'),
      throttleMs: 0,
    });
    for (let attempt = 0; attempt < 10; attempt += 1) {
      const outcome = await outbox.dispatchNext();
      if (outcome.status === 'idle') break;
    }

    expect(sent.map(({ text }) => text)).toEqual([
      'Received\nMessage received.',
      'Action required\nYour input is required.\nReply to this message so Zylos can route your response.',
      'Action required\nAllow the requested workspace write?\nReply to this message so Zylos can route your response.',
    ]);
    const commands = database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ?
      ORDER BY aggregate_version ASC
    `).all(accepted.turn_id).map(({ command_json: commandJson }) => JSON.parse(commandJson));
    expect(commands.every((command) => command.operation === 'send_text')).toBe(true);
    expect(new Set(commands.map((command) => command.mapping.mapping_id)).size).toBe(3);

    const reply = compatibilityMessage('interaction-reply');
    reply.reply = {
      root_message_id: 'telegram-interaction-3',
      parent_message_id: 'telegram-interaction-3',
      reply_to_message_id: 'telegram-interaction-3',
    };
    const acceptedReply = acceptCompatibilityInbound(database, reply, {
      now: () => '2026-07-19T08:40:03Z',
      generateId: deterministicIds('interaction-reply'),
    });
    expect(acceptedReply).toMatchObject({
      status: 'accepted',
      conversation_id: accepted.conversation_id,
      lineage_id: accepted.lineage_id,
    });
    expect(database.prepare(`
      SELECT state, queue_sequence
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(acceptedReply.turn_id)).toEqual({ state: 'queued', queue_sequence: 2 });

    database.close();
  });
});
