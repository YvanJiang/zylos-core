import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateInboundEnvelope,
} from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createPermissionService } from '../runtime/permissions/permission-service.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-detached-background-'));
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

function normalEnvelope(suffix, text) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.content = { kind: 'text', text, attachments: [] };
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function acceptDetached(database, suffix, text, timestamp) {
  return acceptNormalInbound(database, normalEnvelope(suffix, text), {
    now: () => timestamp,
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

function readBackgroundTask(database, taskId) {
  return database.prepare(`
    SELECT task.background_task_id, task.origin_conversation_id,
      task.dispatch_turn_id, task.execution_conversation_id,
      task.execution_turn_id, task.state, task.side_effect_status,
      dispatch.state AS dispatch_turn_state,
      execution.state AS execution_turn_state
    FROM runtime_background_tasks AS task
    JOIN runtime_turns AS dispatch ON dispatch.turn_id = task.dispatch_turn_id
    JOIN runtime_turns AS execution ON execution.turn_id = task.execution_turn_id
    WHERE task.background_task_id = ?
  `).get(taskId);
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('Core-owned detached background dispatch', () => {
  test('finishes foreground dispatch immediately and runs later inputs in fresh provider sessions', async () => {
    const database = openTestDatabase();
    const first = acceptDetached(
      database,
      'first',
      'perform the long first task',
      '2026-07-23T01:00:00Z',
    );
    const second = acceptDetached(
      database,
      'second',
      'answer the second input',
      '2026-07-23T01:00:01Z',
    );

    expect(first).toMatchObject({
      status: 'accepted',
      dispatch_status: 'background_dispatched',
      background_task_id: expect.any(String),
      background_execution_turn_id: expect.any(String),
    });
    expect(second).toMatchObject({
      status: 'accepted',
      conversation_id: first.conversation_id,
      dispatch_status: 'background_dispatched',
      background_task_id: expect.any(String),
      background_execution_turn_id: expect.any(String),
    });
    expect(second.background_task_id).not.toBe(first.background_task_id);
    expect(second.background_execution_turn_id).not.toBe(first.background_execution_turn_id);
    expect(acceptDetached(
      database,
      'first',
      'perform the long first task',
      '2026-07-23T01:00:02Z',
    )).toMatchObject({
      deduplicated: true,
      background_task_id: first.background_task_id,
      background_execution_turn_id: first.background_execution_turn_id,
      turn_id: first.turn_id,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_background_tasks
    `).get()).toEqual({ count: 2 });
    expect(database.prepare(`
      SELECT state, terminal_at
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(first.turn_id)).toEqual({
      state: 'completed',
      terminal_at: '2026-07-23T01:00:00Z',
    });
    const executionInbound = database.prepare(`
      SELECT inbound.inbound_event_id, inbound.idempotency_key, inbound.envelope_json
      FROM runtime_turns AS turn
      JOIN runtime_inbound_events AS inbound
        ON inbound.inbound_event_id = turn.inbound_event_id
      WHERE turn.turn_id = ?
    `).get(first.background_execution_turn_id);
    const executionEnvelope = JSON.parse(executionInbound.envelope_json);
    expect(validateInboundEnvelope(executionEnvelope).forwarded).toEqual(executionEnvelope);
    expect(executionEnvelope).toMatchObject({
      inbound_event_id: executionInbound.inbound_event_id,
      idempotency_key: executionInbound.idempotency_key,
    });

    expect(readBackgroundTask(database, first.background_task_id)).toMatchObject({
      origin_conversation_id: first.conversation_id,
      dispatch_turn_id: first.turn_id,
      execution_turn_id: first.background_execution_turn_id,
      state: 'queued',
      side_effect_status: 'none',
      dispatch_turn_state: 'completed',
      execution_turn_state: 'queued',
    });
    expect(readBackgroundTask(database, second.background_task_id)).toMatchObject({
      origin_conversation_id: first.conversation_id,
      dispatch_turn_id: second.turn_id,
      execution_turn_id: second.background_execution_turn_id,
      state: 'queued',
      side_effect_status: 'none',
      dispatch_turn_state: 'completed',
      execution_turn_state: 'queued',
    });

    const calls = [];
    let markFirstStarted;
    let releaseFirst;
    const firstStarted = new Promise((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise((resolve) => { releaseFirst = resolve; });
    const adapter = {
      getWorkspaceAccess() {
        return {
          root: process.cwd(),
          mode: 'read_only',
          read_only_enforced: true,
          authority: 'provider_sandbox',
        };
      },
      async *execute(context) {
        calls.push({
          conversation_id: context.conversation_id,
          turn_id: context.turn_id,
          text: context.input.text,
        });
        context.reportProviderState({ state: 'started', provider_native_id: null });
        if (context.input.text.includes('long first')) {
          markFirstStarted();
          await firstGate;
        }
        yield {
          kind: 'text_snapshot',
          payload: {
            text: `completed: ${context.input.text}`,
            end_offset: `completed: ${context.input.text}`.length,
          },
          provider_native_id: null,
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-detached-concurrent',
      now: () => '2026-07-23T01:01:00Z',
      generateId: deterministicIds('executor-detached-concurrent'),
    });

    const firstRun = service.runNext();
    await firstStarted;
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.background_execution_turn_id,
    });
    expect(readBackgroundTask(database, first.background_task_id)).toMatchObject({
      state: 'running',
      side_effect_status: 'none',
    });
    expect(readBackgroundTask(database, second.background_task_id)).toMatchObject({
      state: 'completed',
      side_effect_status: 'none',
    });
    expect(calls).toEqual([
      expect.objectContaining({
        turn_id: first.background_execution_turn_id,
        text: 'perform the long first task',
      }),
      expect.objectContaining({
        turn_id: second.background_execution_turn_id,
        text: 'answer the second input',
      }),
    ]);
    expect(calls[1].conversation_id).not.toBe(calls[0].conversation_id);

    const resultProjection = database.prepare(`
      SELECT render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ? AND terminal = 1
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(second.background_execution_turn_id);
    expect(JSON.parse(resultProjection.render_model_json)).toMatchObject({
      phase: 'completed',
      text: 'completed: answer the second input',
      terminal: true,
    });
    const delivery = JSON.parse(database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
      ORDER BY aggregate_version ASC
      LIMIT 1
    `).get(second.background_execution_turn_id).command_json);
    const secondTask = readBackgroundTask(database, second.background_task_id);
    expect(delivery.mapping).toMatchObject({
      conversation_id: secondTask.execution_conversation_id,
      turn_id: second.background_execution_turn_id,
    });
    expect(delivery.target.chat_id).toBe(normalEnvelope('second', '').chat_id);

    releaseFirst();
    await expect(firstRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.background_execution_turn_id,
    });
    await service.close();
    database.close();
  });

  test('binds background interaction replies to the execution conversation', async () => {
    const database = openTestDatabase();
    const accepted = acceptDetached(
      database,
      'interaction',
      'ask before continuing',
      '2026-07-23T01:30:00Z',
    );
    const adapter = {
      async *execute(context) {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-interaction-background',
            tool_use_id: 'tool-background',
            kind: 'tool_approval',
            prompt: 'Allow the background action?',
            choices: [],
            authorized_subjects: context.interaction.authorized_subjects,
            allowed_sources: context.interaction.allowed_sources,
          },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-detached-interaction',
      now: () => '2026-07-23T01:31:00Z',
      generateId: deterministicIds('executor-detached-interaction'),
    });

    const waiting = await service.runNext();
    expect(waiting).toMatchObject({
      status: 'waiting_user',
      turn_id: accepted.background_execution_turn_id,
      request: {
        conversation_id: expect.any(String),
        turn_id: accepted.background_execution_turn_id,
      },
    });
    expect(waiting.request.conversation_id).not.toBe(accepted.conversation_id);

    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-detached-interaction',
      now: () => '2026-07-23T01:32:00Z',
      generateId: deterministicIds('delivery-detached-interaction'),
      throttleMs: 0,
    });
    let platformMessageId = null;
    for (let count = 0; count < 10; count += 1) {
      const command = outbox.claimNext();
      if (command === null) break;
      const result = deliveredResult(command, '2026-07-23T01:32:00Z');
      platformMessageId = result.platform_message_id;
      expect(outbox.recordResult(result)).toMatchObject({
        status: 'applied',
        outbox_status: 'delivered',
      });
    }

    expect(service.resolveInteractionTarget({
      region: 'cn',
      tenantId: 'tenant-A',
      channel: 'feishu',
      botId: 'bot-A',
      platformMessageId,
      interactionId: waiting.request.interaction_id,
    })).toMatchObject({
      mapping: {
        conversation_id: waiting.request.conversation_id,
        turn_id: waiting.request.turn_id,
        lineage_id: waiting.request.lineage_id,
      },
      interactions: [waiting.request],
    });

    const answer = {
      contract: 'zylos.interaction-answer',
      contract_version: '1.0',
      trace_id: 'trace-background-answer',
      interaction_id: waiting.request.interaction_id,
      interaction_version: waiting.request.version,
      answer_id: 'answer-background',
      source_event_or_action_id: 'message-background-reply',
      actor: {
        type: 'user',
        actor_id: 'user-A',
        authenticated: true,
        roles: ['member'],
      },
      source_context: {
        region: 'cn',
        tenant_id: 'tenant-A',
        channel: 'feishu',
        bot_id: 'bot-A',
        chat_id: 'chat-dm-A',
        native_thread_or_topic_id: null,
        platform_message_or_action_id: 'message-background-reply',
      },
      source: 'main_card_reply',
      value: { kind: 'decision', decision: 'approve' },
      answered_at: '2026-07-23T01:32:00Z',
    };
    answer.idempotency_key = createIdempotencyKey('interaction', {
      interaction_id: answer.interaction_id,
      source_event_or_action_id: answer.source_event_or_action_id,
    });
    expect(service.submitInteractionAnswer(answer, {
      replyToMessageId: platformMessageId,
    })).toMatchObject({
      status: 'accepted',
      interaction_id: waiting.request.interaction_id,
      turn_id: accepted.background_execution_turn_id,
    });

    database.close();
  });

  test('keeps conflicting background writers fenced without blocking later input acceptance', async () => {
    const database = openTestDatabase();
    const first = acceptDetached(
      database,
      'writer-first',
      'first writer',
      '2026-07-23T02:00:00Z',
    );
    let markFirstStarted;
    let releaseFirst;
    const firstStarted = new Promise((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise((resolve) => { releaseFirst = resolve; });
    const adapter = {
      async *execute(context) {
        if (context.input.text === 'first writer') {
          markFirstStarted();
          await firstGate;
        }
        yield {
          kind: 'text_snapshot',
          payload: { text: 'done', end_offset: 4 },
          provider_native_id: null,
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-detached-writers',
      now: () => '2026-07-23T02:01:00Z',
      generateId: deterministicIds('executor-detached-writers'),
    });

    const firstRun = service.runNext();
    await firstStarted;
    const second = acceptDetached(
      database,
      'writer-second',
      'second writer',
      '2026-07-23T02:00:01Z',
    );
    expect(second).toMatchObject({
      status: 'accepted',
      dispatch_status: 'background_dispatched',
      conversation_id: first.conversation_id,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'workspace_wait',
      turn_id: second.background_execution_turn_id,
      wait_reason: 'workspace_lease',
    });
    expect(readBackgroundTask(database, second.background_task_id)).toMatchObject({
      state: 'queued',
      execution_turn_state: 'queued',
    });

    releaseFirst();
    await firstRun;
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.background_execution_turn_id,
    });
    await service.close();
    database.close();
  });

  test('keeps permission grants and revocation barriers scoped to the origin conversation', () => {
    const database = openTestDatabase();
    expect(acceptDetached(
      database,
      'permission-grant',
      '/permission trusted 10m',
      '2026-07-23T02:30:00Z',
    )).toMatchObject({
      status: 'accepted',
      control_id: expect.any(String),
    });
    const accepted = acceptDetached(
      database,
      'permission-work',
      'perform trusted background work',
      '2026-07-23T02:30:01Z',
    );
    const permissionService = createPermissionService({
      database,
      now: () => '2026-07-23T02:30:02Z',
      generateId: deterministicIds('detached-permission'),
    });
    expect(permissionService.authorizeProtectedAction({
      turn_id: accepted.background_execution_turn_id,
      action_ref: 'background-protected-before-safe',
      action_kind: 'filesystem_write',
    })).toMatchObject({
      trusted: true,
      basis_kind: 'timed_conversation',
    });

    expect(acceptDetached(
      database,
      'permission-safe',
      '/permission safe',
      '2026-07-23T02:31:00Z',
    )).toMatchObject({
      status: 'accepted',
      control_id: expect.any(String),
    });
    expect(permissionService.authorizeProtectedAction({
      turn_id: accepted.background_execution_turn_id,
      action_ref: 'background-protected-after-safe',
      action_kind: 'filesystem_write',
    })).toMatchObject({
      trusted: false,
      reason: 'revocation_barrier',
    });
    database.close();
  });

  test('persists side-effect-unknown background failures as recovery barriers', async () => {
    const database = openTestDatabase();
    const accepted = acceptDetached(
      database,
      'unknown',
      'uncertain task',
      '2026-07-23T03:00:00Z',
    );
    const providerError = Object.assign(new Error('provider disconnected'), {
      providerError: {
        code: 'provider_disconnected',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'Provider disconnected after work may have started.',
      },
    });
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {
          throw providerError;
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-detached-unknown',
      now: () => '2026-07-23T03:01:00Z',
      generateId: deterministicIds('executor-detached-unknown'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: accepted.background_execution_turn_id,
    });
    expect(readBackgroundTask(database, accepted.background_task_id)).toMatchObject({
      state: 'recovering',
      side_effect_status: 'unknown',
      execution_turn_state: 'recovering',
    });
    expect(database.prepare(`
      SELECT wait_reason
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(accepted.background_execution_turn_id)).toEqual({
      wait_reason: 'execution_recovery_decision',
    });

    await service.close();
    database.close();
  });

  test('exposes durable task status and cancels the detached execution by task identity', async () => {
    const database = openTestDatabase();
    const accepted = acceptDetached(
      database,
      'cancel',
      'cancel this task',
      '2026-07-23T04:00:00Z',
    );
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'executor-detached-cancel',
      now: () => '2026-07-23T04:01:00Z',
      generateId: deterministicIds('executor-detached-cancel'),
    });

    expect(service.getBackgroundTask(accepted.background_task_id)).toMatchObject({
      background_task_id: accepted.background_task_id,
      origin_conversation_id: accepted.conversation_id,
      dispatch_turn_id: accepted.turn_id,
      execution_turn_id: accepted.background_execution_turn_id,
      state: 'queued',
      side_effect_status: 'none',
      wait_reason: null,
    });
    await expect(service.stopBackgroundTask({
      background_task_id: accepted.background_task_id,
      stop_id: 'stop-background-wrong-target',
      target_turn_id: 'turn-from-another-task',
    })).rejects.toMatchObject({
      code: 'background_task_target_mismatch',
    });
    await expect(service.stopBackgroundTask({
      background_task_id: accepted.background_task_id,
      stop_id: 'stop-background-cancel',
    })).resolves.toMatchObject({
      status: 'queue_cleared',
      background_task_id: accepted.background_task_id,
      cancelled_turn_ids: [accepted.background_execution_turn_id],
    });
    expect(service.getBackgroundTask(accepted.background_task_id)).toMatchObject({
      state: 'cancelled',
      queue_status: 'cancelled',
      side_effect_status: 'none',
    });
    database.prepare(`
      DELETE FROM runtime_turn_queue WHERE turn_id = ?
    `).run(accepted.background_execution_turn_id);
    expect(service.getBackgroundTask(accepted.background_task_id)).toMatchObject({
      state: 'cancelled',
      queue_status: null,
      side_effect_status: 'none',
    });

    await service.close();
    database.close();
  });
});
