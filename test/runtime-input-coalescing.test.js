import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateInboundResult,
} from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createRuntimeUpgradeService } from '../runtime/migration/runtime-upgrade-service.js';
import { createRuntimeSnapshotPublisher } from '../runtime/observability/snapshot-publisher.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { readQueuedTaskStatus } from '../runtime/persistence/queue-status-summary.js';
import { createConversationWorkspaceProvisioner } from '../runtime/workspace/conversation-workspace-provisioner.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-input-coalescing-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function conversationWorkspaceOptions(database, namespace, now) {
  const directory = path.dirname(database.name);
  const workspaceStoreRoot = path.join(directory, `workspaces-${namespace}`);
  const baseSnapshotRoot = path.join(directory, `base-${namespace}`);
  fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
  fs.mkdirSync(baseSnapshotRoot, { mode: 0o700 });
  return {
    workspaceStoreRoot,
    baseSnapshotRoot,
    baseSnapshotRef: `empty:${namespace}`,
    snapshotFiles: [],
    now,
    generateId: deterministicIds(`workspace-${namespace}`),
  };
}

function executionConversationId(database, accepted) {
  return database.prepare(`
    SELECT execution_conversation_id
    FROM runtime_background_tasks
    WHERE background_task_id = ?
  `).get(accepted.background_task_id).execution_conversation_id;
}

function provisionWorkspace(database, accepted, namespace, now) {
  const conversationId = executionConversationId(database, accepted);
  createConversationWorkspaceProvisioner({
    database,
    ...conversationWorkspaceOptions(database, namespace, now),
  }).ensure(conversationId);
  return conversationId;
}

function workspaceAccess(store, conversationId) {
  const binding = store.resolveConversationWorkspaceBinding(conversationId);
  return Object.freeze({
    binding_kind: binding.binding_kind,
    claimable: true,
    mode: 'writable',
    read_only_enforced: false,
    workspace_generation: binding.workspace_generation,
    workspace_id: binding.workspace_id,
    workspace_root: binding.workspace_root,
    workspace_state: binding.workspace_state,
  });
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function envelope(suffix, text, {
  actorId = 'user-A',
  chatId,
  nativeThreadId,
  replyToMessageId,
} = {}) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const value = structuredClone(fixture);
  value.inbound_event_id = `evt-coalescing-${suffix}`;
  value.trace_id = `trace-coalescing-${suffix}`;
  value.message_id = `message-coalescing-${suffix}`;
  value.actor.actor_id = actorId;
  value.content = { kind: 'text', text, attachments: [] };
  value.content.task_summary = text;
  if (chatId !== undefined) value.chat_id = chatId;
  if (nativeThreadId !== undefined) {
    value.chat_type = 'thread';
    value.native_thread_or_topic_id = nativeThreadId;
    value.reply.root_message_id = `root-${nativeThreadId}`;
    value.reply.parent_message_id = `root-${nativeThreadId}`;
  }
  if (replyToMessageId !== undefined) {
    value.reply.reply_to_message_id = replyToMessageId;
    value.reply.root_message_id = `root-${replyToMessageId}`;
    value.reply.parent_message_id = `parent-${replyToMessageId}`;
  }
  value.idempotency_key = createIdempotencyKey('inbound', {
    region: value.region,
    tenant_id: value.tenant_id,
    channel: value.channel,
    bot_id: value.bot_id,
    inbound_event_id: value.inbound_event_id,
  });
  return value;
}

function acceptAt(database, suffix, text, timestamp, options = {}) {
  return acceptNormalInbound(database, envelope(suffix, text, options.envelope), {
    now: () => timestamp,
    generateId: deterministicIds(`coalescing-${suffix}`),
    ...(options.policy === undefined
      ? {}
      : { inputCoalescingPolicy: options.policy }),
  });
}

function enterMaintenance(database, suffix) {
  const upgrade = createRuntimeUpgradeService({
    database,
    now: () => '2026-07-25T00:10:00.000Z',
    generateId: deterministicIds(`upgrade-${suffix}`),
  });
  const upgradeId = `upgrade-${suffix}`;
  upgrade.preflight({
    upgrade_id: upgradeId,
    from_release: '0.6.0',
    to_release: '0.7.0',
    scope: { kind: 'installation', bot_id: null },
    checks: {
      sqlite_integrity: 'ok',
      codex_transport: 'official_app_server_only',
      delivery_contract: 'zylos.delivery-command@1.1',
      workspace_lease_fencing: 'intact',
      retention_cleanup: 'intact',
      normal_runtime_paths: 'new_only',
    },
  });
  upgrade.recordSnapshot(upgradeId, {
    package_release_ref: `release-${suffix}`,
    database_snapshot_ref: `sqlite-${suffix}`,
    snapshot_sha256: 'a'.repeat(64),
  });
  upgrade.enterMaintenance(upgradeId);
  return upgradeId;
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('Feishu trailing-edge input coalescing', () => {
  test('t=0 and t=9 append to one durable task and move eligibility to t=19', () => {
    const database = openTestDatabase();
    const first = acceptAt(
      database,
      'trailing-first',
      'Prepare the proposal',
      '2026-07-25T00:00:00.000Z',
    );
    const supplement = acceptAt(
      database,
      'trailing-supplement',
      'Include the updated pricing',
      '2026-07-25T00:00:09.000Z',
    );

    expect(validateInboundResult(first).forwarded).toEqual(first);
    expect(validateInboundResult(supplement).forwarded).toEqual(supplement);
    expect(first).toMatchObject({
      input_group_action: 'opened',
      input_group_member_count: 1,
      input_group_supplement_count: 0,
      input_group_collect_until: '2026-07-25T00:00:10.000Z',
    });
    expect(supplement).toMatchObject({
      input_group_id: first.input_group_id,
      input_group_action: 'appended',
      input_group_member_count: 2,
      input_group_supplement_count: 1,
      input_group_collect_until: '2026-07-25T00:00:19.000Z',
      background_task_id: first.background_task_id,
      background_execution_turn_id: first.background_execution_turn_id,
    });
    expect(supplement.turn_id).not.toBe(first.turn_id);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_background_tasks
    `).get()).toEqual({ count: 1 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_input_group_members
      WHERE input_group_id = ?
    `).get(first.input_group_id)).toEqual({ count: 2 });
    expect(database.prepare(`
      SELECT status, wait_reason, available_at
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(first.background_execution_turn_id)).toEqual({
      status: 'queued',
      wait_reason: 'input_settling',
      available_at: '2026-07-25T00:00:19.000Z',
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_inbound_events
      WHERE conversation_id = ?
    `).get(first.conversation_id)).toEqual({ count: 2 });
    database.close();
  });

  test('an idempotent replay preserves the first group result without extending its deadline', () => {
    const database = openTestDatabase();
    const originalEnvelope = envelope('duplicate', 'Only count me once');
    const first = acceptNormalInbound(database, originalEnvelope, {
      now: () => '2026-07-25T00:01:00.000Z',
      generateId: deterministicIds('coalescing-duplicate-first'),
    });
    const duplicate = structuredClone(originalEnvelope);
    duplicate.trace_id = 'trace-coalescing-duplicate-replay';
    const replayed = acceptNormalInbound(database, duplicate, {
      now: () => '2026-07-25T00:01:09.000Z',
      generateId: deterministicIds('coalescing-duplicate-replay'),
    });

    expect(replayed).toEqual({
      ...first,
      trace_id: duplicate.trace_id,
      deduplicated: true,
    });
    expect(database.prepare(`
      SELECT member_count, collect_until
      FROM runtime_input_groups
      WHERE input_group_id = ?
    `).get(first.input_group_id)).toEqual({
      member_count: 1,
      collect_until: '2026-07-25T00:01:10.000Z',
    });
    database.close();
  });

  test('does not claim before t=10 and seals one message into provider input at t=10', async () => {
    const database = openTestDatabase();
    const accepted = acceptAt(
      database,
      'single-deadline',
      'Run only after the quiet window',
      '2026-07-25T00:02:00.000Z',
    );
    let clock = '2026-07-25T00:02:09.999Z';
    const providerInputs = [];
    const service = createExecutorService({
      database,
      provider: 'codex',
      serviceInstanceId: 'executor-coalescing-single',
      now: () => clock,
      generateId: deterministicIds('executor-coalescing-single'),
      conversationWorkspaceOptions: conversationWorkspaceOptions(
        database,
        'coalescing-single',
        () => clock,
      ),
      adapter: {
        async *execute(context) {
          providerInputs.push(context.input);
          context.reportProviderState({ state: 'started', provider_native_id: null });
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
    });

    expect(service.getBackgroundTask(accepted.background_task_id)).toMatchObject({
      state: 'input_settling',
      input_group_id: accepted.input_group_id,
      input_group_member_count: 1,
      input_group_supplement_count: 0,
      input_group_collect_until: '2026-07-25T00:02:10.000Z',
    });
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });

    clock = '2026-07-25T00:02:10.000Z';
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.background_execution_turn_id,
    });
    expect(providerInputs).toEqual([expect.objectContaining({
      kind: 'text',
      input_group_id: accepted.input_group_id,
      messages: [{
        message_id: 'message-coalescing-single-deadline',
        actor: expect.objectContaining({ actor_id: 'user-A', authenticated: true }),
        occurred_at: expect.any(String),
        content: expect.objectContaining({ text: 'Run only after the quiet window' }),
        reply: {
          root_message_id: null,
          parent_message_id: null,
          reply_to_message_id: null,
        },
      }],
    })]);
    expect(database.prepare(`
      SELECT state, sealed_at
      FROM runtime_input_groups
      WHERE input_group_id = ?
    `).get(accepted.input_group_id)).toEqual({
      state: 'sealed',
      sealed_at: '2026-07-25T00:02:10.000Z',
    });
    await service.close();
    database.close();
  });

  test('t=0 and t=10 open two groups while distinct actor, thread, and reply intents never merge', () => {
    const database = openTestDatabase();
    const boundaryFirst = acceptAt(
      database,
      'boundary-first',
      'First boundary message',
      '2026-07-25T00:03:00.000Z',
    );
    const boundarySecond = acceptAt(
      database,
      'boundary-second',
      'Second boundary message',
      '2026-07-25T00:03:10.000Z',
    );
    expect(boundarySecond.input_group_id).not.toBe(boundaryFirst.input_group_id);

    const actorFirst = acceptAt(
      database,
      'actor-first',
      'Actor A',
      '2026-07-25T00:04:00.000Z',
      { envelope: { chatId: 'chat-actor-isolation', actorId: 'actor-A' } },
    );
    const actorSecond = acceptAt(
      database,
      'actor-second',
      'Actor B',
      '2026-07-25T00:04:01.000Z',
      { envelope: { chatId: 'chat-actor-isolation', actorId: 'actor-B' } },
    );
    expect(actorSecond.input_group_id).not.toBe(actorFirst.input_group_id);

    const mainConversation = acceptAt(
      database,
      'main-conversation',
      'Main conversation',
      '2026-07-25T00:05:00.000Z',
      { envelope: { chatId: 'chat-thread-isolation' } },
    );
    const nativeThread = acceptAt(
      database,
      'native-thread',
      'Native thread',
      '2026-07-25T00:05:01.000Z',
      {
        envelope: {
          chatId: 'chat-thread-isolation',
          nativeThreadId: 'native-thread-A',
        },
      },
    );
    expect(nativeThread.conversation_id).not.toBe(mainConversation.conversation_id);
    expect(nativeThread.input_group_id).not.toBe(mainConversation.input_group_id);

    const replyOne = acceptAt(
      database,
      'reply-one',
      'Reply intent one',
      '2026-07-25T00:06:00.000Z',
      {
        envelope: {
          chatId: 'chat-reply-isolation',
          replyToMessageId: 'model-message-one',
        },
      },
    );
    const replyTwo = acceptAt(
      database,
      'reply-two',
      'Reply intent two',
      '2026-07-25T00:06:01.000Z',
      {
        envelope: {
          chatId: 'chat-reply-isolation',
          replyToMessageId: 'model-message-two',
        },
      },
    );
    expect(replyTwo.input_group_id).not.toBe(replyOne.input_group_id);
    const ambiguousNoReply = acceptAt(
      database,
      'reply-ambiguous-no-reply',
      'Do not guess between two reply groups',
      '2026-07-25T00:06:02.000Z',
      { envelope: { chatId: 'chat-reply-isolation' } },
    );
    expect(ambiguousNoReply.input_group_id).not.toBe(replyOne.input_group_id);
    expect(ambiguousNoReply.input_group_id).not.toBe(replyTwo.input_group_id);
    database.close();
  });

  test('a no-reply supplement joins the actor’s unique collecting reply group', () => {
    const database = openTestDatabase();
    const reply = acceptAt(
      database,
      'unique-reply',
      'Start from this reply',
      '2026-07-25T00:07:00.000Z',
      {
        envelope: {
          chatId: 'chat-unique-reply',
          replyToMessageId: 'model-message-unique',
        },
      },
    );
    const supplement = acceptAt(
      database,
      'unique-no-reply-supplement',
      'One more detail',
      '2026-07-25T00:07:04.000Z',
      { envelope: { chatId: 'chat-unique-reply' } },
    );

    expect(supplement).toMatchObject({
      input_group_id: reply.input_group_id,
      input_group_action: 'appended',
      background_task_id: reply.background_task_id,
    });
    database.close();
  });

  test('member and open-window safety caps force deterministic sealing boundaries', () => {
    const memberCapDatabase = openTestDatabase();
    const memberPolicy = {
      quietWindowMs: 10_000,
      maxOpenWindowMs: 120_000,
      maxMembers: 2,
    };
    const first = acceptAt(
      memberCapDatabase,
      'member-cap-first',
      'First member',
      '2026-07-25T00:08:00.000Z',
      { policy: memberPolicy },
    );
    const second = acceptAt(
      memberCapDatabase,
      'member-cap-second',
      'Second member reaches cap',
      '2026-07-25T00:08:09.000Z',
      { policy: memberPolicy },
    );
    const overflow = acceptAt(
      memberCapDatabase,
      'member-cap-overflow',
      'Overflow starts another group',
      '2026-07-25T00:08:09.000Z',
      { policy: memberPolicy },
    );
    expect(second).toMatchObject({
      input_group_id: first.input_group_id,
      input_group_member_count: 2,
      input_group_collect_until: '2026-07-25T00:08:09.000Z',
    });
    expect(overflow.input_group_id).not.toBe(first.input_group_id);
    memberCapDatabase.close();

    const windowCapDatabase = openTestDatabase();
    const windowPolicy = {
      quietWindowMs: 10_000,
      maxOpenWindowMs: 15_000,
      maxMembers: 20,
    };
    const opened = acceptAt(
      windowCapDatabase,
      'window-cap-first',
      'Open capped group',
      '2026-07-25T00:09:00.000Z',
      { policy: windowPolicy },
    );
    acceptAt(
      windowCapDatabase,
      'window-cap-second',
      'Move toward cap',
      '2026-07-25T00:09:09.000Z',
      { policy: windowPolicy },
    );
    const capped = acceptAt(
      windowCapDatabase,
      'window-cap-third',
      'Remain capped',
      '2026-07-25T00:09:14.000Z',
      { policy: windowPolicy },
    );
    expect(capped).toMatchObject({
      input_group_id: opened.input_group_id,
      input_group_member_count: 3,
      input_group_collect_until: '2026-07-25T00:09:15.000Z',
    });
    windowCapDatabase.close();

    const initialCapDatabase = openTestDatabase();
    const initiallyCapped = acceptAt(
      initialCapDatabase,
      'initial-window-cap',
      'The maximum window also caps the initial deadline',
      '2026-07-25T00:09:20.000Z',
      {
        policy: {
          quietWindowMs: 20_000,
          maxOpenWindowMs: 5_000,
          maxMembers: 20,
        },
      },
    );
    expect(initiallyCapped.input_group_collect_until).toBe(
      '2026-07-25T00:09:25.000Z',
    );
    initialCapDatabase.close();
  });

  test('stop cancels a collecting task before provider start and steer text is never appended', async () => {
    const database = openTestDatabase();
    const accepted = acceptAt(
      database,
      'stop-before-start',
      'Do not start this',
      '2026-07-25T00:11:00.000Z',
    );
    let clock = '2026-07-25T00:11:05.000Z';
    const service = createExecutorService({
      database,
      provider: 'codex',
      serviceInstanceId: 'executor-coalescing-stop',
      now: () => clock,
      generateId: deterministicIds('executor-coalescing-stop'),
      conversationWorkspaceOptions: conversationWorkspaceOptions(
        database,
        'coalescing-stop',
        () => clock,
      ),
      adapter: {
        async *execute() {
          throw new Error('stopped input group must not reach the provider');
        },
      },
    });
    await expect(service.stopBackgroundTask({
      background_task_id: accepted.background_task_id,
      stop_id: 'stop-coalescing-before-start',
    })).resolves.toMatchObject({
      status: 'queue_cleared',
      cancelled_turn_ids: [accepted.background_execution_turn_id],
    });
    expect(database.prepare(`
      SELECT state FROM runtime_input_groups WHERE input_group_id = ?
    `).get(accepted.input_group_id)).toEqual({ state: 'cancelled' });
    clock = '2026-07-25T00:11:11.000Z';
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });

    const collecting = acceptAt(
      database,
      'steer-collecting',
      'Ordinary work',
      '2026-07-25T00:12:00.000Z',
    );
    const steer = acceptAt(
      database,
      'steer-control',
      '/steer change direction',
      '2026-07-25T00:12:01.000Z',
    );
    expect(steer.input_group_id).toBeUndefined();
    expect(steer.background_task_id).not.toBe(collecting.background_task_id);
    expect(database.prepare(`
      SELECT member_count FROM runtime_input_groups WHERE input_group_id = ?
    `).get(collecting.input_group_id)).toEqual({ member_count: 1 });
    await service.close();
    database.close();
  });

  test('maintenance keeps an overdue group durable and unclaimable until the fence clears', async () => {
    const database = openTestDatabase();
    const upgradeId = enterMaintenance(database, 'coalescing-maintenance');
    const accepted = acceptAt(
      database,
      'maintenance',
      'Settle while maintenance is active',
      '2026-07-25T00:10:01.000Z',
    );
    let clock = '2026-07-25T00:10:12.000Z';
    const calls = [];
    const service = createExecutorService({
      database,
      provider: 'codex',
      serviceInstanceId: 'executor-coalescing-maintenance',
      now: () => clock,
      generateId: deterministicIds('executor-coalescing-maintenance'),
      conversationWorkspaceOptions: conversationWorkspaceOptions(
        database,
        'coalescing-maintenance',
        () => clock,
      ),
      adapter: {
        async *execute(context) {
          calls.push(context.turn_id);
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
    });

    expect(database.prepare(`
      SELECT status, wait_reason, available_at
      FROM runtime_turn_queue WHERE turn_id = ?
    `).get(accepted.background_execution_turn_id)).toEqual({
      status: 'queued',
      wait_reason: 'maintenance',
      available_at: '2026-07-25T00:10:11.000Z',
    });
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    expect(calls).toEqual([]);

    database.prepare(`
      UPDATE runtime_upgrade_runs
      SET state = 'rolled_back', state_version = state_version + 1, updated_at = ?
      WHERE upgrade_id = ? AND state = 'maintenance'
    `).run(clock, upgradeId);
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.background_execution_turn_id,
    });
    expect(calls).toEqual([accepted.background_execution_turn_id]);
    await service.close();
    database.close();
  });

  test('restart executes one overdue group exactly once with deterministic member order', async () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const first = acceptAt(
      database,
      'restart-first',
      'First after restart',
      '2026-07-25T00:13:00.000Z',
    );
    acceptAt(
      database,
      'restart-second',
      'Second after restart',
      '2026-07-25T00:13:05.000Z',
    );
    database.close();

    const reopened = new Database(databasePath);
    const providerInputs = [];
    const service = createExecutorService({
      database: reopened,
      provider: 'codex',
      serviceInstanceId: 'executor-coalescing-restart',
      now: () => '2026-07-25T00:13:16.000Z',
      generateId: deterministicIds('executor-coalescing-restart'),
      conversationWorkspaceOptions: conversationWorkspaceOptions(
        reopened,
        'coalescing-restart',
        () => '2026-07-25T00:13:16.000Z',
      ),
      adapter: {
        async *execute(context) {
          providerInputs.push(context.input);
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.background_execution_turn_id,
    });
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    expect(providerInputs).toHaveLength(1);
    expect(providerInputs[0].messages.map(({ message_id: messageId }) => messageId)).toEqual([
      'message-coalescing-restart-first',
      'message-coalescing-restart-second',
    ]);
    await service.close();
    reopened.close();
  });

  test('one explicit card is created and supplements update that same target with exact copy', async () => {
    const database = openTestDatabase();
    let clock = '2026-07-25T00:14:00.000Z';
    const first = acceptAt(
      database,
      'card-first',
      'Initial card input',
      clock,
    );
    const delivered = [];
    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-coalescing-card',
      now: () => clock,
      generateId: deterministicIds('delivery-coalescing-card'),
      throttleMs: 0,
      renderer: {
        async deliver(command) {
          delivered.push(command);
          return deliveredResult(command, clock);
        },
      },
    });

    clock = '2026-07-25T00:14:05.000Z';
    const supplement = acceptAt(
      database,
      'card-supplement',
      'Supplement card input',
      clock,
    );
    expect(supplement.input_group_id).toBe(first.input_group_id);

    await expect(outbox.dispatchNext()).resolves.toMatchObject({ status: 'applied' });
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toMatchObject({
      operation: 'create_main',
      render_model: { text: '已收到' },
    });

    await expect(outbox.dispatchNext()).resolves.toMatchObject({ status: 'applied' });
    expect(delivered).toHaveLength(2);
    expect(delivered[1]).toMatchObject({
      operation: 'update_main',
      target: delivered[0].target,
      target_platform_message_id: expect.any(String),
      render_model: { text: '已收到 1 条补充' },
    });
    expect(delivered.filter(({ operation }) => operation === 'create_main')).toHaveLength(1);
    database.close();
  });

  test('append and claim transactions expose only the two legal linearized outcomes', () => {
    const appendFirstDatabase = openTestDatabase();
    const appendFirstPath = appendFirstDatabase.name;
    const appendFirst = acceptAt(
      appendFirstDatabase,
      'race-append-first-open',
      'Open before append wins',
      '2026-07-25T00:15:00.000Z',
    );
    const appendWriter = new Database(appendFirstPath);
    const appended = acceptAt(
      appendWriter,
      'race-append-first-supplement',
      'Append wins the transaction',
      '2026-07-25T00:15:09.000Z',
    );
    expect(appended.input_group_id).toBe(appendFirst.input_group_id);
    let appendClock = '2026-07-25T00:15:10.000Z';
    const appendStore = createExecutorStore({
      database: appendFirstDatabase,
      provider: 'codex',
      serviceInstanceId: 'executor-race-append-first',
      now: () => appendClock,
      generateId: deterministicIds('executor-race-append-first'),
    });
    const appendConversationId = provisionWorkspace(
      appendFirstDatabase,
      appendFirst,
      'race-append-first',
      () => appendClock,
    );
    const appendWorkspaceAccess = workspaceAccess(appendStore, appendConversationId);
    expect(appendStore.claimNextQueuedTurn({
      conversationId: appendConversationId,
      workspaceAccess: appendWorkspaceAccess,
    })).toBeNull();
    appendClock = '2026-07-25T00:15:19.000Z';
    expect(appendStore.claimNextQueuedTurn({
      conversationId: appendConversationId,
      workspaceAccess: appendWorkspaceAccess,
    })).toMatchObject({
      turn_id: appendFirst.background_execution_turn_id,
      input: {
        input_group_id: appendFirst.input_group_id,
        messages: [{ message_id: expect.any(String) }, { message_id: expect.any(String) }],
      },
    });
    appendWriter.close();
    appendFirstDatabase.close();

    const claimFirstDatabase = openTestDatabase();
    const claimFirstPath = claimFirstDatabase.name;
    const claimFirst = acceptAt(
      claimFirstDatabase,
      'race-claim-first-open',
      'Open before claim wins',
      '2026-07-25T00:16:00.000Z',
    );
    const claimStore = createExecutorStore({
      database: claimFirstDatabase,
      provider: 'codex',
      serviceInstanceId: 'executor-race-claim-first',
      now: () => '2026-07-25T00:16:10.000Z',
      generateId: deterministicIds('executor-race-claim-first'),
    });
    const claimConversationId = provisionWorkspace(
      claimFirstDatabase,
      claimFirst,
      'race-claim-first',
      () => '2026-07-25T00:16:10.000Z',
    );
    expect(claimStore.claimNextQueuedTurn({
      conversationId: claimConversationId,
      workspaceAccess: workspaceAccess(claimStore, claimConversationId),
    })).toMatchObject({
      turn_id: claimFirst.background_execution_turn_id,
    });
    const claimWriter = new Database(claimFirstPath);
    const afterClaim = acceptAt(
      claimWriter,
      'race-claim-first-supplement',
      'Claim sealed before this input',
      '2026-07-25T00:16:10.000Z',
    );
    expect(afterClaim.input_group_id).not.toBe(claimFirst.input_group_id);
    expect(claimFirstDatabase.prepare(`
      SELECT state FROM runtime_input_groups WHERE input_group_id = ?
    `).get(claimFirst.input_group_id)).toEqual({ state: 'sealed' });
    claimWriter.close();
    claimFirstDatabase.close();
  });

  test('queue status and observability expose the durable input-settling projection', () => {
    const database = openTestDatabase();
    const first = acceptAt(
      database,
      'observability-first',
      'Observe the first message',
      '2026-07-25T00:17:00.000Z',
    );
    acceptAt(
      database,
      'observability-second',
      'Observe the supplement',
      '2026-07-25T00:17:05.000Z',
    );

    expect(readQueuedTaskStatus(
      database,
      first.background_execution_turn_id,
      '2026-07-25T00:17:06.000Z',
    )).toMatchObject({
      input_group_id: first.input_group_id,
      input_group_state: 'input_settling',
      input_group_member_count: 2,
      input_group_supplement_count: 1,
      input_group_collect_until: '2026-07-25T00:17:15.000Z',
    });

    const publisher = createRuntimeSnapshotPublisher({
      database,
      serviceInstanceId: 'executor-observability-input-settling',
      hostId: 'host-observability-input-settling',
      startedAt: '2026-07-25T00:17:00.000Z',
      now: () => '2026-07-25T00:17:06.000Z',
      generateId: deterministicIds('observability-input-settling'),
    });
    const snapshot = publisher.publish();
    expect(snapshot.turns.items).toContainEqual(expect.objectContaining({
      turn_id: first.background_execution_turn_id,
      state: 'queued',
      phase: 'queued',
      input_group_id: first.input_group_id,
      input_group_state: 'input_settling',
      input_group_member_count: 2,
      input_group_supplement_count: 1,
      input_group_collect_until: '2026-07-25T00:17:15.000Z',
    }));
    database.close();
  });

  test('the default policy coalesces Feishu only', () => {
    const database = openTestDatabase();
    const firstEnvelope = envelope('lark-first', 'Lark first');
    firstEnvelope.channel = 'lark';
    firstEnvelope.idempotency_key = createIdempotencyKey('inbound', {
      region: firstEnvelope.region,
      tenant_id: firstEnvelope.tenant_id,
      channel: firstEnvelope.channel,
      bot_id: firstEnvelope.bot_id,
      inbound_event_id: firstEnvelope.inbound_event_id,
    });
    const secondEnvelope = envelope('lark-second', 'Lark second');
    secondEnvelope.channel = 'lark';
    secondEnvelope.idempotency_key = createIdempotencyKey('inbound', {
      region: secondEnvelope.region,
      tenant_id: secondEnvelope.tenant_id,
      channel: secondEnvelope.channel,
      bot_id: secondEnvelope.bot_id,
      inbound_event_id: secondEnvelope.inbound_event_id,
    });
    const first = acceptNormalInbound(database, firstEnvelope, {
      now: () => '2026-07-25T00:18:00.000Z',
      generateId: deterministicIds('lark-first'),
    });
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-25T00:18:01.000Z',
      generateId: deterministicIds('lark-second'),
    });

    expect(first.input_group_id).toBeUndefined();
    expect(second.input_group_id).toBeUndefined();
    expect(second.background_task_id).not.toBe(first.background_task_id);
    database.close();
  });
});
