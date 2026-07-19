import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PassThrough } from 'node:stream';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateObservabilitySnapshot,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { createCodexAppServerAdapter } from '../runtime/providers/codex-app-server-adapter.js';
import {
  createWorkspaceLeaseCoordinator,
  normalizeWorkspaceRoot,
  resolveProviderWorkspaceAccess,
} from '../runtime/workspace/lease-coordinator.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const observabilityFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function createFixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-workspace-lease-'));
  temporaryDirectories.push(directory);
  const database = new Database(path.join(directory, 'c4.db'));
  const workspacePath = path.join(directory, 'workspace');
  const alias = path.join(directory, 'workspace-alias');
  fs.mkdirSync(workspacePath);
  const workspace = fs.realpathSync.native(workspacePath);
  fs.symlinkSync(workspacePath, alias, 'dir');
  return { alias, database, directory, workspace };
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => { resolve = resolvePromise; });
  return { promise, resolve };
}

function createDeferredThreadStartAppServer() {
  const child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  const threadStart = deferred();
  const killSignals = [];
  let buffer = '';

  function send(message) {
    child.stdout.write(`${JSON.stringify(message)}\n`);
  }

  child.kill = (signal) => {
    killSignals.push(signal);
    queueMicrotask(() => child.emit('close', 0, signal));
    return true;
  };
  child.stdin.on('data', (chunk) => {
    buffer += chunk.toString('utf8');
    while (buffer.includes('\n')) {
      const newline = buffer.indexOf('\n');
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.length === 0) continue;
      const message = JSON.parse(line);
      if (message.method === 'initialize') {
        send({ id: message.id, result: { userAgent: 'workspace-fence-test' } });
      } else if (message.method === 'thread/start') {
        threadStart.resolve(message);
      }
    }
  });

  return { child, killSignals, send, threadStart: threadStart.promise };
}

function deliverTurnNotifications(database, turnId, suffix, deliveredAt) {
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `workspace-delivery-${suffix}`,
    now: () => deliveredAt,
    generateId: deterministicIds(`workspace-delivery-${suffix}`),
    throttleMs: 0,
  });
  const delivered = [];
  while (true) {
    const command = outbox.claimNext();
    if (!command) break;
    outbox.recordResult(deliveredResult(command, deliveredAt));
    if (command.mapping?.turn_id === turnId) delivered.push(command);
  }
  return delivered;
}

function acceptQueuedTurn(database, suffix, chatId) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = JSON.parse(JSON.stringify(fixture));
  envelope.inbound_event_id = `evt-workspace-${suffix}`;
  envelope.trace_id = `trace-workspace-${suffix}`;
  envelope.message_id = `message-workspace-${suffix}`;
  envelope.chat_id = chatId;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return acceptNormalInbound(database, envelope, {
    now: () => '2026-07-19T13:00:00Z',
    generateId: deterministicIds(`workspace-inbound-${suffix}`),
  });
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('durable workspace lease coordinator', () => {
  test('uses the production Codex sandbox declaration for trusted workspace access', () => {
    const readOnlyAdapter = createCodexAppServerAdapter({
      spawnProcess() {},
      cwd: '/workspace/review',
      sandbox: 'read-only',
    });
    const writableAdapter = createCodexAppServerAdapter({
      spawnProcess() {},
      cwd: '/workspace',
      sandbox: 'workspace-write',
    });

    expect(resolveProviderWorkspaceAccess(readOnlyAdapter, {}, {
      defaultRoot: '/fallback',
    })).toEqual({
      workspace_root: '/workspace/review',
      mode: 'read_only',
      read_only_enforced: true,
    });
    expect(resolveProviderWorkspaceAccess(writableAdapter, {}, {
      defaultRoot: '/fallback',
    })).toEqual({
      workspace_root: '/workspace',
      mode: 'writable',
      read_only_enforced: false,
    });
  });

  test('moves a production Codex pre-turn fence race into durable unknown recovery', async () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'codex-pre-turn-race', 'chat-codex-pre-turn-race');
    const server = createDeferredThreadStartAppServer();
    let clock = '2026-07-19T13:19:00.000Z';
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: workspace,
      sandbox: 'workspace-write',
    });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'workspace-codex-pre-turn-race',
      now: () => clock,
      generateId: deterministicIds('workspace-codex-pre-turn-race'),
      workspaceRoot: workspace,
      workspaceLeaseDurationMs: 1_000,
      workspaceHeartbeatIntervalMs: 300,
      scheduleWorkspaceHeartbeat() { return { unref() {} }; },
      cancelWorkspaceHeartbeat() {},
    });

    const execution = service.runNext();
    const threadStart = await server.threadStart;
    clock = '2026-07-19T13:19:02.000Z';
    server.send({
      id: threadStart.id,
      result: { thread: { id: 'codex-thread-pre-turn-race' } },
    });

    await expect(execution).resolves.toMatchObject({
      status: 'recovering',
      turn_id: turn.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'uncertain' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
    `).get(turn.turn_id).count).toBeGreaterThan(0);
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [],
      notification_pending: [turn.turn_id],
      isolation_pending: [],
    });
    expect(deliverTurnNotifications(
      database,
      turn.turn_id,
      'codex-pre-turn-race',
      clock,
    )).not.toHaveLength(0);
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toMatchObject({
      isolated: [turn.turn_id],
      notification_pending: [],
      isolation_pending: [],
    });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'released' });
    expect(server.killSignals).toContain('SIGTERM');

    await service.close();
    database.close();
  });

  test('normalizes roots and fences an expired holder from write, renew, or release', () => {
    const { alias, database, workspace } = createFixture();
    const firstTurn = acceptQueuedTurn(database, 'first', 'chat-workspace-first');
    const secondTurn = acceptQueuedTurn(database, 'second', 'chat-workspace-second');
    let clock = '2026-07-19T13:00:01.000Z';
    const first = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-old',
      now: () => clock,
      generateId: deterministicIds('workspace-old'),
      leaseDurationMs: 10_000,
    });

    const oldLease = first.acquire({
      workspace_root: path.join(alias, '.'),
      mode: 'writable',
      holder_conversation_id: firstTurn.conversation_id,
      holder_turn_id: firstTurn.turn_id,
    });

    expect(normalizeWorkspaceRoot(path.join(alias, '.'))).toBe(workspace);
    expect(oldLease).toMatchObject({
      status: 'acquired',
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: firstTurn.conversation_id,
      holder_turn_id: firstTurn.turn_id,
      lease_epoch: 1,
      lease_expires_at: '2026-07-19T13:00:11.000Z',
    });

    clock = '2026-07-19T13:00:12.000Z';
    const second = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-current',
      now: () => clock,
      generateId: deterministicIds('workspace-current'),
      leaseDurationMs: 10_000,
    });
    const currentLease = second.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: secondTurn.conversation_id,
      holder_turn_id: secondTurn.turn_id,
    });

    expect(currentLease).toMatchObject({
      status: 'acquired',
      workspace_root: workspace,
      lease_epoch: 2,
      lease_expires_at: '2026-07-19T13:00:22.000Z',
    });
    for (const operation of [
      () => first.assertWritable(oldLease),
      () => first.renew(oldLease),
      () => first.release(oldLease),
    ]) {
      expect(operation).toThrow(expect.objectContaining({ code: 'stale_workspace_lease' }));
    }
    expect(second.assertWritable(currentLease)).toMatchObject({ lease_epoch: 2 });
    expect(second.listActive()).toEqual([
      expect.objectContaining({
        workspace_root: workspace,
        holder_turn_id: secondTurn.turn_id,
        lease_epoch: 2,
      }),
    ]);

    database.close();
  });

  test('renews owned leases and blocks blind takeover when an active writer expires', () => {
    const { database, workspace } = createFixture();
    const firstTurn = acceptQueuedTurn(database, 'heartbeat-first', 'chat-heartbeat-first');
    const secondTurn = acceptQueuedTurn(database, 'heartbeat-second', 'chat-heartbeat-second');
    let clock = '2026-07-19T13:05:00.000Z';
    const first = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-heartbeat-old',
      now: () => clock,
      generateId: deterministicIds('workspace-heartbeat-old'),
      leaseDurationMs: 10_000,
    });
    const lease = first.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: firstTurn.conversation_id,
      holder_turn_id: firstTurn.turn_id,
    });

    clock = '2026-07-19T13:05:08.000Z';
    expect(first.heartbeatOwned()).toBe(1);
    expect(first.listActive()).toEqual([
      expect.objectContaining({ lease_expires_at: '2026-07-19T13:05:18.000Z' }),
    ]);

    database.prepare(`
      UPDATE runtime_turns SET state = 'running' WHERE turn_id = ?
    `).run(firstTurn.turn_id);
    clock = '2026-07-19T13:05:19.000Z';
    const second = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-heartbeat-current',
      now: () => clock,
      generateId: deterministicIds('workspace-heartbeat-current'),
      leaseDurationMs: 10_000,
    });
    expect(second.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: secondTurn.conversation_id,
      holder_turn_id: secondTurn.turn_id,
    })).toMatchObject({
      status: 'wait',
      wait_reason: 'workspace_lease',
      holder_turn_id: firstTurn.turn_id,
      recovery_required: true,
    });
    expect(() => first.renew(lease)).toThrow(expect.objectContaining({
      code: 'stale_workspace_lease',
    }));
    expect(first.releaseAfterIsolation(lease, {
      reason: 'provider_process_exit_confirmed',
    })).toMatchObject({ status: 'released_after_isolation' });
    expect(second.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: secondTurn.conversation_id,
      holder_turn_id: secondTurn.turn_id,
    })).toMatchObject({ status: 'acquired', lease_epoch: 2 });

    database.close();
  });

  test('rebuilds an expired active-writer fence from SQLite after service restart', () => {
    const fixture = createFixture();
    const firstTurn = acceptQueuedTurn(
      fixture.database,
      'restart-first',
      'chat-workspace-restart-first',
    );
    const secondTurn = acceptQueuedTurn(
      fixture.database,
      'restart-second',
      'chat-workspace-restart-second',
    );
    const databasePath = fixture.database.name;
    const old = createWorkspaceLeaseCoordinator({
      database: fixture.database,
      serviceInstanceId: 'workspace-service-before-restart',
      now: () => '2026-07-19T13:07:00.000Z',
      generateId: deterministicIds('workspace-before-restart'),
      leaseDurationMs: 10_000,
    });
    old.acquire({
      workspace_root: fixture.workspace,
      mode: 'writable',
      holder_conversation_id: firstTurn.conversation_id,
      holder_turn_id: firstTurn.turn_id,
    });
    fixture.database.prepare(`
      UPDATE runtime_turns SET state = 'running' WHERE turn_id = ?
    `).run(firstTurn.turn_id);
    fixture.database.close();

    const restartedDatabase = new Database(databasePath);
    const restarted = createWorkspaceLeaseCoordinator({
      database: restartedDatabase,
      serviceInstanceId: 'workspace-service-after-restart',
      now: () => '2026-07-19T13:07:11.000Z',
      generateId: deterministicIds('workspace-after-restart'),
      leaseDurationMs: 10_000,
    });
    expect(restarted.acquire({
      workspace_root: fixture.workspace,
      mode: 'writable',
      holder_conversation_id: secondTurn.conversation_id,
      holder_turn_id: secondTurn.turn_id,
    })).toMatchObject({
      status: 'wait',
      holder_turn_id: firstTurn.turn_id,
      recovery_required: true,
    });
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(firstTurn.turn_id)).toEqual({ state: 'uncertain' });

    restartedDatabase.close();
  });

  test('adopts an expired Claude resident into durable recovery after service restart', async () => {
    const fixture = createFixture();
    const databasePath = fixture.database.name;
    const turn = acceptQueuedTurn(
      fixture.database,
      'restart-recovery',
      'chat-restart-recovery',
    );
    const oldStore = createExecutorStore({
      database: fixture.database,
      provider: 'claude',
      serviceInstanceId: 'workspace-service-restart-old',
      now: () => '2026-07-19T13:07:30.000Z',
      generateId: deterministicIds('workspace-restart-old'),
      leaseDurationMs: 1_000,
      residentLeaseDurationMs: 1_000,
      workspaceLeaseDurationMs: 1_000,
    });
    const access = { workspace_root: fixture.workspace, mode: 'writable' };
    const reservation = oldStore.reserveNextExecutor({
      maxResidentExecutorsPerBot: 2,
      workspaceAccessByConversation: new Map([[turn.conversation_id, access]]),
    });
    const staleContext = oldStore.claimNextQueuedTurn({
      conversationId: turn.conversation_id,
      requireResident: true,
      workspaceAccess: access,
      workspaceLease: reservation.workspace,
    });
    oldStore.transitionTurn(staleContext, 'starting', 'running');
    fixture.database.close();

    const restartedDatabase = new Database(databasePath);
    const isolations = [];
    const restartedService = createExecutorService({
      database: restartedDatabase,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'workspace-service-restart-new',
      now: () => '2026-07-19T13:07:32.000Z',
      generateId: deterministicIds('workspace-restart-new'),
      leaseDurationMs: 1_000,
      residentLeaseDurationMs: 1_000,
      residentHeartbeatIntervalMs: 300,
      workspaceRoot: fixture.workspace,
      workspaceLeaseDurationMs: 1_000,
      workspaceHeartbeatIntervalMs: 300,
      isolateOrphanedWorkspace(recovery) {
        isolations.push(recovery);
        return { isolated: true };
      },
      scheduleResidentHeartbeat() { return { unref() {} }; },
      cancelResidentHeartbeat() {},
      scheduleWorkspaceHeartbeat() { return { unref() {} }; },
      cancelWorkspaceHeartbeat() {},
    });

    restartedService.start();
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'recovering' });
    const adopted = restartedDatabase.prepare(`
      SELECT holder_service_instance_id, lease_epoch, state
      FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id);
    expect(adopted).toMatchObject({
      holder_service_instance_id: 'workspace-service-restart-new',
      state: 'uncertain',
    });
    expect(adopted.lease_epoch).toBeGreaterThan(staleContext.workspace.lease_epoch);
    const staleStore = createExecutorStore({
      database: restartedDatabase,
      provider: 'claude',
      serviceInstanceId: 'workspace-service-restart-old',
      now: () => '2026-07-19T13:07:32.000Z',
      generateId: deterministicIds('workspace-restart-stale'),
    });
    expect(() => staleStore.assertWorkspaceWritable(staleContext)).toThrow(
      expect.objectContaining({ code: 'stale_attempt' }),
    );
    const staleCoordinator = createWorkspaceLeaseCoordinator({
      database: restartedDatabase,
      serviceInstanceId: 'workspace-service-restart-old',
      now: () => '2026-07-19T13:07:32.000Z',
      generateId: deterministicIds('workspace-restart-stale-release'),
    });
    expect(() => staleCoordinator.releaseAfterIsolation(staleContext.workspace, {
      reason: 'stale_service_claimed_isolation',
    })).toThrow(expect.objectContaining({ code: 'stale_workspace_lease' }));
    await expect(restartedService.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [],
      notification_pending: [turn.turn_id],
      isolation_pending: [],
    });
    expect(isolations).toEqual([]);
    deliverTurnNotifications(
      restartedDatabase,
      turn.turn_id,
      'restart-recovery',
      '2026-07-19T13:07:33.000Z',
    );
    await expect(restartedService.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [turn.turn_id],
      notification_pending: [],
      isolation_pending: [],
    });
    expect(isolations).toHaveLength(1);
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'released' });
    expect(restartedDatabase.prepare(`
      SELECT owner_service_instance_id FROM runtime_executor_residents
      WHERE conversation_id = ?
    `).get(turn.conversation_id)).toEqual({ owner_service_instance_id: null });

    await restartedService.close();
    restartedDatabase.close();
  });

  test('keeps a timed-out workspace fenced across the isolation restart window', async () => {
    const fixture = createFixture();
    const databasePath = fixture.database.name;
    const turn = acceptQueuedTurn(fixture.database, 'timeout-restart', 'chat-timeout-restart');
    let clock = '2026-07-19T13:09:00.000Z';
    const oldStore = createExecutorStore({
      database: fixture.database,
      provider: 'codex',
      serviceInstanceId: 'workspace-timeout-old',
      now: () => clock,
      generateId: deterministicIds('workspace-timeout-old'),
      leaseDurationMs: 1_000,
      workspaceLeaseDurationMs: 1_000,
      interactionTimeoutMs: 1_000,
    });
    const reservation = oldStore.reserveNextExecutor({
      maxResidentExecutorsPerBot: 1,
      workspaceAccessByConversation: new Map([[
        turn.conversation_id,
        { workspace_root: fixture.workspace, mode: 'writable' },
      ]]),
    });
    const oldContext = oldStore.claimNextQueuedTurn({
      conversationId: turn.conversation_id,
      workspaceAccess: { workspace_root: fixture.workspace, mode: 'writable' },
      workspaceLease: reservation.workspace,
    });
    oldStore.transitionTurn(oldContext, 'starting', 'running');
    oldStore.transitionTurn(oldContext, 'running', 'recovering', {
      reasonCode: 'workspace_lease_expired',
      recovery: {
        waitForDecision: true,
        error: {
          code: 'workspace_lease_expired',
          category: 'conflict',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'A prior recovery was announced before execution resumed.',
        },
      },
    });
    expect(deliverTurnNotifications(
      fixture.database,
      turn.turn_id,
      'timeout-restart-prior-recovery',
      clock,
    )).not.toHaveLength(0);
    oldStore.transitionTurn(oldContext, 'recovering', 'running');
    const interaction = oldStore.requestInteraction(oldContext, {
      provider_interaction_ref: 'provider-question-timeout-restart',
      tool_use_id: 'tool-use-timeout-restart',
      kind: 'question',
      prompt: 'Continue after the restart window?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action', 'main_card_reply'],
    });
    clock = '2026-07-19T13:09:02.000Z';
    expect(oldStore.expireInteraction({
      interaction_id: interaction.interaction_id,
      interaction_version: interaction.version,
    })).toMatchObject({
      status: 'expired',
      newly_expired: true,
      turn_state: 'timed_out',
    });
    const terminalStream = fixture.database.prepare(`
      SELECT COUNT(*) AS count, MAX(event_sequence) AS terminal_sequence
      FROM runtime_normalized_events WHERE turn_id = ?
    `).get(turn.turn_id);
    const terminalEventCount = terminalStream.count;
    expect(JSON.parse(fixture.database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
    `).get(turn.turn_id).event_json)).toMatchObject({
      kind: 'turn_state_changed',
      payload: { to_state: 'timed_out' },
    });
    fixture.database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id,
        aggregate_version, status, command_json, priority, supersedable,
        terminal, created_at, updated_at
      ) VALUES (?, ?, 'text_notice', ?, ?, 1, 'delivered', ?, 100, 0, 0, ?, ?)
    `).run(
      'outbox-timeout-restart-generic-notice',
      'delivery-timeout-restart-generic-notice',
      `${turn.turn_id}-generic-notice`,
      turn.turn_id,
      JSON.stringify({
        aggregate_type: 'text_notice',
        event_sequence_through: terminalStream.terminal_sequence,
        render_model: {
          text: 'Message received. Rich delivery is temporarily unavailable.',
          terminal: false,
        },
      }),
      clock,
      clock,
    );
    fixture.database.close();

    const restartedDatabase = new Database(databasePath);
    const service = createExecutorService({
      database: restartedDatabase,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'workspace-timeout-new',
      now: () => '2026-07-19T13:09:02.000Z',
      generateId: deterministicIds('workspace-timeout-new'),
      leaseDurationMs: 1_000,
      workspaceRoot: fixture.workspace,
      workspaceLeaseDurationMs: 1_000,
      workspaceHeartbeatIntervalMs: 300,
      isolateOrphanedWorkspace: async () => ({ isolated: true }),
      scheduleWorkspaceHeartbeat() { return { unref() {} }; },
      cancelWorkspaceHeartbeat() {},
    });

    service.start();
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'timed_out' });
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'uncertain' });
    expect(restartedDatabase.prepare(`
      SELECT COUNT(*) AS count FROM runtime_normalized_events WHERE turn_id = ?
    `).get(turn.turn_id).count).toBe(terminalEventCount);
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toMatchObject({
      notification_pending: [turn.turn_id],
    });
    deliverTurnNotifications(
      restartedDatabase,
      turn.turn_id,
      'timeout-restart',
      '2026-07-19T13:09:03.000Z',
    );
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toMatchObject({
      isolated: [turn.turn_id],
    });
    expect(restartedDatabase.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'released' });
    expect(restartedDatabase.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(turn.conversation_id)).toEqual({ lease_owner: null });

    await service.close();
    restartedDatabase.close();
  });

  test('releases an expired workspace fence only after recovering provider isolation', () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'recover-expired', 'chat-recover-expired');
    let clock = '2026-07-19T13:08:00.000Z';
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId: 'workspace-service-recover-expired',
      now: () => clock,
      generateId: deterministicIds('workspace-recover-expired'),
      workspaceLeaseDurationMs: 1_000,
    });
    const turnContext = store.claimNextQueuedTurn({
      conversationId: turn.conversation_id,
      workspaceAccess: {
        workspace_root: workspace,
        mode: 'writable',
      },
    });
    store.transitionTurn(turnContext, 'starting', 'running');
    store.transitionTurn(turnContext, 'running', 'recovering', {
      reasonCode: 'workspace_lease_expired',
      recovery: {
        waitForDecision: true,
        error: {
          code: 'workspace_lease_expired',
          category: 'conflict',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'Workspace ownership expired while work was active.',
        },
      },
    });

    clock = '2026-07-19T13:08:02.000Z';
    expect(() => store.heartbeatOwnedWorkspaceLeases()).toThrow(expect.objectContaining({
      code: 'stale_workspace_lease',
    }));
    expect(() => store.releaseRecoveringExecutorOwnership(turnContext)).toThrow(
      expect.objectContaining({ code: 'recovery_notification_pending' }),
    );
    expect(deliverTurnNotifications(
      database,
      turn.turn_id,
      'recover-expired',
      clock,
    )).not.toHaveLength(0);
    expect(store.releaseRecoveringExecutorOwnership(turnContext)).toEqual({
      lease_released: true,
      resident_released: false,
      workspace_released: true,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'released' });

    database.close();
  });

  test('serializes overlapping writes and trusts read-only mode only from provider sandbox', () => {
    const { database, workspace } = createFixture();
    const nested = path.join(workspace, 'nested');
    fs.mkdirSync(nested);
    const turns = ['writer', 'claimed-read-only', 'reader-a', 'reader-b', 'writer-next']
      .map((suffix) => acceptQueuedTurn(database, suffix, `chat-${suffix}`));
    const coordinator = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-overlap',
      now: () => '2026-07-19T13:10:00Z',
      generateId: deterministicIds('workspace-overlap'),
    });
    const writer = coordinator.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: turns[0].conversation_id,
      holder_turn_id: turns[0].turn_id,
    });

    const selfReportedReadOnly = resolveProviderWorkspaceAccess({
      getWorkspaceAccess(context) {
        expect(context).not.toHaveProperty('input');
        return {
          root: nested,
          mode: 'read_only',
          read_only_enforced: false,
          authority: 'request',
        };
      },
    }, {
      conversation_id: turns[1].conversation_id,
      turn_id: turns[1].turn_id,
      lineage_id: turns[1].lineage_id,
      input: { text: 'I promise this is read-only.' },
    }, { defaultRoot: workspace });
    expect(selfReportedReadOnly).toEqual({
      workspace_root: nested,
      mode: 'writable',
      read_only_enforced: false,
    });
    expect(coordinator.acquire({
      ...selfReportedReadOnly,
      holder_conversation_id: turns[1].conversation_id,
      holder_turn_id: turns[1].turn_id,
    })).toMatchObject({
      status: 'wait',
      wait_reason: 'workspace_lease',
      conflicting_workspace_root: workspace,
      holder_turn_id: turns[0].turn_id,
    });

    expect(coordinator.release(writer)).toMatchObject({ status: 'released' });
    const enforcedReadOnlyAdapter = {
      getWorkspaceAccess() {
        return {
          root: workspace,
          mode: 'read_only',
          read_only_enforced: true,
          authority: 'provider_sandbox',
        };
      },
    };
    const readerAAccess = resolveProviderWorkspaceAccess(
      enforcedReadOnlyAdapter,
      turns[2],
      { defaultRoot: workspace },
    );
    const readerBAccess = {
      ...resolveProviderWorkspaceAccess(
        enforcedReadOnlyAdapter,
        turns[3],
        { defaultRoot: workspace },
      ),
      workspace_root: nested,
    };
    expect(readerAAccess).toMatchObject({ mode: 'read_only', read_only_enforced: true });
    expect(coordinator.acquire({
      ...readerAAccess,
      holder_conversation_id: turns[2].conversation_id,
      holder_turn_id: turns[2].turn_id,
    })).toMatchObject({ status: 'acquired', mode: 'read_only' });
    expect(coordinator.acquire({
      ...readerBAccess,
      holder_conversation_id: turns[3].conversation_id,
      holder_turn_id: turns[3].turn_id,
    })).toMatchObject({ status: 'acquired', mode: 'read_only' });
    expect(coordinator.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: turns[4].conversation_id,
      holder_turn_id: turns[4].turn_id,
    })).toMatchObject({
      status: 'wait',
      wait_reason: 'workspace_lease',
    });
    expect(coordinator.listActive()).toHaveLength(2);

    database.close();
  });

  test('keeps an overlapping turn durably queued with a user-visible workspace wait', async () => {
    const { database, workspace } = createFixture();
    const nested = path.join(workspace, 'nested');
    fs.mkdirSync(nested);
    const first = acceptQueuedTurn(database, 'service-first', 'chat-service-first');
    const second = acceptQueuedTurn(database, 'service-second', 'chat-service-second');
    const firstStarted = deferred();
    const finishFirst = deferred();
    const adapter = {
      getWorkspaceAccess({ conversation_id: conversationId }) {
        return {
          root: conversationId === first.conversation_id ? workspace : nested,
          mode: 'writable',
          read_only_enforced: false,
          authority: 'provider_sandbox',
        };
      },
      async *execute(context) {
        if (context.conversation_id === first.conversation_id) {
          firstStarted.resolve();
          await finishFirst.promise;
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
      provider: 'claude',
      serviceInstanceId: 'workspace-executor-service',
      now: () => '2026-07-19T13:20:00Z',
      generateId: deterministicIds('workspace-service'),
      workspaceRoot: workspace,
      maxResidentExecutorsPerBot: 2,
    });

    const firstRun = service.runNext();
    await firstStarted.promise;
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'workspace_wait',
      conversation_id: second.conversation_id,
      turn_id: second.turn_id,
      wait_reason: 'workspace_lease',
      wait_detail: {
        workspace_root: nested,
        conflicting_workspace_root: workspace,
        holder_conversation_id: first.conversation_id,
        holder_turn_id: first.turn_id,
      },
    });
    expect(service.snapshot()).toMatchObject({
      executors: expect.arrayContaining([
        expect.objectContaining({
          conversation_id: second.conversation_id,
          queued_turn_ids: [second.turn_id],
          wait_reason: 'workspace_lease',
          wait_detail: expect.objectContaining({
            conflicting_workspace_root: workspace,
          }),
        }),
      ]),
      workspace_leases: {
        complete: true,
        error: null,
        items: [expect.objectContaining({
          workspace_root: workspace,
          mode: 'write',
          holder_turn_id: first.turn_id,
          holder_background_work_id: null,
          waiter_count: 1,
        })],
      },
    });
    expect(database.prepare(`
      SELECT conversation_id FROM runtime_executor_residents ORDER BY conversation_id
    `).all()).toEqual([{ conversation_id: first.conversation_id }]);
    expect(JSON.parse(database.prepare(`
      SELECT render_model_json FROM runtime_projection_snapshots
      WHERE turn_id = ? ORDER BY aggregate_version DESC LIMIT 1
    `).get(second.turn_id).render_model_json)).toMatchObject({
      phase: 'queued',
      text: 'Waiting for another conversation to finish using this workspace.',
    });
    const publicWorkspaceSnapshot = service.snapshot().workspace_leases;
    expect(validateObservabilitySnapshot({
      ...observabilityFixture.cases.complete,
      workspace_leases: publicWorkspaceSnapshot,
    }).known.workspace_leases).toEqual(publicWorkspaceSnapshot);

    finishFirst.resolve();
    await expect(firstRun).resolves.toMatchObject({ status: 'completed', turn_id: first.turn_id });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(service.snapshot().workspace_leases).toEqual({
      complete: true,
      items: [],
      error: null,
    });

    await service.close();
    database.close();
  });

  test('reserves workspace ownership before resident admission can race another service', () => {
    const { database, directory, workspace } = createFixture();
    const first = acceptQueuedTurn(database, 'reservation-race-first', 'chat-reservation-race-first');
    const second = acceptQueuedTurn(
      database,
      'reservation-race-second',
      'chat-reservation-race-second',
    );
    const competingDatabase = new Database(path.join(directory, 'c4.db'));
    const firstStore = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'workspace-reservation-first',
      now: () => '2026-07-19T13:24:00Z',
      generateId: deterministicIds('workspace-reservation-first'),
    });
    const secondStore = createExecutorStore({
      database: competingDatabase,
      provider: 'claude',
      serviceInstanceId: 'workspace-reservation-second',
      now: () => '2026-07-19T13:24:00Z',
      generateId: deterministicIds('workspace-reservation-second'),
    });
    const workspaceAccessByConversation = new Map([
      [first.conversation_id, { workspace_root: workspace, mode: 'writable' }],
      [second.conversation_id, { workspace_root: workspace, mode: 'writable' }],
    ]);

    const reservation = firstStore.reserveNextExecutor({
      maxResidentExecutorsPerBot: 2,
      workspaceAccessByConversation,
    });
    expect(reservation).toMatchObject({
      status: 'ready',
      conversation_id: first.conversation_id,
      workspace: { holder_turn_id: first.turn_id },
    });
    expect(secondStore.reserveNextExecutor({
      maxResidentExecutorsPerBot: 2,
      workspaceAccessByConversation,
    })).toMatchObject({
      status: 'workspace_wait',
      conversation_id: second.conversation_id,
      turn_id: second.turn_id,
    });
    expect(database.prepare(`
      SELECT conversation_id, owner_service_instance_id
      FROM runtime_executor_residents ORDER BY conversation_id
    `).all()).toEqual([{
      conversation_id: first.conversation_id,
      owner_service_instance_id: 'workspace-reservation-first',
    }]);
    expect(firstStore.claimNextQueuedTurn({
      conversationId: first.conversation_id,
      requireResident: true,
      workspaceAccess: workspaceAccessByConversation.get(first.conversation_id),
      workspaceLease: reservation.workspace,
    })).toMatchObject({ turn_id: first.turn_id });

    competingDatabase.close();
    database.close();
  });

  test('heartbeats a live service workspace lease while provider work is active', async () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'service-heartbeat', 'chat-service-heartbeat');
    const started = deferred();
    const finished = deferred();
    let clock = '2026-07-19T13:25:00.000Z';
    let heartbeat;
    let heartbeatCancelled = false;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {
          started.resolve();
          await finished.promise;
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'workspace-executor-heartbeat',
      now: () => clock,
      generateId: deterministicIds('workspace-executor-heartbeat'),
      workspaceRoot: workspace,
      workspaceLeaseDurationMs: 10_000,
      workspaceHeartbeatIntervalMs: 3_000,
      scheduleWorkspaceHeartbeat(callback) {
        heartbeat = callback;
        return { unref() {} };
      },
      cancelWorkspaceHeartbeat() { heartbeatCancelled = true; },
    });

    const execution = service.runNext();
    await started.promise;
    clock = '2026-07-19T13:25:08.000Z';
    heartbeat();
    expect(database.prepare(`
      SELECT lease_expires_at FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ lease_expires_at: '2026-07-19T13:25:18.000Z' });

    finished.resolve();
    await expect(execution).resolves.toMatchObject({ status: 'completed' });
    await service.close();
    expect(heartbeatCancelled).toBe(true);
    database.close();
  });

  test('notifies recovery before isolating a provider whose workspace heartbeat expired', async () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'service-expired', 'chat-service-expired');
    const started = deferred();
    const isolated = deferred();
    let abortCalls = 0;
    let clock = '2026-07-19T13:26:00.000Z';
    let heartbeat;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {
          started.resolve();
          await isolated.promise;
        },
        async abort() {
          abortCalls += 1;
          isolated.resolve();
        },
      },
      provider: 'codex',
      serviceInstanceId: 'workspace-executor-expired',
      now: () => clock,
      generateId: deterministicIds('workspace-executor-expired'),
      workspaceRoot: workspace,
      workspaceLeaseDurationMs: 1_000,
      workspaceHeartbeatIntervalMs: 300,
      scheduleWorkspaceHeartbeat(callback) {
        heartbeat = callback;
        return { unref() {} };
      },
      cancelWorkspaceHeartbeat() {},
    });

    const execution = service.runNext();
    await started.promise;
    clock = '2026-07-19T13:26:02.000Z';
    await Promise.race([
      heartbeat(),
      new Promise((_, reject) => setTimeout(
        () => reject(new Error('workspace heartbeat recovery did not settle')),
        250,
      )),
    ]);
    expect(database.prepare(`
      SELECT state, holder_service_instance_id, lease_expires_at
      FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({
      state: 'uncertain',
      holder_service_instance_id: 'workspace-executor-expired',
      lease_expires_at: '2026-07-19T13:26:01.000Z',
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'recovering' });
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [],
      notification_pending: [turn.turn_id],
      isolation_pending: [],
    });
    expect(deliverTurnNotifications(
      database,
      turn.turn_id,
      'service-expired',
      clock,
    )).not.toHaveLength(0);
    const firstReconciliation = service.reconcileWorkspaceRecoveries();
    const overlappingReconciliation = service.reconcileWorkspaceRecoveries();
    await expect(Promise.all([
      firstReconciliation,
      overlappingReconciliation,
    ])).resolves.toEqual([
      {
        isolated: [turn.turn_id],
        notification_pending: [],
        isolation_pending: [],
      },
      {
        isolated: [turn.turn_id],
        notification_pending: [],
        isolation_pending: [],
      },
    ]);
    expect(abortCalls).toBe(1);
    await expect(execution).resolves.toMatchObject({ status: 'recovering', turn_id: turn.turn_id });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(turn.turn_id)).toEqual({ state: 'released' });
    expect(database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
    `).get(turn.turn_id).event_json).toContain('workspace_lease_expired');

    await service.close();
    database.close();
  });

  test('close waits for an in-flight workspace recovery isolation', async () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'recovery-close-race', 'chat-recovery-close-race');
    const started = deferred();
    const abortStarted = deferred();
    const allowAbort = deferred();
    const providerStopped = deferred();
    let clock = '2026-07-19T13:27:00.000Z';
    let heartbeat;
    let closeCalls = 0;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {
          started.resolve();
          await providerStopped.promise;
        },
        async abort() {
          abortStarted.resolve();
          await allowAbort.promise;
          providerStopped.resolve();
        },
        async close() {
          closeCalls += 1;
          return [];
        },
      },
      provider: 'codex',
      serviceInstanceId: 'workspace-recovery-close-race',
      now: () => clock,
      generateId: deterministicIds('workspace-recovery-close-race'),
      workspaceRoot: workspace,
      workspaceLeaseDurationMs: 1_000,
      workspaceHeartbeatIntervalMs: 300,
      scheduleWorkspaceHeartbeat(callback) {
        heartbeat = callback;
        return { unref() {} };
      },
      cancelWorkspaceHeartbeat() {},
    });

    const execution = service.runNext();
    await started.promise;
    clock = '2026-07-19T13:27:02.000Z';
    await heartbeat();
    deliverTurnNotifications(
      database,
      turn.turn_id,
      'recovery-close-race',
      '2026-07-19T13:27:03.000Z',
    );
    const recovery = service.reconcileWorkspaceRecoveries();
    await abortStarted.promise;
    const closing = service.close();
    await Promise.resolve();
    expect(closeCalls).toBe(0);
    allowAbort.resolve();
    await expect(recovery).resolves.toMatchObject({ isolated: [turn.turn_id] });
    await expect(execution).resolves.toMatchObject({ status: 'recovering' });
    await expect(closing).resolves.toBeUndefined();
    expect(closeCalls).toBe(1);

    database.close();
  });

  test('retains a lease and LRU protection until durable background work really ends', () => {
    const { database, workspace } = createFixture();
    const turn = acceptQueuedTurn(database, 'background-store', 'chat-background-store');
    const coordinator = createWorkspaceLeaseCoordinator({
      database,
      serviceInstanceId: 'workspace-service-background-store',
      now: () => '2026-07-19T13:30:00Z',
      generateId: deterministicIds('workspace-background-store'),
    });
    const lease = coordinator.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: turn.conversation_id,
      holder_turn_id: turn.turn_id,
    });

    const background = coordinator.startBackgroundWork(lease, {
      provider_task_id: 'provider-background-1',
    });
    expect(background).toMatchObject({
      status: 'started',
      background_work_id: 'background-work-workspace-background-store-1',
      provider_task_id: 'provider-background-1',
      state: 'active',
    });
    expect(coordinator.hasBlockingBackgroundWork(turn.conversation_id)).toBe(true);
    expect(coordinator.listActive()).toEqual([
      expect.objectContaining({
        workspace_lease_id: lease.workspace_lease_id,
        holder_background_work_ids: [background.background_work_id],
      }),
    ]);
    expect(() => coordinator.release(lease)).toThrow(expect.objectContaining({
      code: 'workspace_background_active',
    }));

    expect(coordinator.finishBackgroundWork(lease, {
      provider_task_id: 'provider-background-1',
      outcome: 'completed',
    })).toMatchObject({
      background_work_id: background.background_work_id,
      state: 'completed',
    });
    expect(coordinator.hasBlockingBackgroundWork(turn.conversation_id)).toBe(false);
    expect(coordinator.release(lease)).toMatchObject({ status: 'released' });

    const uncertainLease = coordinator.acquire({
      workspace_root: workspace,
      mode: 'writable',
      holder_conversation_id: turn.conversation_id,
      holder_turn_id: turn.turn_id,
    });
    coordinator.startBackgroundWork(uncertainLease, {
      provider_task_id: 'provider-background-unknown',
    });
    coordinator.finishBackgroundWork(uncertainLease, {
      provider_task_id: 'provider-background-unknown',
      outcome: 'unknown',
    });
    expect(coordinator.hasBlockingBackgroundWork(turn.conversation_id)).toBe(true);
    coordinator.releaseAfterIsolation(uncertainLease, {
      reason: 'provider_background_isolated',
    });
    expect(coordinator.hasBlockingBackgroundWork(turn.conversation_id)).toBe(false);

    database.close();
  });
});
