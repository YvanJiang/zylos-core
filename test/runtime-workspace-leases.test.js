import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import {
  createWorkspaceLeaseCoordinator,
  normalizeWorkspaceRoot,
  resolveProviderWorkspaceAccess,
} from '../runtime/workspace/lease-coordinator.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
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
      workspace_leases: [
        expect.objectContaining({
          workspace_root: workspace,
          mode: 'writable',
          holder_turn_id: first.turn_id,
          holder_background_work_ids: [],
          waiters: [{
            conversation_id: second.conversation_id,
            turn_id: second.turn_id,
            workspace_root: nested,
            mode: 'writable',
          }],
        }),
      ],
    });
    expect(database.prepare(`
      SELECT conversation_id FROM runtime_executor_residents ORDER BY conversation_id
    `).all()).toEqual([{ conversation_id: first.conversation_id }]);

    finishFirst.resolve();
    await expect(firstRun).resolves.toMatchObject({ status: 'completed', turn_id: first.turn_id });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(service.snapshot().workspace_leases).toEqual([]);

    await service.close();
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

    database.close();
  });
});
