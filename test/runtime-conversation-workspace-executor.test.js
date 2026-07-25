import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import {
  acceptNormalInbound,
  acceptQueuedInbound,
} from '../runtime/persistence/inbound-acceptance.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { initializeRuntimePersistence } from '../runtime/persistence/schema.js';
import { createClaudeConversationAdapter } from '../runtime/providers/claude/conversation-adapter.js';
import {
  createConversationWorkspaceProvisioner,
  requestConversationWorkspaceInTransaction,
} from '../runtime/workspace/conversation-workspace-provisioner.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const temporaryDirectories = [];

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

function createFixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-conversation-workspace-'));
  temporaryDirectories.push(directory);
  const legacyRoot = path.join(directory, 'legacy');
  const workspacesRoot = path.join(directory, 'workspaces');
  fs.mkdirSync(legacyRoot);
  fs.mkdirSync(workspacesRoot);
  return {
    database: new Database(path.join(directory, 'c4.db')),
    directory,
    legacyRoot: fs.realpathSync.native(legacyRoot),
    workspacesRoot: fs.realpathSync.native(workspacesRoot),
  };
}

function conversationWorkspaceOptions(
  fixture,
  namespace,
  timestamp = '2026-07-24T01:00:01Z',
) {
  const baseSnapshotRoot = path.join(fixture.directory, `base-${namespace}`);
  fs.mkdirSync(baseSnapshotRoot, { mode: 0o700 });
  return {
    workspaceStoreRoot: fixture.workspacesRoot,
    baseSnapshotRoot,
    baseSnapshotRef: `empty:${namespace}`,
    snapshotFiles: [],
    now: () => timestamp,
    generateId: deterministicIds(`provisioner-${namespace}`),
  };
}

function normalEnvelope(suffix, { chatId = `chat-${suffix}`, text = `turn ${suffix}` } = {}) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.chat_id = chatId;
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

function acceptDetached(database, suffix) {
  return acceptNormalInbound(database, normalEnvelope(suffix), {
    now: () => '2026-07-24T01:00:00Z',
    generateId: deterministicIds(`detached-${suffix}`),
    inputCoalescingPolicy: { enabled: false },
  });
}

function executionConversationId(database, accepted) {
  return database.prepare(`
    SELECT execution_conversation_id
    FROM runtime_background_tasks
    WHERE background_task_id = ?
  `).get(accepted.background_task_id).execution_conversation_id;
}

function installFoundationWorkspaceTables(database) {
  database.exec(`
    CREATE TABLE IF NOT EXISTS runtime_conversation_workspaces (
      workspace_id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL UNIQUE,
      workspace_root TEXT UNIQUE,
      staging_root TEXT UNIQUE,
      base_snapshot_root TEXT,
      base_snapshot_ref TEXT,
      base_snapshot_manifest_json TEXT,
      generation INTEGER NOT NULL,
      state TEXT NOT NULL,
      provisioning_owner TEXT,
      provisioning_expires_at TEXT,
      requested_at TEXT NOT NULL,
      provisioning_started_at TEXT,
      ready_at TEXT,
      quarantined_at TEXT,
      failed_at TEXT,
      retired_at TEXT,
      updated_at TEXT NOT NULL,
      last_error_json TEXT,
      retention_reason TEXT,
      retain_until TEXT,
      UNIQUE (workspace_id, conversation_id, workspace_root, generation)
    );
    CREATE TABLE IF NOT EXISTS runtime_lineage_workspace_bindings (
      lineage_id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL,
      conversation_id TEXT NOT NULL,
      workspace_root TEXT NOT NULL,
      generation INTEGER NOT NULL,
      bound_at TEXT NOT NULL
    );
  `);
}

function putWorkspaceBinding(database, {
  conversationId,
  generation = 1,
  root = null,
  state,
  workspaceId = `workspace-${conversationId}`,
}) {
  const existing = database.prepare(`
    SELECT state
    FROM runtime_conversation_workspaces
    WHERE conversation_id = ?
  `).get(conversationId);
  const manifestJson = root === null ? null : JSON.stringify(['README.md']);
  if (existing?.state === 'requested' && state === 'ready') {
    database.transaction(() => {
      database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET workspace_root = ?, staging_root = ?, base_snapshot_root = ?,
          base_snapshot_ref = 'snapshot-1', base_snapshot_manifest_json = ?,
          generation = ?, state = 'provisioning',
          provisioning_owner = 'test-provisioner',
          provisioning_expires_at = '2026-07-24T01:05:00Z',
          provisioning_started_at = '2026-07-24T01:00:00Z',
          updated_at = '2026-07-24T01:00:00Z'
        WHERE conversation_id = ? AND state = 'requested'
      `).run(root, `${root}.staging`, root, manifestJson, generation, conversationId);
      database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET staging_root = NULL, state = 'ready', provisioning_owner = NULL,
          provisioning_expires_at = NULL, ready_at = '2026-07-24T01:00:00Z',
          updated_at = '2026-07-24T01:00:00Z'
        WHERE conversation_id = ? AND state = 'provisioning'
      `).run(conversationId);
    }).immediate();
    return;
  }
  database.prepare(`
    INSERT INTO runtime_conversation_workspaces (
      workspace_id, conversation_id, workspace_root, base_snapshot_root,
      base_snapshot_ref, base_snapshot_manifest_json, generation, state,
      requested_at, ready_at, updated_at
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, '2026-07-24T01:00:00Z', ?,
      '2026-07-24T01:00:00Z'
    )
    ON CONFLICT(conversation_id) DO UPDATE SET
      workspace_root = excluded.workspace_root,
      base_snapshot_root = excluded.base_snapshot_root,
      base_snapshot_ref = excluded.base_snapshot_ref,
      base_snapshot_manifest_json = excluded.base_snapshot_manifest_json,
      generation = excluded.generation,
      state = excluded.state,
      ready_at = excluded.ready_at,
      updated_at = excluded.updated_at
  `).run(
    workspaceId,
    conversationId,
    root,
    root,
    root === null ? null : 'snapshot-1',
    manifestJson,
    generation,
    state,
    state === 'ready' ? '2026-07-24T01:00:00Z' : null,
  );
}

function idleSession(sessionId) {
  return {
    type: 'system',
    subtype: 'session_state_changed',
    state: 'idle',
    session_id: sessionId,
  };
}

function createObservedClaudeQuery({ sessionId, beforeResult = null }) {
  const calls = [];
  function query({ prompt, options }) {
    calls.push(options);
    const stream = (async function* messages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        if (beforeResult) await beforeResult(options, input);
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: 'done',
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { calls, query };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('per-conversation detached execution workspaces', () => {
  test('fails closed without a detached binding, waits through provisioning, then claims ready', async () => {
    const fixture = createFixture();
    const accepted = acceptDetached(fixture.database, 'provisioning');
    const conversationId = executionConversationId(fixture.database, accepted);
    installFoundationWorkspaceTables(fixture.database);
    const workspaceRoot = path.join(fixture.workspacesRoot, 'provisioned');
    fs.mkdirSync(workspaceRoot);
    const calls = [];
    const service = createExecutorService({
      database: fixture.database,
      adapter: {
        getWorkspaceAccess() {
          return { mode: 'writable', read_only_enforced: false, authority: 'provider_sandbox' };
        },
        async *execute(context) {
          calls.push(context.workspace);
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-provisioning',
      now: () => '2026-07-24T01:00:01Z',
      generateId: deterministicIds('executor-provisioning'),
      workspaceRoot: fixture.legacyRoot,
    });

    fixture.database.exec('DROP TRIGGER runtime_conversation_workspace_delete_forbidden;');
    fixture.database.prepare(`
      DELETE FROM runtime_conversation_workspaces
      WHERE conversation_id = ?
    `).run(conversationId);
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'workspace_wait',
      wait_reason: 'workspace_binding_missing',
    });
    fixture.database.transaction(() => requestConversationWorkspaceInTransaction(
      fixture.database,
      {
        workspaceId: 'workspace-restored-provisioning',
        conversationId,
        requestedAt: '2026-07-24T01:00:01Z',
      },
    )).immediate();
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'workspace_wait',
      wait_reason: 'workspace_provisioning',
      wait_detail: {
        workspace_state: 'requested',
        workspace_generation: 1,
      },
    });
    expect(calls).toHaveLength(0);
    expect(fixture.database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_executor_residents
    `).get()).toEqual({ count: 0 });
    expect(fixture.database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_workspace_leases
    `).get()).toEqual({ count: 0 });

    putWorkspaceBinding(fixture.database, {
      conversationId,
      root: workspaceRoot,
      state: 'ready',
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.background_execution_turn_id,
    });
    expect(calls).toEqual([
      expect.objectContaining({
        binding_kind: 'conversation',
        workspace_generation: 1,
        workspace_root: fs.realpathSync.native(workspaceRoot),
      }),
    ]);
    expect(fixture.database.prepare(`
      SELECT conversation_id, workspace_root, generation
      FROM runtime_lineage_workspace_bindings
    `).get()).toEqual({
      conversation_id: conversationId,
      workspace_root: fs.realpathSync.native(workspaceRoot),
      generation: 1,
    });

    await service.close();
    fixture.database.close();
  });

  test('automatically provisions distinct roots before detached provider execution', async () => {
    const fixture = createFixture();
    const first = acceptDetached(fixture.database, 'automatic-first');
    const second = acceptDetached(fixture.database, 'automatic-second');
    const firstStarted = deferred();
    const secondStarted = deferred();
    const releaseFirst = deferred();
    const observed = [];
    const service = createExecutorService({
      database: fixture.database,
      adapter: {
        getWorkspaceAccess() {
          return { mode: 'writable', read_only_enforced: false, authority: 'provider_sandbox' };
        },
        async *execute(context) {
          observed.push(context.workspace);
          if (observed.length === 1) {
            firstStarted.resolve();
            await releaseFirst.promise;
          } else {
            secondStarted.resolve();
          }
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-automatic',
      now: () => '2026-07-24T01:00:02Z',
      generateId: deterministicIds('executor-automatic'),
      workspaceRoot: fixture.legacyRoot,
      conversationWorkspaceOptions: conversationWorkspaceOptions(fixture, 'automatic'),
    });

    const firstRun = service.runNext();
    await firstStarted.promise;
    const secondRun = service.runNext();
    await secondStarted.promise;

    expect(observed).toHaveLength(2);
    expect(observed[0]).toMatchObject({
      binding_kind: 'conversation',
      workspace_generation: 1,
    });
    expect(observed[1]).toMatchObject({
      binding_kind: 'conversation',
      workspace_generation: 1,
    });
    expect(observed[0].workspace_root).not.toBe(observed[1].workspace_root);
    expect(path.dirname(observed[0].workspace_root)).toBe(fixture.workspacesRoot);
    expect(path.dirname(observed[1].workspace_root)).toBe(fixture.workspacesRoot);
    expect(fixture.database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_lineage_workspace_bindings
    `).get()).toEqual({ count: 2 });

    releaseFirst.resolve();
    await expect(firstRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.background_execution_turn_id,
    });
    await expect(secondRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.background_execution_turn_id,
    });
    await service.close();
    fixture.database.close();
  });

  test('keeps a ready detached workspace claimable across proven no-side-effect retries', async () => {
    const fixture = createFixture();
    const accepted = acceptDetached(fixture.database, 'safe-retry');
    const conversationId = executionConversationId(fixture.database, accepted);
    let currentTimeMs = Date.parse('2026-07-24T01:00:02Z');
    let executions = 0;
    const service = createExecutorService({
      database: fixture.database,
      adapter: {
        getWorkspaceAccess() {
          return { mode: 'writable', read_only_enforced: false, authority: 'provider_sandbox' };
        },
        async *execute() {
          executions += 1;
          if (executions === 1) {
            const error = new Error('private transient provider failure');
            error.providerError = {
              code: 'delivery_transient',
              category: 'provider',
              retryable: true,
              side_effect_status: 'none',
              user_message: 'The provider is temporarily unavailable.',
            };
            throw error;
          }
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-conversation-workspace-safe-retry',
      now: () => new Date(currentTimeMs).toISOString(),
      generateId: deterministicIds('executor-safe-retry'),
      workspaceRoot: fixture.legacyRoot,
      conversationWorkspaceOptions: conversationWorkspaceOptions(fixture, 'safe-retry'),
      providerRetryBaseDelayMs: 1_000,
      providerRetryJitterRatio: 0,
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'retry_scheduled',
      turn_id: accepted.background_execution_turn_id,
      retry: { backoff_ms: 1_000 },
    });
    expect(fixture.database.prepare(`
      SELECT state FROM runtime_conversation_workspaces WHERE conversation_id = ?
    `).get(conversationId)).toEqual({ state: 'ready' });

    fixture.database.exec(`
      DROP TRIGGER runtime_workspace_quarantine_recovering_turn;
      CREATE TRIGGER runtime_workspace_quarantine_recovering_turn
      AFTER UPDATE OF state ON runtime_turns
      WHEN NEW.state = 'recovering'
      BEGIN
        UPDATE runtime_conversation_workspaces
        SET state = 'quarantined',
          quarantined_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
          last_error_json = json_object(
            'code', 'workspace_runtime_uncertain',
            'message', 'Conversation execution entered recovering state.',
            'terminal', json('true'),
            'occurred_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          )
        WHERE conversation_id = NEW.conversation_id
          AND state IN ('requested', 'provisioning', 'ready');
      END;
      DELETE FROM runtime_schema_migrations
      WHERE migration_id = 'conversation-workspace-safe-provider-retry-v1';
    `);
    fixture.database.prepare(`
      UPDATE runtime_conversation_workspaces
      SET state = 'quarantined',
        quarantined_at = '2026-07-24T01:00:02Z',
        last_error_json = json_object(
          'code', 'workspace_runtime_uncertain',
          'message', 'Conversation execution entered recovering state.',
          'terminal', json('true'),
          'occurred_at', '2026-07-24T01:00:02Z'
        )
      WHERE conversation_id = ?
    `).run(conversationId);
    initializeRuntimePersistence(fixture.database);
    expect(fixture.database.prepare(`
      SELECT state, quarantined_at, last_error_json
      FROM runtime_conversation_workspaces
      WHERE conversation_id = ?
    `).get(conversationId)).toEqual({
      state: 'ready',
      quarantined_at: null,
      last_error_json: null,
    });
    expect(fixture.database.prepare(`
      SELECT migration_id
      FROM runtime_schema_migrations
      WHERE migration_id = 'conversation-workspace-safe-provider-retry-v1'
    `).get()).toEqual({
      migration_id: 'conversation-workspace-safe-provider-retry-v1',
    });

    fixture.database.exec(`
      DELETE FROM runtime_schema_migrations
      WHERE migration_id = 'conversation-workspace-safe-provider-retry-repair-v2';
    `);
    fixture.database.prepare(`
      UPDATE runtime_conversation_workspaces
      SET state = 'quarantined',
        quarantined_at = '2026-07-24T01:00:03Z',
        last_error_json = json_object(
          'code', 'workspace_runtime_uncertain',
          'message', 'Conversation execution entered recovering state.',
          'terminal', json('true'),
          'occurred_at', '2026-07-24T01:00:03Z'
        )
      WHERE conversation_id = ?
    `).run(conversationId);
    initializeRuntimePersistence(fixture.database);
    expect(fixture.database.prepare(`
      SELECT state, quarantined_at, last_error_json
      FROM runtime_conversation_workspaces
      WHERE conversation_id = ?
    `).get(conversationId)).toEqual({
      state: 'ready',
      quarantined_at: null,
      last_error_json: null,
    });
    expect(fixture.database.prepare(`
      SELECT migration_id
      FROM runtime_schema_migrations
      WHERE migration_id = 'conversation-workspace-safe-provider-retry-repair-v2'
    `).get()).toEqual({
      migration_id: 'conversation-workspace-safe-provider-retry-repair-v2',
    });

    fixture.database.exec('SAVEPOINT stale_safe_retry_probe');
    try {
      fixture.database.prepare(`
        INSERT INTO runtime_provider_attempts (
          attempt_id, turn_id, conversation_id, attempt_no, lease_epoch,
          provider, service_instance_id, executor_instance_id, state,
          last_lease_renewed_at, side_effect_status, started_at, updated_at
        ) VALUES (
          'attempt-stale-safe-retry-probe',
          ?, ?, 2, 2,
          'codex', 'executor-conversation-workspace-safe-retry',
          'executor-stale-safe-retry-probe', 'starting',
          '2026-07-24T01:00:02Z', 'none',
          '2026-07-24T01:00:02Z', '2026-07-24T01:00:02Z'
        )
      `).run(accepted.background_execution_turn_id, conversationId);
      fixture.database.prepare(`
        UPDATE runtime_turns
        SET attempt_id = 'attempt-stale-safe-retry-probe',
          attempt_no = 2, lease_epoch = 2
        WHERE turn_id = ?
      `).run(accepted.background_execution_turn_id);
      fixture.database.prepare(`
        UPDATE runtime_turns SET state = state WHERE turn_id = ?
      `).run(accepted.background_execution_turn_id);
      expect(fixture.database.prepare(`
        SELECT state, json_extract(last_error_json, '$.code') AS error_code
        FROM runtime_conversation_workspaces
        WHERE conversation_id = ?
      `).get(conversationId)).toEqual({
        state: 'quarantined',
        error_code: 'workspace_runtime_uncertain',
      });
    } finally {
      fixture.database.exec(`
        ROLLBACK TO stale_safe_retry_probe;
        RELEASE stale_safe_retry_probe;
      `);
    }

    currentTimeMs += 1_000;
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.background_execution_turn_id,
      attempt_no: 2,
    });
    expect(executions).toBe(2);
    expect(fixture.database.prepare(`
      SELECT state FROM runtime_conversation_workspaces WHERE conversation_id = ?
    `).get(conversationId)).toEqual({ state: 'ready' });

    await service.close();
    fixture.database.close();
  });

  test('never lets the low-level store claim a detached turn without its workspace fence', () => {
    const fixture = createFixture();
    const accepted = acceptDetached(fixture.database, 'low-level-claim');
    const conversationId = executionConversationId(fixture.database, accepted);
    const store = createExecutorStore({
      database: fixture.database,
      provider: 'codex',
      serviceInstanceId: 'executor-low-level-claim',
      now: () => '2026-07-24T01:00:02Z',
      generateId: deterministicIds('executor-low-level-claim'),
      legacyWorkspaceRoot: fixture.legacyRoot,
    });

    expect(store.claimNextQueuedTurn({
      conversationId,
    })).toMatchObject({
      status: 'workspace_wait',
      wait_reason: 'workspace_provisioning',
    });

    createConversationWorkspaceProvisioner({
      database: fixture.database,
      ...conversationWorkspaceOptions(fixture, 'low-level-claim'),
    }).ensure(conversationId);
    expect(() => store.claimNextQueuedTurn({
      conversationId,
    })).toThrow('ready conversation workspace fence');
    expect(fixture.database.prepare(`
      SELECT status
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(accepted.background_execution_turn_id)).toEqual({ status: 'queued' });

    fixture.database.close();
  });

  test('keeps the explicit legacy shared root for compatibility queued conversations', async () => {
    const fixture = createFixture();
    const accepted = acceptQueuedInbound(
      fixture.database,
      normalEnvelope('compat', { chatId: 'chat-compat' }),
      {
        now: () => '2026-07-24T01:01:00Z',
        generateId: deterministicIds('compat'),
      },
    );
    const workspaces = [];
    const service = createExecutorService({
      database: fixture.database,
      adapter: {
        getWorkspaceAccess() {
          return {
            root: path.join(fixture.directory, 'adapter-must-not-select-root'),
            mode: 'writable',
            read_only_enforced: false,
            authority: 'provider_sandbox',
          };
        },
        async *execute(context) {
          workspaces.push(context.workspace);
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-compat',
      now: () => '2026-07-24T01:01:01Z',
      generateId: deterministicIds('executor-compat'),
      workspaceRoot: fixture.legacyRoot,
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(workspaces).toEqual([
      expect.objectContaining({
        binding_kind: 'legacy_shared',
        workspace_generation: 0,
        workspace_root: fixture.legacyRoot,
      }),
    ]);

    await service.close();
    fixture.database.close();
  });

  test('runs sibling roots concurrently and does not let one uncertain lease block the other', async () => {
    const fixture = createFixture();
    const first = acceptDetached(fixture.database, 'sibling-first');
    const second = acceptDetached(fixture.database, 'sibling-second');
    installFoundationWorkspaceTables(fixture.database);
    const firstConversation = executionConversationId(fixture.database, first);
    const secondConversation = executionConversationId(fixture.database, second);
    const firstRoot = path.join(fixture.workspacesRoot, 'first');
    const secondRoot = path.join(fixture.workspacesRoot, 'second');
    fs.mkdirSync(firstRoot);
    fs.mkdirSync(secondRoot);
    putWorkspaceBinding(fixture.database, {
      conversationId: firstConversation,
      root: firstRoot,
      state: 'ready',
    });
    putWorkspaceBinding(fixture.database, {
      conversationId: secondConversation,
      root: secondRoot,
      state: 'ready',
    });
    const release = deferred();
    const started = [];
    const firstStarted = deferred();
    const secondStarted = deferred();
    const service = createExecutorService({
      database: fixture.database,
      adapter: {
        getWorkspaceAccess() {
          return { mode: 'writable', read_only_enforced: false, authority: 'provider_sandbox' };
        },
        async *execute(context) {
          started.push(context.workspace.workspace_root);
          if (started.length === 1) firstStarted.resolve();
          if (started.length === 2) secondStarted.resolve();
          await release.promise;
          yield {
            kind: 'text_snapshot',
            payload: { text: 'done', end_offset: 4 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-siblings',
      now: () => '2026-07-24T01:02:00Z',
      generateId: deterministicIds('executor-siblings'),
      workspaceRoot: fixture.legacyRoot,
      maxResidentExecutorsPerBot: 2,
    });

    const firstRun = service.runNext();
    await firstStarted.promise;
    const firstLease = fixture.database.prepare(`
      SELECT workspace_lease_id
      FROM runtime_workspace_leases
      WHERE holder_conversation_id = ? AND state = 'active'
    `).get(firstConversation);
    fixture.database.prepare(`
      UPDATE runtime_workspace_leases
      SET state = 'uncertain', updated_at = '2026-07-24T01:02:00Z'
      WHERE workspace_lease_id = ?
    `).run(firstLease.workspace_lease_id);

    const secondRun = service.runNext();
    await secondStarted.promise;
    expect(new Set(started)).toEqual(new Set([
      fs.realpathSync.native(firstRoot),
      fs.realpathSync.native(secondRoot),
    ]));
    expect(fixture.database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE workspace_lease_id = ?
    `).get(firstLease.workspace_lease_id)).toEqual({ state: 'uncertain' });

    fixture.database.prepare(`
      UPDATE runtime_workspace_leases
      SET state = 'active', updated_at = '2026-07-24T01:02:00Z'
      WHERE workspace_lease_id = ?
    `).run(firstLease.workspace_lease_id);
    release.resolve();
    await expect(firstRun).resolves.toMatchObject({ status: 'completed' });
    await expect(secondRun).resolves.toMatchObject({ status: 'completed' });

    await service.close();
    fixture.database.close();
  });

  test('fences reservation and claim to the same durable workspace generation', () => {
    const fixture = createFixture();
    const accepted = acceptDetached(fixture.database, 'generation-cas');
    installFoundationWorkspaceTables(fixture.database);
    const conversationId = executionConversationId(fixture.database, accepted);
    const firstRoot = path.join(fixture.workspacesRoot, 'generation-one');
    const secondRoot = path.join(fixture.workspacesRoot, 'generation-two');
    fs.mkdirSync(firstRoot);
    fs.mkdirSync(secondRoot);
    putWorkspaceBinding(fixture.database, {
      conversationId,
      root: firstRoot,
      state: 'ready',
    });
    const store = createExecutorStore({
      database: fixture.database,
      provider: 'claude',
      serviceInstanceId: 'executor-generation-cas',
      now: () => '2026-07-24T01:02:30Z',
      generateId: deterministicIds('executor-generation-cas'),
    });
    const binding = store.resolveConversationWorkspaceBinding(
      conversationId,
      { legacyWorkspaceRoot: fixture.legacyRoot },
    );
    const workspaceAccess = Object.freeze({
      binding_kind: binding.binding_kind,
      claimable: true,
      mode: 'writable',
      read_only_enforced: false,
      workspace_generation: binding.workspace_generation,
      workspace_id: binding.workspace_id,
      workspace_root: binding.workspace_root,
      workspace_state: binding.workspace_state,
    });
    const reservation = store.reserveNextExecutor({
      maxResidentExecutorsPerBot: 20,
      workspaceAccessByConversation: new Map([[conversationId, workspaceAccess]]),
    });
    expect(reservation).toMatchObject({ status: 'ready', conversation_id: conversationId });

    let mutationRejected = false;
    try {
      fixture.database.prepare(`
        UPDATE runtime_conversation_workspaces
        SET workspace_root = ?, base_snapshot_root = ?, generation = 2
        WHERE conversation_id = ?
      `).run(secondRoot, secondRoot, conversationId);
    } catch {
      // Foundation's immutable-ready trigger is the first line of defense after integration.
      mutationRejected = true;
    }
    if (!mutationRejected) {
      expect(() => store.claimNextQueuedTurn({
        conversationId,
        legacyWorkspaceRoot: fixture.legacyRoot,
        requireResident: true,
        workspaceAccess,
        workspaceLease: reservation.workspace,
      })).toThrow(/workspace binding changed after executor reservation/i);
    }
    store.releaseWorkspaceReservation(reservation.workspace);

    fixture.database.close();
  });

  test('restores Claude cwd and generation from durable bindings across service rebuild', async () => {
    const fixture = createFixture();
    const accepted = acceptDetached(fixture.database, 'claude-rebuild');
    installFoundationWorkspaceTables(fixture.database);
    const conversationId = executionConversationId(fixture.database, accepted);
    const workspaceRoot = path.join(fixture.workspacesRoot, 'claude-rebuild');
    fs.mkdirSync(workspaceRoot);
    putWorkspaceBinding(fixture.database, {
      conversationId,
      root: workspaceRoot,
      state: 'ready',
    });
    const firstQuery = createObservedClaudeQuery({ sessionId: 'claude-workspace-session' });
    const firstService = createExecutorService({
      database: fixture.database,
      adapter: createClaudeConversationAdapter({ query: firstQuery.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-rebuild-first',
      now: () => '2026-07-24T01:03:00Z',
      generateId: deterministicIds('executor-rebuild-first'),
      workspaceRoot: fixture.legacyRoot,
    });

    await expect(firstService.runNext()).resolves.toMatchObject({ status: 'completed' });
    expect(firstQuery.calls[0]).toMatchObject({
      cwd: fs.realpathSync.native(workspaceRoot),
      sandbox: {
        enabled: true,
        failIfUnavailable: true,
        allowUnsandboxedCommands: false,
      },
    });
    await firstService.close();

    const resumedQuery = createObservedClaudeQuery({ sessionId: 'claude-workspace-session' });
    const reopenedStore = createExecutorStore({
      database: fixture.database,
      provider: 'claude',
      serviceInstanceId: 'executor-conversation-workspace-rebuild-second',
      now: () => '2026-07-24T01:04:00Z',
      generateId: deterministicIds('executor-rebuild-second'),
    });
    const reopenedBinding = reopenedStore.resolveConversationWorkspaceBinding(
      conversationId,
      { legacyWorkspaceRoot: fixture.legacyRoot },
    );
    const lineage = fixture.database.prepare(`
      SELECT lineage_id, provider_native_id
      FROM runtime_lineages
      WHERE conversation_id = ?
    `).get(conversationId);
    const resumedAdapter = createClaudeConversationAdapter({ query: resumedQuery.query });
    const resumedRecords = [];
    for await (const record of resumedAdapter.execute({
      conversation_id: conversationId,
      turn_id: 'rebuild-probe-turn',
      lineage_id: lineage.lineage_id,
      provider_native_id: lineage.provider_native_id,
      input: { kind: 'text', text: 'resume after rebuild', attachments: [] },
      workspace: {
        binding_kind: reopenedBinding.binding_kind,
        workspace_id: reopenedBinding.workspace_id,
        workspace_root: reopenedBinding.workspace_root,
        workspace_generation: reopenedBinding.workspace_generation,
      },
    }, {
      async requestPermission() {
        return { behavior: 'deny', message: 'not used' };
      },
    })) {
      resumedRecords.push(record);
    }
    expect(resumedRecords).toContainEqual({
      type: 'turn_result',
      outcome: 'completed',
    });
    expect(resumedQuery.calls[0]).toMatchObject({
      cwd: fs.realpathSync.native(workspaceRoot),
      resume: 'claude-workspace-session',
    });
    expect(fixture.database.prepare(`
      SELECT workspace_root, generation
      FROM runtime_lineage_workspace_bindings
      WHERE conversation_id = ?
    `).get(conversationId)).toEqual({
      workspace_root: fs.realpathSync.native(workspaceRoot),
      generation: 1,
    });

    await resumedAdapter.close();
    fixture.database.close();
  });

  test('refuses to move a live Claude resident to another root or generation', async () => {
    const fixture = createFixture();
    const firstRoot = path.join(fixture.workspacesRoot, 'resident-first');
    const secondRoot = path.join(fixture.workspacesRoot, 'resident-second');
    fs.mkdirSync(firstRoot);
    fs.mkdirSync(secondRoot);
    const observed = createObservedClaudeQuery({ sessionId: 'claude-fixed-workspace-session' });
    const adapter = createClaudeConversationAdapter({ query: observed.query });
    const controls = {
      async requestPermission() {
        return { behavior: 'deny', message: 'not used' };
      },
    };
    const baseContext = {
      conversation_id: 'conversation-fixed-workspace',
      turn_id: 'turn-fixed-workspace-1',
      lineage_id: 'lineage-fixed-workspace',
      provider_native_id: null,
      input: { kind: 'text', text: 'first turn', attachments: [] },
      workspace: {
        binding_kind: 'conversation',
        workspace_id: 'workspace-fixed',
        workspace_root: fs.realpathSync.native(firstRoot),
        workspace_generation: 1,
      },
    };
    for await (const record of adapter.execute(baseContext, controls)) {
      record.acknowledge?.();
    }
    async function consumeChangedWorkspace(workspace) {
      for await (const _record of adapter.execute({
        ...baseContext,
        turn_id: 'turn-fixed-workspace-2',
        provider_native_id: 'claude-fixed-workspace-session',
        workspace,
      }, controls)) {
        // The adapter must reject before a second provider query can be created.
      }
    }

    await expect(consumeChangedWorkspace({
      ...baseContext.workspace,
      workspace_generation: 2,
    })).rejects.toMatchObject({
      code: 'workspace_generation_mismatch',
      side_effect_status: 'none',
    });
    await expect(consumeChangedWorkspace({
      ...baseContext.workspace,
      workspace_root: fs.realpathSync.native(secondRoot),
    })).rejects.toMatchObject({
      code: 'workspace_generation_mismatch',
      side_effect_status: 'none',
    });
    expect(observed.calls).toHaveLength(1);

    await adapter.close();
    fixture.database.close();
  });

  test('rejects caller options that weaken Claude workspace isolation', () => {
    expect(() => createClaudeConversationAdapter({
      query() {},
      queryOptions: { additionalDirectories: ['/tmp/outside'] },
    })).toThrow(/additionalDirectories is managed by Core workspace authority/);
    expect(() => createClaudeConversationAdapter({
      query() {},
      queryOptions: { sandbox: { failIfUnavailable: false } },
    })).toThrow(/cannot weaken Core workspace isolation/);
    expect(() => createClaudeConversationAdapter({
      query() {},
      queryOptions: { sandbox: { filesystem: { allowWrite: ['/tmp/outside'] } } },
    })).toThrow(/cannot weaken Core workspace isolation/);
  });

  test.each([
    ['parent traversal', '../outside.txt', false],
    ['absolute escape', null, false],
    ['symlink escape', 'escape/outside.txt', true],
  ])('rejects Claude %s before the write tool executes', async (_name, requestedPath, symlink) => {
    const fixture = createFixture();
    const suffix = _name.replaceAll(' ', '-');
    const outside = path.join(fixture.directory, 'outside');
    fs.mkdirSync(outside);
    if (symlink) fs.symlinkSync(outside, path.join(fixture.legacyRoot, 'escape'), 'dir');
    const writePath = requestedPath ?? path.join(outside, 'absolute.txt');
    acceptQueuedInbound(
      fixture.database,
      normalEnvelope(`write-${suffix}`, { chatId: `chat-write-${suffix}` }),
      {
        now: () => '2026-07-24T01:05:00Z',
        generateId: deterministicIds(`write-${suffix}`),
      },
    );
    let writeExecuted = false;
    let rejectedCode = null;
    const observed = createObservedClaudeQuery({
      sessionId: `claude-write-${suffix}`,
      async beforeResult(options) {
        try {
          await options.canUseTool('Write', { file_path: writePath, content: 'unsafe' }, {});
          writeExecuted = true;
        } catch (error) {
          rejectedCode = error.code;
        }
      },
    });
    const service = createExecutorService({
      database: fixture.database,
      adapter: createClaudeConversationAdapter({ query: observed.query }),
      provider: 'claude',
      serviceInstanceId: `executor-write-${suffix}`,
      now: () => '2026-07-24T01:05:01Z',
      generateId: deterministicIds(`executor-write-${suffix}`),
      workspaceRoot: fixture.legacyRoot,
    });

    await expect(service.runNext()).rejects.toThrow(/provider write escaped its workspace root/i);
    expect(writeExecuted).toBe(false);
    expect(rejectedCode).toBe('provider_context_invalid');

    await service.close();
    fixture.database.close();
  });

  test('rejects a Claude write after its workspace lease becomes stale', async () => {
    const fixture = createFixture();
    acceptQueuedInbound(
      fixture.database,
      normalEnvelope('stale-write', { chatId: 'chat-stale-write' }),
      {
        now: () => '2026-07-24T01:06:00Z',
        generateId: deterministicIds('stale-write'),
      },
    );
    let rejectedCode = null;
    const observed = createObservedClaudeQuery({
      sessionId: 'claude-stale-write',
      async beforeResult(options) {
        fixture.database.prepare(`
          UPDATE runtime_workspace_leases
          SET state = 'expired', released_at = '2026-07-24T01:06:01Z',
            updated_at = '2026-07-24T01:06:01Z'
          WHERE state = 'active'
        `).run();
        try {
          await options.canUseTool('Write', {
            file_path: path.join(fixture.legacyRoot, 'allowed.txt'),
            content: 'stale',
          }, {});
        } catch (error) {
          rejectedCode = error.code;
        }
      },
    });
    const service = createExecutorService({
      database: fixture.database,
      adapter: createClaudeConversationAdapter({ query: observed.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-stale-write',
      now: () => '2026-07-24T01:06:01Z',
      generateId: deterministicIds('executor-stale-write'),
      workspaceRoot: fixture.legacyRoot,
    });

    await expect(service.runNext()).rejects.toThrow(/workspace lease no longer matches/i);
    expect(rejectedCode).toBe('stale_workspace_lease');

    await service.close();
    fixture.database.close();
  });
});
