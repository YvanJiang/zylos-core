import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import {
  acceptNormalInbound,
  acceptQueuedInbound,
  initializeRuntimePersistence,
} from '../runtime/persistence/inbound-acceptance.js';
import {
  bindLineageWorkspaceInTransaction,
  createConversationWorkspaceProvisioner,
  getConversationWorkspaceBinding,
  getLineageWorkspaceBinding,
  requestConversationWorkspaceInTransaction,
} from '../runtime/workspace/conversation-workspace-provisioner.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function createFixture(prefix = 'zylos-conversation-workspace-') {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), prefix));
  temporaryDirectories.push(root);
  const databasePath = path.join(root, 'c4.db');
  return {
    root,
    databasePath,
    database: new Database(databasePath),
  };
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function normalEnvelope(suffix = 'workspace') {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.content = {
    kind: 'text',
    text: `workspace request ${suffix}`,
    attachments: [],
    task_summary: `workspace request ${suffix}`,
  };
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function admissionOptions(namespace, timestamp = '2026-07-24T01:00:00Z') {
  return {
    now: () => timestamp,
    generateId: deterministicIds(namespace),
  };
}

function acceptDetachedExecution(database, namespace, timestamp = '2026-07-24T01:00:00Z') {
  const accepted = acceptNormalInbound(
    database,
    normalEnvelope(namespace),
    admissionOptions(namespace, timestamp),
  );
  const task = database.prepare(`
    SELECT execution_conversation_id
    FROM runtime_background_tasks
    WHERE background_task_id = ?
  `).get(accepted.background_task_id);
  return {
    accepted,
    executionConversationId: task.execution_conversation_id,
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('detached conversation workspace admission', () => {
  test('atomically requests one workspace only for the detached execution conversation', () => {
    const { database } = createFixture();
    const envelope = normalEnvelope('detached-admission');
    const accepted = acceptNormalInbound(
      database,
      envelope,
      admissionOptions('detached-admission'),
    );

    const executionTask = database.prepare(`
      SELECT execution_conversation_id
      FROM runtime_background_tasks
      WHERE background_task_id = ?
    `).get(accepted.background_task_id);
    const binding = getConversationWorkspaceBinding(
      database,
      executionTask.execution_conversation_id,
    );

    expect(binding).toMatchObject({
      workspace_id: 'conversation-workspace-detached-admission-1',
      conversation_id: executionTask.execution_conversation_id,
      workspace_root: null,
      base_snapshot_ref: null,
      generation: 1,
      state: 'requested',
      wait_reason: 'workspace_provisioning',
      requested_at: '2026-07-24T01:00:00Z',
      error: null,
    });
    expect(getConversationWorkspaceBinding(database, accepted.conversation_id)).toBeNull();

    const replayEnvelope = structuredClone(envelope);
    replayEnvelope.trace_id = 'trace-detached-admission-replay';
    const replayed = acceptNormalInbound(database, replayEnvelope, {
      now: () => {
        throw new Error('idempotent replay must not allocate a workspace timestamp');
      },
      generateId: () => {
        throw new Error('idempotent replay must not allocate a workspace ID');
      },
    });
    expect(replayed).toMatchObject({
      deduplicated: true,
      background_task_id: accepted.background_task_id,
      background_execution_turn_id: accepted.background_execution_turn_id,
    });
    expect(getConversationWorkspaceBinding(
      database,
      executionTask.execution_conversation_id,
    )).toEqual(binding);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_conversation_workspaces
    `).get()).toEqual({ count: 1 });

    database.close();
  });

  test('keeps queued compatibility admission free of conversation workspace bindings', () => {
    const { database } = createFixture();
    const accepted = acceptQueuedInbound(
      database,
      normalEnvelope('queued-compatibility'),
      admissionOptions('queued-compatibility'),
    );

    expect(accepted.status).toBe('accepted');
    expect(getConversationWorkspaceBinding(database, accepted.conversation_id)).toBeNull();
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_conversation_workspaces
    `).get()).toEqual({ count: 0 });
    expect(() => database.transaction(() => (
      requestConversationWorkspaceInTransaction(database, {
        workspaceId: 'workspace-must-not-bind-queued-origin',
        conversationId: accepted.conversation_id,
        requestedAt: '2026-07-24T01:00:00Z',
      })
    )).immediate()).toThrow(/Only a detached execution conversation/);
    expect(() => database.prepare(`
      INSERT INTO runtime_conversation_workspaces (
        workspace_id, conversation_id, state, requested_at, updated_at
      ) VALUES (?, ?, 'requested', ?, ?)
    `).run(
      'workspace-direct-origin-insert',
      accepted.conversation_id,
      '2026-07-24T01:00:00Z',
      '2026-07-24T01:00:00Z',
    )).toThrow(/requires a detached execution conversation/);

    database.close();
  });

  test('rolls the requested binding back with the detached background admission', () => {
    const { database } = createFixture();
    initializeRuntimePersistence(database);
    database.exec(`
      CREATE TRIGGER force_detached_idempotency_failure
      BEFORE INSERT ON runtime_inbound_idempotency
      BEGIN
        SELECT RAISE(ABORT, 'forced detached idempotency persistence failure');
      END;
    `);

    expect(() => acceptNormalInbound(
      database,
      normalEnvelope('atomic-rollback'),
      admissionOptions('atomic-rollback'),
    )).toThrow(/forced detached idempotency persistence failure/);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_conversation_workspaces
    `).get()).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_background_tasks
    `).get()).toEqual({ count: 0 });

    database.close();
  });

  test('backfills requested and quarantined bindings for pre-existing detached work', () => {
    const { database, databasePath } = createFixture();
    const queued = acceptDetachedExecution(database, 'backfill-queued');
    const uncertain = acceptDetachedExecution(
      database,
      'backfill-uncertain',
      '2026-07-24T01:05:00Z',
    );
    database.prepare(`
      UPDATE runtime_background_tasks
      SET state = 'recovering', side_effect_status = 'unknown'
      WHERE execution_conversation_id = ?
    `).run(uncertain.executionConversationId);
    database.exec(`
      DROP TABLE runtime_lineage_workspace_bindings;
      DROP TABLE runtime_conversation_workspaces;
    `);

    initializeRuntimePersistence(database);

    expect(getConversationWorkspaceBinding(
      database,
      queued.executionConversationId,
    )).toMatchObject({
      workspace_id: expect.stringMatching(/^conversation-workspace-backfill-/),
      state: 'requested',
      workspace_root: null,
      wait_reason: 'workspace_provisioning',
    });
    expect(getConversationWorkspaceBinding(
      database,
      uncertain.executionConversationId,
    )).toMatchObject({
      workspace_id: expect.stringMatching(/^conversation-workspace-backfill-/),
      state: 'quarantined',
      workspace_root: null,
      wait_reason: 'workspace_quarantined',
      error: expect.objectContaining({
        code: 'workspace_runtime_uncertain',
        terminal: true,
      }),
    });
    database.close();
    const reopenedDatabase = new Database(databasePath);
    initializeRuntimePersistence(reopenedDatabase);
    expect(reopenedDatabase.prepare(`
      SELECT COUNT(*) AS count FROM runtime_conversation_workspaces
    `).get()).toEqual({ count: 2 });

    reopenedDatabase.close();
  });
});

describe('conversation workspace provisioning', () => {
  test('atomically activates a private snapshot and reopens the same ready binding idempotently', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'base-snapshot');
    const workspaceStoreRoot = path.join(fixture.root, 'conversation-workspaces');
    fs.mkdirSync(path.join(snapshotRoot, 'src'), { recursive: true });
    fs.mkdirSync(path.join(snapshotRoot, '.pm2'), { recursive: true });
    fs.writeFileSync(path.join(snapshotRoot, 'README.md'), 'safe snapshot\n');
    fs.writeFileSync(path.join(snapshotRoot, 'src', 'run.sh'), '#!/bin/sh\nexit 0\n', {
      mode: 0o755,
    });
    fs.writeFileSync(path.join(snapshotRoot, '.env'), 'SECRET=must-not-copy\n');
    fs.writeFileSync(path.join(snapshotRoot, 'runtime.sqlite'), 'must-not-copy\n');
    fs.writeFileSync(path.join(snapshotRoot, 'machine-local.json'), 'must-not-copy\n');
    fs.writeFileSync(path.join(snapshotRoot, '.pm2', 'dump.pm2'), 'must-not-copy\n');
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'ready-reopen',
    );

    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'git:0ae20c7c',
      snapshotFiles: ['README.md', path.join('src', 'run.sh')],
      now: () => '2026-07-24T01:01:00Z',
      generateId: deterministicIds('ready-reopen-provisioner'),
    });
    const ready = provisioner.ensure(executionConversationId);

    expect(ready).toMatchObject({
      conversation_id: executionConversationId,
      base_snapshot_root: fs.realpathSync.native(snapshotRoot),
      base_snapshot_ref: 'git:0ae20c7c',
      generation: 1,
      state: 'ready',
      wait_reason: null,
      claimable: true,
      ready_at: '2026-07-24T01:01:00Z',
      error: null,
    });
    expect(path.dirname(ready.workspace_root)).toBe(fs.realpathSync.native(workspaceStoreRoot));
    expect(fs.statSync(ready.workspace_root).mode & 0o777).toBe(0o700);
    expect(fs.readFileSync(path.join(ready.workspace_root, 'README.md'), 'utf8'))
      .toBe('safe snapshot\n');
    expect(fs.statSync(path.join(ready.workspace_root, 'src')).mode & 0o777).toBe(0o700);
    expect(fs.statSync(path.join(ready.workspace_root, 'src', 'run.sh')).mode & 0o777)
      .toBe(0o700);
    expect(fs.existsSync(path.join(ready.workspace_root, '.env'))).toBe(false);
    expect(fs.existsSync(path.join(ready.workspace_root, 'runtime.sqlite'))).toBe(false);
    expect(fs.existsSync(path.join(ready.workspace_root, 'machine-local.json'))).toBe(false);
    expect(fs.existsSync(path.join(ready.workspace_root, '.pm2'))).toBe(false);
    expect(provisioner.ensure(executionConversationId)).toEqual(ready);

    fixture.database.close();
    const reopenedDatabase = new Database(fixture.databasePath);
    const reopened = createConversationWorkspaceProvisioner({
      database: reopenedDatabase,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'git:a-different-current-default-must-not-rebind',
      snapshotFiles: [],
      now: () => '2026-07-24T01:02:00Z',
      generateId: deterministicIds('ready-reopened-provisioner'),
    });
    expect(reopened.ensure(executionConversationId)).toEqual(ready);
    expect(reopenedDatabase.prepare(`
      SELECT COUNT(*) AS count FROM runtime_conversation_workspaces
    `).get()).toEqual({ count: 1 });
    expect(reopenedDatabase.pragma('foreign_key_check')).toEqual([]);
    expect(reopenedDatabase.pragma('integrity_check')).toEqual([{ integrity_check: 'ok' }]);

    reopenedDatabase.close();
  });

  test('durably binds a lineage to the immutable ready root and generation', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'lineage-base');
    const workspaceStoreRoot = path.join(fixture.root, 'lineage-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(path.join(snapshotRoot, 'lineage.txt'), 'lineage workspace\n');
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'lineage-binding',
    );
    const lineageId = fixture.database.prepare(`
      SELECT lineage_id
      FROM runtime_lineages
      WHERE conversation_id = ? AND is_default = 1
    `).get(executionConversationId).lineage_id;
    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:lineage-binding',
      snapshotFiles: ['lineage.txt'],
      now: () => '2026-07-24T01:30:00Z',
      generateId: deterministicIds('lineage-binding-provisioner'),
    });
    const ready = provisioner.ensure(executionConversationId);
    const bind = () => fixture.database.transaction(() => (
      bindLineageWorkspaceInTransaction(fixture.database, {
        lineageId,
        conversationId: executionConversationId,
        boundAt: '2026-07-24T01:30:01Z',
      })
    )).immediate();

    expect(bind()).toEqual({
      lineage_id: lineageId,
      workspace_id: ready.workspace_id,
      conversation_id: executionConversationId,
      workspace_root: ready.workspace_root,
      generation: 1,
      bound_at: '2026-07-24T01:30:01Z',
    });
    expect(bind()).toEqual(getLineageWorkspaceBinding(fixture.database, lineageId));
    expect(() => fixture.database.prepare(`
      UPDATE runtime_conversation_workspaces
      SET workspace_root = ?, generation = 2
      WHERE conversation_id = ?
    `).run(
      path.join(workspaceStoreRoot, 'silently-rebound'),
      executionConversationId,
    )).toThrow(/(?:ready|lineage-bound) workspace identity is immutable/);
    expect(getConversationWorkspaceBinding(fixture.database, executionConversationId))
      .toMatchObject({
        workspace_root: ready.workspace_root,
        generation: 1,
        state: 'ready',
      });
    expect(() => fixture.database.prepare(`
      UPDATE runtime_lineage_workspace_bindings
      SET generation = 2
      WHERE lineage_id = ?
    `).run(lineageId)).toThrow(/lineage workspace binding is immutable/);
    expect(() => fixture.database.prepare(`
      DELETE FROM runtime_lineage_workspace_bindings
      WHERE lineage_id = ?
    `).run(lineageId)).toThrow(/lineage workspace binding is immutable/);
    expect(getLineageWorkspaceBinding(fixture.database, lineageId)).toMatchObject({
      workspace_root: ready.workspace_root,
      generation: 1,
    });

    fixture.database.close();
  });

  test('reconciles an activated root when SQLite ready commit crashes', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'activation-base');
    const workspaceStoreRoot = path.join(fixture.root, 'activation-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(path.join(snapshotRoot, 'work.txt'), 'activated before sqlite ready\n');
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'activation-crash',
    );
    fixture.database.exec(`
      CREATE TRIGGER fail_workspace_ready_commit
      BEFORE UPDATE OF state ON runtime_conversation_workspaces
      WHEN NEW.state = 'ready'
      BEGIN
        SELECT RAISE(ABORT, 'simulated ready commit crash');
      END;
    `);
    const crashing = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:activation-crash',
      snapshotFiles: ['work.txt'],
      now: () => '2026-07-24T02:00:00Z',
      generateId: deterministicIds('activation-crash-provisioner'),
    });

    expect(() => crashing.ensure(executionConversationId))
      .toThrow(/simulated ready commit crash/);
    const interrupted = fixture.database.prepare(`
      SELECT state, workspace_root, staging_root, last_error_json
      FROM runtime_conversation_workspaces
      WHERE conversation_id = ?
    `).get(executionConversationId);
    expect(interrupted).toMatchObject({
      state: 'provisioning',
      staging_root: expect.any(String),
      last_error_json: expect.stringContaining('simulated ready commit crash'),
    });
    expect(fs.existsSync(interrupted.workspace_root)).toBe(true);
    expect(fs.existsSync(interrupted.staging_root)).toBe(false);

    fixture.database.exec('DROP TRIGGER fail_workspace_ready_commit;');
    fixture.database.close();
    const reopenedDatabase = new Database(fixture.databasePath);
    const reopened = createConversationWorkspaceProvisioner({
      database: reopenedDatabase,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:activation-crash',
      snapshotFiles: [],
      now: () => '2026-07-24T02:00:01Z',
      generateId: deterministicIds('activation-crash-reopened'),
    });

    expect(reopened.ensure(executionConversationId)).toMatchObject({
      state: 'ready',
      workspace_root: interrupted.workspace_root,
      claimable: true,
      error: null,
    });
    reopenedDatabase.close();
  });

  test('reclaims an expired partial staging directory and resumes the same generation', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'partial-base');
    const workspaceStoreRoot = path.join(fixture.root, 'partial-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(path.join(snapshotRoot, 'complete.txt'), 'complete snapshot\n');
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'partial-reopen',
    );
    const interrupted = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:partial-reopen',
      snapshotFiles: ['complete.txt'],
      provisioningLeaseMs: 1_000,
      now: () => '2026-07-24T02:10:00Z',
      generateId: deterministicIds('partial-reopen-interrupted'),
      provisioningFault(step, { staging_root: stagingRoot }) {
        if (step === 'after_snapshot_copy') {
          fs.writeFileSync(path.join(stagingRoot, 'partial.txt'), 'incomplete\n');
          throw new Error('simulated process interruption during copy');
        }
      },
    });

    expect(() => interrupted.ensure(executionConversationId))
      .toThrow(/simulated process interruption during copy/);
    const partial = fixture.database.prepare(`
      SELECT state, generation, workspace_root, staging_root, last_error_json
      FROM runtime_conversation_workspaces
      WHERE conversation_id = ?
    `).get(executionConversationId);
    expect(partial).toMatchObject({
      state: 'provisioning',
      generation: 1,
      last_error_json: expect.stringContaining('simulated process interruption during copy'),
    });
    expect(fs.existsSync(path.join(partial.staging_root, 'partial.txt'))).toBe(true);
    expect(fs.existsSync(partial.workspace_root)).toBe(false);

    fixture.database.close();
    const reopenedDatabase = new Database(fixture.databasePath);
    const reopened = createConversationWorkspaceProvisioner({
      database: reopenedDatabase,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:partial-reopen',
      snapshotFiles: [],
      provisioningLeaseMs: 1_000,
      now: () => '2026-07-24T02:10:02Z',
      generateId: deterministicIds('partial-reopen-resumer'),
    });
    const ready = reopened.ensure(executionConversationId);

    expect(ready).toMatchObject({
      state: 'ready',
      generation: 1,
      workspace_root: partial.workspace_root,
      claimable: true,
      error: null,
    });
    expect(fs.existsSync(partial.staging_root)).toBe(false);
    expect(fs.readFileSync(path.join(ready.workspace_root, 'complete.txt'), 'utf8'))
      .toBe('complete snapshot\n');
    expect(fs.existsSync(path.join(ready.workspace_root, 'partial.txt'))).toBe(false);
    reopenedDatabase.close();
  });

  test('fences an expired provisioner after a new owner completes the same generation', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'owner-fence-base');
    const workspaceStoreRoot = path.join(fixture.root, 'owner-fence-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(path.join(snapshotRoot, 'owned.txt'), 'new owner wins\n');
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'owner-fence',
    );
    let takeover;
    const expiredOwner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:owner-fence',
      snapshotFiles: ['owned.txt'],
      provisioningLeaseMs: 1_000,
      now: () => '2026-07-24T02:20:00Z',
      generateId: deterministicIds('expired-owner'),
      provisioningFault(step) {
        if (step !== 'after_snapshot_copy') return;
        const newOwner = createConversationWorkspaceProvisioner({
          database: fixture.database,
          workspaceStoreRoot,
          baseSnapshotRoot: snapshotRoot,
          baseSnapshotRef: 'snapshot:must-not-rebind',
          snapshotFiles: [],
          provisioningLeaseMs: 1_000,
          now: () => '2026-07-24T02:20:02Z',
          generateId: deterministicIds('new-owner'),
        });
        takeover = newOwner.ensure(executionConversationId);
      },
    });

    expect(() => expiredOwner.ensure(executionConversationId))
      .toThrow(/provisioning ownership was lost/);
    expect(takeover).toMatchObject({
      state: 'ready',
      generation: 1,
      base_snapshot_ref: 'snapshot:owner-fence',
    });
    expect(expiredOwner.get(executionConversationId)).toEqual(takeover);
    expect(fs.readFileSync(path.join(takeover.workspace_root, 'owned.txt'), 'utf8'))
      .toBe('new owner wins\n');

    fixture.database.close();
  });

  test('rejects unsafe store and snapshot roots before filesystem side effects', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'validation-base');
    const workspaceStoreRoot = path.join(fixture.root, 'validation-store');
    const storeFile = path.join(fixture.root, 'not-a-store');
    const linkedSnapshot = path.join(fixture.root, 'linked-base');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(storeFile, 'not a directory\n');
    fs.symlinkSync(snapshotRoot, linkedSnapshot, 'dir');
    const common = {
      database: fixture.database,
      baseSnapshotRef: 'snapshot:validation',
      snapshotFiles: [],
      now: () => '2026-07-24T03:00:00Z',
      generateId: deterministicIds('validation'),
    };

    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot: 'relative-store',
      baseSnapshotRoot: snapshotRoot,
    })).toThrow(/workspaceStoreRoot must be an absolute directory/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot: '/',
      baseSnapshotRoot: snapshotRoot,
    })).toThrow(/workspaceStoreRoot must not be the filesystem root/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot: storeFile,
      baseSnapshotRoot: snapshotRoot,
    })).toThrow(/workspaceStoreRoot must be a non-symlink directory/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot,
      baseSnapshotRoot: 'relative-snapshot',
    })).toThrow(/baseSnapshotRoot must be an absolute directory/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot,
      baseSnapshotRoot: linkedSnapshot,
    })).toThrow(/baseSnapshotRoot must be a non-symlink directory/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      snapshotFiles: ['.env'],
    })).toThrow(/snapshotFiles entry is excluded/);
    expect(() => createConversationWorkspaceProvisioner({
      ...common,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      snapshotFiles: [path.join('..', 'escaped.txt')],
    })).toThrow(/snapshotFiles entry must be a canonical relative path/);
    expect(fs.readdirSync(workspaceStoreRoot)).toEqual([]);

    fixture.database.close();
  });

  test('fails durably on a snapshot symlink escape without copying the target', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'escape-base');
    const workspaceStoreRoot = path.join(fixture.root, 'escape-store');
    const outsideRoot = path.join(fixture.root, 'outside');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.mkdirSync(outsideRoot);
    fs.writeFileSync(path.join(outsideRoot, 'credential.txt'), 'never copy\n');
    fs.symlinkSync(
      path.join(outsideRoot, 'credential.txt'),
      path.join(snapshotRoot, 'escaped-credential.txt'),
    );
    const { executionConversationId } = acceptDetachedExecution(
      fixture.database,
      'snapshot-escape',
    );
    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:escape',
      snapshotFiles: ['escaped-credential.txt'],
      now: () => '2026-07-24T03:10:00Z',
      generateId: deterministicIds('snapshot-escape-provisioner'),
    });

    expect(() => provisioner.ensure(executionConversationId))
      .toThrow(/Base snapshot entry escaped its root|Base snapshot symlink is not allowed/);
    const failed = provisioner.get(executionConversationId);
    expect(failed).toMatchObject({
      state: 'failed',
      wait_reason: 'workspace_failed',
      claimable: false,
      generation: 1,
      error: {
        code: expect.stringMatching(/base_snapshot_(path_escape|symlink_rejected)/),
        terminal: true,
      },
    });
    expect(fs.existsSync(failed.workspace_root)).toBe(false);
    expect(fs.readFileSync(path.join(outsideRoot, 'credential.txt'), 'utf8'))
      .toBe('never copy\n');

    fixture.database.close();
  });

  test('quarantines path tampering and uncertain runtime evidence without deleting files', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'quarantine-base');
    const workspaceStoreRoot = path.join(fixture.root, 'quarantine-store');
    const escapedRoot = path.join(fixture.root, 'escaped-workspace');
    const escapedStaging = path.join(fixture.root, 'escaped-staging');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.mkdirSync(escapedRoot);
    fs.mkdirSync(escapedStaging);
    fs.writeFileSync(path.join(escapedRoot, 'evidence.txt'), 'preserve me\n');
    const tampered = acceptDetachedExecution(
      fixture.database,
      'path-tampering',
    );
    fixture.database.prepare(`
      UPDATE runtime_conversation_workspaces
      SET state = 'provisioning', workspace_root = ?, staging_root = ?,
        base_snapshot_root = ?, base_snapshot_ref = 'snapshot:tampered',
        base_snapshot_manifest_json = '[]',
        provisioning_owner = 'foreign-owner',
        provisioning_expires_at = '2026-07-24T03:19:00Z',
        provisioning_started_at = '2026-07-24T03:18:00Z',
        updated_at = '2026-07-24T03:18:00Z'
      WHERE conversation_id = ?
    `).run(
      escapedRoot,
      escapedStaging,
      snapshotRoot,
      tampered.executionConversationId,
    );
    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:quarantine',
      snapshotFiles: [],
      now: () => '2026-07-24T03:20:00Z',
      generateId: deterministicIds('quarantine-provisioner'),
    });

    expect(provisioner.reconcile()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        conversation_id: tampered.executionConversationId,
        state: 'quarantined',
        wait_reason: 'workspace_quarantined',
        claimable: false,
        error: expect.objectContaining({
          code: 'workspace_path_escape',
          terminal: true,
        }),
      }),
    ]));
    expect(fs.readFileSync(path.join(escapedRoot, 'evidence.txt'), 'utf8'))
      .toBe('preserve me\n');
    expect(fs.existsSync(escapedStaging)).toBe(true);

    const uncertain = acceptDetachedExecution(
      fixture.database,
      'runtime-uncertain',
      '2026-07-24T03:21:00Z',
    );
    const ready = provisioner.ensure(uncertain.executionConversationId);
    fixture.database.prepare(`
      UPDATE runtime_background_tasks
      SET state = 'recovering', side_effect_status = 'unknown'
      WHERE execution_conversation_id = ?
    `).run(uncertain.executionConversationId);
    expect(provisioner.get(uncertain.executionConversationId)).toMatchObject({
      workspace_root: ready.workspace_root,
      state: 'quarantined',
      wait_reason: 'workspace_quarantined',
      claimable: false,
      error: expect.objectContaining({
        code: 'workspace_runtime_uncertain',
        terminal: true,
      }),
    });
    expect(provisioner.reconcile()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        conversation_id: uncertain.executionConversationId,
        workspace_root: ready.workspace_root,
        state: 'quarantined',
        wait_reason: 'workspace_quarantined',
        claimable: false,
        error: expect.objectContaining({
          code: 'workspace_runtime_uncertain',
          terminal: true,
        }),
      }),
    ]));
    expect(fs.existsSync(ready.workspace_root)).toBe(true);
    expect(provisioner.ensure(uncertain.executionConversationId)).toMatchObject({
      state: 'quarantined',
      workspace_root: ready.workspace_root,
      claimable: false,
    });

    fixture.database.close();
  });

  test('quarantines requested and in-flight bindings immediately when runtime becomes uncertain', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'uncertain-race-base');
    const workspaceStoreRoot = path.join(fixture.root, 'uncertain-race-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });

    const requested = acceptDetachedExecution(
      fixture.database,
      'requested-uncertain',
    );
    fixture.database.prepare(`
      UPDATE runtime_background_tasks
      SET state = 'recovering', side_effect_status = 'unknown'
      WHERE execution_conversation_id = ?
    `).run(requested.executionConversationId);
    expect(getConversationWorkspaceBinding(
      fixture.database,
      requested.executionConversationId,
    )).toMatchObject({
      workspace_root: null,
      state: 'quarantined',
      wait_reason: 'workspace_quarantined',
      claimable: false,
      error: expect.objectContaining({ code: 'workspace_runtime_uncertain' }),
    });

    const inFlight = acceptDetachedExecution(
      fixture.database,
      'inflight-uncertain',
      '2026-07-24T03:25:00Z',
    );
    let stagingRoot;
    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:uncertain-race',
      snapshotFiles: [],
      now: () => '2026-07-24T03:25:01Z',
      generateId: deterministicIds('uncertain-race-provisioner'),
      provisioningFault(step, paths) {
        if (step !== 'after_snapshot_copy') return;
        stagingRoot = paths.staging_root;
        fixture.database.prepare(`
          UPDATE runtime_background_tasks
          SET state = 'recovering', side_effect_status = 'unknown'
          WHERE execution_conversation_id = ?
        `).run(inFlight.executionConversationId);
      },
    });

    expect(() => provisioner.ensure(inFlight.executionConversationId))
      .toThrow(/provisioning ownership was lost/);
    const quarantined = provisioner.get(inFlight.executionConversationId);
    expect(quarantined).toMatchObject({
      state: 'quarantined',
      wait_reason: 'workspace_quarantined',
      claimable: false,
      error: expect.objectContaining({ code: 'workspace_runtime_uncertain' }),
    });
    expect(fs.existsSync(stagingRoot)).toBe(true);
    expect(fs.existsSync(quarantined.workspace_root)).toBe(false);

    fixture.database.close();
  });

  test('retires only terminal, lease-free workspaces and retains their root for audit', () => {
    const fixture = createFixture();
    const snapshotRoot = path.join(fixture.root, 'retire-base');
    const workspaceStoreRoot = path.join(fixture.root, 'retire-store');
    fs.mkdirSync(snapshotRoot);
    fs.mkdirSync(workspaceStoreRoot, { mode: 0o700 });
    fs.writeFileSync(path.join(snapshotRoot, 'retained.txt'), 'retain after retire\n');
    const execution = acceptDetachedExecution(
      fixture.database,
      'retirement',
    );
    const provisioner = createConversationWorkspaceProvisioner({
      database: fixture.database,
      workspaceStoreRoot,
      baseSnapshotRoot: snapshotRoot,
      baseSnapshotRef: 'snapshot:retirement',
      snapshotFiles: ['retained.txt'],
      now: () => '2026-07-24T03:30:00Z',
      generateId: deterministicIds('retirement-provisioner'),
    });
    const ready = provisioner.ensure(execution.executionConversationId);

    expect(() => provisioner.retire({
      conversationId: execution.executionConversationId,
      reason: 'task completed',
    })).toThrow(/cannot retire while nonterminal_turn remains/);
    fixture.database.transaction(() => {
      fixture.database.prepare(`
        UPDATE runtime_turns
        SET state = 'completed', committed_at = '2026-07-24T03:31:00Z'
        WHERE conversation_id = ?
      `).run(execution.executionConversationId);
      fixture.database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'completed', wait_reason = NULL
        WHERE conversation_id = ?
      `).run(execution.executionConversationId);
      fixture.database.prepare(`
        UPDATE runtime_background_tasks
        SET state = 'completed', completed_at = '2026-07-24T03:31:00Z'
        WHERE execution_conversation_id = ?
      `).run(execution.executionConversationId);
    }).immediate();
    const retired = provisioner.retire({
      conversationId: execution.executionConversationId,
      reason: 'terminal task retention',
      retainUntil: '2026-08-23T03:31:00Z',
    });

    expect(retired).toMatchObject({
      state: 'retired',
      wait_reason: 'workspace_retired',
      claimable: false,
      workspace_root: ready.workspace_root,
      generation: 1,
      retired_at: '2026-07-24T03:30:00Z',
      retention_reason: 'terminal task retention',
      retain_until: '2026-08-23T03:31:00Z',
    });
    expect(fs.readFileSync(path.join(retired.workspace_root, 'retained.txt'), 'utf8'))
      .toBe('retain after retire\n');
    expect(provisioner.retire({
      conversationId: execution.executionConversationId,
      reason: 'idempotent replay',
    })).toEqual(retired);

    fixture.database.close();
  });
});
