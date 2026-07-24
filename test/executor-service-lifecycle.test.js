import fs from 'node:fs';
import { createRequire } from 'node:module';
import { EventEmitter } from 'node:events';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, jest, test } from '@jest/globals';

const require = createRequire(new URL('../skills/comm-bridge/package.json', import.meta.url));
const Database = require('better-sqlite3');

import {
  createExecutorServiceHost,
  requestExecutorService,
} from '../runtime/executor/service-host.js';
import {
  prepareConversationWorkspaceOptions,
  runExecutorDaemon,
} from '../runtime/executor/daemon.js';
import { createExecutorPrerequisiteOwner } from '../runtime/executor/prerequisite-owner.js';
import {
  cleanupObsoleteLifecycleArtifacts,
  legacyLifecycleArtifactPaths,
  obsoleteHookBaseKeys,
} from '../runtime/migration/legacy-lifecycle-artifacts.js';
import { findResumableRuntimeUpgrade } from '../runtime/migration/upgrade-state.js';

const directories = [];
const hosts = [];

async function settleWithin(promise, timeoutMs, timeoutValue) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => { timer = setTimeout(() => resolve(timeoutValue), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

afterEach(async () => {
  await Promise.allSettled(hosts.splice(0).map((host) => host.close()));
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

function fixture() {
  const tempRoot = fs.existsSync('/tmp') ? fs.realpathSync('/tmp') : os.tmpdir();
  const directory = fs.mkdtempSync(path.join(tempRoot, 'zylos-executor-host-'));
  directories.push(directory);
  fs.mkdirSync(path.join(directory, 'runtime'), { recursive: true });
  fs.writeFileSync(path.join(directory, 'runtime', 'executor-start-fence.json'), JSON.stringify({
    contract: 'zylos.executor-start-fence@1',
    runtime_generation: 'executor_only',
    reconciled_at: '2026-07-21T00:00:00.000Z',
    issuance_kind: 'committed_reconciliation',
    upgrade_id: 'upgrade-executor-fixture',
    legacy_services_quiesced: true,
    legacy_registrations_absent: true,
    legacy_artifacts_reconciled: true,
  }));
  return {
    directory,
    database: new Database(path.join(directory, 'c4.db')),
    socketPath: path.join(directory, 'runtime', 'executor-service.sock'),
  };
}

function inertAdapter(provider = 'claude') {
  return Object.freeze({
    provider,
    provider_transport: 'injected_test_seam',
    async *execute() {},
    async close() { return []; },
  });
}

describe('executor service lifecycle host', () => {
  test('rejects a symlinked runtime parent for conversation workspace control paths', () => {
    const zylosDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-workspace-parent-'));
    const redirectedRuntime = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-workspace-redirect-'));
    directories.push(zylosDir, redirectedRuntime);
    fs.symlinkSync(redirectedRuntime, path.join(zylosDir, 'runtime'), 'dir');

    expect(() => prepareConversationWorkspaceOptions(zylosDir))
      .toThrow('Conversation workspace control path must be a real directory');
    expect(fs.readdirSync(redirectedRuntime)).toEqual([]);
  });

  test('dispatches a second executor run without awaiting a long first run', async () => {
    const state = fixture();
    let releaseFirst;
    let markFirstStarted;
    let markSecondStarted;
    const firstGate = new Promise((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise((resolve) => { markFirstStarted = resolve; });
    const secondStarted = new Promise((resolve) => { markSecondStarted = resolve; });
    let calls = 0;
    const service = {
      start: jest.fn(),
      runNext: jest.fn(() => {
        calls += 1;
        if (calls === 1) {
          markFirstStarted();
          return firstGate;
        }
        markSecondStarted();
        return Promise.resolve({ status: 'idle' });
      }),
      publishObservabilitySnapshot: jest.fn(() => ({ contract: 'fixture' })),
      close: jest.fn(async () => {}),
    };
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'codex',
      serviceInstanceId: 'service-fixture-detached-poll',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      pollIntervalMs: 10_000,
      maxConcurrentRuns: 2,
      createService: () => service,
    });
    hosts.push(host);

    const starting = host.start();
    await firstStarted;
    try {
      await expect(settleWithin(starting.then(() => 'started'), 100, 'blocked'))
        .resolves.toBe('started');
      await expect(settleWithin(secondStarted.then(() => 'started'), 100, 'blocked'))
        .resolves.toBe('started');
      expect(service.runNext).toHaveBeenCalledTimes(2);
    } finally {
      releaseFirst();
      await starting;
    }
  });

  test('reports background poll failures instead of discarding the provider error', async () => {
    const state = fixture();
    const pollFailure = new Error('provider hook rejected');
    let reportPollError;
    const pollErrorReported = new Promise((resolve) => { reportPollError = resolve; });
    const onPollError = jest.fn(reportPollError);
    let polls = 0;
    const service = {
      start: jest.fn(),
      runNext: jest.fn(async () => {
        polls += 1;
        if (polls > 1) throw pollFailure;
      }),
      publishObservabilitySnapshot: jest.fn(() => ({ contract: 'fixture' })),
      close: jest.fn(async () => {}),
    };
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'codex',
      serviceInstanceId: 'service-fixture-poll-error',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      pollIntervalMs: 5,
      onPollError,
      createService: () => service,
    });
    hosts.push(host);
    await host.start();

    await expect(settleWithin(pollErrorReported, 200, 'timeout')).resolves.toBe(pollFailure);
    expect(onPollError).toHaveBeenCalledTimes(1);
  });

  test('reports canonical Core identity and health and acknowledges graceful shutdown', async () => {
    const state = fixture();
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-1',
      hostId: 'host-fixture-1',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      pollIntervalMs: 10,
    });
    hosts.push(host);
    await host.start();

    await expect(requestExecutorService(state.socketPath, { action: 'health' }))
      .resolves.toMatchObject({
        ok: true,
        result: {
          executor: { provider: 'claude', service_instance_id: 'service-fixture-1' },
          snapshot: {
            contract: 'zylos.observability-snapshot',
            core_service_instance_id: 'service-fixture-1',
            service: {
              health: 'healthy',
              service_instance_id: 'service-fixture-1',
              host_id: 'host-fixture-1',
            },
          },
        },
      });

    await expect(requestExecutorService(state.socketPath, { action: 'shutdown' }))
      .resolves.toMatchObject({
        ok: true,
        result: { status: 'completed', service_instance_id: 'service-fixture-1' },
      });
    await host.closed;
    expect(fs.existsSync(state.socketPath)).toBe(false);
  });

  test('accepts bounded health responses larger than the control request limit', async () => {
    const state = fixture();
    const padding = 'x'.repeat(96 * 1024);
    const service = {
      start: jest.fn(),
      runNext: jest.fn(async () => ({ status: 'idle' })),
      publishObservabilitySnapshot: jest.fn(() => ({
        contract: 'zylos.observability-snapshot',
        padding,
      })),
      close: jest.fn(async () => {}),
    };
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'codex',
      serviceInstanceId: 'service-fixture-large-health',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      pollIntervalMs: 10_000,
      createService: () => service,
    });
    hosts.push(host);
    await host.start();

    await expect(requestExecutorService(state.socketPath, { action: 'health' }))
      .resolves.toMatchObject({
        ok: true,
        result: {
          executor: { service_instance_id: 'service-fixture-large-health' },
          snapshot: { padding },
        },
      });
    await expect(requestExecutorService(
      state.socketPath,
      { action: 'health' },
      { maxResponseBytes: 64 * 1024 },
    )).rejects.toMatchObject({ code: 'EMSGSIZE' });
    await expect(requestExecutorService(state.socketPath, { action: 'health' }))
      .resolves.toMatchObject({ ok: true });
  });

  test('resolves and submits channel interactions through the running executor owner', async () => {
    const state = fixture();
    const resolution = {
      platform_message_id: 'platform-main-card-A',
      mapping: { mapping_id: 'mapping-A' },
      request_scope: { channel: 'feishu' },
      interactions: [{ interaction_id: 'interaction-A' }],
    };
    const answerResult = {
      status: 'accepted',
      handoff_state: 'pending',
      handoff_id: 'handoff-A',
    };
    const service = {
      start: jest.fn(),
      runNext: jest.fn(async () => ({ status: 'idle' })),
      publishObservabilitySnapshot: jest.fn(() => ({ contract: 'fixture' })),
      resolveInteractionTarget: jest.fn(() => resolution),
      submitInteractionAnswer: jest.fn(() => answerResult),
      deliverInteractionAnswer: jest.fn(async () => ({ acknowledgement: { resumed: true } })),
      close: jest.fn(async () => {}),
    };
    const onPollError = jest.fn();
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'codex',
      serviceInstanceId: 'service-fixture-channel-interaction',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      pollIntervalMs: 10_000,
      onPollError,
      createService: () => service,
    });
    hosts.push(host);
    await host.start();

    const target = {
      region: 'cn',
      tenantId: 'tenant-A',
      channel: 'feishu',
      botId: 'bot-A',
      platformMessageId: 'platform-main-card-A',
      mappingId: 'mapping-A',
      interactionId: 'interaction-A',
    };
    await expect(requestExecutorService(state.socketPath, {
      action: 'resolve_interaction',
      target,
    })).resolves.toEqual({ ok: true, result: resolution });
    expect(service.resolveInteractionTarget).toHaveBeenCalledWith(target);

    const answer = { contract: 'zylos.interaction-answer', interaction_id: 'interaction-A' };
    const sourceEvidence = { platformMessageId: 'platform-main-card-A' };
    await expect(requestExecutorService(state.socketPath, {
      action: 'submit_interaction_answer',
      answer,
      source_evidence: sourceEvidence,
    })).resolves.toEqual({ ok: true, result: answerResult });
    expect(service.submitInteractionAnswer).toHaveBeenCalledWith(answer, sourceEvidence);
    await new Promise((resolve) => setImmediate(resolve));
    expect(service.deliverInteractionAnswer).toHaveBeenCalledWith('handoff-A');
    expect(onPollError).not.toHaveBeenCalled();
  });

  test('queries and stops detached work through the task identity control seam', async () => {
    const state = fixture();
    const task = {
      background_task_id: 'background-task-control-A',
      state: 'running',
      side_effect_status: 'none',
    };
    const service = {
      start: jest.fn(),
      runNext: jest.fn(async () => ({ status: 'idle' })),
      publishObservabilitySnapshot: jest.fn(() => ({ contract: 'fixture' })),
      getBackgroundTask: jest.fn(() => task),
      stopBackgroundTask: jest.fn(async () => ({
        status: 'stopped',
        background_task_id: task.background_task_id,
      })),
      close: jest.fn(async () => {}),
    };
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'codex',
      serviceInstanceId: 'service-fixture-background-control',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      createService: () => service,
    });
    hosts.push(host);
    await host.start();

    await expect(requestExecutorService(state.socketPath, {
      action: 'get_background_task',
      background_task_id: task.background_task_id,
    })).resolves.toEqual({ ok: true, result: task });
    await expect(requestExecutorService(state.socketPath, {
      action: 'stop_background_task',
      background_task_id: task.background_task_id,
      stop_id: 'stop-background-control-A',
    })).resolves.toEqual({
      ok: true,
      result: {
        status: 'stopped',
        background_task_id: task.background_task_id,
      },
    });
    expect(service.getBackgroundTask).toHaveBeenCalledWith(task.background_task_id);
    expect(service.stopBackgroundTask).toHaveBeenCalledWith({
      action: 'stop_background_task',
      background_task_id: task.background_task_id,
      stop_id: 'stop-background-control-A',
    });
  });

  test('refuses to replace a non-socket control path', async () => {
    const state = fixture();
    fs.mkdirSync(path.dirname(state.socketPath), { recursive: true });
    fs.writeFileSync(state.socketPath, 'preserve-me');
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-2',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
    });
    hosts.push(host);

    await expect(host.start()).rejects.toThrow('non-socket control path');
    expect(fs.readFileSync(state.socketPath, 'utf8')).toBe('preserve-me');
  });

  test('refuses to replace a live executor control socket', async () => {
    const state = fixture();
    fs.mkdirSync(path.dirname(state.socketPath), { recursive: true });
    const existing = net.createServer((socket) => {
      socket.once('data', () => socket.end('{"ok":true}\n'));
    });
    await new Promise((resolve, reject) => {
      existing.once('error', reject);
      existing.listen(state.socketPath, resolve);
    });
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-live-conflict',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
    });
    hosts.push(host);

    await expect(host.start()).rejects.toThrow('already active');
    expect(existing.listening).toBe(true);
    await host.close();
    expect(fs.existsSync(state.socketPath)).toBe(true);
    await expect(requestExecutorService(state.socketPath, { action: 'health' }))
      .resolves.toEqual({ ok: true });
    await new Promise((resolve, reject) => existing.close((error) => (error ? reject(error) : resolve())));
    fs.rmSync(state.socketPath, { force: true });
  });

  test('does not unlink a replacement that appears during stale-socket probing', async () => {
    const state = fixture();
    fs.mkdirSync(path.dirname(state.socketPath), { recursive: true });
    const stale = net.createServer();
    await new Promise((resolve, reject) => {
      stale.once('error', reject);
      stale.listen(state.socketPath, resolve);
    });
    const replacement = net.createServer((socket) => {
      socket.once('data', () => socket.end('{"ok":true,"owner":"replacement"}\n'));
    });
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-probe-race',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      probeControl: async () => {
        await new Promise((resolve, reject) => stale.close((error) => (error ? reject(error) : resolve())));
        await new Promise((resolve, reject) => {
          replacement.once('error', reject);
          replacement.listen(state.socketPath, resolve);
        });
        throw Object.assign(new Error('stale observation'), { code: 'ECONNREFUSED' });
      },
    });
    hosts.push(host);
    try {
      await expect(host.start()).rejects.toThrow('changed during stale-path probing');
      await expect(requestExecutorService(state.socketPath, { action: 'health' }))
        .resolves.toEqual({ ok: true, owner: 'replacement' });
    } finally {
      await new Promise((resolve, reject) => replacement.close((error) => (
        error ? reject(error) : resolve()
      )));
      fs.rmSync(state.socketPath, { force: true });
    }
  });

  test('marks reset and malformed mutating responses uncertain after dispatch', async () => {
    for (const response of [null, 'not-json\n']) {
      const state = fixture();
      fs.mkdirSync(path.dirname(state.socketPath), { recursive: true });
      const server = net.createServer((socket) => {
        socket.once('data', () => {
          if (response === null) socket.destroy();
          else socket.end(response);
        });
      });
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(state.socketPath, resolve);
      });
      try {
        await expect(requestExecutorService(state.socketPath, { action: 'upgrade', target: {} }))
          .rejects.toMatchObject({ outcome: 'unknown' });
      } finally {
        await new Promise((resolve, reject) => server.close((error) => (
          error ? reject(error) : resolve()
        )));
        fs.rmSync(state.socketPath, { force: true });
      }
    }
  });

  test('does not classify a failed interaction resolution as an unknown mutation', async () => {
    const state = fixture();
    fs.mkdirSync(path.dirname(state.socketPath), { recursive: true });
    const server = net.createServer((socket) => {
      socket.once('data', () => socket.destroy());
    });
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(state.socketPath, resolve);
    });
    try {
      await expect(requestExecutorService(state.socketPath, {
        action: 'resolve_interaction',
        target: {},
      })).rejects.not.toHaveProperty('outcome');
    } finally {
      await new Promise((resolve, reject) => server.close((error) => (
        error ? reject(error) : resolve()
      )));
      fs.rmSync(state.socketPath, { force: true });
    }
  });

  test('does not unlink a replacement socket that rebinds the control path', async () => {
    const state = fixture();
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-rebound-socket',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
    });
    hosts.push(host);
    await host.start();

    fs.unlinkSync(state.socketPath);
    const replacement = net.createServer((socket) => {
      socket.once('data', () => socket.end('{"ok":true,"owner":"replacement"}\n'));
    });
    await new Promise((resolve, reject) => {
      replacement.once('error', reject);
      replacement.listen(state.socketPath, resolve);
    });
    try {
      await host.close();
      expect(fs.existsSync(state.socketPath)).toBe(true);
      await expect(requestExecutorService(state.socketPath, { action: 'health' }))
        .resolves.toEqual({ ok: true, owner: 'replacement' });
    } finally {
      await new Promise((resolve, reject) => replacement.close((error) => (
        error ? reject(error) : resolve()
      )));
      fs.rmSync(state.socketPath, { force: true });
    }
  });

  test('an idle control client cannot block executor shutdown', async () => {
    const state = fixture();
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-idle-client',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
    });
    hosts.push(host);
    await host.start();
    const idleClient = net.createConnection(state.socketPath);
    await new Promise((resolve, reject) => {
      idleClient.once('connect', resolve);
      idleClient.once('error', reject);
    });

    try {
      const result = await settleWithin(host.close().then(() => 'closed'), 100, 'blocked');
      expect(result).toBe('closed');
      if (!idleClient.destroyed) {
        await new Promise((resolve) => idleClient.once('close', resolve));
      }
      expect(idleClient.destroyed).toBe(true);
    } finally {
      idleClient.destroy();
      await host.close();
    }
  });

  test('serializes executor upgrade control requests', async () => {
    const state = fixture();
    let releaseUpgrade;
    let markStarted;
    const upgradeGate = new Promise((resolve) => { releaseUpgrade = resolve; });
    const upgradeStarted = new Promise((resolve) => { markStarted = resolve; });
    let invocations = 0;
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-serialized-upgrade',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onUpgrade: async () => {
        invocations += 1;
        markStarted();
        await upgradeGate;
        return { state: 'rolled_back' };
      },
    });
    hosts.push(host);
    await host.start();
    const first = requestExecutorService(state.socketPath, { action: 'upgrade', target: {} });
    await upgradeStarted;
    const second = requestExecutorService(state.socketPath, { action: 'upgrade', target: {} });

    try {
      const result = await settleWithin(second, 100, { blocked: true });
      expect(result).toEqual({ ok: false, error: 'executor_lifecycle_operation_in_progress' });
      expect(invocations).toBe(1);
    } finally {
      releaseUpgrade();
      await Promise.allSettled([first, second]);
    }
  });

  test('joins an in-flight upgrade before closing executor resources', async () => {
    const state = fixture();
    let releaseUpgrade;
    let markStarted;
    const upgradeGate = new Promise((resolve) => { releaseUpgrade = resolve; });
    const upgradeStarted = new Promise((resolve) => { markStarted = resolve; });
    const cleanup = [];
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-upgrade-close-join',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onUpgrade: async () => {
        markStarted();
        await upgradeGate;
        cleanup.push('upgrade-complete');
        return { state: 'rolled_back' };
      },
      onClose: async () => { cleanup.push('resources-closed'); },
    });
    hosts.push(host);
    await host.start();
    const upgrade = requestExecutorService(state.socketPath, { action: 'upgrade', target: {} });
    await upgradeStarted;
    const closing = host.close();

    expect(await settleWithin(closing.then(() => 'closed'), 100, 'blocked')).toBe('blocked');
    expect(cleanup).toEqual([]);
    releaseUpgrade();
    await expect(closing).resolves.toBeUndefined();
    expect(cleanup).toEqual(['upgrade-complete', 'resources-closed']);
    await Promise.allSettled([upgrade]);
  });

  test('rejects new lifecycle work after resource shutdown begins', async () => {
    const state = fixture();
    let releaseClose;
    let markClosing;
    const closeGate = new Promise((resolve) => { releaseClose = resolve; });
    const closingStarted = new Promise((resolve) => { markClosing = resolve; });
    let upgrades = 0;
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-closing-rejects-upgrade',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onUpgrade: async () => { upgrades += 1; return { state: 'rolled_back' }; },
      onClose: async () => { markClosing(); await closeGate; },
    });
    hosts.push(host);
    await host.start();
    const closing = host.close();
    await closingStarted;

    await expect(requestExecutorService(state.socketPath, { action: 'upgrade', target: {} }))
      .resolves.toEqual({ ok: false, error: 'executor_service_closing' });
    expect(upgrades).toBe(0);
    releaseClose();
    await closing;
  });

  test('runs the owning resource cleanup when remote shutdown completes', async () => {
    const state = fixture();
    const cleanup = [];
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-resource-cleanup',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onClose: async () => cleanup.push('closed'),
    });
    hosts.push(host);
    await host.start();

    await requestExecutorService(state.socketPath, { action: 'shutdown' });
    await host.closed;
    expect(cleanup).toEqual(['closed']);
  });

  test('reports shutdown cleanup failure before acknowledging completion', async () => {
    const state = fixture();
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-shutdown-failure',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onClose: async () => { throw new Error('database close failed'); },
    });
    hosts.push(host);
    await host.start();

    await expect(requestExecutorService(state.socketPath, { action: 'shutdown' }))
      .resolves.toEqual({ ok: false, error: 'database close failed' });
    await expect(host.closed).resolves.toEqual({ ok: false, error: 'database close failed' });
  });

  test('routes upgrade requests through the installed runtime owner', async () => {
    const state = fixture();
    const requests = [];
    const host = createExecutorServiceHost({
      database: state.database,
      adapter: inertAdapter(),
      provider: 'claude',
      serviceInstanceId: 'service-fixture-upgrade',
      socketPath: state.socketPath,
      workspaceRoot: state.directory,
      onUpgrade: async (request) => {
        requests.push(request);
        return { success: true, state: 'committed', from: 'release-A', to: 'release-B' };
      },
    });
    hosts.push(host);
    await host.start();

    await expect(requestExecutorService(state.socketPath, {
      action: 'upgrade',
      target: { release: 'release-B' },
    })).resolves.toMatchObject({
      ok: true,
      result: { success: true, state: 'committed', to: 'release-B' },
    });
    expect(requests).toEqual([{ action: 'upgrade', target: { release: 'release-B' } }]);
    await host.closed;
    expect(fs.existsSync(state.socketPath)).toBe(false);
  });
});

describe('executor daemon resource ownership', () => {
  test('reconciles stale prerequisites before opening SQLite and starts children after upgrade probing', async () => {
    const state = fixture();
    const events = [];
    const database = { close: () => events.push('database-close') };
    let hostOptions;
    const owner = {
      async acquire() { events.push('prerequisites-acquire'); },
      async start() { events.push('prerequisites-start'); },
      health() { return { ok: true }; },
      async close() { events.push('prerequisites-close'); },
    };
    const host = {
      closed: Promise.resolve(),
      async start() { events.push('host-start'); },
      async close() {
        events.push('host-close');
        await hostOptions.onClose();
      },
    };

    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() {
        events.push('database-open');
        return database;
      },
      createPrerequisiteOwner: () => owner,
      hasResumableUpgrade: () => {
        events.push('upgrade-probe');
        return false;
      },
      createAdapter: () => {
        events.push('adapter-create');
        return inertAdapter();
      },
      createHost: (options) => {
        hostOptions = options;
        return host;
      },
    });

    expect(events).toEqual([
      'prerequisites-acquire',
      'database-open',
      'upgrade-probe',
      'prerequisites-start',
      'adapter-create',
      'host-start',
    ]);
    expect(hostOptions.conversationWorkspaceOptions).toMatchObject({
      workspaceStoreRoot: fs.realpathSync.native(path.join(
        state.directory,
        'runtime',
        'conversation-workspaces',
      )),
      baseSnapshotRoot: fs.realpathSync.native(path.join(
        state.directory,
        'runtime',
        'conversation-workspace-base-v1',
      )),
      baseSnapshotRef: 'zylos-empty-conversation-workspace@1',
      snapshotFiles: [],
    });
    expect(fs.statSync(hostOptions.conversationWorkspaceOptions.workspaceStoreRoot).mode & 0o777)
      .toBe(0o700);
    expect(fs.statSync(hostOptions.conversationWorkspaceOptions.baseSnapshotRoot).mode & 0o777)
      .toBe(0o500);
    await daemon.close();
    expect(events.slice(-3)).toEqual([
      'host-close',
      'prerequisites-close',
      'database-close',
    ]);
  });

  test('exits for supervisor restart after a resumed rollback restores the old release', async () => {
    const state = fixture();
    const events = [];
    const database = { close: () => events.push('database-close') };
    const upgradeHandler = async () => ({ state: 'committed' });
    upgradeHandler.resumeBlocking = async () => {
      events.push('upgrade-resume');
      return { state: 'rolled_back' };
    };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => { throw new Error('stale target adapter must not be created'); },
      createUpgradeHandler: () => upgradeHandler,
      hasResumableUpgrade: () => true,
      createHost: () => { throw new Error('stale target host must not be created'); },
    });

    expect(daemon.restartRequired).toBe(true);
    expect(events).toEqual(['upgrade-resume', 'database-close']);
  });

  test('exits for supervisor restart after a resumed upgrade commits', async () => {
    const state = fixture();
    const events = [];
    const database = { close: () => events.push('database-close') };
    const upgradeHandler = async () => ({ state: 'committed' });
    upgradeHandler.resumeBlocking = async () => {
      events.push('upgrade-resume');
      return { state: 'committed', success: true };
    };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => { throw new Error('old adapter must not be created'); },
      createUpgradeHandler: () => upgradeHandler,
      hasResumableUpgrade: () => true,
      createHost: () => { throw new Error('old host must not be created'); },
    });

    expect(daemon.restartRequired).toBe(true);
    expect(events).toEqual(['upgrade-resume', 'database-close']);
  });

  test('resumes an orphaned durable upgrade plan before starting normal runtime work', async () => {
    const state = fixture();
    fs.mkdirSync(path.join(state.directory, 'runtime', 'upgrade-plans'), { recursive: true });
    fs.writeFileSync(path.join(state.directory, 'runtime', 'upgrade-plans', 'orphan.json'), '{}\n');
    const events = [];
    const database = { close: () => events.push('database-close') };
    const upgradeHandler = async () => ({ state: 'committed' });
    upgradeHandler.resumeBlocking = async () => {
      events.push('upgrade-resume');
      return { state: 'rolled_back' };
    };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => { throw new Error('normal adapter must not be created'); },
      createUpgradeHandler: () => upgradeHandler,
      createHost: () => { throw new Error('normal host must not be created'); },
    });

    expect(daemon.restartRequired).toBe(true);
    expect(events).toEqual(['upgrade-resume', 'database-close']);
  });

  test('fails closed when durable-upgrade probing fails', async () => {
    const state = fixture();
    const events = [];
    const database = { close: () => events.push('database-close') };
    await expect(runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      hasResumableUpgrade: () => { throw new Error('upgrade probe unavailable'); },
      createAdapter: () => { throw new Error('normal adapter must not be created'); },
      createPrerequisiteOwner: () => ({
        async acquire() { events.push('prerequisites-acquire'); },
        async start() { throw new Error('prerequisites must not start'); },
        health() { return { ok: true }; },
        async close() { events.push('prerequisites-close'); },
      }),
      createHost: () => { throw new Error('normal host must not be created'); },
    })).rejects.toThrow('upgrade probe unavailable');
    expect(events).toEqual([
      'prerequisites-acquire',
      'database-close',
      'prerequisites-close',
    ]);
  });

  test('does not start executor prerequisites without the one-time reconciliation fence', async () => {
    const state = fixture();
    fs.rmSync(path.join(state.directory, 'runtime', 'executor-start-fence.json'));
    const events = [];
    const database = { close: () => events.push('database-close') };
    await expect(runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => { throw new Error('normal adapter must not be created'); },
      createPrerequisiteOwner: () => { throw new Error('prerequisites must not start'); },
      createHost: () => { throw new Error('normal host must not be created'); },
    })).rejects.toThrow('one-time runtime reconciliation');
    expect(events).toEqual([]);
  });

  test('recovers a committed upgrade until postcommit cleanup is durable', async () => {
    const state = fixture();
    const events = [];
    state.database.exec(`
      CREATE TABLE runtime_upgrade_runs (upgrade_id TEXT, scope_kind TEXT, bot_id TEXT, state TEXT, state_version INTEGER, created_at TEXT);
      CREATE TABLE runtime_upgrade_effects (upgrade_id TEXT, step_key TEXT, state TEXT);
      INSERT INTO runtime_upgrade_runs VALUES ('upgrade-pending', 'installation', NULL, 'committed', 1, '2026-07-21T00:00:00.000Z');
    `);
    expect(findResumableRuntimeUpgrade(state.database)).toMatchObject({
      upgrade_id: 'upgrade-pending', state: 'committed',
    });
    const database = state.database;
    const close = database.close.bind(database);
    database.close = () => { events.push('database-close'); close(); };
    const handler = async () => ({ state: 'committed' });
    handler.resumeBlocking = async () => { events.push('upgrade-resume'); return { state: 'committed' }; };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory, Database: function DatabaseFixture() { return database; },
      createUpgradeHandler: () => handler,
      createHost: () => { throw new Error('host must not start'); },
    });
    expect(daemon.restartRequired).toBe(true);
    expect(events).toEqual(['upgrade-resume', 'database-close']);
  });

  test('ignores a stale terminal upgrade plan after durable cleanup', async () => {
    const state = fixture();
    fs.mkdirSync(path.join(state.directory, 'runtime', 'upgrade-plans'), { recursive: true });
    fs.writeFileSync(path.join(state.directory, 'runtime', 'upgrade-plans', 'finished.json'), '{}\n');
    const events = [];
    state.database.exec(`
      CREATE TABLE runtime_upgrade_runs (upgrade_id TEXT, scope_kind TEXT, bot_id TEXT, state TEXT, state_version INTEGER, created_at TEXT);
      CREATE TABLE runtime_upgrade_effects (upgrade_id TEXT, step_key TEXT, state TEXT);
      INSERT INTO runtime_upgrade_runs VALUES ('finished', 'installation', NULL, 'committed', 1, '2026-07-21T00:00:00.000Z');
      INSERT INTO runtime_upgrade_effects VALUES ('finished', 'postcommit-cleanup', 'completed');
    `);
    expect(findResumableRuntimeUpgrade(state.database)).toBeNull();
    const database = state.database;
    const host = { closed: Promise.resolve(), async start() { events.push('host-start'); }, async close() {} };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory, Database: function DatabaseFixture() { return database; },
      createAdapter: () => inertAdapter(),
      createPrerequisiteOwner: () => ({
        async acquire() {},
        async start() {},
        health() { return { ok: true }; },
        async close() {},
      }),
      createHost: () => host,
    });
    expect(events).toEqual(['host-start']);
    await daemon.close();
  });

  test('gives the service host sole ownership of closing the Core database', async () => {
    const state = fixture();
    const events = [];
    const database = { close: () => events.push('database-close') };
    let hostOptions;
    const host = {
      closed: Promise.resolve(),
      async start() { events.push('host-start'); },
      async close() {
        events.push('host-close');
        await hostOptions.onClose();
      },
    };
    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => inertAdapter(),
      createUpgradeHandler: () => async () => ({ state: 'committed' }),
      createPrerequisiteOwner: () => ({
        async acquire() { events.push('prerequisites-acquire'); },
        async start() { events.push('prerequisites-start'); },
        health() { return { ok: true }; },
        async close() { events.push('prerequisites-close'); },
      }),
      createHost: (options) => {
        hostOptions = options;
        return host;
      },
    });

    await daemon.close();
    expect(events).toEqual([
      'prerequisites-acquire', 'prerequisites-start', 'host-start', 'host-close',
      'prerequisites-close', 'database-close',
    ]);
  });

  test('passes provider orphaned workspace isolation into the service host', async () => {
    const state = fixture();
    const database = state.database;
    const isolateOrphanedWorkspace = jest.fn();
    let hostOptions;
    const host = {
      closed: Promise.resolve(),
      async start() {},
      async close() {},
    };

    const daemon = await runExecutorDaemon({
      zylosDir: state.directory,
      Database: function DatabaseFixture() { return database; },
      createAdapter: () => ({
        ...inertAdapter('codex'),
        isolateOrphanedWorkspace,
      }),
      createUpgradeHandler: () => async () => ({ state: 'committed' }),
      createPrerequisiteOwner: () => ({
        async acquire() {},
        async start() {},
        health() { return { ok: true }; },
        async close() {},
      }),
      createHost: (options) => {
        hostOptions = options;
        return host;
      },
    });

    expect(hostOptions.isolateOrphanedWorkspace).toBe(isolateOrphanedWorkspace);
    await daemon.close();
  });
});

describe('executor prerequisite ownership', () => {
  test('owns scheduler and web-console children without starting retired runtime daemons', async () => {
    const state = fixture();
    const releasePath = path.join(state.directory, 'runtime', 'releases', 'executor-fixture');
    const scheduler = path.join(releasePath, 'skills', 'scheduler', 'scripts', 'daemon.js');
    const webConsole = path.join(releasePath, 'skills', 'web-console', 'scripts', 'server.js');
    fs.mkdirSync(path.dirname(scheduler), { recursive: true });
    fs.mkdirSync(path.dirname(webConsole), { recursive: true });
    fs.writeFileSync(scheduler, '');
    fs.writeFileSync(webConsole, '');
    const spawned = [];
    const childrenByPid = new Map();
    const liveGroups = new Set();
    let nextPid = 91_000;

    class ChildFixture extends EventEmitter {
      constructor(pid) {
        super();
        this.pid = pid;
      }
      exitCode = null;
      signalCode = null;
      kill(signal) {
        this.signalCode = signal;
        liveGroups.delete(this.pid);
        setImmediate(() => {
          this.emit('exit', null, signal);
          this.emit('close', null, signal);
        });
        return true;
      }
    }
    const identity = (pid, command = process.execPath) => ({
      pid,
      ppid: pid === process.pid ? process.ppid : process.pid,
      pgid: pid,
      uid: process.getuid?.() ?? 0,
      start_token: `fixture:${pid}`,
      command: [command],
    });
    const owner = createExecutorPrerequisiteOwner({
      zylosDir: state.directory,
      releasePath,
      spawnFn: (command, args, options) => {
        spawned.push({ command, args, options });
        const child = new ChildFixture(nextPid++);
        childrenByPid.set(child.pid, child);
        liveGroups.add(child.pid);
        setImmediate(() => {
          child.emit('spawn');
          setImmediate(() => {
            child.emit('message', {
              contract: 'zylos.prerequisite-child@1',
              type: 'ready',
              service: args[0] === scheduler ? 'scheduler' : 'web-console',
              pid: child.pid,
            });
          });
        });
        return child;
      },
      inspectProcessFn: (pid) => identity(pid),
      probePortFn: async () => ({ available: true, code: null }),
      signalProcessGroupFn: (pgid, signal) => childrenByPid.get(pgid)?.kill(signal),
      processGroupIsAliveFn: (pgid) => liveGroups.has(pgid),
      waitForProcessGroupExitFn: async (pgid) => !liveGroups.has(pgid),
    });

    await expect(owner.start()).resolves.toMatchObject({
      ok: true,
      services: ['scheduler', 'web-console'],
    });
    expect(spawned.map(({ args }) => args[0])).toEqual([scheduler, webConsole]);
    expect(spawned.every(({ options }) => (
      options.detached === true && options.stdio.at(-1) === 'ipc'
    ))).toBe(true);
    expect(JSON.stringify(spawned)).not.toMatch(/activity-monitor|c4-dispatcher|tmux/);
    await owner.close();
    expect(owner.health()).toMatchObject({ ok: true, services: [] });
  });
});

describe('one-time lifecycle cleanup', () => {
  test('removes exact obsolete artifacts only after durable commit', () => {
    const state = fixture();
    const artifacts = legacyLifecycleArtifactPaths(state.directory);
    for (const artifact of artifacts) {
      if (artifact.endsWith('activity-monitor')) {
        fs.mkdirSync(artifact, { recursive: true });
        fs.writeFileSync(path.join(artifact, 'legacy-service.js'), 'legacy');
      } else {
        fs.mkdirSync(path.dirname(artifact), { recursive: true });
        fs.writeFileSync(artifact, 'legacy');
      }
    }
    const codexHooks = path.join(state.directory, '.codex', 'hooks.json');
    fs.mkdirSync(path.dirname(codexHooks), { recursive: true });
    fs.writeFileSync(codexHooks, JSON.stringify({
      hooks: {
        SessionStart: [{ hooks: [
          {
            type: 'command',
            command: `node ${path.join(state.directory, '.codex', 'skills', 'comm-bridge', 'scripts', 'c4-session-init.js')}`,
          },
          { type: 'command', command: 'node retained-hook.js' },
        ] }],
      },
    }));
    const claudeSettings = path.join(state.directory, '.claude', 'settings.json');
    fs.writeFileSync(claudeSettings, JSON.stringify({
      hooks: {
        SessionStart: [{ hooks: [
          ...[...obsoleteHookBaseKeys()].map((key) => ({
            type: 'command', command: `node ${path.join(state.directory, '.claude', key)}`,
          })),
          { type: 'command', command: 'node retained-claude-hook.js' },
        ] }],
      },
    }));

    expect(() => cleanupObsoleteLifecycleArtifacts({
      zylosDir: state.directory,
      upgradeState: 'ready_to_commit',
    })).toThrow('durably committed');
    expect(artifacts.every((artifact) => fs.existsSync(artifact))).toBe(true);

    expect(cleanupObsoleteLifecycleArtifacts({
      zylosDir: state.directory,
      upgradeState: 'committed',
    })).toEqual({ removed: [codexHooks, claudeSettings, ...artifacts] });
    expect(artifacts.every((artifact) => !fs.existsSync(artifact))).toBe(true);
    expect(JSON.parse(fs.readFileSync(codexHooks, 'utf8'))).toEqual({
      hooks: {
        SessionStart: [{ hooks: [{ type: 'command', command: 'node retained-hook.js' }] }],
      },
    });
    expect(JSON.parse(fs.readFileSync(claudeSettings, 'utf8'))).toEqual({
      hooks: {
        SessionStart: [{ hooks: [{ type: 'command', command: 'node retained-claude-hook.js' }] }],
      },
    });
  });

  test('removes exact owned home-relative hooks from the supported flat Codex config', () => {
    const state = fixture();
    const homeDir = path.dirname(state.directory);
    const relativeRoot = path.basename(state.directory);
    const codexHooks = path.join(state.directory, '.codex', 'hooks.json');
    fs.mkdirSync(path.dirname(codexHooks), { recursive: true });
    fs.writeFileSync(codexHooks, JSON.stringify([
        {
          event: 'SessionStart',
          command: `node ~/${relativeRoot}/.codex/skills/activity-monitor/scripts/session-start-orchestrator.js`,
        },
        {
          event: 'PreToolUse',
          command: `node $HOME/${relativeRoot}/.claude/skills/activity-monitor/scripts/hook-activity.js`,
        },
        {
          event: 'PostToolUse',
          command: `node \${HOME}/${relativeRoot}/.claude/skills/zylos-memory/scripts/session-start-inject.js`,
        },
        {
          event: 'SessionStart',
          command: `node ~/${relativeRoot}/.codex/skills/activity-monitor/scripts/session-start-orchestrator.js.bak`,
        },
        { event: 'SessionStart', command: 'node retained-flat-hook.js' },
    ]));

    expect(cleanupObsoleteLifecycleArtifacts({
      zylosDir: state.directory,
      upgradeState: 'committed',
      homeDir,
    })).toEqual({ removed: [codexHooks] });
    expect(JSON.parse(fs.readFileSync(codexHooks, 'utf8'))).toEqual([
      {
        event: 'SessionStart',
        command: `node ~/${relativeRoot}/.codex/skills/activity-monitor/scripts/session-start-orchestrator.js.bak`,
      },
      { event: 'SessionStart', command: 'node retained-flat-hook.js' },
    ]);
  });

  test('fails closed rather than following a symlinked hook configuration', () => {
    const state = fixture();
    const externalDirectory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-external-hooks-'));
    const externalConfig = path.join(externalDirectory, 'hooks.json');
    fs.writeFileSync(externalConfig, JSON.stringify({
      hooks: {
        SessionStart: [{ hooks: [{
          type: 'command',
          command: `node ${path.join(state.directory, '.claude', 'skills', 'activity-monitor', 'scripts', 'session-start-orchestrator.js')}`,
        }] }],
      },
    }));
    const codexHooks = path.join(state.directory, '.codex', 'hooks.json');
    fs.mkdirSync(path.dirname(codexHooks), { recursive: true });
    fs.symlinkSync(externalConfig, codexHooks);

    try {
      expect(() => cleanupObsoleteLifecycleArtifacts({
        zylosDir: state.directory,
        upgradeState: 'committed',
      })).toThrow('symlinked hook configuration');
      expect(JSON.parse(fs.readFileSync(externalConfig, 'utf8')).hooks.SessionStart[0].hooks)
        .toHaveLength(1);
    } finally {
      fs.rmSync(externalDirectory, { recursive: true, force: true });
    }
  });

  test('fails closed rather than rewriting a multiply linked hook configuration', () => {
    const state = fixture();
    const externalDirectory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-external-hooks-'));
    const externalConfig = path.join(externalDirectory, 'hooks.json');
    fs.writeFileSync(externalConfig, JSON.stringify({
      hooks: {
        SessionStart: [{ hooks: [{
          type: 'command',
          command: `node ${path.join(state.directory, '.claude', 'skills', 'activity-monitor', 'scripts', 'session-start-orchestrator.js')}`,
        }] }],
      },
    }));
    const codexHooks = path.join(state.directory, '.codex', 'hooks.json');
    fs.mkdirSync(path.dirname(codexHooks), { recursive: true });
    fs.linkSync(externalConfig, codexHooks);

    try {
      expect(() => cleanupObsoleteLifecycleArtifacts({
        zylosDir: state.directory,
        upgradeState: 'committed',
      })).toThrow('unexpected hook configuration type');
      expect(JSON.parse(fs.readFileSync(externalConfig, 'utf8')).hooks.SessionStart[0].hooks)
        .toHaveLength(1);
    } finally {
      fs.rmSync(externalDirectory, { recursive: true, force: true });
    }
  });
});
