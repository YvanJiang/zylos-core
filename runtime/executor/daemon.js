#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import dotenv from 'dotenv';

import { createClaudeConversationAdapter } from '../providers/claude/conversation-adapter.js';
import { createCodexAppServerAdapter } from '../providers/codex-app-server-adapter.js';
import { createExecutorServiceHost } from './service-host.js';
import { createExecutorPrerequisiteOwner } from './prerequisite-owner.js';
import { assertExecutorStartFence } from './start-fence.js';

const require = createRequire(new URL('../../skills/comm-bridge/package.json', import.meta.url));

function readConfig(zylosDir) {
  const configPath = path.join(zylosDir, '.zylos', 'config.json');
  try {
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    return config && typeof config === 'object' ? config : {};
  } catch (error) {
    if (error?.code === 'ENOENT') return {};
    throw error;
  }
}

export function resolveCodexExecutionPolicy(environment = process.env) {
  return Object.freeze({
    approvalPolicy: environment.CODEX_APPROVAL_POLICY || 'on-request',
    sandbox: environment.CODEX_SANDBOX_MODE || 'workspace-write',
  });
}

export function createConfiguredProviderAdapter({ provider, zylosDir, environment = process.env }) {
  if (provider === 'claude') {
    return createClaudeConversationAdapter({
      queryOptions: { env: environment },
    });
  }
  if (provider === 'codex') {
    const { approvalPolicy, sandbox } = resolveCodexExecutionPolicy(environment);
    return createCodexAppServerAdapter({
      cwd: zylosDir,
      env: environment,
      approvalPolicy,
      sandbox,
    });
  }
  throw new Error(`Unsupported executor provider: ${provider}`);
}

function hasResumableRuntimeUpgrade({ database, zylosDir }) {
  let resumableRun = null;
  let hasUpgradeTable = false;
  if (typeof database?.prepare === 'function') {
    const table = database.prepare(`
      SELECT 1 FROM sqlite_master
      WHERE type = 'table' AND name = 'runtime_upgrade_runs'
    `).get();
    hasUpgradeTable = table !== undefined;
    if (hasUpgradeTable) {
      resumableRun = database.prepare(`
        SELECT run.upgrade_id FROM runtime_upgrade_runs AS run
        WHERE run.state NOT IN ('committed', 'rolled_back')
           OR (run.state = 'committed' AND NOT EXISTS (
             SELECT 1 FROM runtime_upgrade_effects AS effect
             WHERE effect.upgrade_id = run.upgrade_id
               AND effect.step_key = 'postcommit-cleanup'
               AND effect.state = 'completed'
           ))
        ORDER BY run.created_at ASC LIMIT 1
      `).get() ?? null;
    }
  }
  const planDirectory = path.join(zylosDir, 'runtime', 'upgrade-plans');
  let plans = [];
  try {
    plans = fs.readdirSync(planDirectory).filter((entry) => entry.endsWith('.json'));
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
  if (resumableRun !== null) return true;
  if (plans.length === 0) return false;
  if (!hasUpgradeTable) return true;
  for (const entry of plans) {
    const run = database.prepare(`
      SELECT state FROM runtime_upgrade_runs WHERE upgrade_id = ?
    `).get(entry.slice(0, -'.json'.length));
    if (run === undefined || !['committed', 'rolled_back'].includes(run.state)) return true;
  }
  return false;
}

async function loadInstalledExecutorUpgradeHandler() {
  const module = await import('../migration/installed-executor-upgrade.js');
  return module.createInstalledExecutorUpgradeHandler;
}

export async function runExecutorDaemon({
  zylosDir = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos'),
  serviceInstanceId = `executor-${crypto.randomUUID()}`,
  hostId = os.hostname(),
  Database = require('better-sqlite3'),
  createAdapter = createConfiguredProviderAdapter,
  createHost = createExecutorServiceHost,
  createUpgradeHandler = null,
  hasResumableUpgrade = hasResumableRuntimeUpgrade,
  createPrerequisiteOwner = createExecutorPrerequisiteOwner,
  maxConcurrentRuns = 20,
} = {}) {
  if (!path.isAbsolute(zylosDir) || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('ZYLOS_DIR must be an explicit absolute non-root path');
  }
  dotenv.config({ path: path.join(zylosDir, '.env'), override: false });
  const config = readConfig(zylosDir);
  const provider = process.env.ZYLOS_RUNTIME || config.runtime || 'claude';
  const currentReleasePath = path.resolve(import.meta.dirname, '..', '..');
  const currentPackage = JSON.parse(fs.readFileSync(
    path.join(currentReleasePath, 'package.json'), 'utf8',
  ));
  if (createUpgradeHandler !== null && typeof createUpgradeHandler !== 'function') {
    throw new TypeError('createUpgradeHandler must be a function or null');
  }
  if (typeof hasResumableUpgrade !== 'function') {
    throw new TypeError('hasResumableUpgrade must be a function');
  }
  assertExecutorStartFence({ zylosDir });
  const prerequisiteOwner = createPrerequisiteOwner({
    zylosDir,
    releasePath: currentReleasePath,
  });
  try {
    await prerequisiteOwner.acquire();
  } catch (error) {
    await prerequisiteOwner.close().catch(() => {});
    throw error;
  }
  const databasePath = path.join(zylosDir, 'comm-bridge', 'c4.db');
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });
  let database;
  try {
    database = new Database(databasePath);
  } catch (error) {
    await prerequisiteOwner.close().catch(() => {});
    throw error;
  }
  let databaseClosed = false;
  const closeDatabase = () => {
    if (databaseClosed) return;
    databaseClosed = true;
    database.close();
  };
  const upgradeHandlerOptions = {
    database,
    Database,
    zylosDir,
    currentReleasePath,
    currentReleaseRef: currentPackage.version,
    provider,
  };
  let upgradeHandler = null;
  async function getUpgradeHandler() {
    if (upgradeHandler !== null) return upgradeHandler;
    const factory = createUpgradeHandler ?? await loadInstalledExecutorUpgradeHandler();
    upgradeHandler = factory(upgradeHandlerOptions);
    return upgradeHandler;
  }
  async function onUpgrade(request) {
    const handler = await getUpgradeHandler();
    return handler(request);
  }
  let resumed = null;
  try {
    if (hasResumableUpgrade({ database, zylosDir })) {
      const handler = await getUpgradeHandler();
      if (typeof handler.resumeBlocking !== 'function') {
        throw new Error('Runtime upgrade recovery handler is unavailable.');
      }
      resumed = await handler.resumeBlocking();
      if (resumed !== null && !['committed', 'rolled_back'].includes(resumed?.state)) {
        throw new Error(
          `Runtime upgrade ${resumed?.state ?? 'unknown'} did not reach a safe terminal state.`,
        );
      }
    }
  } catch (error) {
    closeDatabase();
    await prerequisiteOwner.close().catch(() => {});
    throw error;
  }
  if (resumed !== null) {
    closeDatabase();
    await prerequisiteOwner.close();
    return Object.freeze({
      close: () => Promise.resolve(),
      closed: Promise.resolve(),
      host: null,
      provider,
      restartRequired: true,
      serviceInstanceId,
      upgrade: resumed,
    });
  }
  let host = null;
  try {
    await prerequisiteOwner.start();
    const adapter = createAdapter({ provider, zylosDir, environment: process.env });
    host = createHost({
      database,
      adapter,
      provider,
      serviceInstanceId,
      hostId,
      socketPath: path.join(zylosDir, 'runtime', 'executor-service.sock'),
      workspaceRoot: zylosDir,
      maxConcurrentRuns,
      releaseRef: process.env.ZYLOS_RELEASE_REF || null,
      upgradeId: process.env.ZYLOS_UPGRADE_ID || null,
      isolateOrphanedWorkspace: typeof adapter.isolateOrphanedWorkspace === 'function'
        ? adapter.isolateOrphanedWorkspace
        : null,
      onUpgrade,
      healthCheck: prerequisiteOwner.health,
      onClose: async () => {
        const failures = [];
        try { await prerequisiteOwner.close(); } catch (error) { failures.push(error); }
        try { closeDatabase(); } catch (error) { failures.push(error); }
        if (failures.length === 1) throw failures[0];
        if (failures.length > 1) throw new AggregateError(failures, 'Executor prerequisite shutdown failed.');
      },
    });
    await host.start();
  } catch (error) {
    if (host === null) {
      await prerequisiteOwner?.close().catch(() => {});
      closeDatabase();
    }
    else await host.close().catch(() => closeDatabase());
    throw error;
  }
  const close = () => host.close();
  return Object.freeze({ close, closed: host.closed, host, provider, serviceInstanceId });
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const daemon = await runExecutorDaemon();
  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    try {
      await daemon.close();
      process.exitCode = 0;
    } catch (error) {
      console.error(error);
      process.exitCode = 1;
    }
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
  const outcome = await daemon.closed;
  if (outcome?.ok === false) {
    console.error(outcome.error);
    process.exitCode = 1;
  }
}
