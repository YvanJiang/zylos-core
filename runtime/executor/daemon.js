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
import { createInstalledExecutorUpgradeHandler } from '../migration/installed-executor-upgrade.js';
import { createExecutorServiceHost } from './service-host.js';
import { createExecutorPrerequisiteOwner } from './prerequisite-owner.js';

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

export function createConfiguredProviderAdapter({ provider, zylosDir, environment = process.env }) {
  if (provider === 'claude') {
    return createClaudeConversationAdapter({
      queryOptions: { cwd: zylosDir, env: environment },
    });
  }
  if (provider === 'codex') {
    return createCodexAppServerAdapter({
      cwd: zylosDir,
      env: environment,
      approvalPolicy: 'on-request',
      sandbox: 'workspace-write',
    });
  }
  throw new Error(`Unsupported executor provider: ${provider}`);
}

export async function runExecutorDaemon({
  zylosDir = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos'),
  serviceInstanceId = `executor-${crypto.randomUUID()}`,
  hostId = os.hostname(),
  Database = require('better-sqlite3'),
  createAdapter = createConfiguredProviderAdapter,
  createHost = createExecutorServiceHost,
  createUpgradeHandler = createInstalledExecutorUpgradeHandler,
  createPrerequisiteOwner = createExecutorPrerequisiteOwner,
} = {}) {
  if (!path.isAbsolute(zylosDir) || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('ZYLOS_DIR must be an explicit absolute non-root path');
  }
  dotenv.config({ path: path.join(zylosDir, '.env'), override: false });
  const config = readConfig(zylosDir);
  const provider = process.env.ZYLOS_RUNTIME || config.runtime || 'claude';
  const databasePath = path.join(zylosDir, 'comm-bridge', 'c4.db');
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });
  const database = new Database(databasePath);
  const currentReleasePath = path.resolve(import.meta.dirname, '..', '..');
  const currentPackage = JSON.parse(fs.readFileSync(
    path.join(currentReleasePath, 'package.json'), 'utf8',
  ));
  let databaseClosed = false;
  const closeDatabase = () => {
    if (databaseClosed) return;
    databaseClosed = true;
    database.close();
  };
  const onUpgrade = createUpgradeHandler({
    database,
    Database,
    zylosDir,
    currentReleasePath,
    currentReleaseRef: currentPackage.version,
    provider,
  });
  let resumed = null;
  try {
    if (typeof onUpgrade.resumeBlocking === 'function') {
      resumed = await onUpgrade.resumeBlocking();
      if (resumed !== null && !['committed', 'rolled_back'].includes(resumed?.state)) {
        throw new Error(
          `Runtime upgrade ${resumed?.state ?? 'unknown'} did not reach a safe terminal state.`,
        );
      }
    }
  } catch (error) {
    closeDatabase();
    throw error;
  }
  if (resumed !== null) {
    closeDatabase();
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
  const prerequisiteOwner = createPrerequisiteOwner({ zylosDir });
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
      releaseRef: process.env.ZYLOS_RELEASE_REF || null,
      upgradeId: process.env.ZYLOS_UPGRADE_ID || null,
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
      await prerequisiteOwner.close().catch(() => {});
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
