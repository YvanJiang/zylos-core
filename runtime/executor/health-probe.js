#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import dotenv from 'dotenv';

import { createClaudeConversationAdapter } from '../providers/claude/conversation-adapter.js';
import { createCodexAppServerAdapter } from '../providers/codex-app-server-adapter.js';
import { createExecutorService } from './service.js';

const require = createRequire(new URL('../../skills/comm-bridge/package.json', import.meta.url));

const HEALTH_PUBLISH_RETRY_MS = 100;
const HEALTH_PUBLISH_TIMEOUT_MS = 10_000;

function isTransientSqliteLock(error) {
  return error?.code === 'SQLITE_BUSY' || error?.code === 'SQLITE_LOCKED';
}

export async function publishTargetHealthSnapshot(service, {
  now = () => Date.now(),
  sleep = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
  retryMs = HEALTH_PUBLISH_RETRY_MS,
  timeoutMs = HEALTH_PUBLISH_TIMEOUT_MS,
} = {}) {
  const deadline = now() + timeoutMs;
  for (;;) {
    try {
      return service.publishObservabilitySnapshot();
    } catch (error) {
      if (!isTransientSqliteLock(error) || now() >= deadline) throw error;
      await sleep(retryMs);
    }
  }
}

function providerAdapter(provider, zylosDir) {
  if (provider === 'claude') {
    return createClaudeConversationAdapter({ queryOptions: { cwd: zylosDir, env: process.env } });
  }
  if (provider === 'codex') {
    return createCodexAppServerAdapter({
      cwd: zylosDir,
      env: process.env,
      approvalPolicy: 'on-request',
      sandbox: 'workspace-write',
    });
  }
  throw new Error(`Unsupported executor provider: ${provider}`);
}

export async function runTargetHealthProbe({
  zylosDir = process.env.ZYLOS_DIR,
  provider = process.env.ZYLOS_RUNTIME_PROVIDER,
  releaseRef = process.env.ZYLOS_RELEASE_REF,
  upgradeId = process.env.ZYLOS_UPGRADE_ID,
  Database = require('better-sqlite3'),
} = {}) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('ZYLOS_DIR must be an explicit absolute non-root path');
  }
  dotenv.config({ path: path.join(zylosDir, '.env'), override: false });
  const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
  const serviceInstanceId = `executor-health-${crypto.randomUUID()}`;
  const service = createExecutorService({
    database,
    adapter: providerAdapter(provider, zylosDir),
    provider,
    serviceInstanceId,
    hostId: serviceInstanceId,
    workspaceRoot: zylosDir,
    releaseRef,
    upgradeId,
  });
  service.start();
  const snapshot = await publishTargetHealthSnapshot(service);
  let closePromise = null;
  const close = () => {
    if (closePromise === null) {
      closePromise = service.close().finally(() => database.close());
    }
    return closePromise;
  };
  return Object.freeze({
    close,
    proof: Object.freeze({
      service_instance_id: snapshot.core_service_instance_id,
      snapshot_version: snapshot.snapshot_version,
      health: snapshot.service.health,
      reconciliation: 'complete',
    }),
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const probe = await runTargetHealthProbe();
  process.stdout.write(`${JSON.stringify(probe.proof)}\n`);
  const keepAlive = setInterval(() => {}, 60_000);
  const shutdown = async () => {
    clearInterval(keepAlive);
    try {
      await probe.close();
      process.exitCode = 0;
    } catch (error) {
      console.error(error);
      process.exitCode = 1;
    }
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
