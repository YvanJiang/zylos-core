#!/usr/bin/env node

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { query as sdkQuery } from '@anthropic-ai/claude-agent-sdk';

import {
  buildClaudeSdkEvidence,
  detectClaudeSdkPrerequisites,
  validateClaudeSdkTarget,
} from './lib/claude-sdk-real-integration.js';
import {
  runLiveClaudeSdkAcceptance,
} from './integration/claude-agent-sdk/live-runner.js';

const repositoryRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const credentialNames = [
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_AUTH_TOKEN',
  'CLAUDE_CODE_OAUTH_TOKEN',
];
const deterministicFiles = [
  'test/claude-sdk-real-integration-gate.test.js',
  'test/runtime-claude-conversation-executor.test.js',
  'test/runtime-executor-service.test.js',
  'test/runtime-reply-mapping-recovery.test.js',
  'test/runtime-interaction-happy-path.test.js',
];
const PROCESS_TERMINATION_GRACE_MS = 1_000;
const PROCESS_HARD_TIMEOUT_MS = 3_000;

function platformPackageName() {
  const platform = process.platform;
  const architecture = process.arch;
  if (!['darwin', 'linux', 'win32'].includes(platform)) {
    throw new Error(`Unsupported Claude SDK platform: ${platform}`);
  }
  if (!['arm64', 'x64'].includes(architecture)) {
    throw new Error(`Unsupported Claude SDK architecture: ${architecture}`);
  }
  const musl = platform === 'linux'
    && !process.report?.getReport?.()?.header?.glibcVersionRuntime;
  return `@anthropic-ai/claude-agent-sdk-${platform}-${architecture}${musl ? '-musl' : ''}`;
}

function bundledCliPath() {
  const executable = process.platform === 'win32' ? 'claude.exe' : 'claude';
  return path.join(repositoryRoot, 'node_modules', platformPackageName(), executable);
}

function runProcess(command, arguments_, {
  environment = process.env,
  passthrough = false,
  timeoutMs = 0,
} = {}) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const child = spawn(command, arguments_, {
      cwd: repositoryRoot,
      env: environment,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const stdout = [];
    const stderr = [];
    let forceKillTimeout = null;
    let hardTimeout = null;
    let settled = false;
    let timedOut = false;
    let timeout = null;
    function finish({ code = null, error = null }) {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      if (forceKillTimeout) clearTimeout(forceKillTimeout);
      if (hardTimeout) clearTimeout(hardTimeout);
      resolve({
        duration_ms: Date.now() - startedAt,
        error,
        exit_code: code,
        stderr: Buffer.concat(stderr).toString('utf8'),
        stdout: Buffer.concat(stdout).toString('utf8'),
        timed_out: timedOut,
      });
    }
    timeout = timeoutMs > 0
      ? setTimeout(() => {
        timedOut = true;
        child.kill('SIGTERM');
        forceKillTimeout = setTimeout(() => {
          child.kill('SIGKILL');
        }, PROCESS_TERMINATION_GRACE_MS);
        hardTimeout = setTimeout(() => {
          const error = new Error('Child process ignored forced termination.');
          error.code = 'process_timeout';
          child.stdout.destroy();
          child.stderr.destroy();
          child.unref();
          finish({ error });
        }, PROCESS_HARD_TIMEOUT_MS);
      }, timeoutMs)
      : null;
    child.stdout.on('data', (chunk) => {
      stdout.push(chunk);
      if (passthrough) process.stdout.write(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderr.push(chunk);
      if (passthrough) process.stderr.write(chunk);
    });
    child.on('error', (error) => {
      finish({ error });
    });
    child.on('close', (code) => {
      finish({ code });
    });
  });
}

function parseCount(output, label) {
  const match = output.match(new RegExp(`${label}:\\s+(?:\\d+ failed,\\s+)?(\\d+) passed`));
  return match ? Number(match[1]) : null;
}

async function runDeterministicLane() {
  const result = await runProcess(process.execPath, [
    '--experimental-vm-modules',
    path.join(repositoryRoot, 'node_modules', '.bin', 'jest'),
    '--runInBand',
    ...deterministicFiles,
  ], { passthrough: true });
  const output = `${result.stdout}\n${result.stderr}`;
  return Object.freeze({
    status: result.exit_code === 0 && !result.timed_out ? 'passed' : 'failed',
    files: Object.freeze([...deterministicFiles]),
    suites_passed: parseCount(output, 'Test Suites'),
    tests_passed: parseCount(output, 'Tests'),
    exit_code: result.exit_code,
    duration_ms: result.duration_ms,
    timed_out: result.timed_out,
  });
}

async function probeQuerySurface(cliPath) {
  async function* noInput() {
    // The native-surface probe must not invoke the provider.
  }
  const abortController = new AbortController();
  const configDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global42-probe-'));
  let providerQuery;
  try {
    providerQuery = sdkQuery({
      prompt: noInput(),
      options: {
        abortController,
        cwd: repositoryRoot,
        env: { ...process.env, CLAUDE_CONFIG_DIR: configDirectory },
        pathToClaudeCodeExecutable: cliPath,
        settingSources: [],
        tools: [],
      },
    });
    return Object.freeze({
      async_iterator: typeof providerQuery[Symbol.asyncIterator] === 'function',
      interrupt: typeof providerQuery.interrupt === 'function',
      cancel_async_message: typeof providerQuery.cancelAsyncMessage === 'function',
      close: typeof providerQuery.close === 'function',
    });
  } finally {
    abortController.abort();
    let closeTimeout;
    try {
      const closed = await Promise.race([
        Promise.resolve(providerQuery?.close?.()).then(() => true, () => false),
        new Promise((resolve) => {
          closeTimeout = setTimeout(() => resolve(false), 15_000);
        }),
      ]);
      if (!closed) throw new Error('The SDK native-surface query did not close cleanly.');
    } finally {
      clearTimeout(closeTimeout);
      fs.rmSync(configDirectory, { recursive: true, force: true });
    }
  }
}

async function probeNativeTarget() {
  const sdkDirectory = path.dirname(fileURLToPath(
    import.meta.resolve('@anthropic-ai/claude-agent-sdk'),
  ));
  const sdkPackage = JSON.parse(fs.readFileSync(
    path.join(sdkDirectory, 'package.json'),
    'utf8',
  ));
  const cliPath = bundledCliPath();
  if (!fs.existsSync(cliPath)) {
    throw new Error(`The pinned SDK platform CLI is unavailable: ${platformPackageName()}`);
  }
  const version = await runProcess(cliPath, ['--version'], { timeoutMs: 15_000 });
  if (version.exit_code !== 0 || version.timed_out) {
    throw new Error('The pinned SDK platform CLI version probe failed.');
  }
  const bundledCliVersion = `${version.stdout}\n${version.stderr}`.match(/\b\d+\.\d+\.\d+\b/)?.[0];
  if (sdkPackage.claudeCodeVersion !== bundledCliVersion) {
    throw new Error('The SDK package and bundled executable versions do not match.');
  }
  const typeDeclarations = fs.readFileSync(path.join(sdkDirectory, 'sdk.d.ts'), 'utf8');
  const capabilities = typeDeclarations.includes('interrupt_receipt_v1')
    ? ['interrupt_receipt_v1']
    : [];
  return validateClaudeSdkTarget({
    sdkVersion: sdkPackage.version,
    bundledCliVersion,
    querySurface: await probeQuerySurface(cliPath),
    capabilities,
  });
}

function parseNativeAuth(result) {
  const candidate = `${result.stdout}\n${result.stderr}`.trim();
  try {
    const status = JSON.parse(candidate);
    return Object.freeze({
      loggedIn: status.loggedIn === true,
      authMethod: typeof status.authMethod === 'string' ? status.authMethod : null,
    });
  } catch {
    return Object.freeze({ loggedIn: false, authMethod: null });
  }
}

async function detectNativeAuth(cliPath) {
  const baseEnvironment = { ...process.env };
  for (const name of credentialNames) delete baseEnvironment[name];
  const original = parseNativeAuth(await runProcess(cliPath, ['auth', 'status'], {
    environment: baseEnvironment,
    timeoutMs: 15_000,
  }));
  const configDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global42-auth-'));
  const isolatedEnvironment = {
    ...baseEnvironment,
    CLAUDE_CONFIG_DIR: configDirectory,
  };
  let result;
  try {
    result = await runProcess(cliPath, ['auth', 'status'], {
      environment: isolatedEnvironment,
      timeoutMs: 15_000,
    });
  } finally {
    fs.rmSync(configDirectory, { recursive: true, force: true });
  }
  const isolated = parseNativeAuth(result);
  return Object.freeze({
    loggedIn: isolated.loggedIn,
    authMethod: isolated.authMethod,
    originalLoggedIn: original.loggedIn,
  });
}

async function detectNetwork() {
  try {
    const target = new URL(process.env.ANTHROPIC_BASE_URL ?? 'https://api.anthropic.com');
    await fetch(target.origin, {
      method: 'HEAD',
      signal: AbortSignal.timeout(5_000),
    });
    return true;
  } catch {
    return false;
  }
}

function safeFailure(error) {
  return Object.freeze({
    status: 'failed',
    error_name: typeof error?.name === 'string' ? error.name : 'Error',
    error_code: typeof error?.code === 'string' ? error.code : null,
  });
}

async function main() {
  const requireLive = process.argv.slice(2).includes('--require-live');
  const deterministic = await runDeterministicLane();

  let nativeTarget;
  try {
    nativeTarget = await probeNativeTarget();
  } catch (error) {
    nativeTarget = safeFailure(error);
  }

  const cliPath = bundledCliPath();
  const [nativeAuth, networkAvailable] = await Promise.all([
    fs.existsSync(cliPath)
      ? detectNativeAuth(cliPath)
      : Promise.resolve({ loggedIn: false, authMethod: null }),
    detectNetwork(),
  ]);
  const prerequisites = detectClaudeSdkPrerequisites({
    environment: process.env,
    nativeAuth,
    networkAvailable,
  });

  let realProvider;
  if (nativeTarget.status !== 'passed') {
    realProvider = Object.freeze({
      status: 'skipped',
      reason: 'native_target_failed',
      prerequisites,
      cases: Object.freeze([]),
    });
  } else if (!prerequisites.available) {
    realProvider = Object.freeze({
      status: 'skipped',
      reason: prerequisites.missing.map((name) => `${name}_required`).join(','),
      prerequisites,
      cases: Object.freeze([]),
    });
  } else {
    realProvider = await runLiveClaudeSdkAcceptance();
  }

  const evidence = buildClaudeSdkEvidence({ deterministic, nativeTarget, realProvider });
  console.log(`ZYLOS_CLAUDE_SDK_ACCEPTANCE_EVIDENCE=${JSON.stringify(evidence)}`);
  if (evidence.release_ready) {
    console.log('CLAUDE_SDK_REAL_INTEGRATION=PASSED');
  } else if (evidence.status === 'partial') {
    console.log('CLAUDE_SDK_REAL_INTEGRATION=PARTIAL');
  } else {
    console.log('CLAUDE_SDK_REAL_INTEGRATION=FAILED');
  }

  const deterministicFailure = deterministic.status !== 'passed';
  const nativeFailure = nativeTarget.status !== 'passed';
  const liveFailure = realProvider.status === 'failed';
  process.exitCode = deterministicFailure || nativeFailure || liveFailure
    || (requireLive && !evidence.release_ready)
    ? 1
    : 0;
}

main().catch((error) => {
  const failure = safeFailure(error);
  console.error(
    `Claude SDK acceptance verifier failed: ${failure.error_code ?? failure.error_name}`,
  );
  process.exitCode = 1;
});
