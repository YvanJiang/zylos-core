import { execFile, spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { promisify } from 'node:util';

import Database from '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../../contracts/public/index.js';
import { createExecutorService } from '../../runtime/executor/service.js';
import { acceptNormalInbound } from '../../runtime/persistence/inbound-acceptance.js';
import { createCodexAppServerAdapter } from '../../runtime/providers/codex-app-server-adapter.js';

const execFileAsync = promisify(execFile);
const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function withTimeout(work, timeoutMs, label) {
  let timer;
  return Promise.race([
    work,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    }),
  ]).finally(() => clearTimeout(timer));
}

function normalEnvelope(suffix, text) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  const now = new Date().toISOString();
  envelope.inbound_event_id = `global43-event-${suffix}`;
  envelope.trace_id = `global43-trace-${suffix}`;
  envelope.message_id = `global43-message-${suffix}`;
  envelope.occurred_at = now;
  envelope.received_at = now;
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

function readTurnEvidence(database, accepted) {
  const lineage = database.prepare(`
    SELECT provider_native_id, provider_native_id_bound_at
    FROM runtime_lineages
    WHERE lineage_id = ?
  `).get(accepted.lineage_id);
  const events = database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence ASC
  `).all(accepted.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson));
  const providerStarted = events.find((event) => (
    event.kind === 'turn_state_changed'
    && event.phase === 'running'
    && event.payload?.reason_code === 'provider_started'
  ));
  const snapshots = events.filter(({ kind }) => kind === 'text_snapshot');
  return {
    thread_id: lineage?.provider_native_id ?? null,
    thread_bound_at: lineage?.provider_native_id_bound_at ?? null,
    provider_started_at: providerStarted?.occurred_at ?? null,
    event_kinds: events.map(({ kind }) => kind),
    final_text: snapshots.at(-1)?.payload?.text ?? null,
  };
}

async function runLifecycleTurn(database, service, suffix, expectedText) {
  const accepted = acceptNormalInbound(database, normalEnvelope(
    suffix,
    `Reply with exactly ${expectedText} and do not use tools.`,
  ), {
    now: () => new Date().toISOString(),
    generateId: deterministicIds(`global43-inbound-${suffix}`),
  });
  const result = await withTimeout(service.runNext(), 60_000, `lifecycle turn ${suffix}`);
  if (result?.status !== 'completed') {
    throw new Error(`Lifecycle turn ${suffix} ended with ${String(result?.status)}`);
  }
  const evidence = readTurnEvidence(database, accepted);
  if (evidence.final_text !== expectedText) {
    throw new Error(`Lifecycle turn ${suffix} returned unexpected text.`);
  }
  return evidence;
}

async function runLifecycleProbe({ codexExecutable, workspaceDirectory, databasePath }) {
  let database = new Database(databasePath);
  let adapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
  });
  let service = createExecutorService({
    database,
    adapter,
    provider: 'codex',
    serviceInstanceId: 'global43-real-service-before-restart',
    workspaceRoot: workspaceDirectory,
  });
  const first = await runLifecycleTurn(database, service, 'first', 'ZYLOS-GLOBAL43-FIRST-OK');
  const second = await runLifecycleTurn(database, service, 'second', 'ZYLOS-GLOBAL43-SECOND-OK');
  await service.close();
  database.close();

  database = new Database(databasePath);
  adapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
  });
  service = createExecutorService({
    database,
    adapter,
    provider: 'codex',
    serviceInstanceId: 'global43-real-service-after-restart',
    workspaceRoot: workspaceDirectory,
  });
  let third;
  try {
    third = await runLifecycleTurn(database, service, 'restart', 'ZYLOS-GLOBAL43-RESTART-OK');
  } finally {
    await service.close();
    database.close();
  }

  const turns = [first, second, third];
  const normalizedEventKinds = [...new Set(turns.flatMap(({ event_kinds: kinds }) => kinds))];
  const earlyDurableBinding = turns.every((turn) => (
    typeof turn.thread_id === 'string'
    && typeof turn.thread_bound_at === 'string'
    && typeof turn.provider_started_at === 'string'
    && Date.parse(turn.thread_bound_at) <= Date.parse(turn.provider_started_at)
  ));
  return {
    initialize_connect: true,
    new_thread: first.thread_id !== null,
    subsequent_turn: second.final_text === 'ZYLOS-GLOBAL43-SECOND-OK',
    early_durable_thread_binding: earlyDurableBinding,
    restart_thread_resume: third.final_text === 'ZYLOS-GLOBAL43-RESTART-OK',
    provider_neutral_translation: [
      'text_delta',
      'text_snapshot',
      'turn_state_changed',
    ].every((kind) => normalizedEventKinds.includes(kind)),
    thread_ids: turns.map(({ thread_id: threadId }) => threadId),
    normalized_event_kinds: normalizedEventKinds,
  };
}

function executionContext({ providerNativeId = null, suffix = 'probe', onStarted = null } = {}) {
  return {
    conversation_id: `global43-conversation-${suffix}`,
    turn_id: `global43-turn-${suffix}`,
    lineage_id: `global43-lineage-${suffix}`,
    trace_id: `global43-trace-${suffix}`,
    executor_instance_id: `global43-executor-${suffix}`,
    input: { kind: 'text', text: 'Reply with exactly GLOBAL43-PROBE.', attachments: [] },
    lineage: { provider_native_id: providerNativeId },
    attempt: { attempt_id: `global43-attempt-${suffix}`, attempt_no: 1, lease_epoch: 1 },
    interaction: {
      authorized_subjects: [{ type: 'actor', actor_id: 'global43-user' }],
      allowed_sources: ['card_action'],
    },
    async bindProviderNativeId() {},
    reportProviderState(state) {
      if (state?.state === 'started') onStarted?.();
    },
    reportRuntimeEvidence() {},
    reportProviderFailure() {},
  };
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}

async function captureAdapterFailure(adapter, context, timeoutMs = 60_000) {
  try {
    await withTimeout(collect(adapter.execute(context)), timeoutMs, context.turn_id);
    throw new Error(`${context.turn_id} unexpectedly completed.`);
  } catch (error) {
    if (typeof error?.code !== 'string') throw error;
    return error.providerError?.code ?? error.code;
  } finally {
    await adapter.close().catch(() => {});
  }
}

async function runSideEffectUnknownProbe({ codexExecutable, workspaceDirectory }) {
  let child;
  let resolveStarted;
  const started = new Promise((resolve) => { resolveStarted = resolve; });
  const adapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
    spawnProcess(command, args, options) {
      child = spawn(command, args, options);
      return child;
    },
  });
  const context = executionContext({ suffix: 'side-effect-unknown', onStarted: resolveStarted });
  context.input = {
    kind: 'text',
    text: 'Think carefully before replying, then reply with exactly GLOBAL43-FAULT-PROBE.',
    attachments: [],
  };
  const execution = collect(adapter.execute(context)).then(
    () => { throw new Error('Side-effect-unknown fault probe unexpectedly completed.'); },
    (error) => error,
  );
  try {
    await withTimeout(started, 30_000, 'side-effect-unknown provider start');
    if (!Number.isSafeInteger(child?.pid) || child.pid <= 0) {
      throw new Error('Side-effect-unknown fault probe did not capture the app-server PID.');
    }
    if (process.platform === 'win32') child.kill('SIGKILL');
    else process.kill(-child.pid, 'SIGKILL');
    const error = await withTimeout(execution, 15_000, 'side-effect-unknown terminal');
    if (typeof error?.code !== 'string') throw error;
    return error.providerError?.code ?? error.code;
  } finally {
    await adapter.close().catch(() => {});
  }
}

async function runFailureProbes({ codexExecutable, workspaceDirectory, temporaryDirectory }) {
  const contextHome = path.join(temporaryDirectory, 'context-home');
  fs.mkdirSync(contextHome, { recursive: true });
  const contextAdapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
    env: {
      PATH: process.env.PATH,
      HOME: contextHome,
      CODEX_HOME: contextHome,
      TERM: 'dumb',
    },
  });
  const context = await captureAdapterFailure(contextAdapter, executionContext({
    providerNativeId: '00000000-0000-0000-0000-000000000000',
    suffix: 'context',
  }), 15_000);

  const authHome = path.join(temporaryDirectory, 'auth-home');
  fs.mkdirSync(authHome, { recursive: true });
  const authAdapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
    env: {
      PATH: process.env.PATH,
      HOME: authHome,
      CODEX_HOME: authHome,
      TERM: 'dumb',
    },
  });
  const auth = await captureAdapterFailure(
    authAdapter,
    executionContext({ suffix: 'auth' }),
    45_000,
  );

  const transientHome = path.join(temporaryDirectory, 'transient-home');
  fs.mkdirSync(transientHome, { recursive: true });
  fs.writeFileSync(path.join(transientHome, 'config.toml'), [
    'model = "gpt-5.4-mini"',
    'model_provider = "global43-local"',
    '[model_providers.global43-local]',
    'name = "Global43 Local Fixture"',
    'base_url = "http://127.0.0.1:9/v1"',
    'env_key = "OPENAI_API_KEY"',
    'wire_api = "responses"',
    'stream_max_retries = 0',
    '',
  ].join('\n'), { mode: 0o600 });
  const transientAdapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
    env: {
      PATH: process.env.PATH,
      HOME: transientHome,
      CODEX_HOME: transientHome,
      OPENAI_API_KEY: 'global43-local-fixture-not-a-credential',
      TERM: 'dumb',
    },
  });
  const transient = await captureAdapterFailure(
    transientAdapter,
    executionContext({ suffix: 'transient' }),
    45_000,
  );
  const sideEffectUnknown = await runSideEffectUnknownProbe({
    codexExecutable,
    workspaceDirectory,
  });
  return {
    auth,
    context,
    transient,
    side_effect_unknown: sideEffectUnknown,
  };
}

async function runInterruptProbe({ codexExecutable, workspaceDirectory, reason }) {
  let resolveStarted;
  const started = new Promise((resolve) => { resolveStarted = resolve; });
  const context = executionContext({ suffix: `interrupt-${reason}`, onStarted: resolveStarted });
  context.input = {
    kind: 'text',
    text: 'Wait before replying. Do not use tools and do not produce an immediate final answer.',
    attachments: [],
  };
  const adapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
  });
  const execution = collect(adapter.execute(context)).catch((error) => error);
  try {
    await withTimeout(started, 30_000, `${reason} provider start`);
    const result = await withTimeout(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason,
    }), 15_000, `${reason} interrupt`);
    await withTimeout(execution, 15_000, `${reason} terminal`);
    return result.status;
  } finally {
    await adapter.close().catch(() => {});
  }
}

async function initializeProtocolProbe({ codexExecutable, workspaceDirectory }) {
  const child = spawn(codexExecutable, ['app-server', '--stdio'], {
    cwd: workspaceDirectory,
    env: process.env,
    shell: false,
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  child.stderr.resume();
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const response = new Promise((resolve, reject) => {
    lines.on('line', (line) => {
      try {
        const message = JSON.parse(line);
        if (message.id === 'global43-initialize') {
          if (message.error) reject(new Error('Target app-server rejected initialize.'));
          else resolve(message.result);
        }
      } catch (error) {
        reject(error);
      }
    });
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (code !== 0 && signal !== 'SIGTERM') {
        reject(new Error(`Target app-server closed during initialize (${code}/${signal}).`));
      }
    });
  });
  child.stdin.write(`${JSON.stringify({
    id: 'global43-initialize',
    method: 'initialize',
    params: {
      clientInfo: {
        name: 'zylos-global43-real-integration',
        title: 'Zylos Global43 Real Integration',
        version: '1.0.0',
      },
      capabilities: {
        experimentalApi: true,
        requestAttestation: false,
        mcpServerOpenaiFormElicitation: false,
      },
    },
  })}\n`);
  try {
    return await withTimeout(response, 15_000, 'initialize protocol probe');
  } finally {
    child.kill('SIGTERM');
  }
}

export async function runCodexAppServerRealIntegration({
  codexExecutable = process.env.CODEX_BIN || 'codex',
} = {}) {
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global43-real-'));
  const workspaceDirectory = path.join(temporaryDirectory, 'workspace');
  const databasePath = path.join(temporaryDirectory, 'c4.db');
  fs.mkdirSync(workspaceDirectory, { recursive: true });
  try {
    const [{ stdout: cliStdout }, authStatus, initialize] = await Promise.all([
      execFileAsync(codexExecutable, ['--version'], { encoding: 'utf8' }),
      execFileAsync(codexExecutable, ['login', 'status'], { encoding: 'utf8' }),
      initializeProtocolProbe({ codexExecutable, workspaceDirectory }),
    ]);
    const cliVersion = cliStdout.trim();
    const credentialStatus = `${authStatus.stdout}\n${authStatus.stderr}`.trim();
    if (cliVersion !== 'codex-cli 0.144.5') {
      throw new Error(`Global43 requires codex-cli 0.144.5, received ${cliVersion}.`);
    }
    if (!/Logged in/.test(credentialStatus)) {
      throw new Error('Global43 real lifecycle requires an authenticated Codex installation.');
    }

    const lifecycle = await runLifecycleProbe({
      codexExecutable,
      workspaceDirectory,
      databasePath,
    });
    const failures = await runFailureProbes({
      codexExecutable,
      workspaceDirectory,
      temporaryDirectory,
    });
    const interrupts = {
      stop: await runInterruptProbe({ codexExecutable, workspaceDirectory, reason: 'stop' }),
      timeout: await runInterruptProbe({ codexExecutable, workspaceDirectory, reason: 'timeout' }),
      steer: await runInterruptProbe({ codexExecutable, workspaceDirectory, reason: 'steer' }),
    };

    return Object.freeze({
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      protocol: Object.freeze({
        cli_version: cliVersion,
        initialize_user_agent: initialize.userAgent,
        experimental_api: true,
        transport: 'stdio',
      }),
      prerequisites: Object.freeze({
        codex_executable: codexExecutable,
        credential_status: credentialStatus,
        network: 'required for authenticated lifecycle and interrupt probes',
        bounded_workspace: true,
      }),
      lifecycle: Object.freeze(lifecycle),
      interrupts: Object.freeze(interrupts),
      failures: Object.freeze(failures),
      unrun_cases: Object.freeze([
        Object.freeze({
          case: 'command_approval',
          reason: 'Requires a controlled target-model/tool fixture that deterministically emits the server request without relying on an external side effect.',
        }),
        Object.freeze({
          case: 'file_approval',
          reason: 'Requires a controlled target-model/tool fixture and an isolated one-shot write approval exercise.',
        }),
        Object.freeze({
          case: 'request_user_input',
          reason: 'The target default-mode model does not deterministically expose requestUserInput to this client.',
        }),
        Object.freeze({
          case: 'mcp_elicitation',
          reason: 'Production lockdown disables MCP because the target protocol does not provide the synchronous Core pre-action fence required by Global14.',
        }),
        Object.freeze({
          case: 'stale_server_traffic',
          reason: 'The official server cannot be instructed to emit forged stale notification/request/response traffic; deterministic protocol-fence tests cover this negative matrix.',
        }),
      ]),
    });
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}
