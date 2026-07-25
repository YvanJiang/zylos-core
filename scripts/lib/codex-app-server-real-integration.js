import { execFile, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { promisify } from 'node:util';

import Database from '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../../contracts/public/index.js';
import { createExecutorService } from '../../runtime/executor/service.js';
import {
  acceptQueuedInbound as acceptNormalInbound,
} from '../../runtime/persistence/inbound-acceptance.js';
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

function installEarlyBindingWitness(database) {
  database.exec(`
    CREATE TEMP TRIGGER global43_require_binding_before_provider_started
    BEFORE INSERT ON runtime_normalized_events
    WHEN json_extract(NEW.event_json, '$.kind') = 'turn_state_changed'
      AND json_extract(NEW.event_json, '$.phase') = 'running'
      AND json_extract(NEW.event_json, '$.payload.reason_code') = 'provider_started'
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM runtime_turns AS turn
        JOIN runtime_lineages AS lineage ON lineage.lineage_id = turn.lineage_id
        WHERE turn.turn_id = NEW.turn_id
          AND lineage.provider = 'codex'
          AND lineage.provider_native_id IS NOT NULL
          AND json_extract(NEW.event_json, '$.provider_native_id')
            = lineage.provider_native_id
      ) THEN RAISE(ABORT, 'provider_started preceded durable native-thread binding') END;
    END;
  `);
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

async function runLifecyclePhase({
  codexExecutable,
  databasePath,
  serviceInstanceId,
  turns,
  workspaceDirectory,
}) {
  const database = new Database(databasePath);
  let service = null;
  try {
    const adapter = createCodexAppServerAdapter({
      codexExecutable,
      cwd: workspaceDirectory,
      sandbox: 'read-only',
    });
    service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId,
      workspaceRoot: workspaceDirectory,
    });
    installEarlyBindingWitness(database);
    const evidence = [];
    for (const [suffix, expectedText] of turns) {
      evidence.push(await runLifecycleTurn(
        database,
        service,
        suffix,
        expectedText,
      ));
    }
    return evidence;
  } finally {
    try {
      await service?.close();
    } finally {
      database.close();
    }
  }
}

async function runLifecycleProbe({ codexExecutable, workspaceDirectory, databasePath }) {
  const [first, second] = await runLifecyclePhase({
    codexExecutable,
    databasePath,
    serviceInstanceId: 'global43-real-service-before-restart',
    turns: [
      ['first', 'ZYLOS-GLOBAL43-FIRST-OK'],
      ['second', 'ZYLOS-GLOBAL43-SECOND-OK'],
    ],
    workspaceDirectory,
  });
  const [third] = await runLifecyclePhase({
    codexExecutable,
    databasePath,
    serviceInstanceId: 'global43-real-service-after-restart',
    turns: [['restart', 'ZYLOS-GLOBAL43-RESTART-OK']],
    workspaceDirectory,
  });

  const turns = [first, second, third];
  const normalizedEventKinds = [...new Set(turns.flatMap(({ event_kinds: kinds }) => kinds))];
  const earlyDurableBinding = turns.every((turn) => (
    typeof turn.thread_id === 'string'
    && typeof turn.thread_bound_at === 'string'
    && typeof turn.provider_started_at === 'string'
  ));
  return {
    initialize_connect: true,
    new_thread: first.thread_id !== null,
    subsequent_turn: second.final_text === 'ZYLOS-GLOBAL43-SECOND-OK',
    early_durable_thread_binding: earlyDurableBinding,
    early_binding_witness: 'sqlite_before_provider_started_insert_trigger',
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

async function runCoreFailureProbe({
  adapter,
  databasePath,
  providerNativeId = null,
  suffix,
  workspaceDirectory,
}) {
  const database = new Database(databasePath);
  let service = null;
  try {
    const accepted = acceptNormalInbound(database, normalEnvelope(
      `failure-${suffix}`,
      'Reply with exactly GLOBAL43-FAILURE-PROBE and do not use tools.',
    ), {
      now: () => new Date().toISOString(),
      generateId: deterministicIds(`global43-failure-${suffix}`),
    });
    if (providerNativeId !== null) {
      const boundAt = new Date().toISOString();
      database.prepare(`
        UPDATE runtime_lineages
        SET provider = 'codex', provider_native_id = ?,
          provider_native_id_bound_at = ?, provider_native_state = 'valid'
        WHERE lineage_id = ?
      `).run(providerNativeId, boundAt, accepted.lineage_id);
    }
    service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: `global43-failure-service-${suffix}`,
      workspaceRoot: workspaceDirectory,
      providerRetryJitterRatio: 0,
    });
    const result = await withTimeout(service.runNext(), 60_000, `${suffix} Core failure`);
    const turn = database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id);
    const attempt = database.prepare(`
      SELECT state, error_json FROM runtime_provider_attempts
      WHERE turn_id = ? AND attempt_no = 1
    `).get(accepted.turn_id);
    const error = attempt?.error_json === null || attempt?.error_json === undefined
      ? null
      : JSON.parse(attempt.error_json);
    const events = database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence ASC
    `).all(accepted.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson));
    if (typeof error?.code !== 'string') {
      throw new Error(`${suffix} did not persist a typed provider error.`);
    }
    return Object.freeze({
      provider_code: error.code,
      core_status: result.status,
      turn_state: turn.state,
      attempt_state: attempt.state,
      durable_error_event: events.some((event) => event.error?.code === error.code),
    });
  } finally {
    try {
      await service?.close();
    } finally {
      database.close();
    }
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
  const context = await runCoreFailureProbe({
    adapter: contextAdapter,
    databasePath: path.join(temporaryDirectory, 'context.db'),
    providerNativeId: '00000000-0000-0000-0000-000000000000',
    suffix: 'context',
    workspaceDirectory,
  });

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
  const auth = await runCoreFailureProbe({
    adapter: authAdapter,
    databasePath: path.join(temporaryDirectory, 'auth.db'),
    suffix: 'auth',
    workspaceDirectory,
  });

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
  const transient = await runCoreFailureProbe({
    adapter: transientAdapter,
    databasePath: path.join(temporaryDirectory, 'transient.db'),
    suffix: 'transient',
    workspaceDirectory,
  });

  let sideEffectChild;
  let sideEffectBuffer = '';
  let sideEffectKilled = false;
  const sideEffectAdapter = createCodexAppServerAdapter({
    codexExecutable,
    cwd: workspaceDirectory,
    sandbox: 'read-only',
    spawnProcess(command, args, options) {
      sideEffectChild = spawn(command, args, options);
      sideEffectChild.stdout.on('data', (chunk) => {
        sideEffectBuffer += chunk.toString('utf8');
        while (sideEffectBuffer.includes('\n')) {
          const newline = sideEffectBuffer.indexOf('\n');
          const line = sideEffectBuffer.slice(0, newline);
          sideEffectBuffer = sideEffectBuffer.slice(newline + 1);
          let message;
          try {
            message = JSON.parse(line);
          } catch {
            continue;
          }
          if (message.method !== 'turn/started' || sideEffectKilled) continue;
          sideEffectKilled = true;
          queueMicrotask(() => {
            if (!Number.isSafeInteger(sideEffectChild?.pid) || sideEffectChild.pid <= 0) return;
            if (process.platform === 'win32') sideEffectChild.kill('SIGKILL');
            else process.kill(-sideEffectChild.pid, 'SIGKILL');
          });
        }
      });
      return sideEffectChild;
    },
  });
  const sideEffectUnknown = await runCoreFailureProbe({
    adapter: sideEffectAdapter,
    databasePath: path.join(temporaryDirectory, 'side-effect-unknown.db'),
    suffix: 'side-effect-unknown',
    workspaceDirectory,
  });
  if (!sideEffectKilled) throw new Error('Side-effect-unknown fault injection did not run.');
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
  const execution = collect(adapter.execute(context)).then(
    () => ({ status: 'completed' }),
    (error) => ({ status: 'failed', error }),
  );
  try {
    await withTimeout(started, 30_000, `${reason} provider start`);
    const result = await withTimeout(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason,
    }), 15_000, `${reason} interrupt`);
    const terminal = await withTimeout(execution, 15_000, `${reason} terminal`);
    if (
      terminal.status !== 'failed'
      || terminal.error?.providerError?.code !== 'side_effect_unknown'
      || !/status interrupted\b/i.test(terminal.error.message)
    ) {
      throw new Error(`${reason} did not end with the matching canonical interrupted terminal.`);
    }
    return Object.freeze({
      request_status: result.status,
      provider_status: result.provider_status ?? 'interrupted',
      terminal_status: 'interrupted',
    });
  } finally {
    await adapter.close().catch(() => {});
  }
}

async function initializeProtocolProbe({
  codexExecutable,
  workspaceDirectory,
  environment = process.env,
  clientInfo = Object.freeze({
    name: 'zylos-global43-real-integration',
    title: 'Zylos Global43 Real Integration',
    version: '1.0.0',
  }),
}) {
  const child = spawn(codexExecutable, ['app-server', '--stdio'], {
    cwd: workspaceDirectory,
    env: environment,
    shell: false,
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  child.stderr.resume();
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const closed = new Promise((resolve) => child.once('close', resolve));
  const response = new Promise((resolve, reject) => {
    lines.on('line', (line) => {
      try {
        const message = JSON.parse(line);
        if (message.id === 'global43-initialize') {
          if (message.error) reject(new Error('Target app-server rejected initialize.'));
          else {
            child.stdin.write(`${JSON.stringify({ method: 'initialized', params: {} })}\n`);
            resolve(message.result);
          }
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
      clientInfo,
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
    lines.close();
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
    try {
      await withTimeout(closed, 5_000, 'initialize process shutdown');
    } catch {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await withTimeout(closed, 5_000, 'initialize process forced shutdown');
    }
  }
}

function isolatedSmokeEnvironment(baseEnvironment, codexHome) {
  const environment = {};
  for (const key of ['PATH', 'SystemRoot', 'WINDIR', 'ComSpec', 'PATHEXT', 'LANG', 'LC_ALL']) {
    if (typeof baseEnvironment[key] === 'string') environment[key] = baseEnvironment[key];
  }
  environment.HOME = codexHome;
  environment.CODEX_HOME = codexHome;
  environment.TERM = 'dumb';
  return environment;
}

export async function runCodexAppServerInitializeSmoke({
  codexExecutable = process.env.CODEX_BIN || 'codex',
  baseEnvironment = process.env,
  execFile = execFileAsync,
  initialize = initializeProtocolProbe,
} = {}) {
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global43-smoke-'));
  const codexHome = path.join(temporaryDirectory, 'codex-home');
  const workspaceDirectory = path.join(temporaryDirectory, 'workspace');
  fs.mkdirSync(codexHome, { recursive: true, mode: 0o700 });
  fs.mkdirSync(workspaceDirectory, { recursive: true, mode: 0o700 });
  const environment = isolatedSmokeEnvironment(baseEnvironment, codexHome);
  try {
    const [{ stdout }, initialized] = await Promise.all([
      execFile(codexExecutable, ['--version'], { encoding: 'utf8', env: environment }),
      initialize({
        codexExecutable,
        workspaceDirectory,
        environment,
        clientInfo: {
          name: 'zylos-global43-native-smoke',
          title: 'Zylos Global43 Native Smoke',
          version: '1.0.0',
        },
      }),
    ]);
    const cliVersion = stdout.trim();
    if (cliVersion !== 'codex-cli 0.144.5') {
      throw new Error(`Global43 requires codex-cli 0.144.5, received ${cliVersion}.`);
    }
    if (fs.realpathSync(initialized.codexHome) !== fs.realpathSync(codexHome)) {
      throw new Error('Initialize did not use the dedicated smoke CODEX_HOME.');
    }
    return Object.freeze({
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      protocol: Object.freeze({
        cli_version: cliVersion,
        initialize_user_agent: initialized.userAgent,
        transport: 'stdio',
        initialized_notification: true,
        platform_family: initialized.platformFamily,
        platform_os: initialized.platformOs,
      }),
      isolation: Object.freeze({
        dedicated_codex_home: true,
        host_codex_home_reused: false,
        credentials_inherited: false,
      }),
    });
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

export function writeCodexAppServerRealEvidenceFile({ evidence, evidenceFile, codexHome }) {
  if (
    evidence?.evidence_schema_version !== 1
    || evidence?.provider_transport !== 'official_app_server'
  ) {
    throw new TypeError('Only Global43 official app-server evidence schema 1 may be persisted.');
  }
  if (typeof evidenceFile !== 'string' || typeof codexHome !== 'string') {
    throw new TypeError('evidenceFile and codexHome must be strings.');
  }
  const lexicalHome = path.resolve(codexHome);
  const resolvedHome = fs.realpathSync(codexHome);
  const resolvedFile = path.resolve(evidenceFile);
  const relativeFile = path.relative(lexicalHome, resolvedFile);
  if (relativeFile.startsWith('..') || path.isAbsolute(relativeFile)) {
    throw new Error('Global43 evidence file must remain inside CODEX_HOME.');
  }
  const evidenceDirectory = path.dirname(resolvedFile);
  fs.mkdirSync(evidenceDirectory, { recursive: true, mode: 0o700 });
  const resolvedDirectory = fs.realpathSync(evidenceDirectory);
  const relativeDirectory = path.relative(resolvedHome, resolvedDirectory);
  if (relativeDirectory.startsWith('..') || path.isAbsolute(relativeDirectory)) {
    throw new Error('Global43 evidence file must remain inside CODEX_HOME.');
  }
  const temporaryFile = path.join(
    resolvedDirectory,
    `.global43-evidence-${randomUUID()}.tmp`,
  );
  try {
    fs.writeFileSync(temporaryFile, `${JSON.stringify(evidence)}\n`, {
      encoding: 'utf8',
      flag: 'wx',
      mode: 0o600,
    });
    fs.renameSync(temporaryFile, resolvedFile);
  } finally {
    fs.rmSync(temporaryFile, { force: true });
  }
}

export function selectRestartResumeThreadId(evidence) {
  const threadIds = evidence?.lifecycle?.thread_ids;
  if (
    evidence?.evidence_schema_version !== 1
    || evidence?.provider_transport !== 'official_app_server'
    || !Array.isArray(threadIds)
    || threadIds.length !== 3
    || typeof threadIds[0] !== 'string'
    || threadIds[0].length === 0
    || !threadIds.every((threadId) => threadId === threadIds[0])
  ) {
    throw new Error('Restart resume requires one durable thread ID across all lifecycle turns.');
  }
  return threadIds[0];
}

export async function runCodexAppServerRestartResume({
  providerNativeId,
  codexExecutable = process.env.CODEX_BIN || 'codex',
  createAdapter = createCodexAppServerAdapter,
} = {}) {
  if (typeof providerNativeId !== 'string' || providerNativeId.length === 0) {
    throw new TypeError('providerNativeId must be a non-empty string.');
  }
  const expectedText = 'ZYLOS-GLOBAL43-CONTAINER-RESTART-OK';
  const context = executionContext({
    providerNativeId,
    suffix: 'container-restart-resume',
  });
  context.input = {
    kind: 'text',
    text: `Reply with exactly ${expectedText} and do not use tools.`,
    attachments: [],
  };
  const adapter = createAdapter({
    codexExecutable,
    sandbox: 'read-only',
  });
  try {
    const events = await withTimeout(
      collect(adapter.execute(context)),
      60_000,
      'container restart resume turn',
    );
    const finalText = events.filter(({ kind }) => kind === 'text_snapshot').at(-1)?.payload?.text;
    if (finalText !== expectedText) {
      throw new Error('Container restart resume returned unexpected text.');
    }
    return Object.freeze({
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      protocol: Object.freeze({
        cli_version: 'codex-cli 0.144.5',
        transport: 'stdio',
      }),
      container_restart: Object.freeze({
        fresh_container: true,
        thread_id: providerNativeId,
        thread_id_reused: true,
        subsequent_turn: true,
        normalized_event_kinds: [...new Set(events.map(({ kind }) => kind))],
      }),
    });
  } finally {
    await adapter.close();
  }
}

const realInteractionScenarios = Object.freeze({
  command_approval: Object.freeze({
    expectedKind: 'tool_approval',
    expectedToolName: 'command',
    prompt: 'Use the shell command `touch global43-command-approved.txt` exactly once. Explicitly request user approval for escalated execution outside the read-only sandbox before running it. Do not attempt the command without approval and do not use any other tool.',
    artifact: 'global43-command-approved.txt',
  }),
  file_approval: Object.freeze({
    expectedKind: 'tool_approval',
    expectedToolName: 'file_change',
    prompt: 'Use the file patch tool exactly once to propose `global43-file-approved.txt` containing `GLOBAL43 FILE APPROVED`. Explicitly request file-change approval before applying it. Do not write without approval and do not use the shell.',
    artifact: 'global43-file-approved.txt',
  }),
  request_user_input: Object.freeze({
    expectedKind: 'question',
    expectedToolName: null,
    prompt: 'Call the request_user_input tool exactly once to ask one non-secret free-form question with no timeout; do not ask in ordinary assistant text. After the tool answer, reply with a short confirmation. Do not use any other tool.',
    artifact: null,
  }),
});

function realInteractionAnswer(request, scenario) {
  const sourceEventId = `global43-${scenario}-answer-action`;
  const value = request.kind === 'question'
    ? { kind: 'text', text: 'GLOBAL43 USER INPUT ANSWER' }
    : request.kind === 'choice'
      ? { kind: 'choice', choice_id: request.choices[0]?.choice_id }
      : { kind: 'decision', decision: 'approve' };
  const answer = {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `global43-${scenario}-answer-trace`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `global43-${scenario}-answer`,
    source_event_or_action_id: sourceEventId,
    actor: {
      type: 'user',
      actor_id: 'user-A',
      authenticated: true,
      roles: ['member'],
    },
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    },
    source: request.allowed_sources.includes('card_action')
      ? 'card_action'
      : request.allowed_sources[0],
    value,
    answered_at: new Date().toISOString(),
  };
  answer.idempotency_key = createIdempotencyKey('interaction', {
    interaction_id: answer.interaction_id,
    source_event_or_action_id: answer.source_event_or_action_id,
  });
  return answer;
}

function readRealInteractionEvidence(database, accepted) {
  const interaction = database.prepare(`
    SELECT state, handoff_state, request_json
    FROM runtime_interactions
    WHERE turn_id = ?
    ORDER BY ordinal ASC
    LIMIT 1
  `).get(accepted.turn_id);
  const handoff = database.prepare(`
    SELECT state
    FROM runtime_interaction_handoffs
    WHERE interaction_id IN (
      SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
    )
    ORDER BY handoff_id ASC
    LIMIT 1
  `).get(accepted.turn_id);
  const audit = database.prepare(`
    SELECT outcome
    FROM runtime_interaction_audit
    WHERE interaction_id IN (
      SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
    )
    ORDER BY audit_id ASC
    LIMIT 1
  `).get(accepted.turn_id);
  const answerCount = database.prepare(`
    SELECT COUNT(*) AS count
    FROM runtime_interaction_answers
    WHERE interaction_id IN (
      SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
    )
  `).get(accepted.turn_id).count;
  const turn = database.prepare(`
    SELECT state FROM runtime_turns WHERE turn_id = ?
  `).get(accepted.turn_id);
  const events = database.prepare(`
    SELECT event_json FROM runtime_normalized_events
    WHERE turn_id = ? ORDER BY event_sequence ASC
  `).all(accepted.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson));
  const toolName = events.find(({ kind }) => kind === 'tool_started')?.payload?.tool_name ?? null;
  const finalTextPresent = events.some(({ kind, payload }) => (
    kind === 'text_snapshot' && typeof payload?.text === 'string' && payload.text.length > 0
  ));
  return {
    interaction,
    handoff,
    audit,
    answerCount,
    turn,
    eventKinds: [...new Set(events.map(({ kind }) => kind))],
    toolName,
    finalTextPresent,
  };
}

export async function runCodexAppServerRealInteractionProbe({
  scenario,
  codexExecutable = process.env.CODEX_BIN || 'codex',
  createAdapter = createCodexAppServerAdapter,
} = {}) {
  const specification = realInteractionScenarios[scenario];
  if (specification === undefined) {
    throw new TypeError('scenario must be command_approval, file_approval, or request_user_input.');
  }
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), `zylos-global43-${scenario}-`));
  const workspaceDirectory = path.join(temporaryDirectory, 'workspace');
  fs.mkdirSync(workspaceDirectory, { recursive: true, mode: 0o700 });
  const database = new Database(path.join(temporaryDirectory, 'c4.db'));
  let service = null;
  try {
    const adapter = createAdapter({
      codexExecutable,
      cwd: workspaceDirectory,
      approvalPolicy: 'on-request',
      sandbox: 'workspace-write',
    });
    service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: `global43-real-interaction-${scenario}`,
      workspaceRoot: workspaceDirectory,
      generateId: deterministicIds(`global43-real-interaction-${scenario}`),
    });
    const envelope = normalEnvelope(`interaction-${scenario}`, specification.prompt);
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => new Date().toISOString(),
      generateId: deterministicIds(`global43-real-interaction-inbound-${scenario}`),
    });
    const waiting = await withTimeout(service.runNext(), 60_000, `${scenario} request`);
    if (waiting?.status !== 'waiting_user') {
      const durable = readRealInteractionEvidence(database, accepted);
      return Object.freeze({
        evidence_schema_version: 1,
        provider_transport: 'official_app_server',
        scenario,
        triggered: false,
        provider_result_status: waiting?.status ?? null,
        provider_neutral_tool_name: durable.toolName,
        bounded_artifact_created: specification.artifact === null
          ? null
          : fs.existsSync(path.join(workspaceDirectory, specification.artifact)),
        final_text_present: durable.finalTextPresent,
        normalized_event_kinds: durable.eventKinds,
      });
    }
    if (waiting.request.kind !== specification.expectedKind) {
      throw new Error(`${scenario} emitted ${waiting.request.kind} instead of the expected interaction kind.`);
    }
    const committed = service.submitInteractionAnswer(
      realInteractionAnswer(waiting.request, scenario),
    );
    const delivered = await withTimeout(
      service.deliverInteractionAnswer(committed.handoff_id),
      60_000,
      `${scenario} answer delivery`,
    );
    const durable = readRealInteractionEvidence(database, accepted);
    if (specification.expectedToolName !== null && durable.toolName !== specification.expectedToolName) {
      throw new Error(`${scenario} emitted ${durable.toolName ?? 'no tool'} instead of ${specification.expectedToolName}.`);
    }
    return Object.freeze({
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      scenario,
      triggered: true,
      pre_action_lease_fenced: specification.expectedToolName !== null,
      provider_neutral_tool_name: durable.toolName,
      provider_acknowledged: delivered?.acknowledgement?.status === 'accepted',
      bounded_artifact_created: specification.artifact === null
        ? null
        : fs.existsSync(path.join(workspaceDirectory, specification.artifact)),
      durable: Object.freeze({
        interaction_state: durable.interaction?.state ?? null,
        handoff_state: durable.handoff?.state ?? null,
        answer_count: durable.answerCount,
        audit_outcome: durable.audit?.outcome ?? null,
        turn_state: durable.turn?.state ?? null,
        normalized_event_kinds: durable.eventKinds,
      }),
    });
  } finally {
    try {
      await service?.close();
    } finally {
      database.close();
      fs.rmSync(temporaryDirectory, { recursive: true, force: true });
    }
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
        credential_status: 'authenticated',
        network: 'required for authenticated lifecycle and interrupt probes',
        bounded_workspace: true,
      }),
      lifecycle: Object.freeze(lifecycle),
      interrupts: Object.freeze(interrupts),
      failures: Object.freeze(failures),
      unrun_cases: Object.freeze([
        Object.freeze({
          case: 'command_approval',
          reason: 'Two bounded official-target prompts in the dedicated Apple Container completed with no tool_started event or server request; the fixed target exposes no controlled fixture that forces command approval.',
        }),
        Object.freeze({
          case: 'file_approval',
          reason: 'Two bounded official-target prompts in the dedicated Apple Container completed with no tool_started event, file artifact, or server request; the fixed target exposes no controlled fixture that forces file approval.',
        }),
        Object.freeze({
          case: 'request_user_input',
          reason: 'Two explicit default-mode official-target prompts completed with text only and no requestUserInput server request; collaboration mode remains disabled on the normal path.',
        }),
        Object.freeze({
          case: 'mcp_elicitation',
          reason: 'Production lockdown disables MCP because 0.144.5 exposes only a conditional model-initiated prompt seam; this candidate does not enable or prove every configuration, reviewer, bypass-exclusion, durable-lease, restart, and acknowledgement condition required by Global14.',
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
