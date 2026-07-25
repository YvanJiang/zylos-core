#!/usr/bin/env node

// Credential-dependent execution stays outside the default deterministic test suite.

import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  createSdkMcpServer,
  query as sdkQuery,
  tool,
} from '@anthropic-ai/claude-agent-sdk';
import { z } from 'zod';
import Database from '../../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
} from '../../../contracts/public/index.js';
import { createExecutorService } from '../../../runtime/executor/service.js';
import {
  acceptQueuedInbound as acceptNormalInbound,
} from '../../../runtime/persistence/inbound-acceptance.js';
import {
  createClaudeConversationAdapter,
} from '../../../runtime/providers/claude/conversation-adapter.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../../../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const DEFAULT_TIMEOUT_MS = 180_000;
const CLEANUP_TIMEOUT_MS = 15_000;
const SDK_SHUTDOWN_SETTLE_MS = 2_500;
const ACCEPTANCE_MCP_SERVER = 'zylos_global42_acceptance';
const ACCEPTANCE_MCP_TOOL = `mcp__${ACCEPTANCE_MCP_SERVER}__approval_probe`;
const REQUIRED_REAL_CASE_IDS = Object.freeze([
  'long_lived_async_multi_turn',
  'durable_session_id',
  'cancel',
  'permission_interaction',
  'idle_eviction_rebuild',
  'service_restart_resume',
]);

function generateId(kind) {
  return `${kind}-global42-${crypto.randomUUID()}`;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function normalEnvelope(suffix, text) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `global42-event-${suffix}-${crypto.randomUUID()}`;
  envelope.trace_id = `global42-trace-${suffix}-${crypto.randomUUID()}`;
  envelope.message_id = `global42-message-${suffix}-${crypto.randomUUID()}`;
  envelope.content.text = text;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function acceptTurn(database, suffix, text) {
  return acceptNormalInbound(database, normalEnvelope(suffix, text), {
    now: () => new Date().toISOString(),
    generateId,
  });
}

function permissionAnswer(request, suffix, decision = 'approve') {
  const sourceEventId = `global42-permission-${suffix}-${crypto.randomUUID()}`;
  return {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `global42-permission-trace-${suffix}-${crypto.randomUUID()}`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `global42-permission-answer-${suffix}-${crypto.randomUUID()}`,
    source_event_or_action_id: sourceEventId,
    actor: { type: 'user', actor_id: 'user-A', authenticated: true, roles: ['member'] },
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    },
    source: 'card_action',
    value: { kind: 'decision', decision },
    answered_at: new Date().toISOString(),
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: request.interaction_id,
      source_event_or_action_id: sourceEventId,
    }),
  };
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function safeError(error) {
  return Object.freeze({
    name: typeof error?.name === 'string' ? error.name : 'Error',
    code: typeof error?.code === 'string' ? error.code : null,
    provider_code: typeof error?.providerError?.code === 'string'
      ? error.providerError.code
      : null,
    category: typeof error?.providerError?.category === 'string'
      ? error.providerError.category
      : null,
  });
}

export async function removeSdkTemporaryDirectoryAfterShutdown(directory, {
  settleMs = SDK_SHUTDOWN_SETTLE_MS,
  wait = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
} = {}) {
  if (typeof directory !== 'string' || directory.length === 0) {
    throw new TypeError('directory must be a non-empty string');
  }
  if (!Number.isFinite(settleMs) || settleMs < 0) {
    throw new TypeError('settleMs must be a non-negative finite number');
  }
  if (typeof wait !== 'function') throw new TypeError('wait must be a function');
  fs.rmSync(directory, { recursive: true, force: true });
  await wait(settleMs);
  fs.rmSync(directory, { recursive: true, force: true });
  if (fs.existsSync(directory)) {
    const error = new Error('Claude SDK temporary directory cleanup was not confirmed.');
    error.code = 'live_cleanup_unconfirmed';
    throw error;
  }
}

function queryOptions({
  abortController,
  configDirectory,
  cwd,
  mcpServers,
  tools,
}) {
  const configuredModel = process.env.ZYLOS_CLAUDE_SDK_LIVE_MODEL;
  const configuredBudget = Number(process.env.ZYLOS_CLAUDE_SDK_LIVE_MAX_BUDGET_USD ?? '0.50');
  fs.mkdirSync(configDirectory, { recursive: true, mode: 0o700 });
  return {
    abortController,
    cwd,
    env: { ...process.env, CLAUDE_CONFIG_DIR: configDirectory },
    maxBudgetUsd: Number.isFinite(configuredBudget) && configuredBudget > 0
      ? configuredBudget
      : 0.50,
    permissionMode: 'default',
    settingSources: [],
    systemPrompt: [
      'You are running a bounded Zylos integration acceptance.',
      'Follow the user request exactly and keep responses brief.',
    ].join(' '),
    tools,
    ...(configuredModel ? { model: configuredModel } : {}),
    ...(mcpServers ? { mcpServers } : {}),
  };
}

function createObservedQuery() {
  const calls = [];
  const closePromises = new WeakMap();
  const initMessages = [];
  const interrupts = [];
  const providerQueries = new Set();
  const sessionStates = [];
  const idleWaiters = new Set();

  function closeProviderQuery(providerQuery) {
    const existing = closePromises.get(providerQuery);
    if (existing) return existing;
    const closing = Promise.resolve()
      .then(() => providerQuery.close())
      .finally(() => providerQueries.delete(providerQuery));
    closePromises.set(providerQuery, closing);
    return closing;
  }

  function waitForIdleCount(minimumCount, { signal }) {
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        idleWaiters.delete(notify);
        reject(signal.reason ?? new Error('Claude idle wait was aborted.'));
      };
      const notify = () => {
        const idleCount = sessionStates.filter(({ state }) => state === 'idle').length;
        if (idleCount < minimumCount) return;
        idleWaiters.delete(notify);
        signal.removeEventListener('abort', onAbort);
        setImmediate(resolve);
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      idleWaiters.add(notify);
      signal.addEventListener('abort', onAbort, { once: true });
      notify();
    });
  }

  function query(arguments_) {
    const providerQuery = sdkQuery(arguments_);
    providerQueries.add(providerQuery);
    calls.push(Object.freeze({
      resume: arguments_.options?.resume ?? null,
      cwd: arguments_.options?.cwd ?? null,
    }));

    const observed = {
      async next(...arguments__) {
        const result = await providerQuery.next(...arguments__);
        if (!result.done) {
          const message = result.value;
          if (message?.type === 'system' && message.subtype === 'init') {
            initMessages.push(Object.freeze({
              capabilities: Object.freeze([...(message.capabilities ?? [])]),
              model: typeof message.model === 'string' ? message.model : null,
              session_id: message.session_id,
            }));
          }
          if (message?.type === 'system' && message.subtype === 'session_state_changed') {
            sessionStates.push(Object.freeze({
              state: message.state,
              session_id: message.session_id,
            }));
            for (const notify of idleWaiters) notify();
          }
        }
        return result;
      },
      async return(value) {
        return typeof providerQuery.return === 'function'
          ? providerQuery.return(value)
          : { done: true, value };
      },
      async throw(error) {
        if (typeof providerQuery.throw === 'function') return providerQuery.throw(error);
        throw error;
      },
      [Symbol.asyncIterator]() {
        return this;
      },
      async interrupt(...arguments__) {
        try {
          const receipt = await providerQuery.interrupt(...arguments__);
          interrupts.push(Object.freeze({
            status: 'resolved',
            valid_receipt: Array.isArray(receipt?.still_queued),
            still_queued_count: Array.isArray(receipt?.still_queued)
              ? receipt.still_queued.length
              : null,
          }));
          return receipt;
        } catch (error) {
          interrupts.push(Object.freeze({
            status: 'rejected',
            valid_receipt: false,
            still_queued_count: null,
          }));
          throw error;
        }
      },
      cancelAsyncMessage: providerQuery.cancelAsyncMessage.bind(providerQuery),
      close: () => closeProviderQuery(providerQuery),
    };
    return observed;
  }

  async function closeAll() {
    await Promise.all([...providerQueries].map(closeProviderQuery));
  }

  return Object.freeze({
    calls,
    closeAll,
    initMessages,
    interrupts,
    query,
    sessionStates,
    waitForIdleCount,
  });
}

function createService({ database, adapter, serviceInstanceId, workspaceRoot }) {
  return createExecutorService({
    database,
    adapter,
    provider: 'claude',
    serviceInstanceId,
    workspaceRoot,
    now: () => new Date().toISOString(),
    generateId,
  });
}

async function closeScenario(service, observed) {
  let cleanupFailed = false;
  try {
    await service?.close?.();
  } catch {
    cleanupFailed = true;
  }
  try {
    await observed.closeAll();
  } catch {
    cleanupFailed = true;
  }
  if (cleanupFailed) {
    const error = new Error('Claude Agent SDK scenario cleanup was not confirmed.');
    error.code = 'live_cleanup_unconfirmed';
    throw error;
  }
}

function closeDatabaseQuietly(database) {
  try {
    database?.close?.();
  } catch {
    // The primary scenario failure remains the acceptance evidence.
  }
}

async function settleBounded(promise, timeoutMs) {
  let timeout;
  try {
    return await Promise.race([
      Promise.resolve(promise).then(
        () => Object.freeze({ fulfilled: true, settled: true }),
        (reason) => Object.freeze({ fulfilled: false, reason, settled: true }),
      ),
      new Promise((resolve) => {
        timeout = setTimeout(
          () => resolve(Object.freeze({ fulfilled: false, settled: false })),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

export async function runWithTimeout(operation, {
  abortController,
  forceClose,
  timeoutMs,
}) {
  const operationPromise = Promise.resolve().then(operation);
  let timeout;
  try {
    return await Promise.race([
      operationPromise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => {
          const error = new Error('Claude Agent SDK live acceptance timed out.');
          error.code = 'live_acceptance_timeout';
          abortController.abort(error);
          reject(error);
        }, timeoutMs);
      }),
    ]);
  } catch (error) {
    if (error?.code !== 'live_acceptance_timeout') throw error;
    const forced = await settleBounded(
      Promise.resolve().then(forceClose),
      CLEANUP_TIMEOUT_MS,
    );
    const operationSettled = await settleBounded(operationPromise, CLEANUP_TIMEOUT_MS);
    const operationCleanupUnconfirmed = operationSettled.settled
      && !operationSettled.fulfilled
      && operationSettled.reason?.code === 'live_cleanup_unconfirmed';
    if (
      !forced.settled
      || !forced.fulfilled
      || !operationSettled.settled
      || operationCleanupUnconfirmed
    ) {
      const cleanupError = new Error(
        'Claude Agent SDK live acceptance cleanup could not be confirmed.',
      );
      cleanupError.code = 'live_cleanup_unconfirmed';
      throw cleanupError;
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function runLifecycleScenario({
  directory,
  databasePath,
  markPassed,
  observations,
  timeoutMs,
}) {
  const observed = createObservedQuery();
  const abortController = new AbortController();
  const configDirectory = path.join(directory, 'claude-config-lifecycle');
  const passedCaseIds = [];
  let database;
  let service;
  await runWithTimeout(async () => {
    try {
      database = new Database(databasePath);
      const first = acceptTurn(
        database,
        'lifecycle-one',
        'Reply with exactly GLOBAL42_LIFECYCLE_ONE and do not use tools.',
      );
      const second = acceptTurn(
        database,
        'lifecycle-two',
        'Reply with exactly GLOBAL42_LIFECYCLE_TWO and do not use tools.',
      );
      const adapter = createClaudeConversationAdapter({
        query: observed.query,
        idleTimeoutMs: 0,
        queryOptions: queryOptions({
          abortController,
          configDirectory,
          cwd: directory,
          tools: [],
        }),
      });
      service = createService({
        database,
        adapter,
        serviceInstanceId: `global42-lifecycle-before-${crypto.randomUUID()}`,
        workspaceRoot: directory,
      });
      const firstResult = await service.runNext();
      const secondResult = await service.runNext();
      await observed.waitForIdleCount(2, { signal: abortController.signal });
      assert(firstResult.status === 'completed', 'The first real Claude turn did not complete.');
      assert(secondResult.status === 'completed', 'The second real Claude turn did not complete.');
      assert(observed.calls.length === 1, 'Two turns did not share one long-lived SDK query.');
      assert(
        observed.initMessages.some(({ capabilities }) => (
          capabilities.includes('interrupt_receipt_v1')
        )),
        'The real Claude target did not advertise interrupt_receipt_v1.',
      );
      passedCaseIds.push('long_lived_async_multi_turn');

      const lineage = database.prepare(`
        SELECT provider_native_id FROM runtime_lineages WHERE lineage_id = ?
      `).get(first.lineage_id);
      assert(
        typeof lineage?.provider_native_id === 'string' && lineage.provider_native_id.length > 0,
        'The real Claude session ID was not durably bound.',
      );
      assert(first.lineage_id === second.lineage_id, 'The real multi-turn lineage changed.');
      const sessionId = lineage.provider_native_id;
      observations.session_id_sha256 = sha256(sessionId);
      passedCaseIds.push('durable_session_id');

      const evicted = await service.evictIdleExecutors();
      assert(evicted.includes(first.conversation_id), 'The idle real SDK query was not evicted.');
      const third = acceptTurn(
        database,
        'lifecycle-after-idle',
        'Reply with exactly GLOBAL42_AFTER_IDLE and do not use tools.',
      );
      const thirdResult = await service.runNext();
      assert(thirdResult.status === 'completed', 'The post-eviction Claude turn did not complete.');
      assert(
        observed.calls.length === 2,
        'Idle rebuild did not create exactly one replacement query.',
      );
      assert(observed.calls[1].resume === sessionId, 'Idle rebuild did not use the durable session ID.');
      passedCaseIds.push('idle_eviction_rebuild');

      await service.close();
      service = null;
      database.close();
      database = null;

      database = new Database(databasePath);
      const fourth = acceptTurn(
        database,
        'lifecycle-after-restart',
        'Reply with exactly GLOBAL42_AFTER_RESTART and do not use tools.',
      );
      const restartedAdapter = createClaudeConversationAdapter({
        query: observed.query,
        queryOptions: queryOptions({
          abortController,
          configDirectory,
          cwd: directory,
          tools: [],
        }),
      });
      service = createService({
        database,
        adapter: restartedAdapter,
        serviceInstanceId: `global42-lifecycle-after-${crypto.randomUUID()}`,
        workspaceRoot: directory,
      });
      const fourthResult = await service.runNext();
      assert(fourthResult.status === 'completed', 'The post-restart Claude turn did not complete.');
      assert(observed.calls.length === 3, 'Service restart did not create one replacement query.');
      assert(
        observed.calls[2].resume === sessionId,
        'Service restart did not resume the durable session.',
      );
      const reopened = database.prepare(`
        SELECT provider_native_id FROM runtime_lineages WHERE lineage_id = ?
      `).get(fourth.lineage_id);
      assert(
        reopened?.provider_native_id === sessionId,
        'The reopened database lost the Claude session ID.',
      );
      passedCaseIds.push('service_restart_resume');

      observations.lifecycle_query_count = observed.calls.length;
      observations.lifecycle_init_count = observed.initMessages.length;
      observations.lifecycle_models = Object.freeze([
        ...new Set(observed.initMessages.map(({ model }) => model).filter(Boolean)),
      ]);
      observations.lifecycle_capabilities = Object.freeze([
        ...new Set(observed.initMessages.flatMap(({ capabilities }) => capabilities)),
      ]);
      observations.idle_boundaries = observed.sessionStates
        .filter(({ state }) => state === 'idle').length;
    } finally {
      try {
        await closeScenario(service, observed);
      } finally {
        closeDatabaseQuietly(database);
      }
    }
  }, {
    abortController,
    forceClose: observed.closeAll,
    timeoutMs,
  });
  for (const caseId of passedCaseIds) markPassed(caseId);
}

function acceptanceMcpServer(onToolCall) {
  return createSdkMcpServer({
    name: ACCEPTANCE_MCP_SERVER,
    version: '1.0.0',
    tools: [tool(
      'approval_probe',
      'A bounded no-side-effect probe used only for Zylos Global42 permission acceptance.',
      { nonce: z.string() },
      async ({ nonce }) => {
        onToolCall(nonce);
        return { content: [{ type: 'text', text: `accepted:${nonce}` }] };
      },
      { annotations: { readOnlyHint: false, destructiveHint: false } },
    )],
  });
}

async function runPermissionScenario({
  directory,
  databasePath,
  markPassed,
  observations,
  timeoutMs,
}) {
  const observed = createObservedQuery();
  const abortController = new AbortController();
  const configDirectory = path.join(directory, 'claude-config-permission');
  const passedCaseIds = [];
  const toolCalls = [];
  let database;
  let service;
  await runWithTimeout(async () => {
    try {
      database = new Database(databasePath);
      const accepted = acceptTurn(
        database,
        'permission',
        `Call ${ACCEPTANCE_MCP_TOOL} exactly once with nonce GLOBAL42_PERMISSION, then report its result.`,
      );
      const server = acceptanceMcpServer((nonce) => toolCalls.push(nonce));
      const adapter = createClaudeConversationAdapter({
        query: observed.query,
        queryOptions: queryOptions({
          abortController,
          configDirectory,
          cwd: directory,
          mcpServers: { [ACCEPTANCE_MCP_SERVER]: server },
          tools: [ACCEPTANCE_MCP_TOOL],
        }),
      });
      service = createService({
        database,
        adapter,
        serviceInstanceId: `global42-permission-${crypto.randomUUID()}`,
        workspaceRoot: directory,
      });
      const waiting = await service.runNext();
      assert(waiting.status === 'waiting_user', 'The real SDK permission did not become durable.');
      const answer = service.submitInteractionAnswer(
        permissionAnswer(waiting.request, 'real-provider'),
      );
      const delivered = await service.deliverInteractionAnswer(answer.handoff_id);
      assert(
        delivered.execution?.status === 'completed',
        'The real SDK permission answer did not resume the provider turn.',
      );
      assert(toolCalls.length === 1, 'The approved real SDK tool did not execute exactly once.');
      assert(
        database.prepare('SELECT state FROM runtime_turns WHERE turn_id = ?')
          .get(accepted.turn_id)?.state === 'completed',
        'The approved real SDK permission turn was not durably completed.',
      );
      observations.permission_tool_calls = toolCalls.length;
      passedCaseIds.push('permission_interaction');
    } finally {
      try {
        await closeScenario(service, observed);
      } finally {
        closeDatabaseQuietly(database);
      }
    }
  }, {
    abortController,
    forceClose: observed.closeAll,
    timeoutMs,
  });
  for (const caseId of passedCaseIds) markPassed(caseId);
}

async function runCancelScenario({ directory, databasePath, markPassed, observations, timeoutMs }) {
  const observed = createObservedQuery();
  const abortController = new AbortController();
  const configDirectory = path.join(directory, 'claude-config-cancel');
  const passedCaseIds = [];
  let database;
  let service;
  await runWithTimeout(async () => {
    try {
      database = new Database(databasePath);
      const accepted = acceptTurn(
        database,
        'cancel',
        `Call ${ACCEPTANCE_MCP_TOOL} exactly once with nonce GLOBAL42_CANCEL and wait for approval.`,
      );
      const server = acceptanceMcpServer(() => {
        throw new Error('The cancelled acceptance tool must never execute.');
      });
      const adapter = createClaudeConversationAdapter({
        query: observed.query,
        queryOptions: queryOptions({
          abortController,
          configDirectory,
          cwd: directory,
          mcpServers: { [ACCEPTANCE_MCP_SERVER]: server },
          tools: [ACCEPTANCE_MCP_TOOL],
        }),
      });
      service = createService({
        database,
        adapter,
        serviceInstanceId: `global42-cancel-${crypto.randomUUID()}`,
        workspaceRoot: directory,
      });
      const waiting = await service.runNext();
      assert(waiting.status === 'waiting_user', 'The real SDK cancel probe never became active.');
      const stopped = await service.stop({
        conversation_id: accepted.conversation_id,
        stop_id: `global42-stop-${crypto.randomUUID()}`,
      });
      assert(stopped.status === 'stopped', 'The real SDK cancel was not durably confirmed.');
      assert(
        stopped.provider_stop_status === 'confirmed',
        'Core isolated the real Claude query without a confirmed SDK cancellation.',
      );
      assert(
        observed.interrupts.length === 1 && observed.interrupts[0].status === 'resolved',
        'The real Claude cancel did not call and resolve SDK query.interrupt exactly once.',
      );
      assert(
        observed.interrupts[0].valid_receipt === true,
        'The real Claude cancel did not return a valid interrupt receipt.',
      );
      assert(
        database.prepare('SELECT state FROM runtime_turns WHERE turn_id = ?')
          .get(accepted.turn_id)?.state === 'stopped',
        'The cancelled real SDK turn was not durably stopped.',
      );
      assert(
        database.prepare('SELECT state FROM runtime_interactions WHERE interaction_id = ?')
          .get(waiting.request.interaction_id)?.state === 'cancelled',
        'The cancelled real SDK permission remained answerable.',
      );
      observations.cancel_query_count = observed.calls.length;
      observations.cancel_interrupt_count = observed.interrupts.length;
      observations.cancel_interrupt_receipt_valid = observed.interrupts[0].valid_receipt;
      passedCaseIds.push('cancel');
    } finally {
      try {
        await closeScenario(service, observed);
      } finally {
        closeDatabaseQuietly(database);
      }
    }
  }, {
    abortController,
    forceClose: observed.closeAll,
    timeoutMs,
  });
  for (const caseId of passedCaseIds) markPassed(caseId);
}

export async function runLiveClaudeSdkAcceptance({
  timeoutMs = DEFAULT_TIMEOUT_MS,
} = {}) {
  const startedAt = new Date().toISOString();
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global42-live-'));
  const cases = [];
  const completed = new Set();
  const observations = {};

  function markPassed(caseId) {
    if (completed.has(caseId)) return;
    completed.add(caseId);
    cases.push(Object.freeze({
      case_id: caseId,
      status: 'passed',
      evidence_ref: `live:${startedAt}:${caseId}`,
    }));
  }

  async function runScenario(caseIds, operation) {
    try {
      await operation();
      return true;
    } catch (error) {
      for (const caseId of caseIds) {
        if (completed.has(caseId)) continue;
        completed.add(caseId);
        cases.push(Object.freeze({
          case_id: caseId,
          status: 'failed',
          evidence_ref: `live:${startedAt}:${caseId}`,
          error: safeError(error),
        }));
      }
      return error?.code !== 'live_cleanup_unconfirmed';
    }
  }

  try {
    const lifecycleCleanupConfirmed = await runScenario([
      'long_lived_async_multi_turn',
      'durable_session_id',
      'idle_eviction_rebuild',
      'service_restart_resume',
    ], () => runLifecycleScenario({
      directory,
      databasePath: path.join(directory, 'lifecycle.db'),
      markPassed,
      observations,
      timeoutMs,
    }));
    if (lifecycleCleanupConfirmed) {
      const permissionCleanupConfirmed = await runScenario(
        ['permission_interaction'],
        () => runPermissionScenario({
          directory,
          databasePath: path.join(directory, 'permission.db'),
          markPassed,
          observations,
          timeoutMs,
        }),
      );
      if (permissionCleanupConfirmed) {
        await runScenario(['cancel'], () => runCancelScenario({
          directory,
          databasePath: path.join(directory, 'cancel.db'),
          markPassed,
          observations,
          timeoutMs,
        }));
      }
    }
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }

  const passedCases = new Set(
    cases.filter(({ status }) => status === 'passed').map(({ case_id: caseId }) => caseId),
  );
  const complete = REQUIRED_REAL_CASE_IDS.every((caseId) => passedCases.has(caseId));
  return Object.freeze({
    status: complete && cases.every(({ status }) => status === 'passed')
      ? 'passed'
      : 'failed',
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    cases: Object.freeze(cases),
    observations: Object.freeze(observations),
  });
}

async function main() {
  if (!process.argv.slice(2).includes('--live')) {
    console.error('Refusing to call the real provider without --live.');
    process.exitCode = 2;
    return;
  }
  const result = await runLiveClaudeSdkAcceptance();
  console.log(`ZYLOS_CLAUDE_SDK_LIVE_EVIDENCE=${JSON.stringify(result)}`);
  process.exitCode = result.status === 'passed' ? 0 : 1;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().catch((error) => {
    console.error(`Claude SDK live acceptance failed: ${safeError(error).code ?? safeError(error).name}`);
    process.exitCode = 1;
  });
}
