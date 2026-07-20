import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';

import { query as sdkQuery } from '@anthropic-ai/claude-agent-sdk';

const DEFAULT_ENVIRONMENT_ALLOWLIST = Object.freeze([
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_AUTH_TOKEN',
  'ANTHROPIC_BASE_URL',
  'CLAUDE_CODE_OAUTH_TOKEN',
  'CLAUDE_CONFIG_DIR',
  'HOME',
  'HTTP_PROXY',
  'HTTPS_PROXY',
  'LANG',
  'LC_ALL',
  'NODE_EXTRA_CA_CERTS',
  'NO_PROXY',
  'PATH',
  'SHELL',
  'TERM',
  'TMPDIR',
  'USER',
]);
const CORE_MANAGED_CONTINUITY_OPTIONS = Object.freeze([
  'continue',
  'forkSession',
  'resume',
  'resumeSessionAt',
  'sessionId',
]);
const require = createRequire(import.meta.url);
const CORE_MANAGED_CONTINUITY_ARGUMENTS = Object.freeze(new Set([
  'c',
  'continue',
  'fork-session',
  'r',
  'resume',
  'resume-session-at',
  'session-id',
]));

function selectEnvironment(environment, allowlist = DEFAULT_ENVIRONMENT_ALLOWLIST) {
  const selected = {};
  for (const key of allowlist) {
    if (typeof environment[key] === 'string') selected[key] = environment[key];
  }
  return selected;
}

function normalizeCliArgument(argument) {
  return argument
    .replace(/([a-z])([A-Z])/g, '$1-$2')
    .replace(/^-+/, '')
    .replaceAll('_', '-')
    .split('=', 1)[0]
    .toLowerCase();
}

function defaultResolveClaudeExecutable(queryOptions) {
  if (typeof queryOptions.pathToClaudeCodeExecutable === 'string') {
    return queryOptions.pathToClaudeCodeExecutable;
  }
  const musl = process.platform === 'linux'
    && !process.report?.getReport?.()?.header?.glibcVersionRuntime;
  const platformPackage = `@anthropic-ai/claude-agent-sdk-${process.platform}-${process.arch}${
    musl ? '-musl' : ''
  }`;
  const executableName = process.platform === 'win32' ? 'claude.exe' : 'claude';
  try {
    return require.resolve(`${platformPackage}/${executableName}`);
  } catch {
    return 'claude';
  }
}

function defaultDetectNativeAuthentication(environment, { executable }) {
  const authEnvironment = { ...environment };
  delete authEnvironment.ANTHROPIC_API_KEY;
  delete authEnvironment.ANTHROPIC_AUTH_TOKEN;
  delete authEnvironment.CLAUDE_CODE_OAUTH_TOKEN;
  try {
    const output = execFileSync(executable, ['auth', 'status'], {
      encoding: 'utf8',
      env: authEnvironment,
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5_000,
    });
    const status = JSON.parse(output);
    return status?.loggedIn === true;
  } catch {
    return false;
  }
}

function defaultResolveEnvironment(environment, { nativeAuthentication }) {
  const resolved = { ...environment };
  if (nativeAuthentication) {
    delete resolved.ANTHROPIC_API_KEY;
    delete resolved.ANTHROPIC_AUTH_TOKEN;
    delete resolved.CLAUDE_CODE_OAUTH_TOKEN;
  }
  return resolved;
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function createAsyncQueue({
  mapValue = (value) => value,
  pauseAfterDelivery = false,
} = {}) {
  const buffered = [];
  const waiters = [];
  let closed = false;
  let failure = null;
  let paused = false;

  function delivered(value) {
    const mapped = mapValue(value);
    if (pauseAfterDelivery) paused = true;
    return { done: false, value: mapped };
  }

  function settleWaiter(waiter) {
    if (failure) waiter.reject(failure);
    else waiter.resolve({ done: true, value: undefined });
  }

  return Object.freeze({
    close(error = null) {
      if (closed) return;
      closed = true;
      failure = error;
      for (const waiter of waiters.splice(0)) settleWaiter(waiter);
    },
    push(value) {
      if (closed) throw new Error('Cannot push to a closed async queue.');
      const waiter = paused ? null : waiters.shift();
      if (waiter) waiter.resolve(delivered(value));
      else buffered.push(value);
    },
    remove(predicate) {
      const index = buffered.findIndex(predicate);
      if (index === -1) return false;
      buffered.splice(index, 1);
      return true;
    },
    resume() {
      if (closed) return;
      paused = false;
      const waiter = waiters.shift();
      if (waiter && buffered.length > 0) waiter.resolve(delivered(buffered.shift()));
      else if (waiter) waiters.unshift(waiter);
    },
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (!paused && buffered.length > 0) {
        return Promise.resolve(delivered(buffered.shift()));
      }
      if (closed) {
        if (failure) return Promise.reject(failure);
        return Promise.resolve({ done: true, value: undefined });
      }
      return new Promise((resolve, reject) => waiters.push({ reject, resolve }));
    },
  });
}

function assertSessionIdentity(executor, message) {
  const sessionId = message?.session_id;
  if (typeof sessionId !== 'string' || sessionId.length === 0) return null;
  if (executor.sessionId !== null && executor.sessionId !== sessionId) {
    throw new Error('Claude query changed session_id inside one conversation executor.');
  }
  executor.sessionId = sessionId;
  return sessionId;
}

async function establishDurableSession(executor, sessionId) {
  if (sessionId === null || executor.sessionBound) return;
  const activeTurn = executor.providerTurn;
  if (!activeTurn) {
    throw new Error('Claude emitted its first session_id without an active turn.');
  }
  const acknowledged = createDeferred();
  activeTurn.output.push({
    type: 'provider_native_id',
    provider_native_id: sessionId,
    acknowledge(error) {
      if (error) acknowledged.reject(error);
      else acknowledged.resolve();
    },
  });
  await acknowledged.promise;
  executor.sessionBound = true;
}

function normalizeAssistantMessage(executor, message) {
  const activeTurn = executor.providerTurn;
  if (!activeTurn || activeTurn.resultSeen || !Array.isArray(message?.message?.content)) return [];
  const records = [];
  for (const block of message.message.content) {
    if (block?.type === 'text' && typeof block.text === 'string') {
      activeTurn.text += block.text;
      records.push({
        type: 'normalized_event',
        event: {
          kind: 'text_snapshot',
          payload: {
            text: activeTurn.text,
            end_offset: activeTurn.text.length,
          },
          provider_native_id: executor.sessionId,
        },
      });
    } else if (
      block?.type === 'tool_use'
      && typeof block.id === 'string'
      && typeof block.name === 'string'
    ) {
      activeTurn.toolNames.set(block.id, block.name);
      records.push({
        type: 'normalized_event',
        event: {
          kind: 'tool_started',
          payload: {
            tool_use_id: block.id,
            tool_name: block.name,
            summary: `${block.name} started.`,
            side_effect_status: 'unknown',
          },
          provider_native_id: executor.sessionId,
        },
      });
    }
  }
  return records;
}

function normalizeToolProgress(executor, message) {
  const activeTurn = executor.providerTurn;
  if (
    !activeTurn
    || activeTurn.resultSeen
    || typeof message?.tool_use_id !== 'string'
    || typeof message?.tool_name !== 'string'
  ) {
    return [];
  }
  activeTurn.toolNames.set(message.tool_use_id, message.tool_name);
  const elapsedSeconds = Number.isFinite(message.elapsed_time_seconds)
    ? Math.max(0, message.elapsed_time_seconds)
    : 0;
  return [{
    type: 'normalized_event',
    event: {
      kind: 'tool_progress',
      payload: {
        tool_use_id: message.tool_use_id,
        tool_name: message.tool_name,
        summary: `${message.tool_name} running for ${elapsedSeconds}s.`,
        side_effect_status: 'unknown',
      },
      provider_native_id: executor.sessionId,
    },
  }];
}

function normalizeToolResults(executor, message) {
  const activeTurn = executor.providerTurn;
  if (!activeTurn || activeTurn.resultSeen || !Array.isArray(message?.message?.content)) return [];
  const records = [];
  for (const block of message.message.content) {
    if (block?.type !== 'tool_result' || typeof block.tool_use_id !== 'string') continue;
    const toolName = activeTurn.toolNames.get(block.tool_use_id) ?? 'Tool';
    records.push({
      type: 'normalized_event',
      event: {
        kind: 'tool_finished',
        payload: {
          tool_use_id: block.tool_use_id,
          tool_name: toolName,
          summary: `${toolName} finished.`,
          side_effect_status: 'known',
        },
        provider_native_id: executor.sessionId,
      },
    });
    activeTurn.toolNames.delete(block.tool_use_id);
  }
  return records;
}

function normalizeProviderMessage(executor, message) {
  if (message?.type === 'assistant') return normalizeAssistantMessage(executor, message);
  if (message?.type === 'tool_progress') return normalizeToolProgress(executor, message);
  if (message?.type === 'user') return normalizeToolResults(executor, message);
  return [];
}

function createSdkUserMessage(input, messageUuid) {
  const content = typeof input === 'string' ? input : input?.text;
  if (typeof content !== 'string' || content.length === 0) {
    throw new TypeError('Claude turn input must contain non-empty text.');
  }
  return {
    type: 'user',
    message: { role: 'user', content },
    parent_tool_use_id: null,
    uuid: messageUuid,
  };
}

function sameTurn(turn, context) {
  return turn?.context.turn_id === context.turn_id
    && turn.context.attempt.attempt_id === context.attempt.attempt_id
    && turn.context.attempt.attempt_no === context.attempt.attempt_no
    && turn.context.attempt.lease_epoch === context.attempt.lease_epoch;
}

function settleTurn(turn, { error = null, outcome = 'failed' } = {}) {
  if (!turn || turn.resultSeen) return;
  if (error) turn.output.close(error);
  else {
    turn.output.push({ type: 'turn_result', outcome });
    turn.output.close();
  }
  turn.resultSeen = true;
}

function settleUnfinishedTurns(executor, options = {}) {
  const turns = new Set([executor.providerTurn, executor.activeTurn]);
  for (const turn of turns) {
    settleTurn(turn, {
      ...options,
      outcome: turn?.cancelRequested ? 'cancelled' : (options.outcome ?? 'failed'),
    });
  }
}

function rejectPendingPermissions(executor, error) {
  const turns = new Set([executor.providerTurn, executor.activeTurn]);
  for (const turn of turns) {
    rejectTurnPermissions(turn, error);
  }
}

function rejectTurnPermissions(turn, error) {
  for (const { permission } of turn?.pendingPermissions?.values() ?? []) {
    permission.reject(error);
  }
}

async function requestToolPermission(executor, toolName, input, sdkContext) {
  const activeTurn = executor.providerTurn;
  if (!activeTurn || activeTurn.resultSeen) {
    return {
      behavior: 'deny',
      message: 'The provider attempt is no longer active.',
      interrupt: true,
    };
  }
  const interactionPolicy = activeTurn.controls.interactionPolicy;
  if (interactionPolicy) {
    const providerInteractionRef = sdkContext?.requestId
      ?? sdkContext?.toolUseID
      ?? `${activeTurn.messageUuid}:permission:${activeTurn.pendingPermissions.size + 1}`;
    const permission = createDeferred();
    activeTurn.pendingPermissions.set(providerInteractionRef, {
      input,
      permission,
      toolName,
    });
    const abort = () => {
      const error = new Error('Claude permission interaction was cancelled.');
      error.name = 'AbortError';
      permission.reject(error);
    };
    sdkContext?.signal?.addEventListener?.('abort', abort, { once: true });
    const descriptor = {
      provider_interaction_ref: providerInteractionRef,
      tool_use_id: sdkContext?.toolUseID ?? providerInteractionRef,
      kind: 'tool_approval',
      prompt: `Allow Claude to use ${toolName}?`,
      choices: [
        { choice_id: 'allow', label: 'Allow' },
        { choice_id: 'deny', label: 'Deny' },
      ],
      authorized_subjects: interactionPolicy.authorized_subjects,
      allowed_sources: interactionPolicy.allowed_sources,
    };
    let request;
    try {
      request = await activeTurn.controls.persistInteraction(descriptor);
    } catch (error) {
      activeTurn.pendingPermissions.delete(providerInteractionRef);
      settleTurn(activeTurn, { error });
      throw error;
    }
    activeTurn.output.push({ type: 'interaction_persisted', request });
    try {
      return await permission.promise;
    } finally {
      sdkContext?.signal?.removeEventListener?.('abort', abort);
      activeTurn.pendingPermissions.delete(providerInteractionRef);
    }
  }
  let decision;
  try {
    decision = await activeTurn.controls.requestPermission(
      Object.freeze({ tool_name: toolName, input }),
      Object.freeze({ signal: sdkContext?.signal }),
    );
  } catch (error) {
    // The SDK catches canUseTool failures on its private control channel. Close the
    // provider-neutral turn explicitly so persistence/fence failures reach Core.
    settleTurn(activeTurn, { error });
    throw error;
  }
  if (decision.behavior === 'allow') {
    return {
      behavior: 'allow',
      updatedInput: decision.updated_input ?? input,
    };
  }
  return {
    behavior: 'deny',
    message: decision.message ?? 'Permission denied.',
    interrupt: decision.interrupt ?? false,
  };
}

function updateBackgroundTasks(executor, message, now) {
  if (message?.type !== 'system') return;
  if (message.subtype === 'background_tasks_changed' && Array.isArray(message.tasks)) {
    const hadBackgroundTasks = executor.backgroundTaskIds.size > 0;
    executor.backgroundTaskIds = new Set(
      message.tasks.map(({ task_id: taskId }) => taskId).filter(
        (taskId) => typeof taskId === 'string' && taskId.length > 0,
      ),
    );
    if (hadBackgroundTasks && executor.backgroundTaskIds.size === 0) {
      executor.lastUsedAt = now();
      executor.notifySwitchable();
    }
    return;
  }
  if (typeof message.task_id !== 'string') return;
  if (message.subtype === 'task_started') {
    executor.backgroundTaskIds.add(message.task_id);
  } else if (message.subtype === 'task_notification') {
    const removed = executor.backgroundTaskIds.delete(message.task_id);
    if (removed && executor.backgroundTaskIds.size === 0) {
      executor.lastUsedAt = now();
      executor.notifySwitchable();
    }
  }
}

function createResidentExecutor({
  conversationId,
  lineageId,
  providerNativeId,
  query,
  queryOptions,
  now,
  onEnded,
}) {
  const executor = {
    activeTurn: null,
    backgroundTaskIds: new Set(),
    conversationId,
    closing: false,
    closePromise: null,
    ended: false,
    input: null,
    outputPump: null,
    query: null,
    queryFactory: query,
    queryOptions,
    providerTurn: null,
    residentContext: null,
    residentEnded: null,
    sessionBound: providerNativeId !== null,
    sessionId: providerNativeId,
    lastUsedAt: now(),
    lineageId,
    switchWaiters: [],
  };
  executor.notifySwitchable = () => {
    if (
      executor.activeTurn !== null
      || executor.providerTurn !== null
      || executor.backgroundTaskIds.size > 0
    ) return;
    for (const resolve of executor.switchWaiters.splice(0)) resolve();
  };
  executor.waitUntilSwitchable = () => {
    if (
      executor.activeTurn === null
      && executor.providerTurn === null
      && executor.backgroundTaskIds.size === 0
    ) return Promise.resolve();
    return new Promise((resolve) => executor.switchWaiters.push(resolve));
  };
  executor.input = createAsyncQueue({
    mapValue({ message, turn }) {
      executor.providerTurn = turn;
      return message;
    },
    pauseAfterDelivery: true,
  });

  executor.start = () => {
    if (executor.query) return;
    const options = { ...executor.queryOptions };
    if (executor.sessionId !== null) options.resume = executor.sessionId;
    options.canUseTool = (toolName, input, sdkContext) => requestToolPermission(
      executor,
      toolName,
      input,
      sdkContext,
    );
    executor.query = executor.queryFactory({
      prompt: executor.input,
      options,
    });
    executor.outputPump = (async () => {
      try {
        for await (const message of executor.query) {
          const sessionId = assertSessionIdentity(executor, message);
          await establishDurableSession(executor, sessionId);
          updateBackgroundTasks(executor, message, now);
          if (
            message?.type === 'system'
            && message.subtype === 'session_state_changed'
            && message.state === 'idle'
            && executor.providerTurn?.resultSeen
          ) {
            executor.providerTurn = null;
            executor.input.resume();
            executor.lastUsedAt = now();
            executor.notifySwitchable();
            continue;
          }
          const providerTurn = executor.providerTurn;
          for (const record of normalizeProviderMessage(executor, message)) {
            providerTurn?.output.push(record);
          }
          if (message?.type === 'result' && providerTurn && !providerTurn.resultSeen) {
            const outcome = providerTurn.cancelRequested
              ? 'cancelled'
              : (message.subtype === 'success' ? 'completed' : 'failed');
            settleTurn(providerTurn, { outcome });
            if (executor.activeTurn === providerTurn) executor.activeTurn = null;
            executor.lastUsedAt = now();
            executor.notifySwitchable();
          }
        }
      } catch (error) {
        rejectPendingPermissions(executor, error);
        settleUnfinishedTurns(executor, { error });
        executor.activeTurn = null;
        executor.providerTurn = null;
        executor.notifySwitchable();
      } finally {
        rejectPendingPermissions(executor, new Error('Claude provider query ended.'));
        settleUnfinishedTurns(executor);
        executor.activeTurn = null;
        executor.providerTurn = null;
        executor.ended = true;
        executor.notifySwitchable();
        if (!executor.closing) {
          onEnded(executor);
          try {
            executor.residentEnded?.(executor.residentContext);
          } catch {
            // Durable ownership reconciliation is retried by Core service startup/heartbeat.
          }
        }
      }
    })();
  };

  return executor;
}

export function createClaudeConversationAdapter({
  query = sdkQuery,
  queryOptions = {},
  idleTimeoutMs = 1_800_000,
  now = () => Date.now(),
  generateMessageUuid = randomUUID,
  environmentAllowlist = DEFAULT_ENVIRONMENT_ALLOWLIST,
  resolveEnvironment = defaultResolveEnvironment,
  detectNativeAuthentication = defaultDetectNativeAuthentication,
  resolveClaudeExecutable = defaultResolveClaudeExecutable,
}) {
  if (typeof query !== 'function') throw new TypeError('query must be a function');
  if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs < 0) {
    throw new TypeError('idleTimeoutMs must be a non-negative finite number');
  }
  if (typeof now !== 'function') throw new TypeError('now must be a function');
  if (typeof generateMessageUuid !== 'function') {
    throw new TypeError('generateMessageUuid must be a function');
  }
  if (typeof resolveEnvironment !== 'function') {
    throw new TypeError('resolveEnvironment must be a function');
  }
  if (typeof detectNativeAuthentication !== 'function') {
    throw new TypeError('detectNativeAuthentication must be a function');
  }
  if (typeof resolveClaudeExecutable !== 'function') {
    throw new TypeError('resolveClaudeExecutable must be a function');
  }
  for (const option of CORE_MANAGED_CONTINUITY_OPTIONS) {
    if (queryOptions[option] !== undefined) {
      throw new TypeError(`queryOptions.${option} is managed by Core lineage authority`);
    }
  }
  for (const option of Object.keys(queryOptions.extraArgs ?? {})) {
    if (
      /^-(?:r.+|c.+)$/i.test(option)
      || CORE_MANAGED_CONTINUITY_ARGUMENTS.has(normalizeCliArgument(option))
    ) {
      throw new TypeError(`queryOptions.extraArgs.${option} is managed by Core lineage authority`);
    }
  }
  if (queryOptions.executableArgs !== undefined && !Array.isArray(queryOptions.executableArgs)) {
    throw new TypeError('queryOptions.executableArgs must be an array');
  }
  for (const [index, argument] of (queryOptions.executableArgs ?? []).entries()) {
    if (typeof argument !== 'string') {
      throw new TypeError(`queryOptions.executableArgs[${index}] must be a string`);
    }
    if (
      /^-(?:r.+|c.+)$/i.test(argument)
      || CORE_MANAGED_CONTINUITY_ARGUMENTS.has(normalizeCliArgument(argument))
    ) {
      throw new TypeError(
        `queryOptions.executableArgs[${index}] is managed by Core lineage authority`,
      );
    }
  }
  if (!Array.isArray(environmentAllowlist) || environmentAllowlist.some(
    (key) => typeof key !== 'string' || key.length === 0,
  )) {
    throw new TypeError('environmentAllowlist must contain non-empty strings');
  }
  const environment = queryOptions.env ?? process.env;
  const frozenEnvironment = Object.freeze({ ...environment });
  const detectionEnvironment = Object.freeze(selectEnvironment(
    frozenEnvironment,
    environmentAllowlist,
  ));
  const hasStaticAuthentication = [
    'ANTHROPIC_API_KEY',
    'ANTHROPIC_AUTH_TOKEN',
    'CLAUDE_CODE_OAUTH_TOKEN',
  ].some((key) => typeof frozenEnvironment[key] === 'string');
  const claudeExecutable = resolveClaudeExecutable(queryOptions);
  if (typeof claudeExecutable !== 'string' || claudeExecutable.length === 0) {
    throw new TypeError('resolveClaudeExecutable must return a non-empty string');
  }
  const nativeAuthentication = hasStaticAuthentication
    && detectNativeAuthentication(detectionEnvironment, Object.freeze({
      executable: claudeExecutable,
    }));
  const resolvedEnvironment = resolveEnvironment(frozenEnvironment, Object.freeze({
    nativeAuthentication,
  }));
  if (!resolvedEnvironment || typeof resolvedEnvironment !== 'object') {
    throw new TypeError('resolveEnvironment must return an environment object');
  }
  const safeQueryOptions = {
    ...queryOptions,
    env: selectEnvironment(resolvedEnvironment, environmentAllowlist),
  };
  const executors = new Map();
  let lifecycle = 'open';

  async function* execute(context, controls = {}) {
    if (lifecycle !== 'open') {
      throw new Error(`Claude conversation adapter is ${lifecycle}.`);
    }
    if (typeof controls.requestPermission !== 'function') {
      throw new TypeError('controls.requestPermission must be a function');
    }
    if (controls.interactionPolicy && typeof controls.persistInteraction !== 'function') {
      throw new TypeError('controls.persistInteraction must be a function for durable interactions');
    }
    let executor = executors.get(context.conversation_id);
    if (executor?.closing) {
      throw new Error('The Claude conversation query is closing or failed to close.');
    }
    if (executor?.ended) {
      try {
        await closeExecutor(executor);
      } catch (error) {
        lifecycle = 'close_failed';
        throw error;
      }
      if (lifecycle !== 'open') {
        throw new Error(`Claude conversation adapter is ${lifecycle}.`);
      }
      executors.delete(context.conversation_id);
      executor = null;
    }
    if (executor && executor.lineageId !== context.lineage_id) {
      await executor.waitUntilSwitchable();
      if (lifecycle !== 'open') {
        throw new Error(`Claude conversation adapter is ${lifecycle}.`);
      }
      if (executors.get(context.conversation_id) === executor) {
        try {
          await closeExecutor(executor);
        } catch (error) {
          lifecycle = 'close_failed';
          throw error;
        }
        if (lifecycle !== 'open') {
          throw new Error(`Claude conversation adapter is ${lifecycle}.`);
        }
        if (executors.get(context.conversation_id) === executor) {
          executors.delete(context.conversation_id);
        }
      }
      executor = null;
    }
    if (!executor) {
      executor = createResidentExecutor({
        conversationId: context.conversation_id,
        lineageId: context.lineage_id,
        providerNativeId: context.provider_native_id,
        query,
        queryOptions: safeQueryOptions,
        now,
        onEnded(endedExecutor) {
          if (executors.get(context.conversation_id) === endedExecutor) {
            executors.delete(context.conversation_id);
          }
        },
      });
      executors.set(context.conversation_id, executor);
    }
    if (executor.activeTurn !== null) {
      throw new Error('A Claude conversation can only execute one active turn.');
    }
    if (
      context.provider_native_id !== null
      && executor.sessionId !== context.provider_native_id
    ) {
      throw new Error('The durable Claude session does not match the resident executor.');
    }

    const output = createAsyncQueue();
    const messageUuid = generateMessageUuid();
    if (typeof messageUuid !== 'string' || messageUuid.length === 0) {
      throw new TypeError('generateMessageUuid must return a non-empty string');
    }
    executor.activeTurn = {
      cancelRequested: false,
      controls,
      context,
      messageUuid,
      output,
      pendingPermissions: new Map(),
      resultSeen: false,
      text: '',
      toolNames: new Map(),
    };
    executor.residentContext = context;
    executor.residentEnded = controls.residentEnded ?? null;
    try {
      executor.start();
      executor.input.push({
        message: createSdkUserMessage(context.input, messageUuid),
        turn: executor.activeTurn,
      });
    } catch (error) {
      const failedTurn = executor.activeTurn;
      executor.activeTurn = null;
      if (executor.providerTurn === failedTurn) executor.providerTurn = null;
      failedTurn.output.close(error);
      executors.delete(context.conversation_id);
      await closeExecutor(executor);
    }
    yield* output;
  }

  async function cancel(context, { reason = 'stop' } = {}) {
    if (!['stop', 'steer'].includes(reason)) {
      throw new TypeError('Claude cancellation reason must be stop or steer.');
    }
    const executor = executors.get(context.conversation_id);
    const activeTurn = executor?.activeTurn;
    if (!sameTurn(activeTurn, context)) {
      throw new Error('Claude cancellation does not match the active provider attempt fence.');
    }
    if (typeof executor.query?.interrupt !== 'function') {
      throw new TypeError('Claude query.interrupt must be available for cancellation.');
    }
    activeTurn.cancelRequested = true;
    if (executor.providerTurn !== activeTurn && executor.input.remove(
      ({ turn }) => turn === activeTurn,
    )) {
      activeTurn.output.push({ type: 'turn_result', outcome: 'cancelled' });
      activeTurn.output.close();
      executor.activeTurn = null;
      executor.lastUsedAt = now();
      executor.notifySwitchable();
      return;
    }
    let receipt;
    try {
      receipt = await executor.query.interrupt();
    } catch (cause) {
      activeTurn.cancelRequested = false;
      const error = new Error(`Claude interruption is uncertain: ${cause.message}`, { cause });
      error.cancellationUncertain = true;
      throw error;
    }
    if (!receipt || !Array.isArray(receipt.still_queued)) {
      activeTurn.cancelRequested = false;
      const error = new Error('Claude interruption is uncertain: no valid receipt was returned.');
      error.cancellationUncertain = true;
      throw error;
    }
    const cancelledPermission = new Error('Claude permission interaction was cancelled.');
    cancelledPermission.name = 'AbortError';
    if (receipt?.still_queued?.includes(activeTurn.messageUuid)) {
      if (typeof executor.query.cancelAsyncMessage !== 'function') {
        activeTurn.cancelRequested = false;
        const error = new Error(
          'Claude queued-message cancellation is uncertain: cancelAsyncMessage is unavailable.',
        );
        error.cancellationUncertain = true;
        throw error;
      }
      let removed;
      try {
        removed = await executor.query.cancelAsyncMessage(activeTurn.messageUuid);
      } catch (cause) {
        activeTurn.cancelRequested = false;
        const error = new Error(
          `Claude queued-message cancellation is uncertain: ${cause.message}`,
          { cause },
        );
        error.cancellationUncertain = true;
        throw error;
      }
      if (removed !== true) {
        activeTurn.cancelRequested = false;
        const error = new Error(
          'Claude queued-message cancellation is uncertain: removal was not confirmed.',
        );
        error.cancellationUncertain = true;
        throw error;
      }
      rejectTurnPermissions(activeTurn, cancelledPermission);
      settleTurn(activeTurn, { outcome: 'cancelled' });
      if (executor.activeTurn === activeTurn) executor.activeTurn = null;
      executor.lastUsedAt = now();
      executor.notifySwitchable();
    } else {
      rejectTurnPermissions(activeTurn, cancelledPermission);
    }
  }

  async function closeExecutor(executor) {
    if (executor.closePromise) return executor.closePromise;
    executor.closing = true;
    executor.closePromise = (async () => {
      executor.input.close();
      rejectPendingPermissions(executor, new Error('Claude provider query closed.'));
      await executor.query?.close?.();
      if (executor.outputPump) await executor.outputPump;
    })();
    return executor.closePromise;
  }

  async function abort(context) {
    const executor = executors.get(context.conversation_id);
    if (!executor) return;
    const residentTurns = [executor.activeTurn, executor.providerTurn].filter(Boolean);
    const turn = sameTurn(executor.activeTurn, context)
      ? executor.activeTurn
      : (sameTurn(executor.providerTurn, context) ? executor.providerTurn : null);
    if (!turn && residentTurns.length > 0) {
      throw new Error('Claude abort does not match the active provider attempt fence.');
    }
    if (turn) {
      executor.input.remove(({ turn: queuedTurn }) => queuedTurn === turn);
      settleTurn(turn, { error: new Error('Claude provider query was isolated.') });
    }
    try {
      await closeExecutor(executor);
    } catch (error) {
      lifecycle = 'close_failed';
      throw error;
    }
    if (executors.get(context.conversation_id) === executor) {
      executors.delete(context.conversation_id);
    }
  }

  async function prepareInteractionAnswer(delivery) {
    const request = delivery?.request;
    const executor = executors.get(request?.conversation_id);
    const activeTurn = executor?.activeTurn;
    if (
      !activeTurn
      || activeTurn.context.turn_id !== request?.turn_id
      || activeTurn.context.attempt.attempt_id
        !== request?.runtime_fence?.provider_attempt_id
      || activeTurn.context.attempt.lease_epoch !== request?.runtime_fence?.lease_epoch
      || activeTurn.context.attempt.attempt_id !== delivery?.handoff?.provider_attempt_id
      || activeTurn.context.attempt.lease_epoch !== delivery?.handoff?.lease_epoch
      || typeof delivery?.handoff?.handoff_attempt_id !== 'string'
      || delivery.handoff.handoff_attempt_id.length === 0
      || !Number.isSafeInteger(delivery?.handoff?.handoff_attempt_no)
      || delivery.handoff.handoff_attempt_no < 1
    ) {
      throw new Error('Claude interaction answer does not match the active provider attempt fence.');
    }
    const providerInteractionRef = request.runtime_fence?.provider_interaction_ref;
    const pending = activeTurn.pendingPermissions.get(providerInteractionRef);
    if (!pending) {
      throw new Error('Claude interaction answer has no matching pending SDK permission.');
    }
    const answerDecision = delivery.answer?.value?.decision;
    const allowed = ['allow', 'approve', 'approved', 'yes'].includes(answerDecision);
    const expected = {
      interaction_id: request.interaction_id,
      request_version: request.version,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      lease_epoch: delivery.handoff.lease_epoch,
      handoff_id: delivery.handoff.handoff_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      answer_value: structuredClone(delivery.answer.value),
    };
    let sent = false;
    return Object.freeze({
      async send(startedDelivery) {
        const currentExecutor = executors.get(request.conversation_id);
        const currentTurn = currentExecutor?.activeTurn;
        const currentPending = currentTurn?.pendingPermissions.get(providerInteractionRef);
        if (
          sent
          || currentExecutor !== executor
          || currentTurn !== activeTurn
          || currentPending !== pending
          || currentTurn.context.turn_id !== startedDelivery?.request?.turn_id
          || startedDelivery?.request?.interaction_id !== expected.interaction_id
          || startedDelivery?.request?.version !== expected.request_version
          || startedDelivery?.request?.runtime_fence?.provider_attempt_id
            !== expected.provider_attempt_id
          || startedDelivery?.request?.runtime_fence?.lease_epoch !== expected.lease_epoch
          || startedDelivery?.handoff?.handoff_id !== expected.handoff_id
          || startedDelivery?.handoff?.provider_attempt_id !== expected.provider_attempt_id
          || startedDelivery?.handoff?.lease_epoch !== expected.lease_epoch
          || startedDelivery?.handoff?.handoff_attempt_id !== expected.handoff_attempt_id
          || startedDelivery?.handoff?.handoff_attempt_no !== expected.handoff_attempt_no
          || typeof startedDelivery?.handoff?.last_send_started_at !== 'string'
          || JSON.stringify(startedDelivery?.answer?.value) !== JSON.stringify(expected.answer_value)
        ) {
          throw new Error('The prepared Claude interaction answer lost its provider handoff fence.');
        }
        sent = true;
        activeTurn.pendingPermissions.delete(providerInteractionRef);
        pending.permission.resolve(allowed ? {
          behavior: 'allow',
          updatedInput: pending.input,
        } : {
          behavior: 'deny',
          message: 'Permission denied by the authorized user.',
          interrupt: false,
        });
        return {
          status: allowed ? 'accepted' : 'deny',
          handoff_id: startedDelivery.handoff.handoff_id,
          provider_attempt_id: startedDelivery.handoff.provider_attempt_id,
          handoff_attempt_id: startedDelivery.handoff.handoff_attempt_id,
          handoff_attempt_no: startedDelivery.handoff.handoff_attempt_no,
          lease_epoch: startedDelivery.handoff.lease_epoch,
          blocking_interactions_remaining: activeTurn.pendingPermissions.size > 0,
        };
      },
    });
  }

  async function queryInteractionHandoffAcceptance(delivery) {
    return Object.freeze({
      status: 'unknown',
      read_only: true,
      idempotent: true,
      handoff_id: delivery?.handoff?.handoff_id,
      provider_attempt_id: delivery?.handoff?.provider_attempt_id,
      handoff_attempt_id: delivery?.handoff?.handoff_attempt_id,
      handoff_attempt_no: delivery?.handoff?.handoff_attempt_no,
      lease_epoch: delivery?.handoff?.lease_epoch,
      accepted_at: null,
      evidence_ref: null,
      reason_code: 'provider_acceptance_query_unavailable',
    });
  }

  async function evictIdle({ canEvict, maxCount = Number.POSITIVE_INFINITY }) {
    if (typeof canEvict !== 'function') {
      throw new TypeError('canEvict must be a function');
    }
    if (!(maxCount === Number.POSITIVE_INFINITY || Number.isSafeInteger(maxCount) && maxCount >= 0)) {
      throw new TypeError('maxCount must be a non-negative safe integer or Infinity');
    }
    const evicted = [];
    const oldestFirst = [...executors.entries()].sort(
      ([, left], [, right]) => left.lastUsedAt - right.lastUsedAt,
    );
    for (const [conversationId, executor] of oldestFirst) {
      if (evicted.length >= maxCount) break;
      if (
        executor.activeTurn !== null
        || executor.providerTurn !== null
        || executor.backgroundTaskIds.size > 0
        || now() - executor.lastUsedAt < idleTimeoutMs
        || !await canEvict(conversationId)
      ) {
        continue;
      }
      if (
        executors.get(conversationId) !== executor
        || executor.activeTurn !== null
        || executor.providerTurn !== null
        || executor.backgroundTaskIds.size > 0
        || now() - executor.lastUsedAt < idleTimeoutMs
        || !await canEvict(conversationId)
      ) {
        continue;
      }
      if (
        executors.get(conversationId) !== executor
        || executor.activeTurn !== null
        || executor.providerTurn !== null
        || executor.backgroundTaskIds.size > 0
        || now() - executor.lastUsedAt < idleTimeoutMs
      ) {
        continue;
      }
      try {
        await closeExecutor(executor);
      } catch (error) {
        lifecycle = 'close_failed';
        throw error;
      }
      if (executors.get(conversationId) === executor) executors.delete(conversationId);
      evicted.push(conversationId);
    }
    return evicted;
  }

  async function close() {
    if (lifecycle === 'closed') return [];
    lifecycle = 'closing';
    const entries = [...executors.entries()];
    const results = await Promise.allSettled(
      entries.map(([, executor]) => closeExecutor(executor)),
    );
    const closedConversationIds = [];
    const failures = [];
    for (const [index, result] of results.entries()) {
      const [conversationId, executor] = entries[index];
      if (result.status === 'fulfilled') {
        if (executors.get(conversationId) === executor) executors.delete(conversationId);
        closedConversationIds.push(conversationId);
      } else {
        failures.push(result.reason);
      }
    }
    if (failures.length > 0) {
      lifecycle = 'close_failed';
      const error = new AggregateError(failures, 'One or more Claude queries failed to close.');
      error.closedConversationIds = closedConversationIds;
      throw error;
    }
    lifecycle = 'closed';
    return closedConversationIds;
  }

  function hasResident(conversationId) {
    return executors.has(conversationId);
  }

  return Object.freeze({
    abort,
    cancel,
    close,
    evictIdle,
    execute,
    prepareInteractionAnswer,
    queryInteractionHandoffAcceptance,
    hasResident,
  });
}
