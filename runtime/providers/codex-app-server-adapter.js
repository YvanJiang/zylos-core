import { spawn } from 'node:child_process';
import path from 'node:path';
import { createInterface } from 'node:readline';

const DEFAULT_ENV_ALLOWLIST = Object.freeze([
  'CODEX_API_KEY',
  'CODEX_HOME',
  'COLORTERM',
  'HOME',
  'HTTPS_PROXY',
  'HTTP_PROXY',
  'LANG',
  'LC_ALL',
  'LC_CTYPE',
  'LOGNAME',
  'NO_COLOR',
  'NO_PROXY',
  'OPENAI_API_KEY',
  'OPENAI_BASE_URL',
  'PATH',
  'SHELL',
  'SSH_AUTH_SOCK',
  'SSL_CERT_DIR',
  'SSL_CERT_FILE',
  'TERM',
  'TMPDIR',
  'USER',
  'https_proxy',
  'http_proxy',
  'no_proxy',
]);

function providerErrorFor(code) {
  if (code === 'provider_context_invalid') {
    return {
      code: 'provider_context_invalid',
      category: 'provider',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The provider conversation could not be loaded safely.',
    };
  }
  if (code === 'unsupported_capability') {
    return {
      code: 'unsupported_capability',
      category: 'provider',
      retryable: false,
      side_effect_status: 'unknown',
      user_message: 'The provider requested an unsupported interaction.',
    };
  }
  return {
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    side_effect_status: 'unknown',
    user_message: 'The provider connection failed after side effects may have occurred.',
  };
}

export class CodexAppServerAdapterError extends Error {
  constructor(code, message, { cause } = {}) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'CodexAppServerAdapterError';
    this.code = code;
    this.providerError = Object.freeze(providerErrorFor(code));
  }
}

function rejectProtocol(message, code = 'provider_protocol_invalid') {
  throw new CodexAppServerAdapterError(code, message);
}

function requestIdKey(value) {
  if (typeof value === 'string') return `s:${value}`;
  if (Number.isSafeInteger(value) && !Object.is(value, -0)) return `n:${value}`;
  rejectProtocol('Codex app-server emitted an invalid JSON-RPC request ID.');
}

function signalSupervisedProcessGroup(processGroupId, child, signal) {
  if (processGroupId === null) return child.kill(signal);
  return process.kill(-processGroupId, signal);
}

function supervisedProcessGroupIsAlive(processGroupId) {
  try {
    process.kill(-processGroupId, 0);
    return true;
  } catch (error) {
    if (error?.code === 'ESRCH') return false;
    throw error;
  }
}

function serverRequestTombstone({ thread_id: threadId, turn_id: turnId, method, params }) {
  return Object.freeze({
    thread_id: threadId,
    turn_id: turnId,
    method,
    params_json: JSON.stringify(params),
  });
}

function selectEnvironment(source, allowlist) {
  const selected = {};
  for (const name of allowlist) {
    if (Object.hasOwn(source, name) && typeof source[name] === 'string') {
      selected[name] = source[name];
    }
  }
  return selected;
}

function requireExecutionContext(context) {
  if (!context || typeof context !== 'object') {
    throw new TypeError('execution context must be an object');
  }
  if (!context.lineage || typeof context.lineage !== 'object') {
    throw new TypeError('execution context.lineage must be an object');
  }
  const providerNativeId = context.lineage.provider_native_id;
  if (
    providerNativeId !== null
    && (typeof providerNativeId !== 'string' || providerNativeId.length === 0)
  ) {
    throw new TypeError('lineage.provider_native_id must be null or a non-empty string');
  }
  if (typeof context.bindProviderNativeId !== 'function') {
    throw new TypeError('bindProviderNativeId must be a function');
  }
  if (typeof context.reportProviderState !== 'function') {
    throw new TypeError('reportProviderState must be a function');
  }
  if (
    context.reportProviderFailure !== undefined
    && typeof context.reportProviderFailure !== 'function'
  ) {
    throw new TypeError('reportProviderFailure must be a function when provided');
  }
  if (
    !context.attempt
    || typeof context.attempt.attempt_id !== 'string'
    || context.attempt.attempt_id.length === 0
    || !Number.isSafeInteger(context.attempt.attempt_no)
    || context.attempt.attempt_no < 1
    || !Number.isSafeInteger(context.attempt.lease_epoch)
    || context.attempt.lease_epoch < 1
  ) {
    throw new TypeError('attempt must contain a complete provider attempt fence');
  }
  if (
    !context.input
    || typeof context.input !== 'object'
    || typeof context.input.text !== 'string'
    || context.input.text.trim().length === 0
  ) {
    throw new TypeError('input.text must be a non-empty string');
  }
}

function parseMessage(line) {
  try {
    const message = JSON.parse(line);
    if (!message || typeof message !== 'object' || Array.isArray(message)) throw new Error();
    return message;
  } catch {
    rejectProtocol('Codex app-server emitted invalid JSON.');
  }
}

function activeRunKey(threadId, turnId) {
  return `${threadId}\u0000${turnId}`;
}

const TOOL_ITEMS = Object.freeze({
  commandExecution: Object.freeze({
    name: 'command',
    label: 'Command',
    sideEffect: 'unknown',
    progressMethods: Object.freeze(['item/commandExecution/outputDelta']),
    statusMode: 'enum',
    terminalStatuses: Object.freeze(['completed', 'failed', 'declined']),
  }),
  fileChange: Object.freeze({
    name: 'file_change',
    label: 'File change',
    sideEffect: 'unknown',
    progressMethods: Object.freeze(['item/fileChange/outputDelta']),
    statusMode: 'enum',
    terminalStatuses: Object.freeze(['completed', 'failed', 'declined']),
  }),
  mcpToolCall: Object.freeze({
    name: 'external_tool',
    label: 'External tool',
    sideEffect: 'unknown',
    progressMethods: Object.freeze(['item/mcpToolCall/progress']),
    statusMode: 'enum',
    terminalStatuses: Object.freeze(['completed', 'failed']),
  }),
  dynamicToolCall: Object.freeze({
    name: 'external_tool',
    label: 'External tool',
    sideEffect: 'unknown',
    progressMethods: Object.freeze([]),
    statusMode: 'enum',
    terminalStatuses: Object.freeze(['completed', 'failed']),
  }),
  collabAgentToolCall: Object.freeze({
    name: 'collaboration',
    label: 'Collaboration',
    sideEffect: 'unknown',
    progressMethods: Object.freeze([]),
    statusMode: 'enum',
    terminalStatuses: Object.freeze(['completed', 'failed']),
  }),
  webSearch: Object.freeze({
    name: 'web_search',
    label: 'Web search',
    sideEffect: 'none',
    progressMethods: Object.freeze([]),
    statusMode: 'absent',
    terminalStatuses: null,
  }),
  imageGeneration: Object.freeze({
    name: 'image_generation',
    label: 'Image generation',
    sideEffect: 'unknown',
    progressMethods: Object.freeze([]),
    statusMode: 'opaque',
    terminalStatuses: null,
  }),
});

const IGNORED_ITEM_TYPES = Object.freeze(new Set([
  'agentMessage',
  'contextCompaction',
  'enteredReviewMode',
  'exitedReviewMode',
  'hookPrompt',
  'imageView',
  'plan',
  'reasoning',
  'sleep',
  'subAgentActivity',
  'userMessage',
]));

const IGNORED_SCOPED_NOTIFICATIONS = Object.freeze(new Set([
  'item/fileChange/patchUpdated',
  'item/plan/delta',
  'item/reasoning/summaryPartAdded',
  'item/reasoning/summaryTextDelta',
  'item/reasoning/textDelta',
  'thread/compacted',
  'thread/tokenUsage/updated',
  'turn/diff/updated',
  'turn/moderationMetadata',
  'turn/plan/updated',
]));

const HOOK_EVENT_NAMES = Object.freeze(new Set([
  'preToolUse',
  'permissionRequest',
  'postToolUse',
  'preCompact',
  'postCompact',
  'sessionStart',
  'userPromptSubmit',
  'subagentStart',
  'subagentStop',
  'stop',
]));
const HOOK_HANDLER_TYPES = Object.freeze(new Set(['command', 'prompt', 'agent']));
const HOOK_EXECUTION_MODES = Object.freeze(new Set(['sync', 'async']));
const HOOK_TERMINAL_STATUSES = Object.freeze(new Set([
  'completed',
  'failed',
  'blocked',
  'stopped',
]));

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function jsonSchemaStringLength(value) {
  return Array.from(value).length;
}

function hasOnlyKeys(value, allowedKeys) {
  return isRecord(value) && Object.keys(value).every((key) => allowedKeys.has(key));
}

function isNullableString(value) {
  return value === null || typeof value === 'string';
}

function isNullableStringArray(value) {
  return value === null || (
    Array.isArray(value)
    && value.every((entry) => typeof entry === 'string' && entry.length > 0)
  );
}

function isPermissionPath(value) {
  if (!isRecord(value) || typeof value.type !== 'string') return false;
  if (value.type === 'path') {
    return hasOnlyKeys(value, new Set(['path', 'type']))
      && typeof value.path === 'string'
      && value.path.length > 0;
  }
  if (value.type === 'glob_pattern') {
    return hasOnlyKeys(value, new Set(['pattern', 'type']))
      && typeof value.pattern === 'string'
      && value.pattern.length > 0;
  }
  if (value.type !== 'special' || !hasOnlyKeys(value, new Set(['type', 'value']))) return false;
  const special = value.value;
  if (!isRecord(special) || typeof special.kind !== 'string') return false;
  if (['root', 'minimal', 'tmpdir', 'slash_tmp'].includes(special.kind)) {
    return hasOnlyKeys(special, new Set(['kind']));
  }
  if (special.kind === 'project_roots') {
    return hasOnlyKeys(special, new Set(['kind', 'subpath']))
      && (special.subpath === undefined || isNullableString(special.subpath));
  }
  if (special.kind === 'unknown') {
    return hasOnlyKeys(special, new Set(['kind', 'path', 'subpath']))
      && typeof special.path === 'string'
      && special.path.length > 0
      && (special.subpath === undefined || isNullableString(special.subpath));
  }
  return false;
}

function isPermissionProfile(value) {
  if (
    !hasOnlyKeys(value, new Set(['fileSystem', 'network']))
    || !Object.hasOwn(value, 'fileSystem')
    || !Object.hasOwn(value, 'network')
  ) {
    return false;
  }
  const network = value.network;
  if (
    network !== undefined
    && network !== null
    && (!hasOnlyKeys(network, new Set(['enabled']))
      || !Object.hasOwn(network, 'enabled')
      || (network.enabled !== null
        && typeof network.enabled !== 'boolean'))
  ) {
    return false;
  }
  const fileSystem = value.fileSystem;
  if (fileSystem === undefined || fileSystem === null) return true;
  if (!hasOnlyKeys(
    fileSystem,
    new Set(['entries', 'globScanMaxDepth', 'read', 'write']),
  ) || !Object.hasOwn(fileSystem, 'read') || !Object.hasOwn(fileSystem, 'write')) {
    return false;
  }
  if (
    fileSystem.entries !== undefined
    && fileSystem.entries !== null
    && (!Array.isArray(fileSystem.entries) || fileSystem.entries.some((entry) => (
      !hasOnlyKeys(entry, new Set(['access', 'path']))
      || !['read', 'write', 'deny'].includes(entry.access)
      || !isPermissionPath(entry.path)
    )))
  ) {
    return false;
  }
  if (
    fileSystem.globScanMaxDepth !== undefined
    && fileSystem.globScanMaxDepth !== null
    && (!Number.isSafeInteger(fileSystem.globScanMaxDepth) || fileSystem.globScanMaxDepth < 1)
  ) {
    return false;
  }
  return isNullableStringArray(fileSystem.read) && isNullableStringArray(fileSystem.write);
}

function toolDescriptor(run, itemId, specification, kind, verb) {
  return {
    kind,
    provider_native_id: run.thread_id,
    payload: {
      tool_use_id: itemId,
      tool_name: specification.name,
      summary: `${specification.label} ${verb}.`,
      side_effect_status: specification.sideEffect,
    },
  };
}

function hookDescriptor(run, toolUseId, kind, verb) {
  return {
    kind,
    provider_native_id: run.thread_id,
    payload: {
      tool_use_id: toolUseId,
      tool_name: 'provider_hook',
      summary: `Provider hook ${verb}.`,
      side_effect_status: 'unknown',
    },
  };
}

function textSnapshot(run) {
  return run.text_item_order.map((itemId) => run.text_by_item.get(itemId) ?? '').join('');
}

class AsyncEventQueue {
  constructor() {
    this.values = [];
    this.waiters = [];
    this.ended = false;
    this.error = null;
  }

  push(value) {
    if (this.ended || this.error) return false;
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ done: false, value });
    else this.values.push(value);
    return true;
  }

  end() {
    if (this.ended || this.error) return;
    this.ended = true;
    for (const waiter of this.waiters.splice(0)) waiter.resolve({ done: true, value: undefined });
  }

  fail(error) {
    if (this.ended || this.error) return;
    this.error = error;
    this.values.length = 0;
    for (const waiter of this.waiters.splice(0)) waiter.reject(error);
  }

  next() {
    if (this.values.length > 0) return Promise.resolve({ done: false, value: this.values.shift() });
    if (this.error) return Promise.reject(this.error);
    if (this.ended) return Promise.resolve({ done: true, value: undefined });
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

function requireThreadResult(result, expectedThreadId = null) {
  const threadId = result?.thread?.id;
  if (typeof threadId !== 'string' || threadId.length === 0) {
    rejectProtocol('Codex app-server returned a thread without an ID.');
  }
  if (expectedThreadId !== null && threadId !== expectedThreadId) {
    rejectProtocol('Codex app-server loaded a different provider lineage.', 'provider_context_invalid');
  }
  return threadId;
}

function requireTurnResult(result) {
  const turnId = result?.turn?.id;
  if (typeof turnId !== 'string' || turnId.length === 0) {
    rejectProtocol('Codex app-server returned a turn without an ID.');
  }
  return turnId;
}

function requireEmptyResult(result, method) {
  if (!isRecord(result) || Object.keys(result).length !== 0) {
    rejectProtocol(`Codex app-server returned an invalid ${method} response.`);
  }
}

function requireThreadHistory(result) {
  const turns = result?.thread?.turns;
  if (!Array.isArray(turns)) {
    rejectProtocol('Codex app-server reloaded a thread without bounded turn history.');
  }
  return turns.map((turn) => {
    if (typeof turn?.id !== 'string' || turn.id.length === 0) {
      rejectProtocol('Codex app-server reloaded a thread with an invalid turn fence.');
    }
    return turn.id;
  });
}

function sameAttempt(left, right) {
  return left.attempt_id === right.attempt_id
    && left.attempt_no === right.attempt_no
    && left.lease_epoch === right.lease_epoch;
}

function coreAttemptKey(coreTurnId, attempt) {
  return [
    coreTurnId,
    attempt.attempt_id,
    attempt.attempt_no,
    attempt.lease_epoch,
  ].join('\u0000');
}

export function createCodexAppServerAdapter({
  codexExecutable = 'codex',
  spawnProcess = spawn,
  cwd,
  env = process.env,
  envAllowlist = DEFAULT_ENV_ALLOWLIST,
  clientInfo = Object.freeze({ name: 'zylos-core', title: 'Zylos Core', version: '0.6.0' }),
  approvalPolicy = 'on-request',
  sandbox = 'workspace-write',
  interruptConfirmationTimeoutMs = 5_000,
  processTerminationGraceMs = 5_000,
  maxConnectionFenceEntries = 4_096,
  signalProcessGroup = signalSupervisedProcessGroup,
  isProcessGroupAlive = supervisedProcessGroupIsAlive,
  setTimeoutFn = setTimeout,
  clearTimeoutFn = clearTimeout,
} = {}) {
  if (typeof codexExecutable !== 'string' || codexExecutable.length === 0) {
    throw new TypeError('codexExecutable must be a non-empty string');
  }
  if (typeof spawnProcess !== 'function') throw new TypeError('spawnProcess must be a function');
  if (!Array.isArray(envAllowlist) || envAllowlist.some((name) => typeof name !== 'string')) {
    throw new TypeError('envAllowlist must be an array of strings');
  }
  if (cwd !== undefined && (typeof cwd !== 'string' || !path.isAbsolute(cwd))) {
    throw new TypeError('cwd must be an absolute path when provided');
  }
  if (!['untrusted', 'on-request', 'never'].includes(approvalPolicy)) {
    throw new TypeError('approvalPolicy must be untrusted, on-request, or never');
  }
  if (!['read-only', 'workspace-write', 'danger-full-access'].includes(sandbox)) {
    throw new TypeError('sandbox must be read-only, workspace-write, or danger-full-access');
  }
  if (
    !Number.isSafeInteger(interruptConfirmationTimeoutMs)
    || interruptConfirmationTimeoutMs <= 0
  ) {
    throw new TypeError('interruptConfirmationTimeoutMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(processTerminationGraceMs) || processTerminationGraceMs <= 0) {
    throw new TypeError('processTerminationGraceMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(maxConnectionFenceEntries) || maxConnectionFenceEntries <= 0) {
    throw new TypeError('maxConnectionFenceEntries must be a positive safe integer');
  }
  if (typeof signalProcessGroup !== 'function' || typeof isProcessGroupAlive !== 'function') {
    throw new TypeError('process-group supervision functions must be callable');
  }
  if (typeof setTimeoutFn !== 'function' || typeof clearTimeoutFn !== 'function') {
    throw new TypeError('timeout functions must be callable');
  }

  const childEnvironment = selectEnvironment(env, envAllowlist);
  const loadedThreads = new Set();
  const activeRuns = new Map();
  const startingRuns = new Map();
  const inFlightTurnStarts = new Set();
  const terminalRuns = new Map();
  const providerRequests = new Map();
  const pendingInteractions = new Map();
  let connection = null;
  let connecting = null;
  let nextConnectionNo = 0;
  let nextHookNo = 0;
  let nextInteractionNo = 0;

  function rememberConnectionFence(target, fences, key, value = true) {
    if (!fences.has(key) && fences.size >= maxConnectionFenceEntries) {
      throw new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server connection reached its safe late-fence retention bound.',
      );
    }
    if (fences instanceof Map) fences.set(key, value);
    else fences.add(key);
  }

  function supervisedProcessGroupExited(target) {
    if (!target.closed_observed) return false;
    if (target.process_group_id === null) return true;
    try {
      return !isProcessGroupAlive(target.process_group_id);
    } catch {
      return false;
    }
  }

  function requestProcessTermination(target) {
    if (target.termination_requested) return;
    target.termination_requested = true;
    if (supervisedProcessGroupExited(target)) return;
    try {
      signalProcessGroup(target.process_group_id, target.child, 'SIGTERM');
    } catch {
      // Escalation below remains responsible for proving process exit.
    }
    if (supervisedProcessGroupExited(target)) return;
    target.termination_timer = setTimeoutFn(() => {
      target.termination_timer = null;
      if (supervisedProcessGroupExited(target)) return;
      try {
        signalProcessGroup(target.process_group_id, target.child, 'SIGKILL');
      } catch {
        // A missing close event remains a fail-closed supervision result.
      }
    }, processTerminationGraceMs);
  }

  function waitForProcessClose(target) {
    if (supervisedProcessGroupExited(target)) return Promise.resolve(true);
    return new Promise((resolve) => {
      let settled = false;
      let pollTimer = null;
      let pollScheduled = false;
      const deadlineTimer = setTimeoutFn(() => {
        if (settled) return;
        settled = true;
        if (pollTimer !== null) clearTimeoutFn(pollTimer);
        resolve(false);
      }, processTerminationGraceMs * 2);
      const check = () => {
        if (settled) return;
        if (supervisedProcessGroupExited(target)) {
          settled = true;
          clearTimeoutFn(deadlineTimer);
          if (pollTimer !== null) clearTimeoutFn(pollTimer);
          resolve(true);
          return;
        }
        if (pollScheduled) return;
        pollScheduled = true;
        pollTimer = setTimeoutFn(() => {
          pollScheduled = false;
          pollTimer = null;
          check();
        }, Math.min(25, processTerminationGraceMs));
      };
      target.closed.then(check);
      check();
    });
  }

  function discardProviderRequestsForRun(target, run, failure) {
    const discardedGroups = new Set();
    for (const [requestKey, group] of providerRequests) {
      if (group.run !== run) continue;
      discardedGroups.add(group);
      rememberConnectionFence(
        target,
        target.retired_server_requests,
        requestIdKey(group.request_id),
        serverRequestTombstone(group),
      );
      group.rejectResolved(failure);
      providerRequests.delete(requestKey);
    }
    for (const [reference, entry] of pendingInteractions) {
      if (discardedGroups.has(entry.group)) pendingInteractions.delete(reference);
    }
    return discardedGroups.size;
  }

  function failConnection(target, error, { terminate = true } = {}) {
    if (target.failed) return;
    target.failed = true;
    const failure = error instanceof CodexAppServerAdapterError
      ? error
      : new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server connection closed unexpectedly.',
        { cause: error },
      );
    for (const pending of target.pending.values()) pending.reject(failure);
    target.pending.clear();
    if (terminate && !target.termination_requested) {
      requestProcessTermination(target);
    }
    for (const [runKey, run] of activeRuns) {
      if (run.connection_id !== target.connection_id) continue;
      target.affected_conversation_ids.add(run.context.conversation_id);
      try {
        run.context.reportProviderFailure?.(failure);
      } catch {
        // The provider failure remains authoritative even if Core cannot persist recovery.
      }
      run.rejectTerminal(failure);
      run.queue.fail(failure);
      activeRuns.delete(runKey);
    }
    for (const [runKey, run] of startingRuns) {
      if (run.connection_id !== target.connection_id) continue;
      target.affected_conversation_ids.add(run.context.conversation_id);
      try {
        run.context.reportProviderFailure?.(failure);
      } catch {
        // The provider failure remains authoritative even if Core cannot persist recovery.
      }
      run.rejectTerminal(failure);
      run.queue.fail(failure);
      startingRuns.delete(runKey);
    }
    for (const run of inFlightTurnStarts) {
      if (run.connection_id !== target.connection_id) continue;
      target.affected_conversation_ids.add(run.context.conversation_id);
      try {
        run.context.reportProviderFailure?.(failure);
      } catch {
        // The provider failure remains authoritative even if Core cannot persist recovery.
      }
      run.rejectTerminal(failure);
      run.queue.fail(failure);
      inFlightTurnStarts.delete(run);
    }
    const failedGroups = new Set();
    for (const [requestKey, group] of providerRequests) {
      if (group.connection_id !== target.connection_id) continue;
      failedGroups.add(group);
      group.rejectResolved(failure);
      providerRequests.delete(requestKey);
    }
    for (const [reference, entry] of pendingInteractions) {
      if (failedGroups.has(entry.group)) pendingInteractions.delete(reference);
    }
  }

  function handleNotification(target, message) {
    const { method, params } = message;
    if (method === 'thread/started') return;
    if (method === 'serverRequest/resolved') {
      const requestId = requestIdKey(params?.requestId);
      const group = providerRequests.get(`${target.connection_id}:${requestId}`);
      if (!group) {
        if (target.retired_server_requests.get(requestId)?.thread_id === params?.threadId) return;
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server resolved a stale or mismatched server request.',
        ));
        return;
      }
      if (group.thread_id !== params?.threadId || group.connection_id !== target.connection_id) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server resolved a stale or mismatched server request.',
        ));
        return;
      }
      if (!group.response_sent) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server resolved a server request before receiving its answer.',
        ));
        return;
      }
      rememberConnectionFence(
        target,
        target.retired_server_requests,
        requestId,
        serverRequestTombstone(group),
      );
      group.resolveResolved();
      return;
    }
    const threadId = params?.threadId;
    const turnId = params?.turnId ?? params?.turn?.id;
    if (typeof threadId !== 'string' || typeof turnId !== 'string') return;
    const runKey = activeRunKey(threadId, turnId);
    if (method === 'turn/started') {
      const startingRun = startingRuns.get(runKey);
      if (!startingRun || startingRun.connection_id !== target.connection_id) {
        if (target.retired_run_keys.has(runKey)) return;
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server started a stale or mismatched turn.',
        ));
        return;
      }
      startingRun.context.reportProviderState({
        state: 'started',
        provider_native_id: startingRun.thread_id,
      });
      startingRuns.delete(runKey);
      activeRuns.set(runKey, startingRun);
      return;
    }
    const run = activeRuns.get(runKey);
    if (!run || run.connection_id !== target.connection_id) {
      if (target.retired_run_keys.has(runKey)) return;
      failConnection(target, new CodexAppServerAdapterError(
        'provider_protocol_invalid',
        'Codex app-server emitted a stale or mismatched turn notification.',
      ));
      return;
    }
    if (IGNORED_SCOPED_NOTIFICATIONS.has(method)) return;
    if (method === 'hook/started') {
      const hook = params.run;
      if (
        !isRecord(hook)
        || typeof hook.id !== 'string'
        || hook.id.length === 0
        || hook.scope !== 'turn'
        || hook.status !== 'running'
        || !HOOK_EVENT_NAMES.has(hook.eventName)
        || !HOOK_HANDLER_TYPES.has(hook.handlerType)
        || !HOOK_EXECUTION_MODES.has(hook.executionMode)
        || run.hook_runs.has(hook.id)
      ) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server emitted an invalid or duplicate turn hook start.',
        ));
        return;
      }
      nextHookNo += 1;
      const started = {
        event_name: hook.eventName,
        execution_mode: hook.executionMode,
        handler_type: hook.handlerType,
        tool_use_id: `provider-hook-${nextHookNo}`,
      };
      run.hook_runs.set(hook.id, started);
      run.queue.push(hookDescriptor(run, started.tool_use_id, 'tool_started', 'started'));
      return;
    }
    if (method === 'hook/completed') {
      const hook = params.run;
      const started = isRecord(hook) ? run.hook_runs.get(hook.id) : null;
      if (
        !started
        || hook.scope !== 'turn'
        || !HOOK_TERMINAL_STATUSES.has(hook.status)
        || hook.eventName !== started.event_name
        || hook.handlerType !== started.handler_type
        || hook.executionMode !== started.execution_mode
      ) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server emitted a mismatched turn hook completion.',
        ));
        return;
      }
      run.hook_runs.delete(hook.id);
      run.queue.push(hookDescriptor(run, started.tool_use_id, 'tool_finished', hook.status));
      return;
    }
    if (method === 'item/agentMessage/delta') {
      if (
        typeof params.itemId !== 'string'
        || params.itemId.length === 0
        || typeof params.delta !== 'string'
        || params.delta.length === 0
      ) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server emitted an invalid agent message delta.',
        ));
        return;
      }
      if (!run.text_by_item.has(params.itemId)) {
        run.text_item_order.push(params.itemId);
        run.text_by_item.set(params.itemId, '');
      }
      const startOffset = textSnapshot(run).length;
      run.text_by_item.set(params.itemId, `${run.text_by_item.get(params.itemId)}${params.delta}`);
      run.queue.push({
        kind: 'text_delta',
        provider_native_id: run.thread_id,
        payload: {
          text: params.delta,
          start_offset: startOffset,
          end_offset: startOffset + params.delta.length,
        },
      });
      return;
    }
    if (method === 'item/started') {
      const item = params.item;
      const specification = TOOL_ITEMS[item?.type];
      if (specification) {
        const invalidStartStatus = specification.statusMode === 'enum'
          ? item.status !== 'inProgress'
          : specification.statusMode === 'absent'
            ? item.status !== undefined
            : typeof item.status !== 'string' || item.status.length === 0;
        if (
          typeof item.id !== 'string'
          || item.id.length === 0
          || run.tool_items.has(item.id)
          || invalidStartStatus
        ) {
          failConnection(target, new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server emitted an invalid or duplicate tool start.',
          ));
          return;
        }
        run.tool_items.set(item.id, {
          item: structuredClone(item),
          specification,
        });
        run.queue.push(toolDescriptor(run, item.id, specification, 'tool_started', 'started'));
      } else if (!IGNORED_ITEM_TYPES.has(item?.type)) {
        failConnection(target, new CodexAppServerAdapterError(
          'unsupported_capability',
          'Codex app-server emitted an unsupported item type.',
        ));
      }
      return;
    }
    if (
      method === 'item/commandExecution/outputDelta'
      || method === 'item/fileChange/outputDelta'
      || method === 'item/mcpToolCall/progress'
    ) {
      const tool = run.tool_items.get(params.itemId);
      if (!tool || !tool.specification.progressMethods.includes(method)) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server emitted mismatched tool progress.',
        ));
        return;
      }
      run.queue.push(toolDescriptor(
        run,
        params.itemId,
        tool.specification,
        'tool_progress',
        'running',
      ));
      return;
    }
    if (method === 'item/completed') {
      const item = params.item;
      if (item?.type === 'agentMessage') {
        if (
          typeof item.id !== 'string'
          || item.id.length === 0
          || typeof item.text !== 'string'
          || item.text.length === 0
        ) {
          failConnection(target, new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server emitted an invalid completed agent message.',
          ));
          return;
        }
        if (!run.text_by_item.has(item.id)) run.text_item_order.push(item.id);
        run.text_by_item.set(item.id, item.text);
        const text = textSnapshot(run);
        run.queue.push({
          kind: 'text_snapshot',
          provider_native_id: run.thread_id,
          payload: { text, end_offset: text.length },
        });
        return;
      }
      const specification = TOOL_ITEMS[item?.type];
      if (specification) {
        const tool = run.tool_items.get(item.id);
        if (tool?.specification !== specification) {
          failConnection(target, new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server completed a tool that was not started.',
          ));
          return;
        }
        const invalidTerminalStatus = specification.statusMode === 'enum'
          ? !specification.terminalStatuses.includes(item.status)
          : specification.statusMode === 'absent'
            ? item.status !== undefined
            : typeof item.status !== 'string' || item.status.length === 0;
        if (invalidTerminalStatus) {
          failConnection(target, new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server completed a tool with an invalid terminal status.',
          ));
          return;
        }
        const verb = specification.statusMode === 'enum' ? item.status : 'finished';
        run.queue.push(toolDescriptor(run, item.id, specification, 'tool_finished', verb));
        run.tool_items.delete(item.id);
      } else if (!IGNORED_ITEM_TYPES.has(item?.type)) {
        failConnection(target, new CodexAppServerAdapterError(
          'unsupported_capability',
          'Codex app-server completed an unsupported item type.',
        ));
      }
      return;
    }
    if (method === 'error') {
      const failure = new CodexAppServerAdapterError(
        'provider_execution_failed',
        'Codex app-server reported an execution error.',
      );
      failConnection(target, failure);
      return;
    }
    if (method === 'turn/completed') {
      const status = params.turn?.status;
      if (!['completed', 'interrupted', 'failed'].includes(status)) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server completed a turn with an invalid terminal status.',
        ));
        return;
      }
      if (status === 'completed' && (run.tool_items.size > 0 || run.hook_runs.size > 0)) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server completed a turn with unfinished tools or hooks.',
        ));
        return;
      }
      run.terminal_status = status;
      terminalRuns.set(coreAttemptKey(run.context.turn_id, run.context.attempt), run);
      const failure = new CodexAppServerAdapterError(
        'provider_execution_failed',
        `Codex app-server completed the turn with status ${String(status)}.`,
      );
      rememberConnectionFence(target, target.retired_run_keys, runKey);
      const discardedRequestCount = discardProviderRequestsForRun(target, run, failure);
      const completionIsInvalid = status !== 'completed' || discardedRequestCount > 0;
      const failureOutcome = discardedRequestCount > 0
        ? run.context.reportProviderFailure?.(failure)
        : null;
      if (failureOutcome?.status === 'recovering') {
        terminalRuns.delete(coreAttemptKey(run.context.turn_id, run.context.attempt));
      }
      activeRuns.delete(runKey);
      run.resolveTerminal(status);
      if (completionIsInvalid) run.queue.fail(failure);
      else run.queue.end();
      return;
    }
    failConnection(target, new CodexAppServerAdapterError(
      'unsupported_capability',
      'Codex app-server emitted an unsupported scoped notification.',
    ));
  }

  function findServerRequestRun(target, method, params) {
    if (typeof params?.threadId !== 'string' || params.threadId.length === 0) return null;
    if (typeof params.turnId === 'string' && params.turnId.length > 0) {
      return activeRuns.get(activeRunKey(params.threadId, params.turnId)) ?? null;
    }
    if (method !== 'mcpServer/elicitation/request' || params.turnId !== null) return null;
    const matches = [...activeRuns.values()].filter((run) => (
      run.connection_id === target.connection_id && run.thread_id === params.threadId
    ));
    return matches.length === 1 ? matches[0] : null;
  }

  function interactionDescriptor(run, providerInteractionRef, {
    toolUseId,
    kind,
    prompt,
    choices = [],
  }) {
    if (
      !run.context.interaction
      || !Array.isArray(run.context.interaction.authorized_subjects)
      || run.context.interaction.authorized_subjects.length === 0
      || !Array.isArray(run.context.interaction.allowed_sources)
      || run.context.interaction.allowed_sources.length === 0
    ) {
      rejectProtocol('Execution context is missing interaction authorization.');
    }
    return {
      provider_interaction_ref: providerInteractionRef,
      tool_use_id: toolUseId ?? null,
      kind,
      prompt,
      choices,
      authorized_subjects: structuredClone(run.context.interaction.authorized_subjects),
      allowed_sources: [...run.context.interaction.allowed_sources],
    };
  }

  function boundedApprovalPrompt(parts) {
    const prompt = parts.filter((part) => part !== null).join('\n');
    if (prompt.trim().length === 0 || prompt.length > 8_000) {
      rejectProtocol(
        'Codex app-server requested approval details that cannot be displayed safely.',
        'unsupported_capability',
      );
    }
    return prompt;
  }

  function requireStartedTool(run, itemId, type) {
    const tool = run.tool_items.get(itemId);
    if (!tool || tool.item.type !== type) {
      rejectProtocol('Codex app-server approval does not match a current tool item.');
    }
    return tool.item;
  }

  function commandApprovalPrompt(run, params) {
    const item = requireStartedTool(run, params.itemId, 'commandExecution');
    if (
      typeof item.command !== 'string'
      || item.command.length === 0
      || typeof item.cwd !== 'string'
      || item.cwd.length === 0
      || (params.command !== undefined
        && params.command !== null
        && params.command !== item.command)
      || (params.cwd !== undefined && params.cwd !== null && params.cwd !== item.cwd)
      || (params.additionalPermissions !== undefined
        && params.additionalPermissions !== null
        && !isPermissionProfile(params.additionalPermissions))
      || (params.networkApprovalContext !== undefined
        && params.networkApprovalContext !== null
        && (!hasOnlyKeys(params.networkApprovalContext, new Set(['host', 'protocol']))
          || typeof params.networkApprovalContext.host !== 'string'
          || params.networkApprovalContext.host.length === 0
          || !['http', 'https', 'socks5Tcp', 'socks5Udp']
            .includes(params.networkApprovalContext.protocol)))
    ) {
      rejectProtocol('Codex app-server requested an invalid command approval.');
    }
    return boundedApprovalPrompt([
      typeof params.reason === 'string' && params.reason.trim().length > 0 ? params.reason : null,
      `Command: ${item.command}`,
      `Working directory: ${item.cwd}`,
      `Environment: ${params.environmentId ?? 'default'}`,
      params.networkApprovalContext == null
        ? null
        : `Network target: ${params.networkApprovalContext.protocol}://${params.networkApprovalContext.host}`,
      params.additionalPermissions == null
        ? null
        : `Additional permissions: ${JSON.stringify(params.additionalPermissions)}`,
    ]);
  }

  function fileChangeKind(change) {
    if (!hasOnlyKeys(change.kind, new Set(['type', 'move_path']))) return null;
    if (['add', 'delete'].includes(change.kind.type)) {
      return Object.keys(change.kind).length === 1 ? change.kind.type : null;
    }
    if (
      change.kind.type === 'update'
      && Object.hasOwn(change.kind, 'move_path')
      && isNullableString(change.kind.move_path)
    ) {
      return change.kind.move_path === null
        ? 'update'
        : `update -> ${change.kind.move_path}`;
    }
    return null;
  }

  function fileApprovalPrompt(run, params) {
    const item = requireStartedTool(run, params.itemId, 'fileChange');
    if (!Array.isArray(item.changes) || item.changes.length === 0) {
      rejectProtocol('Codex app-server requested file approval without changes.');
    }
    const changes = item.changes.map((change) => {
      const kind = isRecord(change) ? fileChangeKind(change) : null;
      if (
        kind === null
        || !hasOnlyKeys(change, new Set(['diff', 'kind', 'path']))
        || typeof change.path !== 'string'
        || change.path.length === 0
        || typeof change.diff !== 'string'
      ) {
        rejectProtocol('Codex app-server requested approval for invalid file changes.');
      }
      return `File ${kind}: ${change.path}\nDiff:\n${change.diff}`;
    });
    if (params.grantRoot !== undefined && !isNullableString(params.grantRoot)) {
      rejectProtocol('Codex app-server requested an invalid file grant root.');
    }
    return boundedApprovalPrompt([
      typeof params.reason === 'string' && params.reason.trim().length > 0 ? params.reason : null,
      ...changes,
      params.grantRoot == null ? null : `Requested write root: ${params.grantRoot}`,
    ]);
  }

  function permissionApprovalPrompt(params) {
    return boundedApprovalPrompt([
      typeof params.reason === 'string' && params.reason.trim().length > 0 ? params.reason : null,
      `Working directory: ${params.cwd}`,
      `Environment: ${params.environmentId ?? 'default'}`,
      `Requested permissions: ${JSON.stringify(params.permissions)}`,
    ]);
  }

  function requestUserInputComponents(params) {
    if (typeof params.itemId !== 'string' || params.itemId.length === 0) {
      rejectProtocol('Codex app-server requested user input without an item ID.');
    }
    if (!Object.hasOwn(params, 'autoResolutionMs') || params.autoResolutionMs !== null) {
      rejectProtocol(
        'Codex app-server requested an auto-resolving question without a durable Core deadline mapping.',
        'unsupported_capability',
      );
    }
    if (!Array.isArray(params.questions) || params.questions.length !== 1) {
      rejectProtocol(
        'Codex app-server multi-question input cannot satisfy Core ordered handoff semantics.',
        'unsupported_capability',
      );
    }
    const questionIds = new Set();
    return params.questions.map((question) => {
      if (
        !question
        || typeof question.id !== 'string'
        || question.id.length === 0
        || questionIds.has(question.id)
        || typeof question.header !== 'string'
        || typeof question.question !== 'string'
        || question.question.trim().length === 0
        || typeof question.isOther !== 'boolean'
        || question.isSecret !== false
        || !Object.hasOwn(question, 'options')
        || (question.options !== null && !Array.isArray(question.options))
      ) {
        rejectProtocol('Codex app-server requested unsupported or invalid user input.');
      }
      questionIds.add(question.id);
      const options = question.options ?? [];
      const choices = options.map((option) => {
        if (
          !option
          || typeof option.label !== 'string'
          || option.label.trim().length === 0
          || typeof option.description !== 'string'
        ) {
          rejectProtocol('Codex app-server supplied an invalid choice.');
        }
        return { choice_id: option.label, label: option.label };
      });
      if (new Set(choices.map(({ choice_id: choiceId }) => choiceId)).size !== choices.length) {
        rejectProtocol('Codex app-server supplied duplicate choices.');
      }
      if (choices.length > 0 && question.isOther) {
        rejectProtocol(
          'Codex app-server requested a choice plus free-form input that Core cannot represent.',
          'unsupported_capability',
        );
      }
      return {
        component_key: question.id,
        toolUseId: params.itemId,
        kind: choices.length > 0 ? 'choice' : 'question',
        prompt: question.question,
        choices,
        answer_constraint: choices.length > 0
          ? { kind: 'choice', allowed_values: choices.map(({ choice_id: choiceId }) => choiceId) }
          : { kind: 'text', allowed_values: null },
      };
    });
  }

  function mcpFormComponent(params) {
    if (params.mode !== 'form') {
      rejectProtocol(
        'Codex app-server requested an MCP elicitation mode that Core cannot represent safely.',
        'unsupported_capability',
      );
    }
    const requestedSchema = params.requestedSchema;
    const properties = requestedSchema?.properties;
    if (
      !hasOnlyKeys(requestedSchema, new Set(['$schema', 'properties', 'required', 'type']))
      || requestedSchema?.type !== 'object'
      || !properties
      || typeof properties !== 'object'
      || Array.isArray(properties)
      || !Array.isArray(requestedSchema.required)
      || (requestedSchema.$schema !== undefined && !isNullableString(requestedSchema.$schema))
    ) {
      rejectProtocol('Codex app-server requested an invalid MCP form.', 'unsupported_capability');
    }
    const entries = Object.entries(properties);
    if (
      entries.length !== 1
      || requestedSchema.required.length !== 1
      || requestedSchema.required[0] !== entries[0][0]
    ) {
      rejectProtocol(
        'Codex app-server multi-field MCP forms cannot satisfy Core ordered handoff semantics.',
        'unsupported_capability',
      );
    }
    const [propertyName, propertySchema] = entries[0];
    if (
      propertyName.length === 0
      || !propertySchema
      || typeof propertySchema !== 'object'
      || Array.isArray(propertySchema)
      || propertySchema.type !== 'string'
      || !hasOnlyKeys(propertySchema, new Set([
        'default',
        'description',
        'enum',
        'enumNames',
        'format',
        'maxLength',
        'minLength',
        'oneOf',
        'title',
        'type',
      ]))
      || (propertySchema.format !== undefined && propertySchema.format !== null)
      || (propertySchema.description !== undefined && !isNullableString(propertySchema.description))
      || (propertySchema.title !== undefined && !isNullableString(propertySchema.title))
      || (propertySchema.default !== undefined && !isNullableString(propertySchema.default))
      || (propertySchema.enum !== undefined && propertySchema.oneOf !== undefined)
      || (propertySchema.enumNames !== undefined && propertySchema.enum === undefined)
    ) {
      rejectProtocol(
        'Codex app-server requested an MCP form field that Core cannot represent safely.',
        'unsupported_capability',
      );
    }
    let values = null;
    let labels = null;
    if (Array.isArray(propertySchema.oneOf)) {
      if (propertySchema.oneOf.some((option) => (
        !hasOnlyKeys(option, new Set(['const', 'title']))
        || typeof option.const !== 'string'
        || option.const.length === 0
        || typeof option.title !== 'string'
        || option.title.trim().length === 0
      ))) {
        rejectProtocol('Codex app-server supplied invalid MCP form choices.');
      }
      values = propertySchema.oneOf.map((option) => option.const);
      labels = propertySchema.oneOf.map((option) => option.title);
    } else if (Array.isArray(propertySchema.enum)) {
      if (propertySchema.enum.some((value) => typeof value !== 'string' || value.length === 0)) {
        rejectProtocol('Codex app-server supplied invalid MCP form choices.');
      }
      values = [...propertySchema.enum];
      if (
        propertySchema.enumNames !== undefined
        && propertySchema.enumNames !== null
        && (!Array.isArray(propertySchema.enumNames)
          || propertySchema.enumNames.length !== values.length
          || propertySchema.enumNames.some((label) => (
            typeof label !== 'string' || label.trim().length === 0
          )))
      ) {
        rejectProtocol('Codex app-server supplied invalid MCP form choice labels.');
      }
      labels = Array.isArray(propertySchema.enumNames)
        ? [...propertySchema.enumNames]
        : [...values];
    } else if (propertySchema.oneOf !== undefined || propertySchema.enum !== undefined) {
      rejectProtocol('Codex app-server supplied invalid MCP form choices.');
    }
    if (values !== null && (values.length === 0 || new Set(values).size !== values.length)) {
      rejectProtocol('Codex app-server supplied duplicate or empty MCP form choices.');
    }
    const minLength = propertySchema.minLength ?? null;
    const maxLength = propertySchema.maxLength ?? null;
    if (
      (minLength !== null && (!Number.isSafeInteger(minLength) || minLength < 0))
      || (maxLength !== null && (!Number.isSafeInteger(maxLength) || maxLength < 0))
      || (minLength !== null && maxLength !== null && minLength > maxLength)
      || (values === null && (minLength !== 1 || maxLength !== null))
      || (values !== null && values.some((value) => (
        (minLength !== null && jsonSchemaStringLength(value) < minLength)
        || (maxLength !== null && jsonSchemaStringLength(value) > maxLength)
      )))
      || (propertySchema.default !== undefined
        && propertySchema.default !== null
        && ((values !== null && !values.includes(propertySchema.default))
          || (minLength !== null
            && jsonSchemaStringLength(propertySchema.default) < minLength)
          || (maxLength !== null
            && jsonSchemaStringLength(propertySchema.default) > maxLength)))
    ) {
      rejectProtocol(
        'Codex app-server supplied MCP form constraints that Core cannot represent safely.',
        'unsupported_capability',
      );
    }
    return {
      component_key: propertyName,
      toolUseId: null,
      kind: values === null ? 'question' : 'choice',
      prompt: params.message,
      choices: values === null ? [] : values.map((choiceId, index) => ({
        choice_id: choiceId,
        label: labels[index],
      })),
      mcp_form: {
        property_name: propertyName,
        allowed_values: values,
        min_length: minLength,
        max_length: maxLength,
      },
    };
  }

  function serverRequestComponents(method, params, run) {
    if (method === 'item/tool/requestUserInput') return requestUserInputComponents(params);
    if (method === 'item/commandExecution/requestApproval') {
      if (
        typeof params.itemId !== 'string'
        || params.itemId.length === 0
        || !Number.isSafeInteger(params.startedAtMs)
        || !Object.hasOwn(params, 'environmentId')
        || !isNullableString(params.environmentId)
        || (params.reason !== undefined && !isNullableString(params.reason))
        || (params.availableDecisions !== undefined
          && params.availableDecisions !== null
          && (!Array.isArray(params.availableDecisions)
            || !params.availableDecisions.includes('accept')
            || !params.availableDecisions.includes('decline')))
      ) {
        rejectProtocol('Codex app-server requested invalid command approval.');
      }
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'tool_approval',
        prompt: commandApprovalPrompt(run, params),
      }];
    }
    if (method === 'item/fileChange/requestApproval') {
      if (
        typeof params.itemId !== 'string'
        || params.itemId.length === 0
        || !Number.isSafeInteger(params.startedAtMs)
        || (params.reason !== undefined && !isNullableString(params.reason))
      ) {
        rejectProtocol('Codex app-server requested invalid file approval.');
      }
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'tool_approval',
        prompt: fileApprovalPrompt(run, params),
      }];
    }
    if (method === 'item/permissions/requestApproval') {
      if (
        typeof params.itemId !== 'string'
        || params.itemId.length === 0
        || !Number.isSafeInteger(params.startedAtMs)
        || typeof params.cwd !== 'string'
        || !path.isAbsolute(params.cwd)
        || !Object.hasOwn(params, 'environmentId')
        || !isNullableString(params.environmentId)
        || !Object.hasOwn(params, 'reason')
        || !isNullableString(params.reason)
        || !isPermissionProfile(params.permissions)
      ) {
        rejectProtocol('Codex app-server requested invalid permissions approval.');
      }
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'permission_approval',
        prompt: permissionApprovalPrompt(params),
      }];
    }
    if (method === 'mcpServer/elicitation/request') {
      if (typeof params.message !== 'string' || params.message.trim().length === 0) {
        rejectProtocol('Codex app-server requested invalid MCP elicitation.');
      }
      return [mcpFormComponent(params)];
    }
    return null;
  }

  function sendServerResponse(target, id, result) {
    if (target.failed || connection !== target) {
      rejectProtocol('Codex app-server connection is not current.');
    }
    try {
      target.child.stdin.write(`${JSON.stringify({ id, result })}\n`);
    } catch (error) {
      const failure = new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Could not send the interaction answer to Codex app-server.',
        { cause: error },
      );
      failConnection(target, failure);
      throw failure;
    }
  }

  function sendServerError(target, id, message) {
    if (target.failed) return;
    try {
      target.child.stdin.write(`${JSON.stringify({
        id,
        error: { code: -32601, message },
      })}\n`);
    } catch (error) {
      failConnection(target, new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Could not reject an invalid Codex app-server request.',
        { cause: error },
      ));
    }
  }

  function handleServerRequest(target, message) {
    if (!['string', 'number'].includes(typeof message.id)) {
      sendServerError(target, message.id, 'Invalid app-server request ID.');
      failConnection(target, new CodexAppServerAdapterError(
        'provider_protocol_invalid',
        'Codex app-server emitted an invalid server request ID.',
      ));
      return;
    }
    const requestId = requestIdKey(message.id);
    if (target.server_request_ids.has(requestId)) {
      sendServerError(target, message.id, 'Duplicate app-server request ID.');
      const retired = target.retired_server_requests.get(requestId);
      if (
        retired
        && retired.method === message.method
        && retired.params_json === JSON.stringify(message.params)
      ) return;
      failConnection(target, new CodexAppServerAdapterError(
        'provider_protocol_invalid',
        'Codex app-server reused a server request ID on the same connection.',
      ));
      return;
    }
    rememberConnectionFence(target, target.server_request_ids, requestId);
    const run = findServerRequestRun(target, message.method, message.params);
    if (!run || run.connection_id !== target.connection_id) {
      sendServerError(target, message.id, 'Unsupported or stale app-server request.');
      const retiredRunKey = typeof message.params?.threadId === 'string'
        && typeof message.params?.turnId === 'string'
        ? activeRunKey(message.params.threadId, message.params.turnId)
        : null;
      if (retiredRunKey !== null && target.retired_run_keys.has(retiredRunKey)) {
        rememberConnectionFence(
          target,
          target.retired_server_requests,
          requestId,
          Object.freeze({
            thread_id: message.params.threadId,
            turn_id: message.params.turnId,
            method: message.method,
            params_json: JSON.stringify(message.params),
          }),
        );
        return;
      }
      failConnection(target, new CodexAppServerAdapterError(
        'provider_protocol_invalid',
        'Codex app-server emitted a stale or mismatched server request.',
      ));
      return;
    }
    const components = serverRequestComponents(message.method, message.params, run);
    if (!components) {
      sendServerError(target, message.id, 'Unsupported or stale app-server request.');
      failConnection(target, new CodexAppServerAdapterError(
        'unsupported_capability',
        'Codex app-server requested an unsupported capability.',
      ));
      return;
    }
    const requestKey = `${target.connection_id}:${requestId}`;
    let resolveResolved;
    let rejectResolved;
    const resolved = new Promise((resolve, reject) => {
      resolveResolved = resolve;
      rejectResolved = reject;
    });
    resolved.catch(() => {});
    const group = {
      connection_id: target.connection_id,
      request_id: message.id,
      request_key: requestKey,
      method: message.method,
      params: structuredClone(message.params),
      thread_id: run.thread_id,
      turn_id: run.turn_id,
      run,
      components: new Map(),
      answers: new Map(),
      answer_constraints: new Map(components.map((component) => (
        [component.component_key, component.answer_constraint ?? null]
      ))),
      mcp_form: components[0]?.mcp_form ?? null,
      response_sent: false,
      resolved,
      resolveResolved,
      rejectResolved,
    };
    providerRequests.set(requestKey, group);
    const descriptors = components.map((component) => {
      nextInteractionNo += 1;
      const providerInteractionRef = `codex-app-server-interaction-${nextInteractionNo}`;
      const entry = { group, component_key: component.component_key };
      group.components.set(component.component_key, entry);
      pendingInteractions.set(providerInteractionRef, entry);
      return interactionDescriptor(run, providerInteractionRef, component);
    });
    run.queue.push({
      kind: 'interaction_requested',
      payload: descriptors.length === 1 ? descriptors[0] : { requests: descriptors },
    });
  }

  function handleMessage(target, message) {
    if (target !== connection && target.failed) return;
    if (Object.hasOwn(message, 'id') && typeof message.method === 'string') {
      try {
        handleServerRequest(target, message);
      } catch (error) {
        sendServerError(target, message.id, 'Invalid app-server request.');
        failConnection(target, error);
      }
      return;
    }
    if (Object.hasOwn(message, 'id') && !Object.hasOwn(message, 'method')) {
      const responseId = requestIdKey(message.id);
      const pending = target.pending.get(responseId);
      if (!pending) {
        if (target.settled_client_request_ids.has(responseId)) return;
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server returned an unknown request ID.',
        ));
        return;
      }
      const hasResult = Object.hasOwn(message, 'result');
      const hasError = Object.hasOwn(message, 'error');
      if (hasResult === hasError) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server returned an ambiguous JSON-RPC response.',
        ));
        return;
      }
      rememberConnectionFence(target, target.settled_client_request_ids, responseId);
      target.pending.delete(responseId);
      if (hasError) {
        pending.reject(new CodexAppServerAdapterError(
          'provider_request_failed',
          `Codex app-server rejected ${pending.method}.`,
        ));
        return;
      }
      try {
        pending.onResult?.(message.result);
        pending.resolve(message.result);
      } catch (error) {
        pending.reject(error);
        failConnection(target, error);
      }
      return;
    }
    if (typeof message.method === 'string' && !Object.hasOwn(message, 'id')) {
      handleNotification(target, message);
      return;
    }
    failConnection(target, new CodexAppServerAdapterError(
      'provider_protocol_invalid',
      'Codex app-server emitted an unsupported protocol message.',
    ));
  }

  function sendNotification(target, method, params) {
    if (target.failed) rejectProtocol('Codex app-server connection is not current.');
    const message = params === undefined ? { method } : { method, params };
    target.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  function sendRequest(target, method, params, { onResult } = {}) {
    if (target.failed) {
      return Promise.reject(new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server connection is not current.',
      ));
    }
    const id = `${target.connection_id}:${target.next_request_no}`;
    const pendingId = requestIdKey(id);
    target.next_request_no += 1;
    return new Promise((resolve, reject) => {
      target.pending.set(pendingId, { method, onResult, resolve, reject });
      try {
        target.child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
      } catch (error) {
        const failure = new CodexAppServerAdapterError(
          'provider_connection_lost',
          `Could not send ${method} to Codex app-server.`,
          { cause: error },
        );
        failConnection(target, failure);
        reject(failure);
      }
    });
  }

  async function createConnection() {
    nextConnectionNo += 1;
    let resolveClosed;
    const closed = new Promise((resolve) => {
      resolveClosed = resolve;
    });
    const detachedProcessGroup = process.platform !== 'win32';
    const child = spawnProcess(codexExecutable, ['app-server', '--stdio'], {
      cwd,
      detached: detachedProcessGroup,
      env: childEnvironment,
      shell: false,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const target = {
      connection_id: `codex-app-server-${nextConnectionNo}`,
      child,
      failed: false,
      affected_conversation_ids: new Set(),
      closed,
      closed_observed: false,
      resolveClosed,
      termination_requested: false,
      termination_timer: null,
      next_request_no: 1,
      pending: new Map(),
      process_group_id: detachedProcessGroup
        && Number.isSafeInteger(child.pid)
        && child.pid > 0
        ? child.pid
        : null,
      retired_run_keys: new Map(),
      retired_server_requests: new Map(),
      server_request_ids: new Set(),
      settled_client_request_ids: new Map(),
    };
    connection = target;
    loadedThreads.clear();
    target.child.stderr.on('error', (error) => failConnection(target, error));
    target.child.stderr.resume();
    target.child.stdin.on('error', (error) => failConnection(target, error));
    target.child.stdout.on('error', (error) => failConnection(target, error));
    const lines = createInterface({ input: target.child.stdout, crlfDelay: Infinity });
    lines.on('line', (line) => {
      if (target.failed) return;
      try {
        handleMessage(target, parseMessage(line));
      } catch (error) {
        failConnection(target, error);
      }
    });
    target.child.once('error', (error) => failConnection(target, error));
    target.child.once('close', () => {
      target.closed_observed = true;
      if (
        target.termination_timer !== null
        && supervisedProcessGroupExited(target)
      ) {
        clearTimeoutFn(target.termination_timer);
        target.termination_timer = null;
      }
      target.resolveClosed();
      failConnection(target, undefined);
    });
    try {
      await sendRequest(target, 'initialize', {
        clientInfo,
        capabilities: {
          experimentalApi: true,
          requestAttestation: false,
          mcpServerOpenaiFormElicitation: false,
        },
      });
      sendNotification(target, 'initialized');
      return target;
    } catch (error) {
      failConnection(target, error);
      throw error;
    }
  }

  async function ensureConnection() {
    if (connecting) return connecting;
    if (connection && !connection.failed) return connection;
    if (connection?.failed) {
      const failedConnection = connection;
      if (!await waitForProcessClose(failedConnection)) {
        throw new CodexAppServerAdapterError(
          'provider_connection_lost',
          'The failed Codex app-server process did not exit after forced termination.',
        );
      }
      if (connection === failedConnection) connection = null;
    }
    if (!connecting) {
      connecting = createConnection().finally(() => {
        connecting = null;
      });
    }
    return connecting;
  }

  async function loadThread(target, context) {
    const persistedThreadId = context.lineage.provider_native_id;
    if (persistedThreadId === null) {
      const result = await sendRequest(target, 'thread/start', {
        cwd,
        approvalPolicy,
        sandbox,
      });
      const threadId = requireThreadResult(result);
      await context.bindProviderNativeId(threadId);
      loadedThreads.add(threadId);
      return threadId;
    }
    if (!loadedThreads.has(persistedThreadId)) {
      await sendRequest(target, 'thread/resume', {
        threadId: persistedThreadId,
        cwd,
        approvalPolicy,
        sandbox,
      }, {
        onResult: (response) => {
          requireThreadResult(response, persistedThreadId);
          for (const turnId of requireThreadHistory(response)) {
            rememberConnectionFence(
              target,
              target.retired_run_keys,
              activeRunKey(persistedThreadId, turnId),
            );
          }
        },
      });
      loadedThreads.add(persistedThreadId);
    }
    return persistedThreadId;
  }

  async function* execute(context) {
    requireExecutionContext(context);
    const target = await ensureConnection();
    const threadId = await loadThread(target, context);
    let resolveTerminal;
    let rejectTerminal;
    const terminal = new Promise((resolve, reject) => {
      resolveTerminal = resolve;
      rejectTerminal = reject;
    });
    terminal.catch(() => {});
    const run = {
      connection_id: target.connection_id,
      context,
      queue: new AsyncEventQueue(),
      terminal,
      resolveTerminal,
      rejectTerminal,
      thread_id: threadId,
      text_by_item: new Map(),
      text_item_order: [],
      tool_items: new Map(),
      hook_runs: new Map(),
      turn_id: null,
      terminal_status: null,
    };
    inFlightTurnStarts.add(run);
    try {
      const result = await sendRequest(target, 'turn/start', {
        threadId,
        input: [{ type: 'text', text: context.input.text }],
      }, {
        onResult: (response) => {
          run.turn_id = requireTurnResult(response);
          inFlightTurnStarts.delete(run);
          startingRuns.set(activeRunKey(threadId, run.turn_id), run);
        },
      });
      requireTurnResult(result);
    } catch (error) {
      inFlightTurnStarts.delete(run);
      throw error;
    }
    try {
      yield* run.queue;
    } finally {
      inFlightTurnStarts.delete(run);
      if (run.turn_id !== null) {
        const runKey = activeRunKey(threadId, run.turn_id);
        startingRuns.delete(runKey);
        activeRuns.delete(runKey);
        terminalRuns.delete(coreAttemptKey(context.turn_id, context.attempt));
      }
    }
  }

  function waitForInterruptConfirmation(work) {
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeoutFn(() => {
        if (settled) return;
        settled = true;
        resolve(null);
      }, interruptConfirmationTimeoutMs);
      timer?.unref?.();
      work.then(
        (result) => {
          if (settled) return;
          settled = true;
          clearTimeoutFn(timer);
          resolve(result);
        },
        (error) => {
          if (settled) return;
          settled = true;
          clearTimeoutFn(timer);
          reject(error);
        },
      );
    });
  }

  async function interrupt({ turn_id: coreTurnId, attempt, reason }) {
    if (!['stop', 'timeout', 'steer'].includes(reason)) {
      throw new TypeError('interrupt reason must be stop, timeout, or steer');
    }
    const run = [...activeRuns.values()].find((candidate) => (
      candidate.context.turn_id === coreTurnId
      && sameAttempt(candidate.context.attempt, attempt)
    ));
    if (!run || run.connection_id !== connection?.connection_id) {
      if (reason === 'timeout') {
        const terminalRun = terminalRuns.get(coreAttemptKey(coreTurnId, attempt));
        if (terminalRun && terminalRun.terminal_status !== null) {
          return Object.freeze({
            status: 'provider_stopped',
            reason,
            provider_status: terminalRun.terminal_status,
          });
        }
      }
      return Object.freeze({ status: 'not_current', reason });
    }
    const target = connection;
    const result = await waitForInterruptConfirmation((async () => {
      await sendRequest(target, 'turn/interrupt', {
        threadId: run.thread_id,
        turnId: run.turn_id,
      }, {
        onResult: (response) => requireEmptyResult(response, 'turn/interrupt'),
      });
      return reason === 'timeout' ? run.terminal : 'interrupt_requested';
    })());
    if (result === null) {
      failConnection(target, new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server did not confirm the interrupt before its deadline.',
      ));
      return Object.freeze({ status: 'uncertain', reason });
    }
    if (reason === 'timeout') {
      return Object.freeze({
        status: 'provider_stopped',
        reason,
        provider_status: result,
      });
    }
    return Object.freeze({ status: 'interrupt_requested', reason });
  }

  function answerText(value) {
    if (value?.kind === 'text' && typeof value.text === 'string' && value.text.length > 0) {
      return value.text;
    }
    if (
      value?.kind === 'choice'
      && typeof value.choice_id === 'string'
      && value.choice_id.length > 0
    ) {
      return value.choice_id;
    }
    rejectProtocol('The persisted interaction answer is incompatible with the provider request.');
  }

  function answerDecision(value) {
    if (value?.kind !== 'decision' || !['approve', 'deny'].includes(value.decision)) {
      rejectProtocol('The persisted interaction answer must be an approval decision.');
    }
    return value.decision;
  }

  function grantedPermissions(requested) {
    const permissions = {};
    if (requested?.network !== null && requested?.network !== undefined) {
      permissions.network = structuredClone(requested.network);
    }
    if (requested?.fileSystem !== null && requested?.fileSystem !== undefined) {
      permissions.fileSystem = structuredClone(requested.fileSystem);
    }
    return permissions;
  }

  function requestUserInputAnswer(value, constraint) {
    if (constraint?.kind === 'text') {
      if (value?.kind !== 'text' || typeof value.text !== 'string' || value.text.length === 0) {
        rejectProtocol('The persisted answer must be free-form text for this provider question.');
      }
      return value.text;
    }
    if (
      constraint?.kind !== 'choice'
      || value?.kind !== 'choice'
      || typeof value.choice_id !== 'string'
      || !constraint.allowed_values.includes(value.choice_id)
    ) {
      rejectProtocol('The persisted answer is not one of the provider question choices.');
    }
    return value.choice_id;
  }

  function buildProviderResponse(group, answers = group.answers) {
    if (group.method === 'item/tool/requestUserInput') {
      return {
        answers: Object.fromEntries([...answers.entries()].map(([key, value]) => (
          [key, { answers: [requestUserInputAnswer(
            value,
            group.answer_constraints.get(key),
          )] }]
        ))),
      };
    }
    const value = answers.values().next().value;
    if (
      group.method === 'item/commandExecution/requestApproval'
      || group.method === 'item/fileChange/requestApproval'
    ) {
      return { decision: answerDecision(value) === 'approve' ? 'accept' : 'decline' };
    }
    if (group.method === 'item/permissions/requestApproval') {
      const approved = answerDecision(value) === 'approve';
      return {
        permissions: approved ? grantedPermissions(group.params.permissions) : {},
        scope: 'turn',
      };
    }
    if (group.method === 'mcpServer/elicitation/request') {
      if (!group.mcp_form) {
        rejectProtocol('The MCP elicitation has no safe provider-neutral form mapping.');
      }
      if (
        (group.mcp_form.allowed_values === null && value?.kind !== 'text')
        || (group.mcp_form.allowed_values !== null && value?.kind !== 'choice')
      ) {
        rejectProtocol('The persisted answer kind does not match the MCP form schema.');
      }
      const answer = answerText(value);
      if (
        (group.mcp_form.allowed_values !== null
          && !group.mcp_form.allowed_values.includes(answer))
        || (group.mcp_form.min_length !== null
          && jsonSchemaStringLength(answer) < group.mcp_form.min_length)
        || (group.mcp_form.max_length !== null
          && jsonSchemaStringLength(answer) > group.mcp_form.max_length)
      ) {
        rejectProtocol('The persisted answer does not satisfy the MCP form schema.');
      }
      return {
        action: 'accept',
        content: { [group.mcp_form.property_name]: answer },
        _meta: null,
      };
    }
    rejectProtocol('The provider request cannot accept an interaction answer.');
  }

  function acknowledgementFor(delivery) {
    return {
      handoff_id: delivery.handoff.handoff_id,
      status: delivery.answer.value?.decision === 'deny' ? 'deny' : 'accepted',
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch,
    };
  }

  async function handleInteractionAnswer(delivery) {
    const providerInteractionRef = delivery?.request?.runtime_fence?.provider_interaction_ref;
    const entry = pendingInteractions.get(providerInteractionRef);
    const target = connection;
    if (!entry || !target || target.failed || entry.group.connection_id !== target.connection_id) {
      rejectProtocol('The interaction answer does not match a current provider request.');
    }
    const { group } = entry;
    const run = activeRuns.get(activeRunKey(group.thread_id, group.turn_id));
    if (
      run !== group.run
      || run.connection_id !== target.connection_id
      || delivery.request.turn_id !== run.context.turn_id
      || delivery.request.runtime_fence.provider_attempt_id !== run.context.attempt.attempt_id
      || delivery.request.runtime_fence.lease_epoch !== run.context.attempt.lease_epoch
      || delivery.handoff.provider_attempt_id !== run.context.attempt.attempt_id
      || delivery.handoff.lease_epoch !== run.context.attempt.lease_epoch
      || typeof delivery.handoff.handoff_attempt_id !== 'string'
      || delivery.handoff.handoff_attempt_id.length === 0
      || !Number.isSafeInteger(delivery.handoff.handoff_attempt_no)
      || delivery.handoff.handoff_attempt_no < 1
    ) {
      rejectProtocol('The interaction answer failed its provider runtime fence.');
    }
    if (group.answers.has(entry.component_key)) {
      rejectProtocol('The provider interaction component was already answered.');
    }
    const candidateAnswers = new Map(group.answers);
    candidateAnswers.set(entry.component_key, structuredClone(delivery.answer.value));
    const providerResponse = buildProviderResponse(group, candidateAnswers);
    group.answers.set(entry.component_key, structuredClone(delivery.answer.value));
    group.response_sent = true;
    sendServerResponse(target, group.request_id, providerResponse);
    await group.resolved;
    providerRequests.delete(group.request_key);
    for (const [reference, component] of pendingInteractions) {
      if (component.group === group) pendingInteractions.delete(reference);
    }
    return acknowledgementFor(delivery);
  }

  async function cancel(context) {
    let result;
    try {
      result = await interrupt({
        turn_id: context?.turn_id,
        attempt: context?.attempt,
        reason: 'stop',
      });
    } catch (cause) {
      const error = cause instanceof Error
        ? cause
        : new CodexAppServerAdapterError(
          'side_effect_unknown',
          'Codex app-server cancellation failed without a typed provider error.',
        );
      error.cancellationUncertain = true;
      throw error;
    }
    if (result.status !== 'interrupt_requested') {
      const error = new CodexAppServerAdapterError(
        'side_effect_unknown',
        'Codex app-server could not confirm cancellation for the current fenced turn.',
      );
      error.cancellationUncertain = true;
      throw error;
    }
    return result;
  }

  async function abort(context) {
    const attempt = context?.attempt;
    const coreTurnId = context?.turn_id;
    const terminalRun = terminalRuns.get(coreAttemptKey(coreTurnId, attempt));
    if (terminalRun && terminalRun.terminal_status !== null) {
      return Object.freeze({
        status: 'provider_stopped',
        provider_status: terminalRun.terminal_status,
      });
    }
    const run = [
      ...activeRuns.values(),
      ...startingRuns.values(),
      ...inFlightTurnStarts,
    ].find((candidate) => (
      candidate.context.turn_id === coreTurnId
      && sameAttempt(candidate.context.attempt, attempt)
    ));
    const target = connection;
    if (!run || !target || run.connection_id !== target.connection_id) {
      if (
        target?.failed
        && target.affected_conversation_ids.has(context?.conversation_id)
        && await waitForProcessClose(target)
      ) {
        return Object.freeze({ status: 'provider_stopped', provider_status: 'process_exited' });
      }
      throw new CodexAppServerAdapterError(
        'side_effect_unknown',
        'Codex app-server cannot prove isolation for a non-current provider turn.',
      );
    }
    if (run.turn_id === null) {
      failConnection(target, new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server lost protocol control while the turn start was in flight.',
      ));
      if (await waitForProcessClose(target)) {
        return Object.freeze({ status: 'provider_stopped', provider_status: 'process_exited' });
      }
      throw new CodexAppServerAdapterError(
        'side_effect_unknown',
        'Codex app-server did not exit after an in-flight turn lost protocol control.',
      );
    }
    let terminalStatus;
    try {
      terminalStatus = await waitForInterruptConfirmation((async () => {
        await sendRequest(target, 'turn/interrupt', {
          threadId: run.thread_id,
          turnId: run.turn_id,
        });
        return run.terminal;
      })());
    } catch (error) {
      if (target.failed && await waitForProcessClose(target)) {
        return Object.freeze({ status: 'provider_stopped', provider_status: 'process_exited' });
      }
      throw error;
    }
    if (terminalStatus !== null) {
      return Object.freeze({ status: 'provider_stopped', provider_status: terminalStatus });
    }
    failConnection(target, new CodexAppServerAdapterError(
      'provider_connection_lost',
      'Codex app-server did not confirm abort before its deadline.',
    ));
    if (await waitForProcessClose(target)) {
      return Object.freeze({ status: 'provider_stopped', provider_status: 'process_exited' });
    }
    throw new CodexAppServerAdapterError(
      'side_effect_unknown',
      'Codex app-server abort could not prove provider isolation.',
    );
  }

  async function close() {
    const target = connection;
    if (!target) return [];
    for (const run of [...activeRuns.values(), ...startingRuns.values(), ...inFlightTurnStarts]) {
      if (run.connection_id === target.connection_id) {
        target.affected_conversation_ids.add(run.context.conversation_id);
      }
    }
    requestProcessTermination(target);
    failConnection(target, new CodexAppServerAdapterError(
      'provider_connection_lost',
      'Codex app-server was stopped with its executor service.',
    ), { terminate: false });
    if (!await waitForProcessClose(target)) {
      const error = new CodexAppServerAdapterError(
        'provider_connection_lost',
        'Codex app-server did not exit after forced termination.',
      );
      error.closedConversationIds = [];
      throw error;
    }
    return [...target.affected_conversation_ids];
  }

  return Object.freeze({ abort, cancel, close, execute, handleInteractionAnswer, interrupt });
}
