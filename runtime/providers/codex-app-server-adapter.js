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
  commandExecution: Object.freeze({ name: 'command', label: 'Command', sideEffect: 'unknown' }),
  fileChange: Object.freeze({ name: 'file_change', label: 'File change', sideEffect: 'unknown' }),
  mcpToolCall: Object.freeze({ name: 'external_tool', label: 'External tool', sideEffect: 'unknown' }),
  dynamicToolCall: Object.freeze({ name: 'external_tool', label: 'External tool', sideEffect: 'unknown' }),
  collabAgentToolCall: Object.freeze({ name: 'collaboration', label: 'Collaboration', sideEffect: 'unknown' }),
  webSearch: Object.freeze({ name: 'web_search', label: 'Web search', sideEffect: 'none' }),
  imageGeneration: Object.freeze({ name: 'image_generation', label: 'Image generation', sideEffect: 'unknown' }),
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

function sameAttempt(left, right) {
  return left.attempt_id === right.attempt_id
    && left.attempt_no === right.attempt_no
    && left.lease_epoch === right.lease_epoch;
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

  const childEnvironment = selectEnvironment(env, envAllowlist);
  const loadedThreads = new Set();
  const activeRuns = new Map();
  const startingRuns = new Map();
  const providerRequests = new Map();
  const pendingInteractions = new Map();
  let connection = null;
  let connecting = null;
  let nextConnectionNo = 0;
  let nextInteractionNo = 0;

  function failConnection(target, error) {
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
    for (const run of activeRuns.values()) {
      if (run.connection_id === target.connection_id) run.queue.fail(failure);
    }
    for (const run of startingRuns.values()) {
      if (run.connection_id === target.connection_id) run.queue.fail(failure);
    }
    for (const group of providerRequests.values()) {
      if (group.connection_id === target.connection_id) group.rejectResolved(failure);
    }
    if (connection === target) connection = null;
  }

  function handleNotification(target, message) {
    const { method, params } = message;
    if (method === 'thread/started') return;
    if (method === 'serverRequest/resolved') {
      const group = providerRequests.get(`${target.connection_id}:${String(params?.requestId)}`);
      if (!group || group.thread_id !== params?.threadId || group.connection_id !== target.connection_id) {
        return;
      }
      group.resolveResolved();
      return;
    }
    const threadId = params?.threadId;
    const turnId = params?.turnId ?? params?.turn?.id;
    if (typeof threadId !== 'string' || typeof turnId !== 'string') return;
    const runKey = activeRunKey(threadId, turnId);
    if (method === 'turn/started') {
      const startingRun = startingRuns.get(runKey);
      if (!startingRun || startingRun.connection_id !== target.connection_id) return;
      startingRuns.delete(runKey);
      activeRuns.set(runKey, startingRun);
      return;
    }
    const run = activeRuns.get(runKey);
    if (!run || run.connection_id !== target.connection_id) return;
    if (method === 'item/agentMessage/delta') {
      if (
        typeof params.itemId !== 'string'
        || params.itemId.length === 0
        || typeof params.delta !== 'string'
        || params.delta.length === 0
      ) {
        run.queue.fail(new CodexAppServerAdapterError(
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
        if (typeof item.id !== 'string' || item.id.length === 0) {
          run.queue.fail(new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server emitted a tool item without an ID.',
          ));
          return;
        }
        run.tool_items.set(item.id, specification);
        run.queue.push(toolDescriptor(run, item.id, specification, 'tool_started', 'started'));
      } else if (!IGNORED_ITEM_TYPES.has(item?.type)) {
        run.queue.fail(new CodexAppServerAdapterError(
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
      const specification = run.tool_items.get(params.itemId);
      if (!specification) {
        run.queue.fail(new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server emitted tool progress before tool start.',
        ));
        return;
      }
      run.queue.push(toolDescriptor(run, params.itemId, specification, 'tool_progress', 'running'));
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
          run.queue.fail(new CodexAppServerAdapterError(
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
        if (run.tool_items.get(item.id) !== specification) {
          run.queue.fail(new CodexAppServerAdapterError(
            'provider_protocol_invalid',
            'Codex app-server completed a tool that was not started.',
          ));
          return;
        }
        const verb = ['failed', 'declined'].includes(item.status) ? item.status : 'completed';
        run.queue.push(toolDescriptor(run, item.id, specification, 'tool_finished', verb));
        run.tool_items.delete(item.id);
      } else if (!IGNORED_ITEM_TYPES.has(item?.type)) {
        run.queue.fail(new CodexAppServerAdapterError(
          'unsupported_capability',
          'Codex app-server completed an unsupported item type.',
        ));
      }
      return;
    }
    if (method === 'error') {
      activeRuns.delete(runKey);
      run.queue.fail(new CodexAppServerAdapterError(
        'provider_execution_failed',
        'Codex app-server reported an execution error.',
      ));
      return;
    }
    if (method === 'turn/completed') {
      activeRuns.delete(runKey);
      const status = params.turn?.status;
      if (status === 'completed') run.queue.end();
      else run.queue.fail(new CodexAppServerAdapterError(
        'provider_execution_failed',
        `Codex app-server completed the turn with status ${String(status)}.`,
      ));
    }
  }

  function findServerRequestRun(target, params) {
    if (typeof params?.threadId !== 'string' || params.threadId.length === 0) return null;
    if (typeof params.turnId === 'string' && params.turnId.length > 0) {
      return activeRuns.get(activeRunKey(params.threadId, params.turnId)) ?? null;
    }
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

  function requestUserInputComponents(params) {
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
        || typeof question.question !== 'string'
        || question.question.trim().length === 0
        || question.isSecret === true
      ) {
        rejectProtocol('Codex app-server requested unsupported or invalid user input.');
      }
      questionIds.add(question.id);
      const options = question.options ?? [];
      if (!Array.isArray(options)) rejectProtocol('Codex app-server supplied invalid choices.');
      const choices = options.map((option) => {
        if (!option || typeof option.label !== 'string' || option.label.trim().length === 0) {
          rejectProtocol('Codex app-server supplied an invalid choice.');
        }
        return { choice_id: option.label, label: option.label };
      });
      if (new Set(choices.map(({ choice_id: choiceId }) => choiceId)).size !== choices.length) {
        rejectProtocol('Codex app-server supplied duplicate choices.');
      }
      return {
        component_key: question.id,
        toolUseId: params.itemId,
        kind: choices.length > 0 ? 'choice' : 'question',
        prompt: question.question,
        choices,
      };
    });
  }

  function serverRequestComponents(method, params) {
    if (method === 'item/tool/requestUserInput') return requestUserInputComponents(params);
    if (method === 'item/commandExecution/requestApproval') {
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'tool_approval',
        prompt: typeof params.reason === 'string' && params.reason.trim().length > 0
          ? params.reason
          : 'Allow Codex to run the requested command?',
      }];
    }
    if (method === 'item/fileChange/requestApproval') {
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'tool_approval',
        prompt: typeof params.reason === 'string' && params.reason.trim().length > 0
          ? params.reason
          : 'Allow Codex to apply the requested file changes?',
      }];
    }
    if (method === 'item/permissions/requestApproval') {
      return [{
        component_key: 'approval',
        toolUseId: params.itemId,
        kind: 'permission_approval',
        prompt: typeof params.reason === 'string' && params.reason.trim().length > 0
          ? params.reason
          : 'Allow Codex to use the requested permissions for this turn?',
      }];
    }
    if (method === 'mcpServer/elicitation/request') {
      if (typeof params.message !== 'string' || params.message.trim().length === 0) {
        rejectProtocol('Codex app-server requested invalid MCP elicitation.');
      }
      return [{
        component_key: 'elicitation',
        toolUseId: null,
        kind: params.mode === 'url' ? 'tool_approval' : 'question',
        prompt: params.message,
      }];
    }
    return null;
  }

  function sendServerResponse(target, id, result) {
    if (target.failed || connection !== target) {
      rejectProtocol('Codex app-server connection is not current.');
    }
    target.child.stdin.write(`${JSON.stringify({ id, result })}\n`);
  }

  function sendServerError(target, id, message) {
    if (target.failed) return;
    target.child.stdin.write(`${JSON.stringify({
      id,
      error: { code: -32601, message },
    })}\n`);
  }

  function handleServerRequest(target, message) {
    const run = findServerRequestRun(target, message.params);
    const components = serverRequestComponents(message.method, message.params);
    if (!run || run.connection_id !== target.connection_id || !components) {
      sendServerError(target, message.id, 'Unsupported or stale app-server request.');
      if (run) run.queue.fail(new CodexAppServerAdapterError(
        'unsupported_capability',
        'Codex app-server requested an unsupported capability.',
      ));
      return;
    }
    const requestKey = `${target.connection_id}:${String(message.id)}`;
    if (providerRequests.has(requestKey)) {
      sendServerError(target, message.id, 'Duplicate app-server request ID.');
      run.queue.fail(new CodexAppServerAdapterError(
        'provider_protocol_invalid',
        'Codex app-server reused an active request ID.',
      ));
      return;
    }
    let resolveResolved;
    let rejectResolved;
    const resolved = new Promise((resolve, reject) => {
      resolveResolved = resolve;
      rejectResolved = reject;
    });
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
        const run = findServerRequestRun(target, message.params);
        if (run) run.queue.fail(error);
      }
      return;
    }
    if (Object.hasOwn(message, 'id') && !Object.hasOwn(message, 'method')) {
      const pending = target.pending.get(String(message.id));
      if (!pending) {
        failConnection(target, new CodexAppServerAdapterError(
          'provider_protocol_invalid',
          'Codex app-server returned an unknown request ID.',
        ));
        return;
      }
      target.pending.delete(String(message.id));
      if (Object.hasOwn(message, 'error')) {
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
    target.next_request_no += 1;
    return new Promise((resolve, reject) => {
      target.pending.set(id, { method, onResult, resolve, reject });
      try {
        target.child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
      } catch (error) {
        target.pending.delete(id);
        reject(new CodexAppServerAdapterError(
          'provider_connection_lost',
          `Could not send ${method} to Codex app-server.`,
          { cause: error },
        ));
      }
    });
  }

  async function createConnection() {
    nextConnectionNo += 1;
    const target = {
      connection_id: `codex-app-server-${nextConnectionNo}`,
      child: spawnProcess(codexExecutable, ['app-server', '--stdio'], {
        cwd,
        env: childEnvironment,
        shell: false,
        stdio: ['pipe', 'pipe', 'pipe'],
      }),
      failed: false,
      next_request_no: 1,
      pending: new Map(),
    };
    connection = target;
    loadedThreads.clear();
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
    target.child.once('close', () => failConnection(target));
    try {
      await sendRequest(target, 'initialize', {
        clientInfo,
        capabilities: {
          experimentalApi: true,
          requestAttestation: false,
          mcpServerOpenaiFormElicitation: true,
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
      const result = await sendRequest(target, 'thread/resume', {
        threadId: persistedThreadId,
        cwd,
        approvalPolicy,
        sandbox,
      });
      requireThreadResult(result, persistedThreadId);
      loadedThreads.add(persistedThreadId);
    }
    return persistedThreadId;
  }

  async function* execute(context) {
    requireExecutionContext(context);
    const target = await ensureConnection();
    const threadId = await loadThread(target, context);
    const run = {
      connection_id: target.connection_id,
      context,
      queue: new AsyncEventQueue(),
      thread_id: threadId,
      text_by_item: new Map(),
      text_item_order: [],
      tool_items: new Map(),
      turn_id: null,
    };
    const result = await sendRequest(target, 'turn/start', {
      threadId,
      input: [{ type: 'text', text: context.input.text }],
    }, {
      onResult: (response) => {
        run.turn_id = requireTurnResult(response);
        startingRuns.set(activeRunKey(threadId, run.turn_id), run);
      },
    });
    requireTurnResult(result);
    try {
      yield* run.queue;
    } finally {
      if (run.turn_id !== null) {
        const runKey = activeRunKey(threadId, run.turn_id);
        startingRuns.delete(runKey);
        activeRuns.delete(runKey);
      }
    }
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
      return Object.freeze({ status: 'not_current', reason });
    }
    await sendRequest(connection, 'turn/interrupt', {
      threadId: run.thread_id,
      turnId: run.turn_id,
    });
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

  function buildProviderResponse(group) {
    if (group.method === 'item/tool/requestUserInput') {
      return {
        answers: Object.fromEntries([...group.answers.entries()].map(([key, value]) => (
          [key, { answers: [answerText(value)] }]
        ))),
      };
    }
    const value = group.answers.values().next().value;
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
      if (value?.kind === 'decision') {
        return {
          action: value.decision === 'approve' ? 'accept' : 'decline',
          content: value.decision === 'approve' ? {} : null,
          _meta: null,
        };
      }
      return { action: 'accept', content: answerText(value), _meta: null };
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
    group.answers.set(entry.component_key, structuredClone(delivery.answer.value));
    group.response_sent = true;
    sendServerResponse(target, group.request_id, buildProviderResponse(group));
    await group.resolved;
    providerRequests.delete(group.request_key);
    for (const [reference, component] of pendingInteractions) {
      if (component.group === group) pendingInteractions.delete(reference);
    }
    return acknowledgementFor(delivery);
  }

  async function close() {
    const target = connection;
    if (!target || target.failed) return Object.freeze({ status: 'not_current' });
    const signalled = target.child.kill('SIGTERM');
    failConnection(target, new CodexAppServerAdapterError(
      'provider_connection_lost',
      'Codex app-server was stopped with its executor service.',
    ));
    return Object.freeze({ status: signalled === false ? 'not_current' : 'signalled' });
  }

  return Object.freeze({ close, execute, handleInteractionAnswer, interrupt });
}
