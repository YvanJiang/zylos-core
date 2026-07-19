import { query as sdkQuery } from '@anthropic-ai/claude-agent-sdk';

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

function createSdkUserMessage(input) {
  const content = typeof input === 'string' ? input : input?.text;
  if (typeof content !== 'string' || content.length === 0) {
    throw new TypeError('Claude turn input must contain non-empty text.');
  }
  return {
    type: 'user',
    message: { role: 'user', content },
    parent_tool_use_id: null,
  };
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
  const decision = await activeTurn.controls.requestPermission(
    Object.freeze({ tool_name: toolName, input }),
    Object.freeze({ signal: sdkContext?.signal }),
  );
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

function updateBackgroundTasks(executor, message) {
  if (message?.type !== 'system') return;
  if (message.subtype === 'background_tasks_changed' && Array.isArray(message.tasks)) {
    executor.backgroundTaskIds = new Set(
      message.tasks.map(({ task_id: taskId }) => taskId).filter(
        (taskId) => typeof taskId === 'string' && taskId.length > 0,
      ),
    );
    return;
  }
  if (typeof message.task_id !== 'string') return;
  if (message.subtype === 'task_started') {
    executor.backgroundTaskIds.add(message.task_id);
  } else if (message.subtype === 'task_notification') {
    executor.backgroundTaskIds.delete(message.task_id);
  }
}

function createResidentExecutor({
  conversationId,
  providerNativeId,
  query,
  queryOptions,
  now,
}) {
  const executor = {
    activeTurn: null,
    backgroundTaskIds: new Set(),
    conversationId,
    ended: false,
    input: null,
    outputPump: null,
    query: null,
    queryFactory: query,
    queryOptions,
    providerTurn: null,
    sessionBound: providerNativeId !== null,
    sessionId: providerNativeId,
    lastUsedAt: now(),
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
          updateBackgroundTasks(executor, message);
          if (
            message?.type === 'system'
            && message.subtype === 'session_state_changed'
            && message.state === 'idle'
            && executor.providerTurn?.resultSeen
          ) {
            executor.providerTurn = null;
            executor.input.resume();
            continue;
          }
          const providerTurn = executor.providerTurn;
          for (const record of normalizeProviderMessage(executor, message)) {
            providerTurn?.output.push(record);
          }
          if (message?.type === 'result' && providerTurn) {
            const outcome = providerTurn.cancelRequested
              ? 'cancelled'
              : (message.subtype === 'success' ? 'completed' : 'failed');
            providerTurn.output.push({
              type: 'turn_result',
              outcome,
            });
            providerTurn.output.close();
            providerTurn.resultSeen = true;
            if (executor.activeTurn === providerTurn) executor.activeTurn = null;
            executor.lastUsedAt = now();
          }
        }
      } catch (error) {
        const failedTurn = executor.providerTurn ?? executor.activeTurn;
        if (failedTurn) {
          failedTurn.output.close(error);
        }
        executor.activeTurn = null;
        executor.providerTurn = null;
      } finally {
        const unfinishedTurn = executor.providerTurn ?? executor.activeTurn;
        if (unfinishedTurn && !unfinishedTurn.resultSeen) {
          unfinishedTurn.output.push({ type: 'turn_result', outcome: 'failed' });
          unfinishedTurn.output.close();
        }
        executor.activeTurn = null;
        executor.providerTurn = null;
        executor.ended = true;
      }
    })();
  };

  return executor;
}

export function createClaudeConversationAdapter({
  query = sdkQuery,
  queryOptions = {},
  idleTimeoutMs = 300_000,
  now = () => Date.now(),
}) {
  if (typeof query !== 'function') throw new TypeError('query must be a function');
  if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs < 0) {
    throw new TypeError('idleTimeoutMs must be a non-negative finite number');
  }
  if (typeof now !== 'function') throw new TypeError('now must be a function');
  const executors = new Map();

  async function* execute(context, controls = {}) {
    if (typeof controls.requestPermission !== 'function') {
      throw new TypeError('controls.requestPermission must be a function');
    }
    let executor = executors.get(context.conversation_id);
    if (executor?.ended) {
      await closeExecutor(executor);
      executors.delete(context.conversation_id);
      executor = null;
    }
    if (!executor) {
      executor = createResidentExecutor({
        conversationId: context.conversation_id,
        providerNativeId: context.provider_native_id,
        query,
        queryOptions,
        now,
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
    executor.activeTurn = {
      cancelRequested: false,
      controls,
      context,
      output,
      resultSeen: false,
      text: '',
      toolNames: new Map(),
    };
    try {
      executor.start();
      executor.input.push({
        message: createSdkUserMessage(context.input),
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

  async function cancel(context) {
    const executor = executors.get(context.conversation_id);
    const activeTurn = executor?.activeTurn;
    if (
      !activeTurn
      || activeTurn.context.turn_id !== context.turn_id
      || activeTurn.context.attempt.attempt_id !== context.attempt.attempt_id
      || activeTurn.context.attempt.attempt_no !== context.attempt.attempt_no
      || activeTurn.context.attempt.lease_epoch !== context.attempt.lease_epoch
    ) {
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
      return;
    }
    await executor.query.interrupt();
  }

  async function closeExecutor(executor) {
    executor.input.close();
    await executor.query?.close?.();
    if (executor.outputPump) await executor.outputPump;
  }

  async function evictIdle({ canEvict }) {
    if (typeof canEvict !== 'function') {
      throw new TypeError('canEvict must be a function');
    }
    const evicted = [];
    for (const [conversationId, executor] of executors) {
      if (
        executor.activeTurn !== null
        || executor.backgroundTaskIds.size > 0
        || now() - executor.lastUsedAt < idleTimeoutMs
        || !await canEvict(conversationId)
      ) {
        continue;
      }
      if (
        executors.get(conversationId) !== executor
        || executor.activeTurn !== null
        || executor.backgroundTaskIds.size > 0
        || now() - executor.lastUsedAt < idleTimeoutMs
        || !await canEvict(conversationId)
      ) {
        continue;
      }
      executors.delete(conversationId);
      await closeExecutor(executor);
      evicted.push(conversationId);
    }
    return evicted;
  }

  async function close() {
    await Promise.allSettled(
      [...executors.values()].map((executor) => closeExecutor(executor)),
    );
    executors.clear();
  }

  return Object.freeze({ cancel, close, evictIdle, execute });
}
