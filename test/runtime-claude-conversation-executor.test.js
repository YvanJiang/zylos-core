import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import {
  createClaudeConversationAdapter,
  enforceWorkspaceFenceBeforeTool,
} from '../runtime/providers/claude/conversation-adapter.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-claude-executor-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function normalEnvelope(suffix) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = JSON.parse(JSON.stringify(fixture));
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.content.text = `turn ${suffix}`;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function acceptQueuedTurn(database, suffix) {
  return acceptNormalInbound(database, normalEnvelope(suffix), {
    now: () => '2026-07-19T09:00:00Z',
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

function stopActiveConversation(service, accepted, suffix = accepted.turn_id) {
  return service.stop({
    conversation_id: accepted.conversation_id,
    stop_id: `stop-${suffix}`,
  });
}

function permissionAnswer(request, suffix, decision = 'approve') {
  const sourceEventId = `permission-action-${suffix}`;
  return {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-permission-answer-${suffix}`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `permission-answer-${suffix}`,
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
    answered_at: '2026-07-19T09:03:00Z',
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: request.interaction_id,
      source_event_or_action_id: sourceEventId,
    }),
  };
}

function createFakeQuery({ sessionId, beforeFirstOutput }) {
  const calls = [];
  const inputs = [];

  function query({ prompt, options }) {
    calls.push({ options });
    let first = true;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        inputs.push(input.message.content);
        if (first) {
          first = false;
          yield {
            type: 'system',
            subtype: 'init',
            session_id: sessionId,
          };
        }
        beforeFirstOutput?.();
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: {
            content: [{ type: 'text', text: `answer for ${input.message.content}` }],
          },
          parent_tool_use_id: null,
        };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: `answer for ${input.message.content}`,
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }

  return { calls, inputs, query };
}

function createSessionPerQuery() {
  const calls = [];
  function query({ prompt, options }) {
    const sessionId = `claude-lineage-session-${calls.length + 1}`;
    calls.push({ options, sessionId });
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `answer ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'answer' };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { calls, query };
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function idleSession(sessionId) {
  return {
    type: 'system',
    subtype: 'session_state_changed',
    state: 'idle',
    session_id: sessionId,
  };
}

function createInterruptibleQuery({ sessionId }) {
  const interrupted = deferred();
  const lateOutputRelease = deferred();
  const firstTurnStarted = deferred();
  const inputs = [];
  let queryCalls = 0;
  let interruptCalls = 0;

  function query({ prompt }) {
    queryCalls += 1;
    const drainedInputs = [];
    const inputWaiters = [];
    let closed = false;
    let drainFinished;
    const drainDone = new Promise((resolve) => { drainFinished = resolve; });
    const takeInput = () => {
      if (drainedInputs.length > 0) return Promise.resolve(drainedInputs.shift());
      if (closed) return Promise.resolve(null);
      return new Promise((resolve) => inputWaiters.push(resolve));
    };
    (async () => {
      for await (const input of prompt) {
        inputs.push(input.message.content);
        const waiter = inputWaiters.shift();
        if (waiter) waiter(input);
        else drainedInputs.push(input);
      }
      drainFinished();
    })();
    const stream = (async function* generateSdkMessages() {
      if (await takeInput() === null) return;
      yield { type: 'system', subtype: 'init', session_id: sessionId };
      firstTurnStarted.resolve();
      await interrupted.promise;
      yield {
        type: 'result',
        subtype: 'success',
        session_id: sessionId,
        result: 'interrupted',
      };
      await lateOutputRelease.promise;
      yield {
        type: 'assistant',
        session_id: sessionId,
        message: { content: [{ type: 'text', text: 'late old-query output' }] },
        parent_tool_use_id: null,
      };
      yield idleSession(sessionId);
      if (await takeInput() === null) return;
      yield {
        type: 'assistant',
        session_id: sessionId,
        message: { content: [{ type: 'text', text: 'second turn answer' }] },
        parent_tool_use_id: null,
      };
      yield {
        type: 'result',
        subtype: 'success',
        session_id: sessionId,
        result: 'second turn answer',
      };
      yield idleSession(sessionId);
      await drainDone;
    }());
    stream.interrupt = async () => {
      interruptCalls += 1;
      interrupted.resolve();
      return { still_queued: [] };
    };
    stream.close = () => {
      closed = true;
      interrupted.resolve();
      lateOutputRelease.resolve();
      for (const waiter of inputWaiters.splice(0)) waiter(null);
    };
    return stream;
  }

  return {
    firstTurnStarted,
    get interruptCalls() { return interruptCalls; },
    get queryCalls() { return queryCalls; },
    inputs,
    lateOutputRelease,
    query,
  };
}

function createPermissionQuery({ sessionId }) {
  const permissionRequested = deferred();
  const permissionResults = [];
  let closeCalls = 0;

  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        permissionRequested.resolve();
        permissionResults.push(await options.canUseTool(
          'Bash',
          { command: 'pwd' },
          {
            signal: new AbortController().signal,
            suggestions: [{ type: 'addRules', destination: 'session' }],
          },
        ));
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `allowed ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: 'allowed',
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => { closeCalls += 1; };
    return stream;
  }

  return {
    get closeCalls() { return closeCalls; },
    permissionRequested,
    permissionResults,
    query,
  };
}

function createAutoAllowedWriteQuery({ sessionId }) {
  const hookResults = [];
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        hookResults.push(await options.hooks.PreToolUse[0].hooks[0]({
          hook_event_name: 'PreToolUse',
          tool_name: 'Bash',
          tool_input: { command: 'touch guarded' },
          tool_use_id: 'auto-allowed-write',
        }));
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: `guarded ${input.message.content}`,
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { hookResults, query };
}

function createCaughtPermissionFailureQuery({ sessionId }) {
  const permissionRequested = deferred();
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        permissionRequested.resolve();
        try {
          await options.canUseTool('Bash', { command: 'pwd' }, {});
        } catch {
          // The SDK reports callback failures on its private control channel and continues.
        }
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: `continued ${input.message.content}`,
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { permissionRequested, query };
}

function createEndingPermissionQuery({ sessionId }) {
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        options.canUseTool('Bash', { command: `echo ${input.message.content}` }, {})
          .catch(() => {});
        return;
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function createCaughtPermissionCancellationQuery({ sessionId }) {
  const interruptRequested = deferred();
  const permissionRequested = deferred();
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        permissionRequested.resolve();
        try {
          await options.canUseTool('Bash', { command: 'sleep 10' }, {});
        } catch {
          // The real SDK reports callback errors over its control channel and keeps streaming.
        }
        await interruptRequested.promise;
        yield {
          type: 'result',
          subtype: 'error_during_execution',
          session_id: sessionId,
          errors: [`interrupted ${input.message.content}`],
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {
      interruptRequested.resolve();
      return { still_queued: [] };
    };
    stream.close = () => interruptRequested.resolve();
    return stream;
  }
  return { permissionRequested, query };
}

function createBackgroundQuery({ sessionId }) {
  const backgroundFinished = deferred();
  const notificationConsumed = deferred();
  const resultEmitted = deferred();

  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'system',
          subtype: 'background_tasks_changed',
          session_id: sessionId,
          tasks: [{
            task_id: 'background-task-1',
            task_type: 'agent',
            description: 'background smoke',
          }],
        };
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `started ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: 'background started',
        };
        resultEmitted.resolve();
        yield idleSession(sessionId);
        await backgroundFinished.promise;
        yield {
          type: 'system',
          subtype: 'background_tasks_changed',
          session_id: sessionId,
          tasks: [],
        };
        yield {
          type: 'system',
          subtype: 'task_notification',
          session_id: sessionId,
          task_id: 'background-task-1',
          status: 'completed',
        };
        notificationConsumed.resolve();
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }

  return { backgroundFinished, notificationConsumed, query, resultEmitted };
}

function createEndingBackgroundQuery({ sessionId, outcome = 'unknown' }) {
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'system',
          subtype: 'task_started',
          session_id: sessionId,
          task_id: 'background-task-ending',
        };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: input.message.content,
        };
        if (outcome === 'unknown') return;
        yield {
          type: 'system',
          subtype: 'task_notification',
          session_id: sessionId,
          task_id: 'background-task-ending',
          status: outcome === 'failed' ? 'failed' : 'completed',
        };
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function createDelayedIdleQuery({ sessionId }) {
  const idleConsumed = deferred();
  const releaseIdle = deferred();
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: input.message.content };
        await releaseIdle.promise;
        yield idleSession(sessionId);
        idleConsumed.resolve();
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => releaseIdle.resolve();
    return stream;
  }
  return { idleConsumed, query, releaseIdle };
}

function createAuthoritativeIdleBoundaryQuery({ sessionId }) {
  const commandCompleted = deferred();
  const releaseResult = deferred();
  const resultConsumed = deferred();
  const releaseIdle = deferred();
  const secondInputConsumed = deferred();
  const inputs = [];

  function query({ prompt }) {
    let turnNo = 0;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        turnNo += 1;
        inputs.push(input.message.content);
        if (turnNo === 1) {
          yield { type: 'system', subtype: 'init', session_id: sessionId };
          yield {
            type: 'system',
            subtype: 'command_lifecycle',
            state: 'completed',
            session_id: sessionId,
          };
          commandCompleted.resolve();
          await releaseResult.promise;
          yield {
            type: 'result',
            subtype: 'success',
            session_id: sessionId,
            result: input.message.content,
          };
          resultConsumed.resolve();
          await releaseIdle.promise;
          yield idleSession(sessionId);
          continue;
        }
        secondInputConsumed.resolve();
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: input.message.content,
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {
      releaseResult.resolve();
      releaseIdle.resolve();
    };
    return stream;
  }

  return {
    commandCompleted,
    inputs,
    query,
    releaseIdle,
    releaseResult,
    resultConsumed,
    secondInputConsumed,
  };
}

function createThrowingQuery({ sessionId }) {
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        throw new Error(`provider stream failed for ${input.message.content}`);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function createEndingThenSuccessfulQuery({ sessionId }) {
  const calls = [];
  function query({ prompt, options }) {
    calls.push({ options });
    const queryNo = calls.length;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        if (queryNo === 1) return;
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `resumed ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'resumed' };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { calls, query };
}

function createParallelPermissionQuery({ sessionId }) {
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        await Promise.all([
          options.canUseTool('Read', { file_path: '/tmp/a' }, {
            signal: new AbortController().signal,
          }),
          options.canUseTool('Read', { file_path: '/tmp/b' }, {
            signal: new AbortController().signal,
          }),
        ]);
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `allowed ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'allowed' };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function createEvictionRaceQuery({ sessionId }) {
  const secondTurnStarted = deferred();
  const finishSecondTurn = deferred();
  let closeCalls = 0;

  function query({ prompt }) {
    let turnNo = 0;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        turnNo += 1;
        if (turnNo === 1) yield { type: 'system', subtype: 'init', session_id: sessionId };
        if (turnNo === 2) {
          secondTurnStarted.resolve();
          await finishSecondTurn.promise;
        }
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `answer ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'answer' };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => { closeCalls += 1; };
    return stream;
  }
  return {
    get closeCalls() { return closeCalls; },
    finishSecondTurn,
    query,
    secondTurnStarted,
  };
}

function createDelayedConsumptionQuery() {
  const consume = deferred();
  const queryStarted = deferred();
  let interruptCalls = 0;
  function query({ prompt }) {
    queryStarted.resolve();
    const stream = (async function* generateSdkMessages() {
      await consume.promise;
      for await (const input of prompt) {
        yield {
          type: 'assistant',
          session_id: 'should-not-be-consumed',
          message: { content: [{ type: 'text', text: input.message.content }] },
          parent_tool_use_id: null,
        };
      }
    }());
    stream.interrupt = async () => { interruptCalls += 1; };
    stream.close = () => consume.resolve();
    return stream;
  }
  return {
    get interruptCalls() { return interruptCalls; },
    query,
    queryStarted,
  };
}

function createSyncThrowThenSuccessQuery({ sessionId }) {
  let calls = 0;
  function query({ prompt }) {
    calls += 1;
    if (calls === 1) throw new Error('synchronous SDK setup failure');
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `recovered ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'recovered' };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { get calls() { return calls; }, query };
}

function createCloseAwareQuery({ sessionId }) {
  const closeRequested = deferred();
  const turnStarted = deferred();
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        turnStarted.resolve(input);
        await closeRequested.promise;
        return;
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => closeRequested.resolve();
    return stream;
  }
  return { query, turnStarted };
}

function createCloseFailingQuery({ sessionId }) {
  let calls = 0;
  function query({ prompt }) {
    calls += 1;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield { type: 'result', subtype: 'success', session_id: sessionId, result: input.message.content };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [] });
    stream.close = () => { throw new Error('forced query close failure'); };
    return stream;
  }
  return { get calls() { return calls; }, query };
}

function createQueuedReceiptQuery({ cancelResult = true, sessionId }) {
  const cancelled = deferred();
  const inputConsumed = deferred();
  const cancelledMessageUuids = [];
  function query({ prompt }) {
    let currentInput = null;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        currentInput = input;
        inputConsumed.resolve(input);
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        await cancelled.promise;
        yield {
          type: 'result',
          subtype: 'error_during_execution',
          session_id: sessionId,
          errors: ['interrupted while queued'],
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [currentInput.uuid] });
    stream.cancelAsyncMessage = async (messageUuid) => {
      cancelledMessageUuids.push(messageUuid);
      cancelled.resolve();
      return cancelResult;
    };
    stream.close = () => cancelled.resolve();
    return stream;
  }
  return { cancelledMessageUuids, inputConsumed, query };
}

function createReceiptRaceQuery({ sessionId }) {
  const cancellationStarted = deferred();
  const inputConsumed = deferred();
  const resultConsumed = deferred();
  function query({ prompt }) {
    let currentInput = null;
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        currentInput = input;
        inputConsumed.resolve();
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        await cancellationStarted.promise;
        yield {
          type: 'result',
          subtype: 'error_during_execution',
          session_id: sessionId,
          errors: ['provider result raced the cancellation receipt'],
        };
        resultConsumed.resolve();
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => ({ still_queued: [currentInput.uuid] });
    stream.cancelAsyncMessage = async () => {
      cancellationStarted.resolve();
      await resultConsumed.promise;
      throw new Error('queued cancellation control failed');
    };
    stream.close = () => cancellationStarted.resolve();
    return stream;
  }
  return { inputConsumed, query };
}

function createBoundaryEndingQuery({ sessionId }) {
  const boundary = deferred();
  let endWithError = false;
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: `done ${input.message.content}`,
        };
        await boundary.promise;
        if (endWithError) throw new Error('query failed before idle boundary');
        return;
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => boundary.resolve();
    return stream;
  }
  return {
    fail() {
      endWithError = true;
      boundary.resolve();
    },
    query,
  };
}

function createPermissionProgressQuery({ sessionId }) {
  const progressEmitted = deferred();
  function query({ prompt, options }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        const first = options.canUseTool('Read', { path: '/tmp/first' }, {});
        const second = options.canUseTool('Read', { path: '/tmp/second' }, {});
        await first;
        yield {
          type: 'tool_progress',
          session_id: sessionId,
          tool_use_id: 'parallel-tool',
          tool_name: 'Read',
          elapsed_time_seconds: 1,
        };
        progressEmitted.resolve();
        await second;
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: `done ${input.message.content}`,
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { progressEmitted, query };
}

function createRejectingInterruptQuery({ sessionId }) {
  const finish = deferred();
  const turnStarted = deferred();
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        turnStarted.resolve();
        await finish.promise;
        return;
      }
    }());
    stream.interrupt = async () => {
      throw new Error('interrupt receipt unavailable');
    };
    stream.close = () => finish.resolve();
    return stream;
  }
  return { query, turnStarted };
}

function createToolQuery({ sessionId }) {
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: {
            content: [{
              type: 'tool_use',
              id: 'tool-use-1',
              name: 'Read',
              input: { file_path: '/tmp/example' },
            }],
          },
          parent_tool_use_id: null,
        };
        yield {
          type: 'tool_progress',
          session_id: sessionId,
          tool_use_id: 'tool-use-1',
          tool_name: 'Read',
          elapsed_time_seconds: 2,
          parent_tool_use_id: null,
        };
        yield {
          type: 'user',
          session_id: sessionId,
          message: {
            role: 'user',
            content: [{
              type: 'tool_result',
              tool_use_id: 'tool-use-1',
              content: 'file content',
            }],
          },
          parent_tool_use_id: null,
        };
        yield {
          type: 'assistant',
          session_id: sessionId,
          message: { content: [{ type: 'text', text: `done ${input.message.content}` }] },
          parent_tool_use_id: null,
        };
        yield {
          type: 'result',
          subtype: 'success',
          session_id: sessionId,
          result: 'done',
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function createFailedQuery({ sessionId }) {
  function query({ prompt }) {
    const stream = (async function* generateSdkMessages() {
      for await (const input of prompt) {
        yield { type: 'system', subtype: 'init', session_id: sessionId };
        yield {
          type: 'result',
          subtype: 'error_during_execution',
          session_id: sessionId,
          errors: [`failed ${input.message.content}`],
        };
        yield idleSession(sessionId);
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }
  return { query };
}

function readEvents(database, turnId) {
  return database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence ASC
  `).all(turnId).map(({ event_json: eventJson }) => JSON.parse(eventJson));
}

function deliverTurnNotifications(database, turnId, suffix, deliveredAt) {
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `claude-delivery-${suffix}`,
    now: () => deliveredAt,
    generateId: deterministicIds(`claude-delivery-${suffix}`),
    throttleMs: 0,
  });
  const delivered = [];
  while (true) {
    const command = outbox.claimNext();
    if (!command) break;
    outbox.recordResult(deliveredResult(command, deliveredAt));
    if (command.mapping?.turn_id === turnId) delivered.push(command);
  }
  return delivered;
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('Claude conversation executor', () => {
  test('rejects caller-controlled SDK continuity options', () => {
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { resume: 'caller-owned-session' },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { extraArgs: { '--session-id': 'caller-owned-session' } },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { extraArgs: { continue: null } },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { extraArgs: { 'resume=foreign': null } },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { extraArgs: { '-rforeign-session': null } },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { executableArgs: ['--resume=foreign'] },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { executableArgs: ['-rforeign-session'] },
    })).toThrow(/managed by Core lineage authority/);
    expect(() => createClaudeConversationAdapter({
      query: () => {},
      queryOptions: { executableArgs: ['-cforeign-session'] },
    })).toThrow(/managed by Core lineage authority/);
  });

  test('fences auto-allowed and permission-bypass write tools before execution', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'workspace-pre-tool-fence');
    const fake = createAutoAllowedWriteQuery({ sessionId: 'claude-session-workspace-fence' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        queryOptions: {
          allowedTools: ['Bash'],
          permissionMode: 'bypassPermissions',
          allowDangerouslySkipPermissions: true,
        },
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-workspace-pre-tool-fence',
      now: () => '2026-07-19T09:00:15Z',
      generateId: deterministicIds('workspace-pre-tool-fence'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(fake.hookResults).toEqual([{}]);

    await service.close();
    database.close();
  });

  test('denies an auto-allowed write when its workspace epoch is stale', async () => {
    const stale = new Error('The workspace epoch is stale.');
    stale.code = 'stale_workspace_lease';
    let closedWith = null;
    let toolExecuted = false;
    const activeTurn = {
      controls: {
        assertWorkspaceWrite() { throw stale; },
      },
      output: {
        close(error) { closedWith = error; },
      },
      resultSeen: false,
    };

    const decision = await enforceWorkspaceFenceBeforeTool(
      { providerTurn: activeTurn },
      {
        hook_event_name: 'PreToolUse',
        tool_name: 'Bash',
        tool_input: { command: 'touch must-not-run' },
        tool_use_id: 'stale-auto-allowed-write',
      },
    );
    if (decision.hookSpecificOutput?.permissionDecision !== 'deny') toolExecuted = true;

    expect(decision).toMatchObject({
      hookSpecificOutput: {
        hookEventName: 'PreToolUse',
        permissionDecision: 'deny',
      },
    });
    expect(toolExecuted).toBe(false);
    expect(closedWith).toBe(stale);
    expect(activeTurn.resultSeen).toBe(true);
  });

  test('reports that Claude has no read-only same-handoff acceptance query', async () => {
    const adapter = createClaudeConversationAdapter({ query: () => {} });
    await expect(adapter.queryInteractionHandoffAcceptance({
      handoff: {
        handoff_id: 'handoff-query-claude',
        provider_attempt_id: 'provider-attempt-query-claude',
        handoff_attempt_id: 'handoff-attempt-query-claude',
        handoff_attempt_no: 2,
        lease_epoch: 7,
      },
    })).resolves.toEqual({
      status: 'unknown',
      read_only: true,
      idempotent: true,
      handoff_id: 'handoff-query-claude',
      provider_attempt_id: 'provider-attempt-query-claude',
      handoff_attempt_id: 'handoff-attempt-query-claude',
      handoff_attempt_no: 2,
      lease_epoch: 7,
      accepted_at: null,
      evidence_ref: null,
      reason_code: 'provider_acceptance_query_unavailable',
    });
  });

  test('forces SDK session-state events after fencing caller environment', async () => {
    for (const [index, callerValue] of [undefined, '0', 'caller-value'].entries()) {
      const database = openTestDatabase();
      const accepted = acceptQueuedTurn(database, `session-state-env-${index}`);
      const fake = createFakeQuery({
        sessionId: `claude-session-state-env-${index}`,
      });
      const environment = {
        PATH: '/test/bin',
        DATABASE_PASSWORD: 'must-not-reach-provider-process',
      };
      if (callerValue !== undefined) {
        environment.CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS = callerValue;
      }
      const service = createExecutorService({
        database,
        adapter: createClaudeConversationAdapter({
          query: fake.query,
          queryOptions: { env: environment },
        }),
        provider: 'claude',
        serviceInstanceId: `executor-service-session-state-env-${index}`,
        now: () => '2026-07-22T09:00:00Z',
        generateId: deterministicIds(`session-state-env-${index}`),
      });

      await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
      expect(fake.calls[0].options.env).toEqual({
        CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS: '1',
        PATH: '/test/bin',
      });

      await service.close();
      database.close();
    }
  });

  test('removes static tokens when native credentials are detected', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'native-auth-env');
    const nativeHome = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-native-auth-'));
    temporaryDirectories.push(nativeHome);
    const fake = createFakeQuery({ sessionId: 'claude-session-native-auth-env' });
    let detectedExecutable;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        resolveClaudeExecutable: () => '/sdk/bundled/claude',
        detectNativeAuthentication(environment, { executable }) {
          detectedExecutable = executable;
          return true;
        },
        queryOptions: {
          env: {
            HOME: nativeHome,
            PATH: '/test/bin',
            ANTHROPIC_API_KEY: 'stale-static-key',
            CLAUDE_CODE_OAUTH_TOKEN: 'stale-static-token',
          },
        },
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-native-auth-env',
      now: () => '2026-07-19T09:00:30Z',
      generateId: deterministicIds('native-auth-env'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    expect(fake.calls[0].options.env).toEqual({
      CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS: '1',
      HOME: nativeHome,
      PATH: '/test/bin',
    });
    expect(detectedExecutable).toBe('/sdk/bundled/claude');
    await service.close();
    database.close();
  });

  test('uses injectable native-auth detection for keychain-backed Claude login', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'native-auth-keychain');
    const fake = createFakeQuery({ sessionId: 'claude-session-native-auth-keychain' });
    let detectedEnvironment;
    let detectedExecutable;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        resolveClaudeExecutable: () => '/sdk/bundled/claude',
        detectNativeAuthentication(environment, { executable }) {
          detectedEnvironment = environment;
          detectedExecutable = executable;
          return true;
        },
        queryOptions: {
          env: {
            HOME: '/Users/keychain-user',
            PATH: '/test/bin',
            ANTHROPIC_AUTH_TOKEN: 'stale-static-token',
            DATABASE_PASSWORD: 'must-not-reach-provider-process',
          },
        },
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-native-auth-keychain',
      now: () => '2026-07-19T09:00:45Z',
      generateId: deterministicIds('native-auth-keychain'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    expect(fake.calls[0].options.env).toEqual({
      CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS: '1',
      HOME: '/Users/keychain-user',
      PATH: '/test/bin',
    });
    expect(detectedEnvironment.DATABASE_PASSWORD).toBeUndefined();
    expect(detectedExecutable).toBe('/sdk/bundled/claude');
    await service.close();
    database.close();
  });

  test('keeps one SDK query across turns and binds its first session ID before output', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'first');
    const second = acceptQueuedTurn(database, 'second');
    const fake = createFakeQuery({
      sessionId: 'claude-session-A',
      beforeFirstOutput() {
        expect(database.prepare(`
          SELECT provider, provider_native_id
          FROM runtime_lineages
          WHERE lineage_id = ?
        `).get(first.lineage_id)).toEqual({
          provider: 'claude',
          provider_native_id: 'claude-session-A',
        });
      },
    });
    const adapter = createClaudeConversationAdapter({
      query: fake.query,
      queryOptions: {
        env: {
          PATH: '/test/bin',
          ANTHROPIC_API_KEY: 'test-key',
          UNAPPROVED_PROVIDER_SECRET: 'must-not-cross-the-adapter',
        },
      },
    });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-claude-A',
      now: () => '2026-07-19T09:01:00Z',
      generateId: deterministicIds('executor'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });

    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0].options.resume).toBeUndefined();
    expect(fake.calls[0].options.env).toEqual({
      ANTHROPIC_API_KEY: 'test-key',
      CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS: '1',
      PATH: '/test/bin',
    });
    expect(fake.inputs).toEqual(['turn first', 'turn second']);
    expect(database.prepare(`
      SELECT runtime_evidence_json FROM runtime_provider_attempts WHERE turn_id = ?
    `).get(first.turn_id)).toEqual({
      runtime_evidence_json: expect.any(String),
    });
    expect(JSON.parse(database.prepare(`
      SELECT runtime_evidence_json FROM runtime_provider_attempts WHERE turn_id = ?
    `).get(first.turn_id).runtime_evidence_json)).toMatchObject({
      runtime_instance_id: expect.any(String),
      handle_kind: 'claude_sdk_query',
      controllable: true,
    });
    expect(readEvents(database, first.turn_id).at(-2)).toMatchObject({
      kind: 'text_snapshot',
      provider: 'claude',
      provider_native_id: 'claude-session-A',
      payload: {
        text: 'answer for turn first',
        end_offset: 21,
      },
    });
    expect(readEvents(database, second.turn_id).at(-2)).toMatchObject({
      kind: 'text_snapshot',
      provider: 'claude',
      provider_native_id: 'claude-session-A',
      payload: {
        text: 'answer for turn second',
        end_offset: 22,
      },
    });

    await service.close();
    database.close();
  });

  test('requires matching SDK idle after command completion and result before queued input', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'authoritative-idle-first');
    const second = acceptQueuedTurn(database, 'authoritative-idle-second');
    const fake = createAuthoritativeIdleBoundaryQuery({
      sessionId: 'claude-session-authoritative-idle',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-authoritative-idle',
      now: () => '2026-07-22T09:01:00Z',
      generateId: deterministicIds('authoritative-idle'),
    });

    let firstRunSettled = false;
    const firstRun = service.runNext();
    void firstRun.then(
      () => { firstRunSettled = true; },
      () => { firstRunSettled = true; },
    );
    await fake.commandCompleted.promise;
    await new Promise((resolve) => setImmediate(resolve));
    expect(firstRunSettled).toBe(false);

    fake.releaseResult.resolve();
    await expect(firstRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    await fake.resultConsumed.promise;

    const secondRun = service.runNext();
    await new Promise((resolve) => setImmediate(resolve));
    expect(fake.inputs).toEqual(['turn authoritative-idle-first']);

    fake.releaseIdle.resolve();
    await fake.secondInputConsumed.promise;
    await expect(secondRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(fake.inputs).toEqual([
      'turn authoritative-idle-first',
      'turn authoritative-idle-second',
    ]);

    await service.close();
    database.close();
  });

  test('switches resident queries when FIFO advances to another lineage', async () => {
    const database = openTestDatabase();
    const firstEnvelope = normalEnvelope('claude-lineage-first');
    const first = acceptNormalInbound(database, firstEnvelope, {
      now: () => '2026-07-19T09:01:30Z',
      generateId: deterministicIds('inbound-claude-lineage-first'),
    });
    const alternateLineageId = 'lineage-claude-alternate';
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES (?, ?, 'normal', 0, ?)
    `).run(alternateLineageId, first.conversation_id, first.committed_at);
    const historical = acceptNormalInbound(
      database,
      normalEnvelope('claude-lineage-historical'),
      {
        now: () => '2026-07-19T09:01:30.500Z',
        generateId: deterministicIds('inbound-claude-lineage-historical'),
      },
    );
    database.prepare(`
      UPDATE runtime_turns SET lineage_id = ?, state = 'completed' WHERE turn_id = ?
    `).run(alternateLineageId, historical.turn_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?')
      .run(historical.turn_id);
    database.prepare(`
      INSERT INTO runtime_message_mappings (
        region, tenant_id, channel, bot_id, platform_message_id,
        conversation_id, turn_id, lineage_id, binding_state, reason,
        mapping_id, mapping_version, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'bound', NULL, ?, 1, ?)
    `).run(
      firstEnvelope.region,
      firstEnvelope.tenant_id,
      firstEnvelope.channel,
      firstEnvelope.bot_id,
      'model-message-claude-alternate',
      first.conversation_id,
      historical.turn_id,
      alternateLineageId,
      'mapping-claude-alternate',
      first.committed_at,
    );
    const replyEnvelope = normalEnvelope('claude-lineage-reply');
    replyEnvelope.reply = {
      root_message_id: 'model-message-claude-root',
      parent_message_id: 'model-message-claude-alternate',
      reply_to_message_id: 'model-message-claude-alternate',
    };
    const reply = acceptNormalInbound(database, replyEnvelope, {
      now: () => '2026-07-19T09:01:31Z',
      generateId: deterministicIds('inbound-claude-lineage-reply'),
    });
    const fake = createSessionPerQuery();
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-claude-lineages',
      now: () => '2026-07-19T09:01:32Z',
      generateId: deterministicIds('claude-lineages'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: first.turn_id });
    await expect(service.runNext()).resolves.toMatchObject({ turn_id: reply.turn_id });
    expect(fake.calls).toHaveLength(2);
    expect(fake.calls.map(({ options }) => options.resume)).toEqual([undefined, undefined]);
    expect(database.prepare(`
      SELECT lineage_id, provider_native_id
      FROM runtime_lineages
      WHERE lineage_id IN (?, ?)
      ORDER BY lineage_id
    `).all(first.lineage_id, alternateLineageId)).toEqual([
      {
        lineage_id: alternateLineageId,
        provider_native_id: 'claude-lineage-session-2',
      },
      {
        lineage_id: first.lineage_id,
        provider_native_id: 'claude-lineage-session-1',
      },
    ]);

    await service.close();
    database.close();
  });

  test('stops the current fenced turn and cancels queued work through the cutoff', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'cancel-first');
    const second = acceptQueuedTurn(database, 'cancel-second');
    const fake = createInterruptibleQuery({ sessionId: 'claude-session-cancel' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-cancel',
      now: () => '2026-07-19T09:02:00Z',
      generateId: deterministicIds('cancel'),
    });

    const firstRun = service.runNext();
    await fake.firstTurnStarted.promise;
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    await expect(stopActiveConversation(service, first)).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: first.turn_id },
    });
    await expect(firstRun).resolves.toMatchObject({
      status: 'stopped',
      turn_id: first.turn_id,
    });

    expect(fake.interruptCalls).toBe(1);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(first.turn_id)).toEqual({ state: 'stopped' });
    expect(database.prepare(`
      SELECT status FROM runtime_turn_queue WHERE turn_id = ?
    `).get(first.turn_id)).toEqual({ status: 'stopped' });

    fake.lateOutputRelease.resolve();
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    expect(fake.queryCalls).toBe(1);
    expect(fake.inputs).toEqual(['turn cancel-first']);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(second.turn_id)).toEqual({ state: 'cancelled' });

    await service.close();
    database.close();
  });

  test('routes permission callbacks through the durable provider-neutral turn fence', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission');
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission',
      now: () => '2026-07-19T09:03:00Z',
      generateId: deterministicIds('permission'),
    });

    const waiting = await service.runNext();
    expect(waiting).toMatchObject({
      status: 'waiting_user',
      request: {
        kind: 'tool_approval',
        runtime_fence: expect.objectContaining({ provider_interaction_ref: expect.any(String) }),
      },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id).state).toBe('waiting_user');
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    const answer = service.submitInteractionAnswer(
      permissionAnswer(waiting.request, 'durable-happy'),
    );
    const handled = await service.deliverInteractionAnswer(answer.handoff_id);
    expect(handled.execution).toMatchObject({ status: 'completed', turn_id: accepted.turn_id });
    expect(fake.permissionResults).toEqual([{
      behavior: 'allow',
      updatedInput: { command: 'pwd' },
    }]);
    expect(readEvents(database, accepted.turn_id).map((event) => event.kind))
      .toEqual(expect.arrayContaining([
        'interaction_requested',
        'interaction_answer_committed',
        'interaction_answered',
      ]));

    await service.close();
    database.close();
  });

  test('persists delivery_unknown when provider ack cannot be durably committed', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-ack-failure');
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission-ack-failure' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-ack-failure',
      now: () => '2026-07-19T09:03:15Z',
      generateId: deterministicIds('permission-ack-failure'),
    });

    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      permissionAnswer(waiting.request, 'ack-failure'),
    );
    database.exec(`
      CREATE TRIGGER fail_permission_ack_audit
      BEFORE INSERT ON runtime_interaction_audit
      WHEN NEW.outcome = 'accepted'
      BEGIN
        SELECT RAISE(ABORT, 'forced permission acknowledgement failure');
      END;
    `);
    await expect(service.deliverInteractionAnswer(answer.handoff_id)).rejects.toMatchObject({
      name: 'SqliteError',
      code: 'SQLITE_CONSTRAINT_TRIGGER',
      message: 'forced permission acknowledgement failure',
    });
    expect(fake.permissionResults).toEqual([{
      behavior: 'allow',
      updatedInput: { command: 'pwd' },
    }]);
    expect(database.prepare(`
      SELECT state, handoff_state FROM runtime_interactions WHERE interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'recovering' });

    database.exec(`DROP TRIGGER fail_permission_ack_audit`);
    await service.close();
    database.close();
  });

  test('cancels without retry when the SDK query ended before answer send', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-ended-before-answer');
    const fake = createEndingPermissionQuery({
      sessionId: 'claude-session-permission-ended-before-answer',
    });
    const adapter = createClaudeConversationAdapter({ query: fake.query });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-ended-before-answer',
      now: () => '2026-07-19T09:03:25Z',
      generateId: deterministicIds('permission-ended-before-answer'),
    });

    const waiting = await service.runNext();
    await new Promise((resolve) => setImmediate(resolve));
    expect(adapter.hasResident(accepted.conversation_id)).toBe(false);
    const answer = service.submitInteractionAnswer(
      permissionAnswer(waiting.request, 'ended-before-answer'),
    );

    await expect(service.deliverInteractionAnswer(answer.handoff_id)).resolves.toMatchObject({
      status: 'recovering',
      interaction_state: 'cancelled',
      handoff_state: 'cancelled',
      turn_state: 'recovering',
    });
    expect(database.prepare(`
      SELECT interaction.state, interaction.handoff_state,
        handoff.state AS durable_handoff_state, turn.state AS turn_state
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'cancelled',
      handoff_state: 'cancelled',
      durable_handoff_state: 'cancelled',
      turn_state: 'recovering',
    });
    const latestEvent = JSON.parse(database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
    `).get(accepted.turn_id).event_json);
    expect(latestEvent).toMatchObject({
      kind: 'recovery_started',
      phase: 'recovering',
    });

    await service.close();
    database.close();
  });

  test('isolates a timed-out SDK permission before releasing its executor lease', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-timeout');
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission-timeout' });
    const clock = { now: '2026-07-19T09:03:15Z' };
    let deadlineCallback;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-timeout',
      now: () => clock.now,
      generateId: deterministicIds('permission-timeout'),
      interactionTimeoutMs: 1_000,
      setTimeoutFn: (callback) => {
        deadlineCallback = callback;
        return { unref() {} };
      },
      clearTimeoutFn: () => {},
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    clock.now = '2026-07-19T09:03:17Z';
    await deadlineCallback();

    expect(fake.closeCalls).toBe(1);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'timed_out' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null, turn_id: null });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'released' });

    await service.close();
    database.close();
  });

  test('preserves provider authority when a permission-state projection cannot persist', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-projection-failure');
    database.exec(`
      CREATE TRIGGER fail_permission_projection
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.critical = 1 AND NEW.terminal = 0
      BEGIN
        SELECT RAISE(ABORT, 'forced permission projection failure');
      END;
    `);
    const fake = createCaughtPermissionFailureQuery({
      sessionId: 'claude-session-permission-projection',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-projection',
      now: () => '2026-07-19T09:03:30Z',
      generateId: deterministicIds('permission-projection'),
    });

    const run = service.runNext();
    await fake.permissionRequested.promise;
    await expect(run).rejects.toThrow(/forced permission projection failure/);
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'running' });
    expect(database.prepare(`SELECT status FROM runtime_turn_queue WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ status: 'claimed' });

    await service.close();
    database.close();
  });

  test('cancels a waiting permission even when the application callback does not settle', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-cancel');
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission-cancel' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-cancel',
      now: () => '2026-07-19T09:03:30Z',
      generateId: deterministicIds('permission-cancel'),
    });

    const waiting = await service.runNext();
    expect(waiting).toMatchObject({ status: 'waiting_user' });
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: accepted.turn_id },
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });
    expect(database.prepare(`
      SELECT state FROM runtime_interactions WHERE interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({ state: 'cancelled' });

    await service.close();
    database.close();
  });

  test('terminalizes an SDK-caught permission cancellation from the durable waiting state', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-cancel-caught');
    const fake = createCaughtPermissionCancellationQuery({
      sessionId: 'claude-session-permission-cancel-caught',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-cancel-caught',
      now: () => '2026-07-19T09:03:45Z',
      generateId: deterministicIds('permission-cancel-caught'),
    });

    const waiting = await service.runNext();
    expect(waiting).toMatchObject({ status: 'waiting_user' });
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: accepted.turn_id },
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });

    await service.close();
    database.close();
  });

  test('keeps waiting_user until all parallel permission callbacks settle', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-parallel');
    const fake = createParallelPermissionQuery({ sessionId: 'claude-session-permission-parallel' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-parallel',
      now: () => '2026-07-19T09:03:45Z',
      generateId: deterministicIds('permission-parallel'),
    });

    const firstWaiting = await service.runNext();
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_interactions WHERE turn_id = ? AND state = 'pending'
    `).get(accepted.turn_id)).toEqual({ count: 2 });
    const firstAnswer = service.submitInteractionAnswer(
      permissionAnswer(firstWaiting.request, 'parallel-1'),
    );
    const firstHandled = await service.deliverInteractionAnswer(firstAnswer.handoff_id);
    expect(firstHandled.execution).toMatchObject({ status: 'waiting_user' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'waiting_user' });
    const secondAnswer = service.submitInteractionAnswer(
      permissionAnswer(firstHandled.execution.request, 'parallel-2'),
    );
    const secondHandled = await service.deliverInteractionAnswer(secondAnswer.handoff_id);
    expect(secondHandled.execution).toMatchObject({ status: 'completed' });

    await service.close();
    database.close();
  });

  test('cancels a turn with buffered parallel permission notifications', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-parallel-cancel');
    const fake = createParallelPermissionQuery({
      sessionId: 'claude-session-permission-parallel-cancel',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-parallel-cancel',
      now: () => '2026-07-19T09:03:48Z',
      generateId: deterministicIds('permission-parallel-cancel'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_interactions WHERE turn_id = ? AND state = 'pending'
    `).get(accepted.turn_id)).toEqual({ count: 2 });

    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: accepted.turn_id },
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });
    expect(database.prepare(`
      SELECT ordinal, state FROM runtime_interactions WHERE turn_id = ? ORDER BY ordinal
    `).all(accepted.turn_id)).toEqual([
      { ordinal: 1, state: 'cancelled' },
      { ordinal: 2, state: 'cancelled' },
    ]);

    await service.close();
    database.close();
  });

  test('isolates a durable stop despite timer projection failure', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-cancel-timer-failure');
    const fake = createPermissionQuery({
      sessionId: 'claude-session-permission-cancel-timer-failure',
    });
    let failTimerProjection = false;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-cancel-timer-failure',
      now: () => '2026-07-19T09:03:49Z',
      generateId: deterministicIds('permission-cancel-timer-failure'),
      setTimeoutFn: () => ({ unref() {} }),
      clearTimeoutFn() {
        if (failTimerProjection) throw new Error('forced timer projection failure');
      },
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    failTimerProjection = true;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'isolated',
      lease_released: true,
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });
    failTimerProjection = false;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      conversation_id: accepted.conversation_id,
      deduplicated: true,
    });

    await service.close();
    database.close();
  });

  test('keeps every parallel SDK permission durable across service shutdown', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-parallel-restart');
    const fake = createParallelPermissionQuery({
      sessionId: 'claude-session-permission-parallel-restart',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-parallel-restart',
      now: () => '2026-07-19T09:03:50Z',
      generateId: deterministicIds('permission-parallel-restart'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    expect(database.prepare(`
      SELECT ordinal, state FROM runtime_interactions WHERE turn_id = ? ORDER BY ordinal
    `).all(accepted.turn_id)).toEqual([
      { ordinal: 1, state: 'pending' },
      { ordinal: 2, state: 'pending' },
    ]);

    await service.close();
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT ordinal, state FROM runtime_interactions WHERE turn_id = ? ORDER BY ordinal
    `).all(accepted.turn_id)).toEqual([
      { ordinal: 1, state: 'pending' },
      { ordinal: 2, state: 'pending' },
    ]);

    database.close();
  });

  test('evicts only truly idle executors and resumes the durable session afterward', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'idle-first');
    const fake = createFakeQuery({ sessionId: 'claude-session-idle' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        idleTimeoutMs: 1_000,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-idle',
      now: () => '2026-07-19T09:04:00Z',
      generateId: deterministicIds('idle'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: first.turn_id });
    const queued = acceptQueuedTurn(database, 'idle-queued');
    clock = 2_000;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    await expect(service.runNext()).resolves.toMatchObject({ turn_id: queued.turn_id });

    clock = 4_000;
    await expect(service.evictIdleExecutors()).resolves.toEqual([first.conversation_id]);
    const resumed = acceptQueuedTurn(database, 'idle-resumed');
    await expect(service.runNext()).resolves.toMatchObject({ turn_id: resumed.turn_id });
    expect(fake.calls).toHaveLength(2);
    expect(fake.calls[1].options.resume).toBe('claude-session-idle');

    await service.close();
    database.close();
  });

  test('uses the consensus thirty-minute idle timeout by default', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'idle-default');
    const fake = createFakeQuery({ sessionId: 'claude-session-idle-default' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-idle-default',
      now: () => '2026-07-19T09:04:30Z',
      generateId: deterministicIds('idle-default'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    clock = 300_001;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    clock = 1_800_001;
    await expect(service.evictIdleExecutors()).resolves.toEqual([accepted.conversation_id]);

    await service.close();
    database.close();
  });

  test('starts the idle clock at the authoritative SDK idle boundary', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'idle-boundary');
    const fake = createDelayedIdleQuery({ sessionId: 'claude-session-idle-boundary' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        idleTimeoutMs: 100,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-idle-boundary',
      now: () => '2026-07-19T09:04:45Z',
      generateId: deterministicIds('idle-boundary'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    clock = 1_000;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    fake.releaseIdle.resolve();
    await fake.idleConsumed.promise;
    clock = 1_099;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    clock = 1_100;
    await expect(service.evictIdleExecutors()).resolves.toEqual([accepted.conversation_id]);

    await service.close();
    database.close();
  });

  test('rebuilds a resident query from durable lineage after service restart', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'restart-first');
    const fake = createFakeQuery({ sessionId: 'claude-session-restart' });
    const firstService = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-before-restart',
      now: () => '2026-07-19T09:05:00Z',
      generateId: deterministicIds('restart-before'),
    });
    await expect(firstService.runNext()).resolves.toMatchObject({ turn_id: first.turn_id });
    await firstService.close();

    const second = acceptQueuedTurn(database, 'restart-second');
    const restartedService = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-after-restart',
      now: () => '2026-07-19T09:06:00Z',
      generateId: deterministicIds('restart-after'),
    });
    await expect(restartedService.runNext()).resolves.toMatchObject({ turn_id: second.turn_id });
    expect(fake.calls).toHaveLength(2);
    expect(fake.calls[1].options.resume).toBe('claude-session-restart');

    await restartedService.close();
    database.close();
  });

  test('does not evict a resident executor while a Claude background task is active', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'background');
    const fake = createBackgroundQuery({ sessionId: 'claude-session-background' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        idleTimeoutMs: 100,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-background',
      now: () => '2026-07-19T09:07:00Z',
      generateId: deterministicIds('background'),
    });

    let executionSettled = false;
    const execution = service.runNext().finally(() => { executionSettled = true; });
    await fake.resultEmitted.promise;
    for (let index = 0; index < 10; index += 1) await Promise.resolve();
    expect(executionSettled).toBe(false);
    expect(service.snapshot().workspace_leases).toEqual({
      complete: true,
      items: [expect.objectContaining({
        holder_turn_id: accepted.turn_id,
        holder_background_work_id: 'background-work-background-1',
      })],
      error: null,
    });
    clock = 1_000;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    fake.backgroundFinished.resolve();
    await fake.notificationConsumed.promise;
    await expect(execution).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(service.snapshot().workspace_leases.items).toEqual([]);
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    clock = 1_101;
    await expect(service.evictIdleExecutors()).resolves.toEqual([accepted.conversation_id]);

    await service.close();
    database.close();
  });

  test('fails only after a known failed background task ends and then releases its workspace', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'background-failed');
    const fake = createEndingBackgroundQuery({
      sessionId: 'claude-session-background-failed',
      outcome: 'failed',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-background-failed',
      now: () => '2026-07-19T09:07:10Z',
      generateId: deterministicIds('background-failed'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'failed',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_background_work WHERE holder_turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'failed' });
    expect(service.snapshot().workspace_leases.items).toEqual([]);

    await service.close();
    database.close();
  });

  test('waits for delivered recovery notice before isolating unknown background work', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'background-unknown');
    const fake = createEndingBackgroundQuery({
      sessionId: 'claude-session-background-unknown',
      outcome: 'unknown',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-background-unknown',
      now: () => '2026-07-19T09:07:20Z',
      generateId: deterministicIds('background-unknown'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_background_work WHERE holder_turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'unknown' });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'active' });
    expect(readEvents(database, accepted.turn_id).at(-2)).toMatchObject({
      phase: 'recovering',
      kind: 'recovery_started',
      payload: expect.objectContaining({ side_effect_status: 'unknown' }),
      error: expect.objectContaining({ side_effect_status: 'unknown' }),
    });
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      phase: 'recovering',
      kind: 'recovery_waiting_decision',
      payload: expect.objectContaining({ side_effect_status: 'unknown' }),
    });
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [],
      notification_pending: [accepted.turn_id],
      isolation_pending: [],
    });
    expect(deliverTurnNotifications(
      database,
      accepted.turn_id,
      'background-unknown',
      '2026-07-19T09:07:21Z',
    )).not.toHaveLength(0);
    await expect(service.reconcileWorkspaceRecoveries()).resolves.toEqual({
      isolated: [accepted.turn_id],
      notification_pending: [],
      isolation_pending: [],
    });
    expect(database.prepare(`
      SELECT state FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'released' });

    await service.close();
    database.close();
  });

  test('rechecks idleness when eviction races with a newly claimed turn', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'eviction-race-first');
    const fake = createEvictionRaceQuery({ sessionId: 'claude-session-eviction-race' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        idleTimeoutMs: 100,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-eviction-race',
      now: () => '2026-07-19T09:07:30Z',
      generateId: deterministicIds('eviction-race'),
    });
    await service.runNext();
    clock = 1_000;

    const eviction = service.evictIdleExecutors();
    const second = acceptQueuedTurn(database, 'eviction-race-second');
    const secondRun = service.runNext();
    await fake.secondTurnStarted.promise;
    await expect(eviction).resolves.toEqual([]);
    expect(fake.closeCalls).toBe(0);
    fake.finishSecondTurn.resolve();
    await expect(secondRun).resolves.toMatchObject({ turn_id: second.turn_id });

    await service.close();
    database.close();
  });

  test('performs a synchronous final guard after the last durable eviction check', async () => {
    const fake = createEvictionRaceQuery({ sessionId: 'claude-session-final-eviction-race' });
    let clock = 0;
    const adapter = createClaudeConversationAdapter({
      query: fake.query,
      idleTimeoutMs: 100,
      now: () => clock,
    });
    const context = (turnId) => ({
      conversation_id: 'conversation-final-eviction-race',
      turn_id: turnId,
      lineage_id: 'lineage-final-eviction-race',
      provider_native_id: null,
      trace_id: `trace-${turnId}`,
      input: { text: `input ${turnId}` },
      attempt: { attempt_id: `attempt-${turnId}`, attempt_no: 1, lease_epoch: 1 },
    });
    const controls = { requestPermission: async () => ({ behavior: 'deny' }) };
    const consume = async (turnId) => {
      const records = [];
      for await (const record of adapter.execute(context(turnId), controls)) {
        records.push(record);
        if (record.type === 'provider_native_id') record.acknowledge();
      }
      return records;
    };

    await consume('turn-final-race-1');
    await new Promise((resolve) => setImmediate(resolve));
    clock = 1_000;
    let durableChecks = 0;
    let secondRun;
    const eviction = adapter.evictIdle({
      async canEvict() {
        durableChecks += 1;
        if (durableChecks === 2) {
          secondRun = consume('turn-final-race-2');
          await fake.secondTurnStarted.promise;
        }
        return true;
      },
    });

    await expect(eviction).resolves.toEqual([]);
    expect(fake.closeCalls).toBe(0);
    fake.finishSecondTurn.resolve();
    await expect(secondRun).resolves.toEqual(expect.arrayContaining([
      expect.objectContaining({ type: 'turn_result', outcome: 'completed' }),
    ]));
    await adapter.close();
  });

  test('rechecks the durable queue before completing an idle eviction', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'eviction-queued-first');
    const fake = createFakeQuery({ sessionId: 'claude-session-eviction-queued' });
    let clock = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        idleTimeoutMs: 100,
        now: () => clock,
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-eviction-queued',
      now: () => '2026-07-19T09:07:45Z',
      generateId: deterministicIds('eviction-queued'),
    });
    await service.runNext();
    clock = 1_000;

    const eviction = service.evictIdleExecutors();
    const queued = acceptQueuedTurn(database, 'eviction-queued-second');
    await expect(eviction).resolves.toEqual([]);
    await expect(service.runNext()).resolves.toMatchObject({ turn_id: queued.turn_id });

    await service.close();
    database.close();
  });

  test('normalizes Claude tool lifecycle messages without exposing SDK objects', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'tool-events');
    const fake = createToolQuery({ sessionId: 'claude-session-tools' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-tools',
      now: () => '2026-07-19T09:08:00Z',
      generateId: deterministicIds('tools'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    expect(readEvents(database, accepted.turn_id).filter(
      (event) => event.kind.startsWith('tool_'),
    ).map(({ kind, payload }) => ({ kind, payload }))).toEqual([
      {
        kind: 'tool_started',
        payload: {
          tool_use_id: 'tool-use-1',
          tool_name: 'Read',
          summary: 'Read started.',
          side_effect_status: 'unknown',
        },
      },
      {
        kind: 'tool_progress',
        payload: {
          tool_use_id: 'tool-use-1',
          tool_name: 'Read',
          summary: 'Read running for 2s.',
          side_effect_status: 'unknown',
        },
      },
      {
        kind: 'tool_finished',
        payload: {
          tool_use_id: 'tool-use-1',
          tool_name: 'Read',
          summary: 'Read finished.',
          side_effect_status: 'known',
        },
      },
    ]);

    await service.close();
    database.close();
  });

  test('persists provider failure as a fenced terminal turn without leaking SDK errors', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'failed-result');
    const fake = createFailedQuery({ sessionId: 'claude-session-failed' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-failed',
      now: () => '2026-07-19T09:09:00Z',
      generateId: deterministicIds('failed'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'failed',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'failed' });
    expect(database.prepare(`
      SELECT status FROM runtime_turn_queue WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'failed' });
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      phase: 'failed',
      error: {
        code: 'provider_execution_failed',
        category: 'provider',
        side_effect_status: 'unknown',
        user_message: 'The provider turn did not complete successfully.',
      },
    });

    await service.close();
    database.close();
  });

  test('settles a thrown provider stream error without leaving a claimed lease', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'throwing-stream');
    const fake = createThrowingQuery({ sessionId: 'claude-session-throwing-stream' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-throwing-stream',
      now: () => '2026-07-19T09:10:00Z',
      generateId: deterministicIds('throwing-stream'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'failed' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'failed' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null, turn_id: null });

    await service.close();
    database.close();
  });

  test('recreates and resumes a query that ended cleanly without a result', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'ended-query-first');
    const second = acceptQueuedTurn(database, 'ended-query-second');
    const fake = createEndingThenSuccessfulQuery({ sessionId: 'claude-session-ended-query' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-ended-query',
      now: () => '2026-07-19T09:11:00Z',
      generateId: deterministicIds('ended-query'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'failed',
      turn_id: first.turn_id,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(fake.calls).toHaveLength(2);
    expect(fake.calls[1].options.resume).toBe('claude-session-ended-query');

    await service.close();
    database.close();
  });

  test('releases capacity promptly when a provider stream ends', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'ended-capacity-first');
    const secondEnvelope = normalEnvelope('ended-capacity-second');
    secondEnvelope.chat_id = 'chat-ended-capacity-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T09:11:30Z',
      generateId: deterministicIds('inbound-ended-capacity-second'),
    });
    let calls = 0;
    const finishFirstQuery = deferred();
    let heartbeat;
    let serviceTime = '2026-07-19T09:11:30Z';
    const adapter = createClaudeConversationAdapter({
      query({ prompt }) {
        calls += 1;
        const queryNo = calls;
        const sessionId = `claude-session-ended-capacity-${queryNo}`;
        const stream = (async function* generateSdkMessages() {
          for await (const input of prompt) {
            yield { type: 'system', subtype: 'init', session_id: sessionId };
            if (queryNo === 1) {
              yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'ok' };
              yield idleSession(sessionId);
              await finishFirstQuery.promise;
              return;
            }
            yield {
              type: 'assistant',
              session_id: sessionId,
              message: { content: [{ type: 'text', text: input.message.content }] },
              parent_tool_use_id: null,
            };
            yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'ok' };
            yield idleSession(sessionId);
          }
        }());
        stream.interrupt = async () => {};
        stream.close = () => {};
        return stream;
      },
    });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-ended-capacity',
      now: () => serviceTime,
      generateId: deterministicIds('ended-capacity'),
      maxResidentExecutorsPerBot: 1,
      scheduleResidentHeartbeat(callback) {
        heartbeat = callback;
        return { unref() {} };
      },
      cancelResidentHeartbeat() {},
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    expect(database.prepare(`SELECT conversation_id FROM runtime_executor_residents`).all())
      .toEqual([{ conversation_id: first.conversation_id }]);
    database.exec(`
      CREATE TRIGGER fail_ended_resident_release
      BEFORE DELETE ON runtime_executor_residents
      BEGIN
        SELECT RAISE(ABORT, 'forced ended resident release failure');
      END;
    `);
    finishFirstQuery.resolve();
    await new Promise((resolve) => setImmediate(resolve));
    expect(database.prepare(`SELECT conversation_id FROM runtime_executor_residents`).all())
      .toEqual([{ conversation_id: first.conversation_id }]);
    serviceTime = '2026-07-19T09:12:00Z';
    heartbeat();
    expect(database.prepare(`
      SELECT owner_expires_at FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(first.conversation_id)).toEqual({
      owner_expires_at: '2026-07-19T09:13:00.000Z',
    });
    database.exec(`DROP TRIGGER fail_ended_resident_release`);
    heartbeat();
    expect(database.prepare(`SELECT conversation_id FROM runtime_executor_residents`).all())
      .toEqual([]);
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });

    await service.close();
    database.close();
  });

  test('cancels a turn whose input has not yet been consumed by the SDK', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'cancel-before-consume');
    const fake = createDelayedConsumptionQuery();
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-cancel-before-consume',
      now: () => '2026-07-19T09:12:00Z',
      generateId: deterministicIds('cancel-before-consume'),
    });

    const run = service.runNext();
    await fake.queryStarted.promise;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(fake.interruptCalls).toBe(0);

    await service.close();
    database.close();
  });

  test('cancels a UUID-stamped input that survives the SDK interrupt receipt', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'cancel-sdk-queued');
    const fake = createQueuedReceiptQuery({ sessionId: 'claude-session-sdk-queued' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        generateMessageUuid: () => '00000000-0000-4000-8000-000000000009',
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-cancel-sdk-queued',
      now: () => '2026-07-19T09:12:15Z',
      generateId: deterministicIds('cancel-sdk-queued'),
    });

    const run = service.runNext();
    await expect(fake.inputConsumed.promise).resolves.toMatchObject({
      uuid: '00000000-0000-4000-8000-000000000009',
    });
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(fake.cancelledMessageUuids).toEqual([
      '00000000-0000-4000-8000-000000000009',
    ]);
    expect(database.prepare(`
      SELECT provider_native_id FROM runtime_lineages WHERE lineage_id = ?
    `).get(accepted.lineage_id)).toEqual({ provider_native_id: null });
    expect(database.prepare(`
      SELECT event_kind, reason_code FROM runtime_provider_event_diagnostics
      WHERE turn_id = ? AND event_kind = 'provider_native_id'
    `).get(accepted.turn_id)).toEqual({
      event_kind: 'provider_native_id',
      reason_code: 'stale_attempt',
    });

    await service.close();
    database.close();
  });

  test('steers a UUID-stamped SDK turn through the adapter-private interrupt seam', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'steer-sdk-queued');
    const fake = createQueuedReceiptQuery({ sessionId: 'claude-session-steer-sdk-queued' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        generateMessageUuid: () => '00000000-0000-4000-8000-000000000019',
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-steer-sdk-queued',
      now: () => '2026-07-19T09:12:17Z',
      generateId: deterministicIds('steer-sdk-queued'),
    });

    const run = service.runNext();
    await expect(fake.inputConsumed.promise).resolves.toMatchObject({
      uuid: '00000000-0000-4000-8000-000000000019',
    });
    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
        .get(accepted.turn_id).state === 'running') break;
      await new Promise((resolve) => setImmediate(resolve));
    }
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'running' });
    const envelope = normalEnvelope('steer-sdk-command');
    envelope.content = { kind: 'text', text: '/steer revise SDK direction', attachments: [] };
    await expect(service.steer({
      conversation_id: accepted.conversation_id,
      turn_id: accepted.turn_id,
      steer_id: 'steer-sdk-control',
      envelope,
    })).resolves.toMatchObject({
      status: 'completed',
      old_turn: { turn_id: accepted.turn_id, state: 'interrupted' },
      priority_turn: {
        status: 'queued',
        lineage_id: accepted.lineage_id,
        redirected_from_turn_id: accepted.turn_id,
      },
      provider_stop_status: 'confirmed',
    });
    await expect(run).resolves.toMatchObject({ status: 'interrupted' });
    expect(fake.cancelledMessageUuids).toEqual([
      '00000000-0000-4000-8000-000000000019',
    ]);

    await service.stop({
      conversation_id: accepted.conversation_id,
      stop_id: 'stop-steer-sdk-cleanup',
    });
    await service.close();
    database.close();
  });

  test('isolates a stopped turn when queued-message removal is unconfirmed', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'cancel-sdk-unconfirmed');
    const fake = createQueuedReceiptQuery({
      cancelResult: false,
      sessionId: 'claude-session-sdk-unconfirmed',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({
        query: fake.query,
        generateMessageUuid: () => '00000000-0000-4000-8000-000000000010',
      }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-cancel-sdk-unconfirmed',
      now: () => '2026-07-19T09:12:20Z',
      generateId: deterministicIds('cancel-sdk-unconfirmed'),
    });

    const run = service.runNext();
    await fake.inputConsumed.promise;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'isolated',
    });
    await expect(run).resolves.toMatchObject({
      status: 'stopped',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });

    await service.close();
    database.close();
  });

  test('bounds a racing cancellation receipt and isolates the stopped provider', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'cancel-receipt-race');
    const fake = createReceiptRaceQuery({ sessionId: 'claude-session-cancel-receipt-race' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-cancel-receipt-race',
      providerStopTimeoutMs: 50,
      now: () => '2026-07-19T09:12:25Z',
      generateId: deterministicIds('cancel-receipt-race'),
    });

    const run = service.runNext();
    await fake.inputConsumed.promise;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'isolated',
    });
    await expect(run).resolves.toMatchObject({
      status: 'stopped',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });

    await service.close();
    database.close();
  });

  test('settles a buffered next turn when the query fails before the prior idle boundary', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'boundary-failure-first');
    const second = acceptQueuedTurn(database, 'boundary-failure-second');
    const fake = createBoundaryEndingQuery({ sessionId: 'claude-session-boundary-failure' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-boundary-failure',
      now: () => '2026-07-19T09:12:30Z',
      generateId: deterministicIds('boundary-failure'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    const secondRun = service.runNext();
    fake.fail();
    await expect(secondRun).resolves.toMatchObject({
      status: 'failed',
      turn_id: second.turn_id,
    });

    await service.close();
    database.close();
  });

  test('settles a buffered next turn when close arrives before the prior idle boundary', async () => {
    const database = openTestDatabase();
    acceptQueuedTurn(database, 'boundary-close-first');
    const second = acceptQueuedTurn(database, 'boundary-close-second');
    const fake = createBoundaryEndingQuery({ sessionId: 'claude-session-boundary-close' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-boundary-close',
      now: () => '2026-07-19T09:12:45Z',
      generateId: deterministicIds('boundary-close'),
    });

    await service.runNext();
    const secondRun = service.runNext();
    await service.close();
    await expect(secondRun).resolves.toMatchObject({
      status: 'failed',
      turn_id: second.turn_id,
    });

    database.close();
  });

  test('persists provider progress while another parallel permission remains pending', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-progress');
    const fake = createPermissionProgressQuery({ sessionId: 'claude-session-permission-progress' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-progress',
      now: () => '2026-07-19T09:13:15Z',
      generateId: deterministicIds('permission-progress'),
    });

    const firstWaiting = await service.runNext();
    const firstAnswer = service.submitInteractionAnswer(
      permissionAnswer(firstWaiting.request, 'progress-1'),
    );
    const firstHandled = await service.deliverInteractionAnswer(firstAnswer.handoff_id);
    expect(firstHandled.execution).toMatchObject({ status: 'waiting_user' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'waiting_user' });
    const secondAnswer = service.submitInteractionAnswer(
      permissionAnswer(firstHandled.execution.request, 'progress-2'),
    );
    const secondHandled = await service.deliverInteractionAnswer(secondAnswer.handoff_id);
    await fake.progressEmitted.promise;
    expect(readEvents(database, accepted.turn_id).find(({ kind }) => kind === 'tool_progress'))
      .toMatchObject({
      kind: 'tool_progress',
      phase: 'running',
    });
    expect(secondHandled.execution).toMatchObject({ status: 'completed' });

    await service.close();
    database.close();
  });

  test('isolates a stopped turn when the interrupt receipt is rejected', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'interrupt-rejection');
    const fake = createRejectingInterruptQuery({ sessionId: 'claude-session-interrupt-rejection' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-interrupt-rejection',
      now: () => '2026-07-19T09:13:30Z',
      generateId: deterministicIds('interrupt-rejection'),
    });

    const run = service.runNext();
    await fake.turnStarted.promise;
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'isolated',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);

    await service.close();
    database.close();
  });

  test('rolls back a synchronous SDK setup failure so the next turn can start', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'sync-setup-first');
    const secondEnvelope = normalEnvelope('sync-setup-second');
    secondEnvelope.chat_id = 'chat-sync-setup-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T09:13:00Z',
      generateId: deterministicIds('inbound-sync-setup-second'),
    });
    const fake = createSyncThrowThenSuccessQuery({ sessionId: 'claude-session-sync-setup' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-sync-setup',
      now: () => '2026-07-19T09:13:00Z',
      generateId: deterministicIds('sync-setup'),
      maxResidentExecutorsPerBot: 1,
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'failed',
      turn_id: first.turn_id,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(fake.calls).toBe(2);

    await service.close();
    database.close();
  });

  test('close waits for active turns to reach a durable terminal state', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'close-drain');
    const fake = createCloseAwareQuery({ sessionId: 'claude-session-close-drain' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-close-drain',
      now: () => '2026-07-19T09:14:00Z',
      generateId: deterministicIds('close-drain'),
    });

    const run = service.runNext();
    await fake.turnStarted.promise;
    await service.close();
    await expect(run).resolves.toMatchObject({ status: 'failed' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'failed' });

    const afterClose = acceptQueuedTurn(database, 'after-close');
    await expect(service.runNext()).rejects.toThrow(/closing|closed/);
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(afterClose.turn_id)).toEqual({ state: 'queued' });

    database.close();
  });

  test('close relinquishes durable ownership for a recovering permission turn', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'close-recovering-ownership');
    const fake = createPermissionQuery({
      sessionId: 'claude-session-close-recovering-ownership',
    });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-close-recovering-ownership',
      now: () => '2026-07-19T09:14:15Z',
      generateId: deterministicIds('close-recovering-ownership'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    await service.close();

    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null });
    expect(database.prepare(`
      SELECT owner_service_instance_id FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ owner_service_instance_id: null });

    database.close();
  });

  test('keeps durable resident ownership when query close is not confirmed', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'close-failure-resident');
    const fake = createCloseFailingQuery({ sessionId: 'claude-session-close-failure' });
    const adapter = createClaudeConversationAdapter({ query: fake.query });
    let heartbeatCancelled = false;
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-close-failure',
      now: () => '2026-07-19T09:14:30Z',
      generateId: deterministicIds('close-failure'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat() { heartbeatCancelled = true; },
    });

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    await expect(service.close()).rejects.toThrow(/failed to close/);
    expect(database.prepare(`
      SELECT conversation_id, owner_service_instance_id
      FROM runtime_executor_residents
    `).all()).toEqual([{
      conversation_id: accepted.conversation_id,
      owner_service_instance_id: 'executor-service-close-failure',
    }]);
    expect(adapter.hasResident(accepted.conversation_id)).toBe(true);
    expect(heartbeatCancelled).toBe(false);

    database.close();
  });

  test('retries resident deletion after provider close was already confirmed', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'close-resident-delete-retry');
    const fake = createFakeQuery({ sessionId: 'claude-session-close-resident-delete-retry' });
    let heartbeatCancelled = false;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-close-resident-delete-retry',
      now: () => '2026-07-19T09:14:45Z',
      generateId: deterministicIds('close-resident-delete-retry'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat() { heartbeatCancelled = true; },
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    database.exec(`
      CREATE TRIGGER fail_close_resident_delete
      BEFORE DELETE ON runtime_executor_residents
      BEGIN
        SELECT RAISE(ABORT, 'forced close resident delete failure');
      END;
    `);

    await expect(service.close()).rejects.toMatchObject({
      name: 'SqliteError',
      code: 'SQLITE_CONSTRAINT_TRIGGER',
      message: 'forced close resident delete failure',
    });
    expect(heartbeatCancelled).toBe(false);
    database.exec('DROP TRIGGER fail_close_resident_delete');
    await expect(service.close()).resolves.toBeUndefined();
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ count: 0 });
    expect(heartbeatCancelled).toBe(true);

    database.close();
  });

  test('evicts least-recently-used idle queries only up to the requested capacity', async () => {
    let clock = 0;
    let queryNo = 0;
    const adapter = createClaudeConversationAdapter({
      idleTimeoutMs: 100,
      now: () => clock,
      query({ prompt }) {
        queryNo += 1;
        const sessionId = `claude-session-lru-${queryNo}`;
        const stream = (async function* generateSdkMessages() {
          for await (const input of prompt) {
            yield { type: 'system', subtype: 'init', session_id: sessionId };
            yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'ok' };
            yield idleSession(sessionId);
          }
        }());
        stream.interrupt = async () => {};
        stream.close = () => {};
        return stream;
      },
    });
    const controls = { requestPermission: async () => ({ behavior: 'deny' }) };
    const consume = async (conversationId) => {
      const records = [];
      for await (const record of adapter.execute({
        conversation_id: conversationId,
        turn_id: `turn-${conversationId}`,
        lineage_id: `lineage-${conversationId}`,
        provider_native_id: null,
        trace_id: `trace-${conversationId}`,
        input: { text: conversationId },
        attempt: { attempt_id: `attempt-${conversationId}`, attempt_no: 1, lease_epoch: 1 },
      }, controls)) {
        records.push(record);
        if (record.type === 'provider_native_id') record.acknowledge();
      }
      return records;
    };

    await consume('conversation-lru-oldest');
    clock = 10;
    await consume('conversation-lru-newest');
    await new Promise((resolve) => setImmediate(resolve));
    clock = 1_000;
    await expect(adapter.evictIdle({
      canEvict: async () => true,
      maxCount: 1,
    })).resolves.toEqual(['conversation-lru-oldest']);
    expect(adapter.hasResident('conversation-lru-newest')).toBe(true);

    await adapter.close();
  });

  test('does not create a query after close wins a lineage-switch race', async () => {
    const closeStarted = deferred();
    const releaseClose = deferred();
    let queryCalls = 0;
    const adapter = createClaudeConversationAdapter({
      query({ prompt }) {
        queryCalls += 1;
        const sessionId = `claude-session-close-race-${queryCalls}`;
        const stream = (async function* generateSdkMessages() {
          for await (const input of prompt) {
            yield { type: 'system', subtype: 'init', session_id: sessionId };
            yield { type: 'result', subtype: 'success', session_id: sessionId, result: 'ok' };
            yield idleSession(sessionId);
          }
        }());
        stream.interrupt = async () => {};
        stream.close = async () => {
          closeStarted.resolve();
          await releaseClose.promise;
        };
        return stream;
      },
    });
    const controls = { requestPermission: async () => ({ behavior: 'deny' }) };
    const context = (turnId, lineageId) => ({
      conversation_id: 'conversation-close-race',
      turn_id: turnId,
      lineage_id: lineageId,
      provider_native_id: null,
      trace_id: `trace-${turnId}`,
      input: { text: turnId },
      attempt: { attempt_id: `attempt-${turnId}`, attempt_no: 1, lease_epoch: 1 },
    });
    const consume = async (turnId, lineageId) => {
      for await (const record of adapter.execute(context(turnId, lineageId), controls)) {
        if (record.type === 'provider_native_id') record.acknowledge();
      }
    };

    await consume('turn-close-race-1', 'lineage-close-race-1');
    await new Promise((resolve) => setImmediate(resolve));
    const switching = consume('turn-close-race-2', 'lineage-close-race-2');
    await closeStarted.promise;
    const closing = adapter.close();
    releaseClose.resolve();
    await closing;
    await expect(switching).rejects.toThrow(/closing|closed/);
    expect(queryCalls).toBe(1);
  });

  test('retains a closing tombstone when a lineage query cannot be closed', async () => {
    const fake = createCloseFailingQuery({ sessionId: 'claude-session-lineage-close-failure' });
    const adapter = createClaudeConversationAdapter({ query: fake.query });
    const controls = { requestPermission: async () => ({ behavior: 'deny' }) };
    const context = (turnId, lineageId) => ({
      conversation_id: 'conversation-lineage-close-failure',
      turn_id: turnId,
      lineage_id: lineageId,
      provider_native_id: null,
      trace_id: `trace-${turnId}`,
      input: { text: turnId },
      attempt: { attempt_id: `attempt-${turnId}`, attempt_no: 1, lease_epoch: 1 },
    });
    const consume = async (turnId, lineageId) => {
      for await (const record of adapter.execute(context(turnId, lineageId), controls)) {
        if (record.type === 'provider_native_id') record.acknowledge();
      }
    };

    await consume('turn-lineage-close-failure-1', 'lineage-close-failure-1');
    await new Promise((resolve) => setImmediate(resolve));
    await expect(consume(
      'turn-lineage-close-failure-2',
      'lineage-close-failure-2',
    )).rejects.toThrow(/forced query close failure/);
    expect(adapter.hasResident('conversation-lineage-close-failure')).toBe(true);
    await expect(consume(
      'turn-lineage-close-failure-3',
      'lineage-close-failure-3',
    )).rejects.toThrow(/close_failed/);
    expect(fake.calls).toBe(1);
  });
});
