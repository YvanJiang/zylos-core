import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createClaudeConversationAdapter } from '../runtime/providers/claude/conversation-adapter.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';

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
    let drainFinished;
    const drainDone = new Promise((resolve) => { drainFinished = resolve; });
    const takeInput = () => {
      if (drainedInputs.length > 0) return Promise.resolve(drainedInputs.shift());
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
      await takeInput();
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
      await takeInput();
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
    };
    stream.close = () => {};
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
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }

  return { permissionRequested, permissionResults, query };
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
        yield idleSession(sessionId);
        await backgroundFinished.promise;
        yield {
          type: 'system',
          subtype: 'background_tasks_changed',
          session_id: sessionId,
          tasks: [],
        };
        notificationConsumed.resolve();
      }
    }());
    stream.interrupt = async () => {};
    stream.close = () => {};
    return stream;
  }

  return { backgroundFinished, notificationConsumed, query };
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
    stream.interrupt = async () => {};
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

function createQueuedReceiptQuery({ sessionId }) {
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
      return false;
    };
    stream.close = () => cancelled.resolve();
    return stream;
  }
  return { cancelledMessageUuids, inputConsumed, query };
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

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('Claude conversation executor', () => {
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
    const adapter = createClaudeConversationAdapter({ query: fake.query });
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
    expect(fake.inputs).toEqual(['turn first', 'turn second']);
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

  test('cancels only the current fenced turn and keeps the resident query resumable', async () => {
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
    await expect(service.cancel(first.conversation_id)).resolves.toMatchObject({
      status: 'cancellation_requested',
      turn_id: first.turn_id,
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

    const secondRun = service.runNext();
    await Promise.resolve();
    expect(fake.inputs).toEqual(['turn cancel-first']);
    fake.lateOutputRelease.resolve();
    await expect(secondRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(fake.queryCalls).toBe(1);
    expect(fake.inputs).toEqual(['turn cancel-first', 'turn cancel-second']);
    expect(readEvents(database, second.turn_id).map((event) => event.payload.text).filter(Boolean))
      .toEqual(['second turn answer']);

    await service.close();
    database.close();
  });

  test('routes permission callbacks through the durable provider-neutral turn fence', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission');
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission' });
    const permissionDecision = deferred();
    const permissionRequests = [];
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission',
      now: () => '2026-07-19T09:03:00Z',
      generateId: deterministicIds('permission'),
      async permissionHandler(request, context) {
        permissionRequests.push({ context, request });
        return permissionDecision.promise;
      },
    });

    const run = service.runNext();
    await fake.permissionRequested.promise;
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id).state).toBe('waiting_user');
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    permissionDecision.resolve({
      behavior: 'allow',
      updated_input: { command: 'pwd' },
    });

    await expect(run).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(permissionRequests).toEqual([{
      request: {
        tool_name: 'Bash',
        input: { command: 'pwd' },
      },
      context: expect.objectContaining({
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
        attempt: expect.objectContaining({ attempt_no: 1 }),
      }),
    }]);
    expect(fake.permissionResults).toEqual([{
      behavior: 'allow',
      updatedInput: { command: 'pwd' },
    }]);
    expect(readEvents(database, accepted.turn_id).map((event) => event.phase))
      .toEqual(expect.arrayContaining(['waiting_user', 'running', 'completed']));

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
    const fake = createPermissionQuery({ sessionId: 'claude-session-permission-projection' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-projection',
      now: () => '2026-07-19T09:03:30Z',
      generateId: deterministicIds('permission-projection'),
      permissionHandler: async () => ({ behavior: 'allow' }),
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
    const neverSettles = deferred();
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-cancel',
      now: () => '2026-07-19T09:03:30Z',
      generateId: deterministicIds('permission-cancel'),
      permissionHandler: () => neverSettles.promise,
    });

    const run = service.runNext();
    await fake.permissionRequested.promise;
    await expect(service.cancel(accepted.conversation_id)).resolves.toMatchObject({
      status: 'cancellation_requested',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });

    await service.close();
    database.close();
  });

  test('terminalizes an SDK-caught permission cancellation from the durable waiting state', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-cancel-caught');
    const fake = createCaughtPermissionCancellationQuery({
      sessionId: 'claude-session-permission-cancel-caught',
    });
    const neverSettles = deferred();
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-cancel-caught',
      now: () => '2026-07-19T09:03:45Z',
      generateId: deterministicIds('permission-cancel-caught'),
      permissionHandler: () => neverSettles.promise,
    });

    const run = service.runNext();
    await fake.permissionRequested.promise;
    await expect(service.cancel(accepted.conversation_id)).resolves.toMatchObject({
      status: 'cancellation_requested',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'stopped' });

    await service.close();
    database.close();
  });

  test('keeps waiting_user until all parallel permission callbacks settle', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'permission-parallel');
    const fake = createParallelPermissionQuery({ sessionId: 'claude-session-permission-parallel' });
    const decisions = [deferred(), deferred()];
    const bothRequested = deferred();
    let requestCount = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-parallel',
      now: () => '2026-07-19T09:03:45Z',
      generateId: deterministicIds('permission-parallel'),
      permissionHandler() {
        const decision = decisions[requestCount];
        requestCount += 1;
        if (requestCount === 2) bothRequested.resolve();
        return decision.promise;
      },
    });

    const run = service.runNext();
    await bothRequested.promise;
    decisions[0].resolve({ behavior: 'allow' });
    await Promise.resolve();
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'waiting_user' });
    decisions[1].resolve({ behavior: 'allow' });
    await expect(run).resolves.toMatchObject({ status: 'completed' });

    await service.close();
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

    await expect(service.runNext()).resolves.toMatchObject({ turn_id: accepted.turn_id });
    clock = 1_000;
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);
    fake.backgroundFinished.resolve();
    await fake.notificationConsumed.promise;
    await expect(service.evictIdleExecutors()).resolves.toEqual([accepted.conversation_id]);

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
    await expect(service.cancel(accepted.conversation_id)).resolves.toMatchObject({
      status: 'cancellation_requested',
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
    await expect(service.cancel(accepted.conversation_id)).resolves.toMatchObject({
      status: 'cancellation_requested',
    });
    await expect(run).resolves.toMatchObject({ status: 'stopped' });
    expect(fake.cancelledMessageUuids).toEqual([
      '00000000-0000-4000-8000-000000000009',
    ]);

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
    const decisions = [deferred(), deferred()];
    let decisionIndex = 0;
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-progress',
      now: () => '2026-07-19T09:13:15Z',
      generateId: deterministicIds('permission-progress'),
      permissionHandler: () => decisions[decisionIndex++].promise,
    });

    const run = service.runNext();
    decisions[0].resolve({ behavior: 'allow' });
    await fake.progressEmitted.promise;
    await new Promise((resolve) => setImmediate(resolve));
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'waiting_user' });
    expect(readEvents(database, accepted.turn_id).find(({ kind }) => kind === 'tool_progress'))
      .toMatchObject({
      kind: 'tool_progress',
      phase: 'running',
    });
    decisions[1].resolve({ behavior: 'allow' });
    await expect(run).resolves.toMatchObject({ status: 'completed' });

    await service.close();
    database.close();
  });

  test('moves an interrupt-rejection turn to recovering instead of reporting cancellation', async () => {
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
    await expect(service.cancel(accepted.conversation_id)).rejects.toThrow(
      /interrupt receipt unavailable/,
    );
    await expect(run).resolves.toMatchObject({ status: 'recovering' });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'recovering' });
    await expect(service.evictIdleExecutors()).resolves.toEqual([]);

    await service.close();
    database.close();
  });

  test('rolls back a synchronous SDK setup failure so the next turn can start', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'sync-setup-first');
    const second = acceptQueuedTurn(database, 'sync-setup-second');
    const fake = createSyncThrowThenSuccessQuery({ sessionId: 'claude-session-sync-setup' });
    const service = createExecutorService({
      database,
      adapter: createClaudeConversationAdapter({ query: fake.query }),
      provider: 'claude',
      serviceInstanceId: 'executor-service-sync-setup',
      now: () => '2026-07-19T09:13:00Z',
      generateId: deterministicIds('sync-setup'),
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
});
