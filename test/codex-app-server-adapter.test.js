import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

import { describe, expect, jest, test } from '@jest/globals';

import { createCodexAppServerAdapter } from '../runtime/providers/codex-app-server-adapter.js';

function createFakeAppServer({ afterTurnStart, autoTurnStarted = true, onClientResponse } = {}) {
  const child = new EventEmitter();
  child.pid = 4102;
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.kill = jest.fn();
  const received = [];
  let buffer = '';
  let turnNumber = 0;

  function send(message) {
    child.stdout.write(`${JSON.stringify(message)}\n`);
  }

  child.stdin.on('data', (chunk) => {
    buffer += chunk.toString('utf8');
    while (buffer.includes('\n')) {
      const newline = buffer.indexOf('\n');
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.length === 0) continue;
      const message = JSON.parse(line);
      received.push(message);
      if (message.method === 'initialize') {
        send({ id: message.id, result: { userAgent: 'codex-test' } });
      } else if (message.method === 'thread/start') {
        send({ id: message.id, result: { thread: { id: 'codex-thread-1' } } });
      } else if (message.method === 'thread/resume') {
        send({ id: message.id, result: { thread: { id: message.params.threadId } } });
      } else if (message.method === 'turn/start') {
        turnNumber += 1;
        const turnId = `codex-turn-${turnNumber}`;
        send({
          id: message.id,
          result: { turn: { id: turnId, status: 'inProgress', items: [] } },
        });
        queueMicrotask(() => {
          const details = { message, send, threadId: message.params.threadId, turnId };
          if (autoTurnStarted) {
            send({
              method: 'turn/started',
              params: {
                threadId: message.params.threadId,
                turn: { id: turnId, status: 'inProgress', items: [] },
              },
            });
          }
          if (afterTurnStart) afterTurnStart(details);
          else {
            send({
              method: 'turn/completed',
              params: {
                threadId: message.params.threadId,
                turn: { id: turnId, status: 'completed', items: [] },
              },
            });
          }
        });
      } else if (message.method === 'turn/interrupt') {
        send({ id: message.id, result: {} });
      } else if (Object.hasOwn(message, 'id') && Object.hasOwn(message, 'result')) {
        onClientResponse?.({ message, send });
      }
    }
  });

  return { child, received, send };
}

function executionContext(overrides = {}) {
  return {
    conversation_id: 'conversation-1',
    turn_id: 'turn-1',
    lineage_id: 'lineage-1',
    trace_id: 'trace-1',
    input: { kind: 'text', text: 'Hello Codex', attachments: [] },
    lineage: { provider_native_id: null },
    bindProviderNativeId: jest.fn(async () => {}),
    reportProviderState: jest.fn(),
    interaction: {
      authorized_subjects: [{ type: 'actor', actor_id: 'user-1' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    },
    attempt: { attempt_id: 'attempt-1', attempt_no: 1, lease_epoch: 3 },
    ...overrides,
  };
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}

async function waitFor(predicate) {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error('Timed out waiting for fake app-server traffic.');
}

function handoffDelivery(providerInteractionRef, value, {
  handoffId = 'handoff-1',
  handoffAttemptId = 'handoff-attempt-1',
} = {}) {
  return {
    request: {
      turn_id: 'turn-1',
      kind: 'question',
      runtime_fence: {
        provider_attempt_id: 'attempt-1',
        lease_epoch: 3,
        provider_interaction_ref: providerInteractionRef,
      },
    },
    answer: { value },
    handoff: {
      handoff_id: handoffId,
      provider_attempt_id: 'attempt-1',
      handoff_attempt_id: handoffAttemptId,
      handoff_attempt_no: 1,
      lease_epoch: 3,
    },
  };
}

describe('Codex app-server provider adapter', () => {
  test('initializes one stdio connection and binds a new thread before starting its turn', async () => {
    const server = createFakeAppServer();
    const spawnProcess = jest.fn(() => server.child);
    const context = executionContext();
    context.bindProviderNativeId = jest.fn(async (threadId) => {
      server.received.push({ method: 'core/thread-bound', params: { threadId } });
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess, cwd: '/workspace' });

    await expect(collect(adapter.execute(context))).resolves.toEqual([]);

    expect(spawnProcess).toHaveBeenCalledTimes(1);
    expect(spawnProcess).toHaveBeenCalledWith(
      'codex',
      ['app-server', '--stdio'],
      expect.objectContaining({ shell: false, stdio: ['pipe', 'pipe', 'pipe'] }),
    );
    expect(server.received[0]).toEqual(expect.objectContaining({
      method: 'initialize',
      params: expect.objectContaining({
        clientInfo: expect.objectContaining({ name: 'zylos-core' }),
        capabilities: expect.objectContaining({
          experimentalApi: true,
          mcpServerOpenaiFormElicitation: false,
        }),
      }),
    }));
    expect(server.child.stderr.readableFlowing).toBe(true);
    expect(server.received[1]).toEqual({ method: 'initialized' });
    expect(server.received[2]).toEqual(expect.objectContaining({
      method: 'thread/start',
      params: {
        cwd: '/workspace',
        approvalPolicy: 'on-request',
        sandbox: 'workspace-write',
      },
    }));
    expect(context.bindProviderNativeId).toHaveBeenCalledWith('codex-thread-1');
    expect(context.reportProviderState).toHaveBeenCalledWith({
      state: 'started',
      provider_native_id: 'codex-thread-1',
    });
    const turnStartIndex = server.received.findIndex(({ method }) => method === 'turn/start');
    expect(turnStartIndex).toBeGreaterThan(2);
    expect(server.received.findIndex(({ method }) => method === 'core/thread-bound'))
      .toBeLessThan(turnStartIndex);
    expect(server.received[turnStartIndex]).toEqual(expect.objectContaining({
      params: expect.objectContaining({
        threadId: 'codex-thread-1',
        input: [{ type: 'text', text: 'Hello Codex' }],
      }),
    }));
  });

  test('resumes and multiplexes persisted lineages over the same supervised process', async () => {
    const server = createFakeAppServer();
    const spawnProcess = jest.fn(() => server.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });

    await Promise.all([
      collect(adapter.execute(executionContext({
        turn_id: 'turn-A',
        lineage_id: 'lineage-A',
        lineage: { provider_native_id: 'codex-thread-A' },
        attempt: { attempt_id: 'attempt-A', attempt_no: 1, lease_epoch: 3 },
      }))),
      collect(adapter.execute(executionContext({
        conversation_id: 'conversation-B',
        turn_id: 'turn-B',
        lineage_id: 'lineage-B',
        lineage: { provider_native_id: 'codex-thread-B' },
        attempt: { attempt_id: 'attempt-B', attempt_no: 1, lease_epoch: 4 },
      }))),
    ]);

    expect(spawnProcess).toHaveBeenCalledTimes(1);
    expect(server.received.slice(0, 2)).toEqual([
      expect.objectContaining({ method: 'initialize' }),
      { method: 'initialized' },
    ]);
    expect(server.received.filter(({ method }) => method === 'thread/resume'))
      .toEqual(expect.arrayContaining([
        expect.objectContaining({ params: expect.objectContaining({ threadId: 'codex-thread-A' }) }),
        expect.objectContaining({ params: expect.objectContaining({ threadId: 'codex-thread-B' }) }),
      ]));
    expect(server.received.filter(({ method }) => method === 'turn/start')).toHaveLength(2);
  });

  test('normalizes text and tool notifications without exposing app-server method names', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'item/agentMessage/delta',
          params: { threadId, turnId, itemId: 'message-1', delta: 'Hello' },
        });
        send({
          method: 'item/started',
          params: {
            threadId,
            turnId,
            startedAtMs: 1,
            item: { type: 'commandExecution', id: 'command-1', command: 'private' },
          },
        });
        send({
          method: 'item/commandExecution/outputDelta',
          params: { threadId, turnId, itemId: 'command-1', delta: 'private output' },
        });
        send({
          method: 'item/completed',
          params: {
            threadId,
            turnId,
            completedAtMs: 2,
            item: { type: 'commandExecution', id: 'command-1', status: 'completed' },
          },
        });
        send({
          method: 'item/completed',
          params: {
            threadId,
            turnId,
            completedAtMs: 3,
            item: { type: 'agentMessage', id: 'message-1', text: 'Hello' },
          },
        });
        send({
          method: 'turn/completed',
          params: { threadId, turn: { id: turnId, status: 'completed', items: [] } },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(adapter.execute(executionContext()))).resolves.toEqual([
      {
        kind: 'text_delta',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Hello', start_offset: 0, end_offset: 5 },
      },
      {
        kind: 'tool_started',
        provider_native_id: 'codex-thread-1',
        payload: {
          tool_use_id: 'command-1',
          tool_name: 'command',
          summary: 'Command started.',
          side_effect_status: 'unknown',
        },
      },
      {
        kind: 'tool_progress',
        provider_native_id: 'codex-thread-1',
        payload: {
          tool_use_id: 'command-1',
          tool_name: 'command',
          summary: 'Command running.',
          side_effect_status: 'unknown',
        },
      },
      {
        kind: 'tool_finished',
        provider_native_id: 'codex-thread-1',
        payload: {
          tool_use_id: 'command-1',
          tool_name: 'command',
          summary: 'Command completed.',
          side_effect_status: 'unknown',
        },
      },
      {
        kind: 'text_snapshot',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Hello', end_offset: 5 },
      },
    ]);
    expect(JSON.stringify(server.received)).not.toContain('exec --json');
  });

  test('durably hands off requestUserInput and acknowledges only after provider resolution', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 71,
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-1',
            autoResolutionMs: null,
            questions: [{
              id: 'question-name',
              header: 'Name',
              question: 'What name should be used?',
              isOther: false,
              isSecret: false,
              options: null,
            }],
          },
        });
      },
      onClientResponse({ message, send }) {
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-1', requestId: message.id },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext())[Symbol.asyncIterator]();

    const interaction = await iterator.next();

    expect(interaction.done).toBe(false);
    expect(interaction.value).toEqual({
      kind: 'interaction_requested',
      payload: {
        provider_interaction_ref: expect.any(String),
        tool_use_id: 'tool-1',
        kind: 'question',
        prompt: 'What name should be used?',
        choices: [],
        authorized_subjects: [{ type: 'actor', actor_id: 'user-1' }],
        allowed_sources: ['main_card_reply', 'card_action'],
      },
    });
    expect(JSON.stringify(interaction.value)).not.toContain('requestUserInput');

    await expect(adapter.handleInteractionAnswer(handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'text', text: 'Alice' },
    ))).resolves.toEqual({
      handoff_id: 'handoff-1',
      status: 'accepted',
      provider_attempt_id: 'attempt-1',
      handoff_attempt_id: 'handoff-attempt-1',
      handoff_attempt_no: 1,
      lease_epoch: 3,
    });
    expect(server.received).toContainEqual({
      id: 71,
      result: { answers: { 'question-name': { answers: ['Alice'] } } },
    });
    await iterator.return();
  });

  test.each([
    {
      label: 'command approval',
      method: 'item/commandExecution/requestApproval',
      params: { itemId: 'command-1', startedAtMs: 1, reason: 'Network access is required.' },
      expected: { kind: 'tool_approval', prompt: 'Network access is required.' },
      answer: { kind: 'decision', decision: 'approve' },
      result: { decision: 'accept' },
    },
    {
      label: 'file approval',
      method: 'item/fileChange/requestApproval',
      params: { itemId: 'patch-1', startedAtMs: 1, reason: null },
      expected: {
        kind: 'tool_approval',
        prompt: 'Allow Codex to apply the requested file changes?',
      },
      answer: { kind: 'decision', decision: 'deny' },
      result: { decision: 'decline' },
    },
    {
      label: 'permission approval',
      method: 'item/permissions/requestApproval',
      params: {
        itemId: 'permission-1',
        startedAtMs: 1,
        reason: 'Read an external directory?',
        environmentId: null,
        cwd: '/workspace',
        permissions: { network: null, fileSystem: { read: ['/external'], write: [] } },
      },
      expected: { kind: 'permission_approval', prompt: 'Read an external directory?' },
      answer: { kind: 'decision', decision: 'approve' },
      result: {
        permissions: { fileSystem: { read: ['/external'], write: [] } },
        scope: 'turn',
      },
    },
    {
      label: 'MCP elicitation',
      method: 'mcpServer/elicitation/request',
      params: {
        turnId: null,
        serverName: 'provider-private',
        mode: 'form',
        message: 'Which environment should the tool use?',
        requestedSchema: {
          type: 'object',
          properties: {
            environment: {
              type: 'string',
              enum: ['staging', 'production'],
              enumNames: ['Staging', 'Production'],
            },
          },
          required: ['environment'],
        },
        _meta: null,
      },
      expected: {
        kind: 'choice',
        prompt: 'Which environment should the tool use?',
        choices: [
          { choice_id: 'staging', label: 'Staging' },
          { choice_id: 'production', label: 'Production' },
        ],
      },
      answer: { kind: 'choice', choice_id: 'staging' },
      result: { action: 'accept', content: { environment: 'staging' }, _meta: null },
    },
  ])('maps $label through a fenced provider-neutral interaction', async ({
    method,
    params,
    expected,
    answer,
    result,
  }) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'provider-request-1',
          method,
          params: { threadId, turnId, ...params },
        });
      },
      onClientResponse({ message, send }) {
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-1', requestId: message.id },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext())[Symbol.asyncIterator]();

    const interaction = await iterator.next();

    expect(interaction.value).toEqual({
      kind: 'interaction_requested',
      payload: expect.objectContaining({
        provider_interaction_ref: expect.any(String),
        ...expected,
      }),
    });
    await expect(adapter.handleInteractionAnswer(handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      answer,
    ))).resolves.toEqual(expect.objectContaining({
      handoff_id: 'handoff-1',
      status: answer.decision === 'deny' ? 'deny' : 'accepted',
    }));
    expect(server.received).toContainEqual({ id: 'provider-request-1', result });
    await iterator.return();
  });

  test('fails closed when one provider request contains multiple questions', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 81,
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-multi',
            autoResolutionMs: null,
            questions: [
              {
                id: 'target',
                header: 'Target',
                question: 'Choose a target.',
                isOther: false,
                isSecret: false,
                options: [{ label: 'A', description: 'Target A' }],
              },
              {
                id: 'note',
                header: 'Note',
                question: 'Add a note.',
                isOther: true,
                isSecret: false,
                options: null,
              },
            ],
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext())[Symbol.asyncIterator]();

    await expect(iterator.next()).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 81,
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
  });

  test.each([
    {
      label: 'multi-field typed form',
      params: {
        mode: 'form',
        message: 'Configure deployment.',
        requestedSchema: {
          type: 'object',
          properties: {
            environment: { type: 'string' },
            region: { type: 'string' },
          },
          required: ['environment', 'region'],
        },
        _meta: null,
      },
    },
    {
      label: 'OpenAI private form',
      params: {
        mode: 'openai/form',
        message: 'Configure deployment.',
        requestedSchema: {},
        _meta: null,
      },
    },
    {
      label: 'URL elicitation',
      params: {
        mode: 'url',
        message: 'Complete authorization.',
        url: 'https://example.invalid/authorize?opaque=1',
        elicitationId: 'elicitation-1',
        _meta: null,
      },
    },
    {
      label: 'formatted typed string',
      params: {
        mode: 'form',
        message: 'Provide an email address.',
        requestedSchema: {
          type: 'object',
          properties: {
            email: { type: 'string', format: 'email', minLength: 1 },
          },
          required: ['email'],
        },
        _meta: null,
      },
    },
  ])('fails closed for an unsupported MCP $label', async ({ params }) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'unsupported-mcp',
          method: 'mcpServer/elicitation/request',
          params: { threadId, turnId, serverName: 'private-server', ...params },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(adapter.execute(executionContext())[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(server.received).toContainEqual({
      id: 'unsupported-mcp',
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
  });

  test('rejects an early provider resolved notification before any answer is sent', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'early-resolution',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-early', startedAtMs: 1 },
        });
        send({
          method: 'serverRequest/resolved',
          params: { threadId, requestId: 'early-resolution' },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(adapter.execute(executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'side_effect_unknown' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('rejects reuse of a completed server request ID on the same connection', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'reused-request',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-first', startedAtMs: 1 },
        });
      },
      onClientResponse({ message, send }) {
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-1', requestId: message.id },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    const interaction = await iterator.next();
    await adapter.handleInteractionAnswer(handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ));
    const next = iterator.next();

    server.send({
      id: 'reused-request',
      method: 'item/fileChange/requestApproval',
      params: {
        threadId: 'codex-thread-1',
        turnId: 'codex-turn-1',
        itemId: 'patch-second',
        startedAtMs: 2,
      },
    });

    await expect(next).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('fails an active turn closed when transport becomes uncertain and reloads on reconnect', async () => {
    const firstServer = createFakeAppServer({ afterTurnStart() {} });
    const secondServer = createFakeAppServer();
    const spawnProcess = jest.fn()
      .mockImplementationOnce(() => firstServer.child)
      .mockImplementationOnce(() => secondServer.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });
    const persistedContext = executionContext({
      lineage: { provider_native_id: 'codex-thread-persisted' },
    });
    const firstExecution = collect(adapter.execute(persistedContext));
    await waitFor(() => firstServer.received.some(({ method }) => method === 'turn/start'));

    firstServer.child.emit('close', 1, null);

    await expect(firstExecution).rejects.toMatchObject({
      providerError: {
        code: 'side_effect_unknown',
        side_effect_status: 'unknown',
      },
    });
    firstServer.send({
      method: 'item/agentMessage/delta',
      params: {
        threadId: 'codex-thread-persisted',
        turnId: 'codex-turn-1',
        itemId: 'late-message',
        delta: 'late',
      },
    });

    await expect(collect(adapter.execute(executionContext({
      turn_id: 'turn-2',
      lineage: { provider_native_id: 'codex-thread-persisted' },
      attempt: { attempt_id: 'attempt-2', attempt_no: 1, lease_epoch: 4 },
    })))).resolves.toEqual([]);

    expect(spawnProcess).toHaveBeenCalledTimes(2);
    expect(secondServer.received).toContainEqual(expect.objectContaining({
      method: 'thread/resume',
      params: expect.objectContaining({ threadId: 'codex-thread-persisted' }),
    }));
  });

  test.each(['stop', 'steer'])(
    'interrupts only the current %s fence through app-server without killing the shared process',
    async (reason) => {
      const server = createFakeAppServer({ afterTurnStart() {} });
      const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
      const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
      const iterator = adapter.execute(context)[Symbol.asyncIterator]();
      const waiting = iterator.next();
      await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

      await expect(adapter.interrupt({
        turn_id: context.turn_id,
        attempt: context.attempt,
        reason,
      })).resolves.toEqual({ status: 'interrupt_requested', reason });
      expect(server.received).toContainEqual(expect.objectContaining({
        method: 'turn/interrupt',
        params: { threadId: 'codex-thread-1', turnId: 'codex-turn-1' },
      }));
      expect(server.child.kill).not.toHaveBeenCalled();

      server.send({
        method: 'turn/completed',
        params: {
          threadId: 'codex-thread-1',
          turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
        },
      });
      await expect(waiting).rejects.toMatchObject({
        providerError: { code: 'side_effect_unknown' },
      });
    },
  );

  test('waits for the matching provider completion before confirming a timeout interrupt', async () => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = adapter.execute(context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));
    let settled = false;
    const interruption = adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'timeout',
    }).then((result) => {
      settled = true;
      return result;
    });
    await waitFor(() => server.received.some(({ method }) => method === 'turn/interrupt'));
    expect(settled).toBe(false);
    const waitingFailure = expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });

    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    });
    await expect(interruption).resolves.toEqual({
      status: 'provider_stopped',
      reason: 'timeout',
      provider_status: 'interrupted',
    });
    await waitingFailure;
  });

  test('bounds timeout stop confirmation when app-server never emits a terminal notification', async () => {
    let confirmTimeout;
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      interruptConfirmationTimeoutMs: 250,
      setTimeoutFn(callback, delay) {
        expect(delay).toBe(250);
        confirmTimeout = callback;
        return { unref() {} };
      },
      clearTimeoutFn: jest.fn(),
    });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const iterator = adapter.execute(context)[Symbol.asyncIterator]();
    const waiting = iterator.next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    const interruption = adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'timeout',
    });
    await waitFor(() => typeof confirmTimeout === 'function');
    confirmTimeout();
    await expect(interruption).resolves.toEqual({
      status: 'uncertain',
      reason: 'timeout',
    });

    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    });
    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
  });

  test('uses a fenced terminal tombstone when completion wins the timeout interrupt race', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'timeout-race-request',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-timeout-race', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const iterator = adapter.execute(context)[Symbol.asyncIterator]();
    await iterator.next();

    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    });

    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'timeout',
    })).resolves.toEqual({
      status: 'provider_stopped',
      reason: 'timeout',
      provider_status: 'interrupted',
    });
    expect(server.received.filter(({ method }) => method === 'turn/interrupt')).toHaveLength(0);
    await iterator.return();
  });

  test('does not interrupt until the provider confirms turn/started', async () => {
    const server = createFakeAppServer({ autoTurnStarted: false, afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = adapter.execute(context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'stop',
    })).resolves.toEqual({ status: 'not_current', reason: 'stop' });

    server.send({
      method: 'turn/started',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'inProgress', items: [] },
      },
    });
    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'stop',
    })).resolves.toEqual({ status: 'interrupt_requested', reason: 'stop' });
    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    });
    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
  });

  test('rejects stale interaction answers before writing to the provider connection', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 91,
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-1', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext())[Symbol.asyncIterator]();
    const interaction = await iterator.next();
    const delivery = handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    );
    delivery.handoff.lease_epoch = 4;
    const before = server.received.length;

    await expect(adapter.handleInteractionAnswer(delivery)).rejects.toThrow(/runtime fence/);
    expect(server.received).toHaveLength(before);
    await iterator.return();
  });

  test('reports connection loss with an outstanding interaction and clears stale answer state', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'lost-interaction',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-lost', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = adapter.execute(executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    const interaction = await iterator.next();

    server.child.emit('close', 1, null);

    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    await expect(adapter.handleInteractionAnswer(handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ))).rejects.toThrow(/current provider request/);
  });

  test('turns stdin stream errors into a fenced connection failure', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const waiting = adapter.execute(executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    server.child.stdin.emit('error', new Error('EPIPE'));

    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('closes only its supervised app-server child during service shutdown', async () => {
    const server = createFakeAppServer();
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    await collect(adapter.execute(executionContext()));

    await expect(adapter.close()).resolves.toEqual({ status: 'signalled' });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });
});
