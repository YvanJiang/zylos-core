import { EventEmitter } from 'node:events';
import { spawn as spawnSubprocess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { PassThrough } from 'node:stream';

import { describe, expect, jest, test } from '@jest/globals';

import { createCodexAppServerAdapter } from '../runtime/providers/codex-app-server-adapter.js';

function createFakeAppServer({
  afterThreadResume,
  afterTurnStart,
  autoTurnStarted = true,
  interruptResult = {},
  onClientResponse,
  respondToInterrupt = true,
  respondToTurnStart = true,
  resumeTurns = [],
  threadResumeError = null,
  turnStartResponsePatch = {},
} = {}) {
  const child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.kill = jest.fn((signal) => {
    if (signal === 'SIGTERM') queueMicrotask(() => child.emit('close', 0, signal));
    return true;
  });
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
        const threadId = message.params.threadId;
        if (threadResumeError !== null) {
          send({ id: message.id, error: threadResumeError });
          continue;
        }
        send({
          id: message.id,
          result: { thread: { id: threadId, turns: resumeTurns } },
        });
        afterThreadResume?.({ send, threadId });
      } else if (message.method === 'turn/start') {
        if (!respondToTurnStart) continue;
        turnNumber += 1;
        const turnId = `codex-turn-${turnNumber}`;
        send({
          id: message.id,
          result: { turn: { id: turnId, status: 'inProgress', items: [] } },
          ...turnStartResponsePatch,
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
        if (respondToInterrupt) send({ id: message.id, result: interruptResult });
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
    reportRuntimeEvidence: jest.fn(),
    interaction: {
      authorized_subjects: [{ type: 'actor', actor_id: 'user-1' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    },
    executor_instance_id: 'executor-1',
    workspace: {
      workspace_lease_id: 'workspace-lease-1',
      workspace_root: '/workspace',
      mode: 'writable',
      holder_service_instance_id: 'service-1',
      holder_conversation_id: 'conversation-1',
      holder_turn_id: 'turn-1',
      lease_epoch: 11,
      lease_expires_at: '2026-07-19T05:10:00Z',
    },
    attempt: { attempt_id: 'attempt-1', attempt_no: 1, lease_epoch: 3 },
    ...overrides,
  };
}

function workspaceControls(overrides = {}) {
  return {
    assertWorkspaceWrite: jest.fn(() => ({ status: 'current' })),
    ...overrides,
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function executeAdapter(adapter, context, controls = workspaceControls()) {
  return adapter.execute(context, controls);
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

async function nextInteraction(iterator) {
  while (true) {
    const next = await iterator.next();
    if (next.done || next.value?.kind === 'interaction_requested') return next;
  }
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

async function deliverPreparedInteractionAnswer(adapter, delivery) {
  const prepared = await adapter.prepareInteractionAnswer(delivery);
  delivery.handoff.last_send_started_at ??= '2026-07-19T05:04:03Z';
  return prepared.send(delivery);
}

function sendStartedFileChange({ send, threadId, turnId }, itemId) {
  send({
    method: 'item/started',
    params: {
      threadId,
      turnId,
      startedAtMs: 1,
      item: {
        type: 'fileChange',
        id: itemId,
        status: 'inProgress',
        changes: [{
          path: `/workspace/${itemId}.txt`,
          kind: { type: 'update', move_path: null },
          diff: '+provider requested change',
        }],
      },
    },
  });
}

function sendStartedCommand({ send, threadId, turnId }, itemId, {
  command = 'touch approved.txt',
  cwd = '/workspace',
} = {}) {
  send({
    method: 'item/started',
    params: {
      threadId,
      turnId,
      startedAtMs: 1,
      item: {
        type: 'commandExecution',
        id: itemId,
        command,
        cwd,
        status: 'inProgress',
      },
    },
  });
}

describe('Codex app-server provider adapter', () => {
  test('classifies the target app-server missing-rollout response as context invalid', async () => {
    const server = createFakeAppServer({
      threadResumeError: {
        code: -32600,
        message: 'no rollout found for thread id codex-thread-missing',
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({
      lineage: { provider_native_id: 'codex-thread-missing' },
    })))).rejects.toMatchObject({
      code: 'provider_context_invalid',
      providerError: {
        code: 'provider_context_invalid',
        category: 'provider',
        retryable: false,
        side_effect_status: 'none',
      },
    });
  });

  test('keeps a transient resume rejection retryable instead of treating it as lost context', async () => {
    const server = createFakeAppServer({
      threadResumeError: {
        code: -32603,
        message: 'provider unavailable with HTTP status 503',
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({
      lineage: { provider_native_id: 'codex-thread-existing' },
    })))).rejects.toMatchObject({
      code: 'delivery_transient',
      providerError: {
        code: 'delivery_transient',
        category: 'provider',
        retryable: true,
        side_effect_status: 'none',
      },
    });
  });

  test('waits through retry progress and classifies the target app-server final 401 as auth', async () => {
    let completeTurn;
    let finalErrorSent = false;
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'error',
          params: {
            error: {
              message: 'Reconnecting... 2/5',
              codexErrorInfo: {
                responseStreamDisconnected: { httpStatusCode: 401 },
              },
              additionalDetails: 'redacted target app-server authentication failure',
            },
            willRetry: true,
            threadId,
            turnId,
          },
        });
        send({
          method: 'error',
          params: {
            error: {
              message: 'unexpected status 401 Unauthorized',
              codexErrorInfo: 'other',
              additionalDetails: null,
            },
            willRetry: false,
            threadId,
            turnId,
          },
        });
        finalErrorSent = true;
        completeTurn = () => {
          send({
            method: 'turn/completed',
            params: {
              threadId,
              turn: {
                id: turnId,
                status: 'failed',
                items: [],
                error: {
                  message: 'unexpected status 401 Unauthorized',
                  codexErrorInfo: 'other',
                  additionalDetails: null,
                },
              },
            },
          });
        };
      },
    });
    const reportProviderFailure = jest.fn();
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const running = collect(executeAdapter(adapter, executionContext({ reportProviderFailure })));
    running.catch(() => {});
    let settled = false;
    running.finally(() => { settled = true; }).catch(() => {});
    await waitFor(() => finalErrorSent);
    await new Promise((resolve) => setImmediate(resolve));

    expect(settled).toBe(false);
    expect(reportProviderFailure).not.toHaveBeenCalled();
    completeTurn();
    await expect(running).rejects.toMatchObject({
        code: 'provider_auth_failed',
        providerError: {
          code: 'provider_auth_failed',
          category: 'authentication',
          retryable: false,
          side_effect_status: 'none',
        },
      });
    expect(reportProviderFailure).not.toHaveBeenCalled();
    expect(server.child.kill).not.toHaveBeenCalled();
  });

  test('does not treat an error notification without turn/completed as timeout stop proof', async () => {
    let finalErrorSent = false;
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'error',
          params: {
            error: { message: 'service unavailable HTTP status 503' },
            willRetry: false,
            threadId,
            turnId,
          },
        });
        finalErrorSent = true;
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      interruptConfirmationTimeoutMs: 5,
    });
    const context = executionContext();
    const running = collect(executeAdapter(adapter, context));
    running.catch(() => {});
    await waitFor(() => finalErrorSent);

    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'timeout',
    })).resolves.toEqual({ status: 'uncertain', reason: 'timeout' });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
    await expect(running).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
  });

  test('uses the canonical terminal error after an earlier retry-progress classification', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'error',
          params: {
            error: { message: 'provider unavailable with HTTP status 503' },
            willRetry: true,
            threadId,
            turnId,
          },
        });
        send({
          method: 'turn/completed',
          params: {
            threadId,
            turn: {
              id: turnId,
              status: 'failed',
              items: [],
              error: { message: 'unexpected status 401 Unauthorized' },
            },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext())))
      .rejects.toMatchObject({
        code: 'provider_auth_failed',
        providerError: {
          code: 'provider_auth_failed',
          category: 'authentication',
          retryable: false,
          side_effect_status: 'none',
        },
      });
  });

  test('does not treat an error notification without turn/completed as abort proof', async () => {
    let finalErrorSent = false;
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'error',
          params: {
            error: { message: 'service unavailable HTTP status 503' },
            willRetry: false,
            threadId,
            turnId,
          },
        });
        finalErrorSent = true;
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      interruptConfirmationTimeoutMs: 5,
    });
    const context = executionContext();
    const running = collect(executeAdapter(adapter, context));
    running.catch(() => {});
    await waitFor(() => finalErrorSent);

    await expect(adapter.abort(context)).resolves.toEqual({
      status: 'provider_stopped',
      provider_status: 'process_exited',
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
    await expect(running).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
  });

  test('advertises the workspace access enforced by its configured sandbox', () => {
    const spawnProcess = jest.fn();

    expect(createCodexAppServerAdapter({
      spawnProcess,
      cwd: '/workspace',
      sandbox: 'workspace-write',
    }).getWorkspaceAccess()).toEqual({
      root: '/workspace',
      mode: 'writable',
      read_only_enforced: false,
      authority: 'core_workspace_lease',
    });

    expect(createCodexAppServerAdapter({
      spawnProcess,
      cwd: '/workspace/review',
      sandbox: 'read-only',
    }).getWorkspaceAccess()).toEqual({
      root: '/workspace/review',
      mode: 'read_only',
      read_only_enforced: true,
      authority: 'provider_sandbox',
    });
  });

  test('rejects full access and any writable turn that would bypass synchronous approval', () => {
    const spawnProcess = jest.fn();

    expect(() => createCodexAppServerAdapter({
      spawnProcess,
      cwd: '/workspace',
      sandbox: 'danger-full-access',
      approvalPolicy: 'never',
    })).toThrow(/danger-full-access/);
    expect(() => createCodexAppServerAdapter({
      spawnProcess,
      cwd: '/workspace',
      sandbox: 'workspace-write',
      approvalPolicy: 'never',
    })).toThrow(/on-request/);
    expect(spawnProcess).not.toHaveBeenCalled();
  });

  test('fails closed before starting a writable turn without a current workspace fence', async () => {
    const spawnProcess = jest.fn();
    const adapter = createCodexAppServerAdapter({ spawnProcess, cwd: '/workspace' });

    await expect(collect(adapter.execute(executionContext()))).rejects.toMatchObject({
      code: 'unsupported_capability',
    });
    expect(spawnProcess).not.toHaveBeenCalled();
  });

  test('checks the current workspace fence before contacting app-server for a writable turn', async () => {
    const spawnProcess = jest.fn();
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const controls = workspaceControls({
      assertWorkspaceWrite: jest.fn(() => {
        throw staleLease;
      }),
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess, cwd: '/workspace' });

    await expect(collect(executeAdapter(adapter, executionContext(), controls)))
      .rejects.toBe(staleLease);
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(1);
    expect(spawnProcess).not.toHaveBeenCalled();
  });

  test('rechecks the workspace fence after thread binding and immediately before turn/start', async () => {
    const server = createFakeAppServer();
    const binding = deferred();
    let current = true;
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const controls = workspaceControls({
      assertWorkspaceWrite: jest.fn(() => {
        if (!current) throw staleLease;
        return { status: 'current' };
      }),
    });
    const context = executionContext({
      bindProviderNativeId: jest.fn(() => binding.promise),
      reportProviderFailure: jest.fn(),
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const running = collect(executeAdapter(adapter, context, controls));
    await waitFor(() => context.bindProviderNativeId.mock.calls.length === 1);
    current = false;
    binding.resolve();

    await expect(running).rejects.toMatchObject({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: expect.objectContaining({ side_effect_status: 'unknown' }),
    });
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(3);
    expect(context.reportProviderFailure).toHaveBeenCalledWith(expect.objectContaining({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: expect.objectContaining({ side_effect_status: 'unknown' }),
    }));
    expect(server.received.filter(({ method }) => method === 'turn/start')).toHaveLength(0);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('rechecks the workspace fence when app-server confirms turn/started', async () => {
    const server = createFakeAppServer({ autoTurnStarted: false, afterTurnStart() {} });
    let current = true;
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const controls = workspaceControls({
      assertWorkspaceWrite: jest.fn(() => {
        if (!current) throw staleLease;
        return { status: 'current' };
      }),
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const waiting = executeAdapter(
      adapter,
      executionContext(),
      controls,
    )[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));
    current = false;
    server.send({
      method: 'turn/started',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'inProgress', items: [] },
      },
    });
    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'completed', items: [] },
      },
    });

    await expect(waiting).rejects.toMatchObject({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: { side_effect_status: 'unknown' },
    });
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(4);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('initializes one stdio connection and binds a new thread before starting its turn', async () => {
    const server = createFakeAppServer();
    const spawnProcess = jest.fn(() => server.child);
    const context = executionContext();
    context.bindProviderNativeId = jest.fn(async (threadId) => {
      server.received.push({ method: 'core/thread-bound', params: { threadId } });
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess,
      cwd: '/workspace',
      now: () => '2026-07-19T05:00:00Z',
    });

    await expect(collect(executeAdapter(adapter, context))).resolves.toEqual([]);

    expect(spawnProcess).toHaveBeenCalledTimes(1);
    expect(spawnProcess).toHaveBeenCalledWith(
      'codex',
      ['app-server', '--stdio'],
      expect.objectContaining({
        detached: process.platform !== 'win32',
        shell: false,
        stdio: ['pipe', 'pipe', 'pipe'],
      }),
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
      params: expect.objectContaining({
        cwd: '/workspace',
        approvalPolicy: 'on-request',
        approvalsReviewer: 'user',
        sandbox: 'read-only',
        environments: [],
        dynamicTools: [],
        config: expect.objectContaining({
          mcp_servers: {},
          web_search: 'disabled',
          features: expect.objectContaining({
            apps: false,
            hooks: false,
            multi_agent: false,
            multi_agent_v2: false,
            enable_fanout: false,
            collaboration_modes: false,
            multi_agent_mode: false,
            code_mode: false,
            image_generation: false,
            request_permissions: false,
          }),
        }),
      }),
    }));
    expect(context.bindProviderNativeId).toHaveBeenCalledWith('codex-thread-1');
    expect(context.reportProviderState).toHaveBeenCalledWith({
      state: 'started',
      provider_native_id: 'codex-thread-1',
    });
    expect(context.reportRuntimeEvidence).toHaveBeenCalledWith({
      runtime_instance_id: 'codex-app-server-1',
      handle_kind: 'codex_app_server_connection',
      controllable: true,
    });
    const turnStartIndex = server.received.findIndex(({ method }) => method === 'turn/start');
    expect(turnStartIndex).toBeGreaterThan(2);
    expect(server.received.findIndex(({ method }) => method === 'core/thread-bound'))
      .toBeLessThan(turnStartIndex);
    expect(server.received[turnStartIndex]).toEqual(expect.objectContaining({
      params: expect.objectContaining({
        threadId: 'codex-thread-1',
        input: [{ type: 'text', text: 'Hello Codex' }],
        cwd: '/workspace',
        approvalPolicy: 'on-request',
        approvalsReviewer: 'user',
        sandboxPolicy: { type: 'readOnly', networkAccess: false },
        environments: [],
      }),
    }));
  });

  test('resumes and multiplexes persisted lineages over the same supervised process', async () => {
    const server = createFakeAppServer();
    const spawnProcess = jest.fn(() => server.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });

    await Promise.all([
      collect(executeAdapter(adapter, executionContext({
        turn_id: 'turn-A',
        lineage_id: 'lineage-A',
        lineage: { provider_native_id: 'codex-thread-A' },
        attempt: { attempt_id: 'attempt-A', attempt_no: 1, lease_epoch: 3 },
      }))),
      collect(executeAdapter(adapter, executionContext({
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
    for (const resume of server.received.filter(({ method }) => method === 'thread/resume')) {
      expect(resume.params).toEqual(expect.objectContaining({
        approvalPolicy: 'on-request',
        approvalsReviewer: 'user',
        sandbox: 'read-only',
        config: expect.objectContaining({
          mcp_servers: {},
          web_search: 'disabled',
        }),
      }));
    }
    expect(server.received.filter(({ method }) => method === 'turn/start')).toHaveLength(2);
  });

  test('recovers one persisted lineage through app-server thread/resume without starting work', async () => {
    const server = createFakeAppServer();
    const spawnProcess = jest.fn(() => server.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });
    const request = {
      recovery_id: 'mapping-recovery-codex-A',
      turn_id: 'turn-recovery-codex-A',
      reason: 'mapping_missing',
      native_recovery_attempt_id: 'native-recovery-attempt-codex-A',
      native_recovery_attempt_no: 1,
      candidate: {
        lineage_id: 'lineage-codex-A',
        provider: 'codex',
        provider_native_id: 'codex-thread-recovery-A',
      },
    };

    await expect(adapter.recoverLineage(request)).resolves.toEqual({
      status: 'recovered',
      recovery_id: request.recovery_id,
      lineage_id: request.candidate.lineage_id,
      provider: 'codex',
      provider_native_id: request.candidate.provider_native_id,
      native_recovery_attempt_id: request.native_recovery_attempt_id,
      native_recovery_attempt_no: request.native_recovery_attempt_no,
      side_effect_status: 'none',
    });
    expect(server.received.filter(({ method }) => method === 'thread/resume'))
      .toEqual([expect.objectContaining({
        params: expect.objectContaining({
          threadId: request.candidate.provider_native_id,
        }),
      })]);
    expect(server.received.filter(({ method }) => method === 'turn/start')).toHaveLength(0);
    await expect(adapter.recoverLineage(request)).resolves.toMatchObject({ status: 'recovered' });
    expect(server.received.filter(({ method }) => method === 'thread/resume')).toHaveLength(1);
  });

  test('ignores tombstoned late traffic without changing another run on the current connection', async () => {
    const reportRunBFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart(details) {
        if (details.turnId !== 'codex-turn-1') return;
        sendStartedFileChange(details, 'late-file-change');
        details.send({
          id: 'run-a-approval',
          method: 'item/fileChange/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'late-file-change',
            startedAtMs: 1,
          },
        });
      },
      onClientResponse({ message, send }) {
        if (message.id !== 'run-a-approval') return;
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-A', requestId: 'run-a-approval' },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const runAContext = executionContext({
      lineage: { provider_native_id: 'codex-thread-A' },
    });
    const runA = executeAdapter(adapter, runAContext)[Symbol.asyncIterator]();
    const interaction = await nextInteraction(runA);
    await deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ));
    server.send({
      method: 'item/completed',
      params: {
        threadId: 'codex-thread-A',
        turnId: 'codex-turn-1',
        item: {
          type: 'fileChange',
          id: 'late-file-change',
          status: 'completed',
          changes: [],
        },
      },
    });
    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-A',
        turn: { id: 'codex-turn-1', status: 'completed', items: [] },
      },
    });
    await collect({ [Symbol.asyncIterator]: () => runA });

    const runBContext = executionContext({
      conversation_id: 'conversation-B',
      turn_id: 'turn-B',
      lineage_id: 'lineage-B',
      lineage: { provider_native_id: 'codex-thread-B' },
      reportProviderFailure: reportRunBFailure,
      attempt: { attempt_id: 'attempt-B', attempt_no: 1, lease_epoch: 4 },
    });
    const runB = executeAdapter(adapter, runBContext)[Symbol.asyncIterator]();
    const waitingB = runB.next();
    await waitFor(() => server.received.some((message) => (
      message.method === 'turn/start' && message.params.threadId === 'codex-thread-B'
    )));
    const runATurnStartId = server.received.find((message) => (
      message.method === 'turn/start' && message.params.threadId === 'codex-thread-A'
    )).id;

    server.send({
      method: 'item/agentMessage/delta',
      params: {
        threadId: 'codex-thread-A',
        turnId: 'codex-turn-1',
        itemId: 'late-message',
        delta: 'late',
      },
    });
    server.send({
      id: 'late-run-a-request',
      method: 'item/fileChange/requestApproval',
      params: {
        threadId: 'codex-thread-A',
        turnId: 'codex-turn-1',
        itemId: 'late-file-change',
        startedAtMs: 2,
      },
    });
    server.send({
      method: 'serverRequest/resolved',
      params: { threadId: 'codex-thread-A', requestId: 'run-a-approval' },
    });
    server.send({ id: runATurnStartId, result: { turn: { id: 'codex-turn-1' } } });
    await new Promise((resolve) => setImmediate(resolve));

    expect(reportRunBFailure).not.toHaveBeenCalled();
    expect(server.child.kill).not.toHaveBeenCalled();
    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-B',
        turn: { id: 'codex-turn-2', status: 'completed', items: [] },
      },
    });
    await expect(waitingB).resolves.toEqual({ done: true, value: undefined });
  });

  test('reports recovery when transport is lost after turn/start write but before its response', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({ respondToTurnStart: false });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const waiting = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    server.child.emit('close', 1, null);

    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown', side_effect_status: 'unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('retires the connection when a successful turn/start response has no usable turn fence', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({ respondToTurnStart: false });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const waiting = executeAdapter(adapter, executionContext({
      lineage: { provider_native_id: 'codex-thread-1' },
      reportProviderFailure,
    }))[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));
    const request = server.received.find(({ method }) => method === 'turn/start');

    server.send({ id: request.id, result: { turn: { status: 'inProgress', items: [] } } });

    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('retires the connection when a JSON-RPC response has both result and error', async () => {
    const server = createFakeAppServer({
      afterTurnStart() {},
      autoTurnStarted: false,
      turnStartResponsePatch: {
        error: { code: -32_000, message: 'ambiguous private response' },
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('retires the shared connection when turn/start cannot be written synchronously', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer();
    const originalWrite = server.child.stdin.write.bind(server.child.stdin);
    server.child.stdin.write = jest.fn((chunk) => {
      if (String(chunk).includes('"method":"turn/start"')) {
        throw new Error('forced synchronous turn/start EPIPE');
      }
      return originalWrite(chunk);
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(executeAdapter(adapter, executionContext({
      lineage: { provider_native_id: 'codex-thread-1' },
      reportProviderFailure,
    }))[Symbol.asyncIterator]().next()).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('retires the shared connection when a later control request cannot be written', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({
      lineage: { provider_native_id: 'codex-thread-1' },
      reportProviderFailure,
    });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));
    server.child.stdin.write = jest.fn(() => {
      throw new Error('forced synchronous control EPIPE');
    });

    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'stop',
    })).rejects.toMatchObject({ providerError: { side_effect_status: 'unknown' } });
    await expect(waiting).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
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
            item: {
              type: 'commandExecution',
              id: 'command-1',
              command: 'private',
              status: 'inProgress',
            },
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

    const events = await collect(executeAdapter(adapter, executionContext()));
    expect(events).toEqual([
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

  test('ignores fenced private telemetry that has no provider-neutral event shape', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'thread/tokenUsage/updated',
          params: {
            threadId,
            turnId,
            tokenUsage: {
              total: {
                totalTokens: 21,
                inputTokens: 13,
                cachedInputTokens: 8,
                outputTokens: 8,
                reasoningOutputTokens: 0,
              },
              last: {
                totalTokens: 21,
                inputTokens: 13,
                cachedInputTokens: 8,
                outputTokens: 8,
                reasoningOutputTokens: 0,
              },
              modelContextWindow: 258_400,
            },
          },
        });
        send({
          method: 'turn/moderationMetadata',
          params: {
            threadId,
            turnId,
            metadata: { privateProviderScores: [0.1, 0.9] },
          },
        });
        send({
          method: 'turn/completed',
          params: {
            threadId,
            turn: { id: turnId, status: 'completed', items: [] },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).resolves.toEqual([]);
  });

  test('fails the supervised connection closed for an unsupported running item', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'item/started',
          params: {
            threadId,
            turnId,
            startedAtMs: 1,
            item: { type: 'futurePrivateWriter', id: 'private-writer-1' },
          },
        });
      },
    });
    server.child.kill = jest.fn(() => true);
    const replacementServer = createFakeAppServer();
    const spawnProcess = jest.fn()
      .mockImplementationOnce(() => server.child)
      .mockImplementationOnce(() => replacementServer.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });

    await expect(executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');

    const replacement = collect(executeAdapter(adapter, executionContext({
      turn_id: 'turn-after-retirement',
      lineage: { provider_native_id: 'codex-thread-1' },
      attempt: { attempt_id: 'attempt-after-retirement', attempt_no: 1, lease_epoch: 4 },
    })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(spawnProcess).toHaveBeenCalledTimes(1);
    server.child.emit('close', 1, null);
    await expect(replacement).resolves.toEqual([]);
    expect(spawnProcess).toHaveBeenCalledTimes(2);
  });

  test('does not replace a closed app-server while its supervised process group is alive', async () => {
    const firstServer = createFakeAppServer();
    firstServer.child.pid = 42_426;
    const replacementServer = createFakeAppServer();
    let processGroupAlive = true;
    const signalProcessGroup = jest.fn((_processGroupId, _child, signal) => {
      if (signal === 'SIGKILL') processGroupAlive = false;
      return true;
    });
    const spawnProcess = jest.fn()
      .mockImplementationOnce(() => firstServer.child)
      .mockImplementationOnce(() => replacementServer.child);
    const adapter = createCodexAppServerAdapter({
      spawnProcess,
      processTerminationGraceMs: 5,
      signalProcessGroup,
      isProcessGroupAlive: () => processGroupAlive,
    });
    await collect(executeAdapter(adapter, executionContext()));

    firstServer.child.emit('close', 1, null);
    const replacement = collect(executeAdapter(adapter, executionContext({
      turn_id: 'turn-after-group-exit',
      lineage: { provider_native_id: 'codex-thread-1' },
      attempt: { attempt_id: 'attempt-after-group-exit', attempt_no: 1, lease_epoch: 4 },
    })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(spawnProcess).toHaveBeenCalledTimes(1);

    await expect(replacement).resolves.toEqual([]);
    expect(signalProcessGroup.mock.calls.map(([, , signal]) => signal))
      .toEqual(['SIGTERM', 'SIGKILL']);
    expect(spawnProcess).toHaveBeenCalledTimes(2);
  });

  test('retires a connection before its late-fence retention bound can be exceeded', async () => {
    const firstServer = createFakeAppServer();
    firstServer.child.kill = jest.fn(() => true);
    const replacementServer = createFakeAppServer();
    const spawnProcess = jest.fn()
      .mockImplementationOnce(() => firstServer.child)
      .mockImplementationOnce(() => replacementServer.child);
    const adapter = createCodexAppServerAdapter({
      spawnProcess,
      maxConnectionFenceEntries: 3,
    });
    await collect(executeAdapter(adapter, executionContext()));

    await expect(collect(executeAdapter(adapter, executionContext({
      turn_id: 'turn-fence-capacity',
      lineage: { provider_native_id: 'codex-thread-1' },
      attempt: { attempt_id: 'attempt-fence-capacity', attempt_no: 1, lease_epoch: 4 },
    })))).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
    });
    expect(firstServer.child.kill).toHaveBeenCalledWith('SIGTERM');

    const replacement = collect(executeAdapter(adapter, executionContext({
      turn_id: 'turn-after-fence-rotation',
      lineage: { provider_native_id: 'codex-thread-1' },
      attempt: { attempt_id: 'attempt-after-fence-rotation', attempt_no: 1, lease_epoch: 5 },
    })));
    await new Promise((resolve) => setImmediate(resolve));
    expect(spawnProcess).toHaveBeenCalledTimes(1);
    firstServer.child.emit('close', 1, null);
    await expect(replacement).resolves.toEqual([]);
    expect(spawnProcess).toHaveBeenCalledTimes(2);
  });

  test.each([
    ['duplicate tool start', ({ send, threadId, turnId }) => {
      const notification = {
        method: 'item/started',
        params: {
          threadId,
          turnId,
          startedAtMs: 1,
          item: { type: 'commandExecution', id: 'duplicate-tool', status: 'inProgress' },
        },
      };
      send(notification);
      send(notification);
    }],
    ['unknown tool terminal status', ({ send, threadId, turnId }) => {
      send({
        method: 'item/started',
        params: {
          threadId,
          turnId,
          startedAtMs: 1,
          item: { type: 'commandExecution', id: 'unknown-status', status: 'inProgress' },
        },
      });
      send({
        method: 'item/completed',
        params: {
          threadId,
          turnId,
          completedAtMs: 2,
          item: { type: 'commandExecution', id: 'unknown-status', status: 'futureStatus' },
        },
      });
    }],
    ['missing tool terminal status', ({ send, threadId, turnId }) => {
      send({
        method: 'item/started',
        params: {
          threadId,
          turnId,
          startedAtMs: 1,
          item: { type: 'commandExecution', id: 'missing-status', status: 'inProgress' },
        },
      });
      send({
        method: 'item/completed',
        params: {
          threadId,
          turnId,
          completedAtMs: 2,
          item: { type: 'commandExecution', id: 'missing-status' },
        },
      });
    }],
    ['completed turn with an unfinished tool', ({ send, threadId, turnId }) => {
      send({
        method: 'item/started',
        params: {
          threadId,
          turnId,
          startedAtMs: 1,
          item: { type: 'commandExecution', id: 'unfinished-tool', status: 'inProgress' },
        },
      });
      send({
        method: 'turn/completed',
        params: { threadId, turn: { id: turnId, status: 'completed', items: [] } },
      });
    }],
  ])('fails the connection closed for %s', async (_label, afterTurnStart) => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({ afterTurnStart });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({ reportProviderFailure }))))
      .rejects.toMatchObject({ providerError: { side_effect_status: 'unknown' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('rejects a progress method that does not match the active tool type', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'item/started',
          params: {
            threadId,
            turnId,
            startedAtMs: 1,
            item: { type: 'commandExecution', id: 'command-cross-progress', status: 'inProgress' },
          },
        });
        send({
          method: 'item/fileChange/outputDelta',
          params: { threadId, turnId, itemId: 'command-cross-progress', delta: 'wrong method' },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({ reportProviderFailure }))))
      .rejects.toMatchObject({ providerError: { side_effect_status: 'unknown' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test.each([
    'mcpToolCall',
    'dynamicToolCall',
    'collabAgentToolCall',
    'webSearch',
    'imageGeneration',
  ])('fails closed if disabled %s execution appears despite the locked thread config', async (type) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'item/started',
          params: {
            threadId,
            turnId,
            startedAtMs: 1,
            item: { type, id: `disabled-${type}`, status: 'inProgress' },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('fails closed if a disabled provider hook appears despite the locked thread config', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'hook/started',
          params: {
            threadId,
            turnId,
            run: {
              id: 'disabled-hook',
              eventName: 'stop',
              handlerType: 'command',
              executionMode: 'sync',
              scope: 'turn',
              status: 'running',
            },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('fails the connection closed for an unknown notification scoped to the current turn', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'future/privateNotification',
          params: { threadId, turnId, privateState: 'unknown' },
        });
        send({
          method: 'turn/completed',
          params: { threadId, turn: { id: turnId, status: 'completed', items: [] } },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({ reportProviderFailure }))))
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test.each([
    ['wrong thread', { threadId: 'wrong-thread', turnId: 'codex-turn-1', itemId: 'patch-1', startedAtMs: 1 }],
    ['wrong turn', { threadId: 'codex-thread-1', turnId: 'wrong-turn', itemId: 'patch-1', startedAtMs: 1 }],
    ['missing turn', { threadId: 'codex-thread-1', itemId: 'patch-1', startedAtMs: 1 }],
    ['missing params', undefined],
  ])('retires the shared connection for a stale server request with %s', async (_label, params) => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'stale-request',
          method: 'item/fileChange/requestApproval',
          ...(params === undefined ? {} : { params }),
        });
        send({
          method: 'turn/completed',
          params: { threadId, turn: { id: turnId, status: 'completed', items: [] } },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext({ reportProviderFailure }))))
      .rejects.toMatchObject({ providerError: { side_effect_status: 'unknown' } });
    expect(server.received).toContainEqual({
      id: 'stale-request',
      error: { code: -32601, message: 'Unsupported or stale app-server request.' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
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
    const iterator = executeAdapter(adapter, executionContext())[Symbol.asyncIterator]();

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

    const delivery = handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'text', text: 'Alice' },
    );
    await expect(adapter.queryInteractionHandoffAcceptance(delivery)).resolves.toMatchObject({
      status: 'unknown',
      read_only: true,
      idempotent: true,
      handoff_id: delivery.handoff.handoff_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      reason_code: 'provider_acceptance_query_unavailable',
    });
    const prepared = await adapter.prepareInteractionAnswer(delivery);
    expect(server.received).not.toContainEqual(expect.objectContaining({ id: 71 }));
    delivery.handoff.last_send_started_at = '2026-07-19T05:04:03Z';
    await expect(prepared.send(delivery)).resolves.toEqual({
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
      params: {
        itemId: 'command-1',
        startedAtMs: 1,
        environmentId: null,
        reason: 'Network access is required.',
        command: 'npm test',
        cwd: '/workspace',
      },
      expected: {
        kind: 'tool_approval',
        prompt: expect.stringMatching(/Command: npm test\nWorking directory: \/workspace/),
      },
      answer: { kind: 'decision', decision: 'approve' },
      result: { decision: 'accept' },
      workspaceFenceChecks: 6,
    },
    {
      label: 'file approval',
      method: 'item/fileChange/requestApproval',
      params: { itemId: 'patch-1', startedAtMs: 1, reason: null },
      expected: {
        kind: 'tool_approval',
        prompt: expect.stringMatching(/File update: \/workspace\/file\.txt\nDiff:\n\+safe change/),
      },
      answer: { kind: 'decision', decision: 'deny' },
      result: { decision: 'decline' },
      workspaceFenceChecks: 5,
    },
  ])('maps $label through a fenced provider-neutral interaction', async ({
    method,
    params,
    expected,
    answer,
    result,
    workspaceFenceChecks = 4,
  }) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        if (method === 'item/commandExecution/requestApproval') {
          send({
            method: 'item/started',
            params: {
              threadId,
              turnId,
              startedAtMs: 1,
              item: {
                type: 'commandExecution',
                id: params.itemId,
                command: params.command,
                cwd: params.cwd,
                status: 'inProgress',
              },
            },
          });
        }
        if (method === 'item/fileChange/requestApproval') {
          send({
            method: 'item/started',
            params: {
              threadId,
              turnId,
              startedAtMs: 1,
              item: {
                type: 'fileChange',
                id: params.itemId,
                status: 'inProgress',
                changes: [{
                  path: '/workspace/file.txt',
                  kind: { type: 'update', move_path: null },
                  diff: '+safe change',
                }],
              },
            },
          });
        }
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
    const controls = workspaceControls();
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const iterator = executeAdapter(
      adapter,
      executionContext(),
      controls,
    )[Symbol.asyncIterator]();

    let interaction = await iterator.next();
    while (interaction.value?.kind !== 'interaction_requested') interaction = await iterator.next();

    expect(interaction.value).toEqual({
      kind: 'interaction_requested',
      payload: expect.objectContaining({
        provider_interaction_ref: expect.any(String),
        ...expected,
      }),
    });
    expect(JSON.stringify(interaction.value)).not.toMatch(/requestApproval|requestUserInput/);
    await expect(deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      answer,
    ))).resolves.toEqual(expect.objectContaining({
      handoff_id: 'handoff-1',
      status: answer.decision === 'deny' ? 'deny' : 'accepted',
    }));
    expect(server.received).toContainEqual({ id: 'provider-request-1', result });
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(workspaceFenceChecks);
    await iterator.return();
  });

  test('auto-approves one native write only after Core validates its complete approval fence', async () => {
    const authorizeProtectedAction = jest.fn(() => ({
      trusted: true,
      basis_kind: 'persistent_bot',
      checked_policy_revision: 7,
    }));
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedFileChange(details, 'trusted-file-change');
        details.send({
          id: 'trusted-provider-request',
          method: 'item/fileChange/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'trusted-file-change',
            startedAtMs: 1,
            reason: 'Trusted policy recheck.',
          },
        });
      },
      onClientResponse({ message, send }) {
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-1', requestId: message.id },
        });
        send({
          method: 'item/completed',
          params: {
            threadId: 'codex-thread-1',
            turnId: 'codex-turn-1',
            item: {
              type: 'fileChange',
              id: 'trusted-file-change',
              status: 'completed',
              changes: [],
            },
          },
        });
        send({
          method: 'turn/completed',
          params: {
            threadId: 'codex-thread-1',
            turn: { id: 'codex-turn-1', status: 'completed', items: [] },
          },
        });
      },
    });
    const assertWorkspaceWrite = jest.fn(() => ({ status: 'current' }));
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    const events = await collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite,
      authorizeProtectedAction,
    }));

    expect(authorizeProtectedAction).toHaveBeenCalledWith({
      action_ref: expect.stringMatching(
        /^codex:codex-app-server-1:s:trusted-provider-request:/,
      ),
      action_kind: 'item/fileChange/requestApproval',
    });
    expect(events).not.toContainEqual(expect.objectContaining({ kind: 'interaction_requested' }));
    expect(server.received).toContainEqual({
      id: 'trusted-provider-request',
      result: { decision: 'accept' },
    });
    expect(assertWorkspaceWrite).toHaveBeenLastCalledWith({
      action_kind: 'item/fileChange/requestApproval',
      connection_id: 'codex-app-server-1',
      conversation_id: 'conversation-1',
      core_turn_id: 'turn-1',
      cwd: '/workspace',
      environment_id: null,
      executor_instance_id: 'executor-1',
      lineage_id: 'lineage-1',
      provider_approval_id: null,
      provider_attempt: { attempt_id: 'attempt-1', attempt_no: 1, lease_epoch: 3 },
      provider_item_id: 'trusted-file-change',
      provider_thread_id: 'codex-thread-1',
      provider_turn_id: 'codex-turn-1',
      workspace: expect.objectContaining({
        workspace_lease_id: 'workspace-lease-1',
        holder_conversation_id: 'conversation-1',
        holder_turn_id: 'turn-1',
        lease_epoch: 11,
      }),
      write_paths: ['/workspace/trusted-file-change.txt'],
    });
  });

  test('chooses only one-shot accept when app-server advertises session approval alternatives', async () => {
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedCommand(details, 'one-shot-command');
        details.send({
          id: 'one-shot-command-approval',
          method: 'item/commandExecution/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'one-shot-command',
            startedAtMs: 1,
            environmentId: null,
            command: 'touch approved.txt',
            cwd: '/workspace',
            availableDecisions: ['accept', 'acceptForSession', 'decline'],
          },
        });
      },
      onClientResponse({ message, send }) {
        send({
          method: 'serverRequest/resolved',
          params: { threadId: 'codex-thread-1', requestId: message.id },
        });
        send({
          method: 'item/completed',
          params: {
            threadId: 'codex-thread-1',
            turnId: 'codex-turn-1',
            item: { type: 'commandExecution', id: 'one-shot-command', status: 'completed' },
          },
        });
        send({
          method: 'turn/completed',
          params: {
            threadId: 'codex-thread-1',
            turn: { id: 'codex-turn-1', status: 'completed', items: [] },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    await expect(collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite: jest.fn(() => ({ status: 'current' })),
      authorizeProtectedAction: jest.fn(() => ({ trusted: true })),
    }))).resolves.toEqual(expect.any(Array));
    expect(server.received).toContainEqual({
      id: 'one-shot-command-approval',
      result: { decision: 'accept' },
    });
    const response = server.received.find(({ id, result }) => (
      id === 'one-shot-command-approval' && result
    ));
    expect(JSON.stringify(response)).not.toMatch(
      /acceptForSession|acceptWithExecpolicyAmendment|applyNetworkPolicyAmendment/,
    );
  });

  test('declines command-level additional permissions instead of creating a sticky turn grant', async () => {
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedCommand(details, 'sticky-permission-command');
        details.send({
          id: 'sticky-permission-approval',
          method: 'item/commandExecution/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'sticky-permission-command',
            startedAtMs: 1,
            environmentId: null,
            command: 'touch approved.txt',
            cwd: '/workspace',
            additionalPermissions: {
              network: null,
              fileSystem: { read: [], write: ['/workspace'] },
            },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    await expect(collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite: jest.fn(() => ({ status: 'current' })),
      authorizeProtectedAction: jest.fn(() => ({ trusted: true })),
    }))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 'sticky-permission-approval',
      result: { decision: 'decline' },
    });
    expect(server.received).not.toContainEqual({
      id: 'sticky-permission-approval',
      result: { decision: 'accept' },
    });
  });

  test.each([
    ['cwd', '/outside', null, null],
    ['environment', '/workspace', 'remote-environment', null],
    ['network', '/workspace', null, { host: 'example.com', protocol: 'https' }],
  ])('declines a trusted command whose %s fence escapes the local workspace', async (
    _label,
    commandCwd,
    environmentId,
    networkApprovalContext,
  ) => {
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedCommand(details, 'mismatched-command', { cwd: commandCwd });
        details.send({
          id: 'mismatched-command-approval',
          method: 'item/commandExecution/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'mismatched-command',
            startedAtMs: 1,
            environmentId,
            command: 'touch approved.txt',
            cwd: commandCwd,
            networkApprovalContext,
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    await expect(collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite: jest.fn(() => ({ status: 'current' })),
      authorizeProtectedAction: jest.fn(() => ({ trusted: true })),
    }))).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(server.received).toContainEqual({
      id: 'mismatched-command-approval',
      result: { decision: 'decline' },
    });
    expect(server.received).not.toContainEqual({
      id: 'mismatched-command-approval',
      result: { decision: 'accept' },
    });
  });

  test('declines a protected write when its durable lease epoch becomes stale before approval', async () => {
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const assertWorkspaceWrite = jest.fn((fence) => {
      if (fence?.provider_item_id === 'stale-file-change') throw staleLease;
      return { status: 'current' };
    });
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedFileChange(details, 'stale-file-change');
        details.send({
          id: 'stale-auto-approval',
          method: 'item/fileChange/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'stale-file-change',
            startedAtMs: 1,
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    await expect(collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite,
      authorizeProtectedAction: jest.fn(() => ({ trusted: true })),
    }))).rejects.toMatchObject({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: { side_effect_status: 'unknown' },
    });
    expect(server.received).toContainEqual({
      id: 'stale-auto-approval',
      result: { decision: 'decline' },
    });
    expect(server.received).not.toContainEqual({
      id: 'stale-auto-approval',
      result: { decision: 'accept' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('never returns session, persistent-policy, network-amendment, or permission grants', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'permission-grant-request',
          method: 'item/permissions/requestApproval',
          params: {
            threadId,
            turnId,
            itemId: 'permission-grant-item',
            startedAtMs: 1,
            environmentId: null,
            cwd: '/workspace',
            reason: 'Request a broad grant.',
            permissions: {
              network: { enabled: true },
              fileSystem: { read: [], write: ['/workspace'] },
            },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });

    await expect(collect(adapter.execute(executionContext(), {
      assertWorkspaceWrite: jest.fn(() => ({ status: 'current' })),
      authorizeProtectedAction: jest.fn(() => ({ trusted: true })),
    }))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 'permission-grant-request',
      result: { permissions: {}, scope: 'turn' },
    });
    expect(JSON.stringify(server.received)).not.toMatch(
      /acceptForSession|acceptWithExecpolicyAmendment|applyNetworkPolicyAmendment|"scope":"session"/,
    );
  });

  test('declines a well-formed MCP elicitation instead of exposing an external side-effect path', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'disabled-mcp-elicitation',
          method: 'mcpServer/elicitation/request',
          params: {
            threadId,
            turnId,
            serverName: 'private-server',
            mode: 'form',
            message: 'Choose an environment.',
            requestedSchema: {
              type: 'object',
              properties: { environment: { type: 'string', minLength: 1 } },
              required: ['environment'],
            },
            _meta: null,
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 'disabled-mcp-elicitation',
      result: { action: 'decline', content: null, _meta: null },
    });
  });

  test('rejects an app-server initiated dynamic tool call before it can execute', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'disabled-dynamic-tool-call',
          method: 'item/tool/call',
          params: {
            threadId,
            turnId,
            callId: 'dynamic-call-1',
            tool: 'writeFile',
            arguments: { path: '/workspace/unapproved.txt' },
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(collect(executeAdapter(adapter, executionContext()))).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 'disabled-dynamic-tool-call',
      error: { code: -32601, message: 'Unsupported or stale app-server request.' },
    });
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
    const iterator = executeAdapter(adapter, executionContext())[Symbol.asyncIterator]();

    await expect(iterator.next()).rejects.toMatchObject({
      providerError: { code: 'unsupported_capability' },
    });
    expect(server.received).toContainEqual({
      id: 81,
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
  });

  test('fails closed when requestUserInput combines fixed choices with an Other answer', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'choice-with-other',
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-choice-with-other',
            autoResolutionMs: null,
            questions: [{
              id: 'environment',
              header: 'Environment',
              question: 'Choose an environment.',
              isOther: true,
              isSecret: false,
              options: [{ label: 'Staging', description: 'Use staging.' }],
            }],
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(executeAdapter(adapter, executionContext())[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(server.received).toContainEqual({
      id: 'choice-with-other',
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
  });

  test.each([
    ['missing', undefined],
    ['null', null],
    ['string', 'false'],
  ])('fails closed when requestUserInput isSecret is %s', async (_label, isSecret) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        const question = {
          id: 'unsafe-secret-shape',
          header: 'Input',
          question: 'Provide input.',
          isOther: false,
          options: null,
        };
        if (isSecret !== undefined) question.isSecret = isSecret;
        send({
          id: 'unsafe-secret-request',
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-unsafe-secret',
            autoResolutionMs: null,
            questions: [question],
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(executeAdapter(adapter, executionContext())[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'side_effect_unknown' } });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('fails closed when requestUserInput has a provider-side auto-resolution deadline', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'auto-resolving-request',
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-auto-resolving',
            autoResolutionMs: 60_000,
            questions: [{
              id: 'auto-resolving-question',
              header: 'Input',
              question: 'Provide input.',
              isOther: false,
              isSecret: false,
              options: null,
            }],
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(executeAdapter(adapter, executionContext())[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test.each([
    {
      label: 'requestUserInput missing itemId',
      method: 'item/tool/requestUserInput',
      params: {
        autoResolutionMs: null,
        questions: [{
          id: 'question-1',
          header: 'Input',
          question: 'Provide input.',
          isOther: false,
          isSecret: false,
          options: null,
        }],
      },
    },
    {
      label: 'requestUserInput missing autoResolutionMs',
      method: 'item/tool/requestUserInput',
      params: {
        itemId: 'tool-auto-resolution-missing',
        questions: [{
          id: 'question-1',
          header: 'Input',
          question: 'Provide input.',
          isOther: false,
          isSecret: false,
          options: null,
        }],
      },
    },
    {
      label: 'requestUserInput missing options',
      method: 'item/tool/requestUserInput',
      params: {
        itemId: 'tool-options-missing',
        autoResolutionMs: null,
        questions: [{
          id: 'question-1',
          header: 'Input',
          question: 'Provide input.',
          isOther: false,
          isSecret: false,
        }],
      },
    },
    {
      label: 'file approval empty itemId',
      method: 'item/fileChange/requestApproval',
      params: { itemId: '', startedAtMs: 1, reason: null },
    },
    {
      label: 'command approval missing startedAtMs',
      method: 'item/commandExecution/requestApproval',
      params: { itemId: 'command-malformed', environmentId: null, reason: null },
    },
    {
      label: 'command approval missing environmentId',
      method: 'item/commandExecution/requestApproval',
      params: { itemId: 'command-environment-missing', startedAtMs: 1, reason: null },
    },
    {
      label: 'permission approval malformed permissions',
      method: 'item/permissions/requestApproval',
      params: {
        itemId: 'permission-malformed',
        startedAtMs: 1,
        cwd: '/workspace',
        environmentId: null,
        reason: null,
        permissions: { network: 'all', fileSystem: null },
      },
    },
    {
      label: 'permission approval missing required environment and reason',
      method: 'item/permissions/requestApproval',
      params: {
        itemId: 'permission-required-missing',
        startedAtMs: 1,
        cwd: '/workspace',
        permissions: { network: null, fileSystem: null },
      },
    },
    {
      label: 'permission approval missing profile keys',
      method: 'item/permissions/requestApproval',
      params: {
        itemId: 'permission-profile-missing',
        startedAtMs: 1,
        cwd: '/workspace',
        environmentId: null,
        reason: null,
        permissions: {},
      },
    },
    {
      label: 'permission approval missing nested profile fields',
      method: 'item/permissions/requestApproval',
      params: {
        itemId: 'permission-nested-missing',
        startedAtMs: 1,
        cwd: '/workspace',
        environmentId: null,
        reason: null,
        permissions: { network: {}, fileSystem: {} },
      },
    },
  ])('fails closed for $label', async ({ method, params }) => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'malformed-provider-request',
          method,
          params: { threadId, turnId, ...params },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });

    await expect(executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { side_effect_status: 'unknown' } });
    expect(server.received).toContainEqual({
      id: 'malformed-provider-request',
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test.each([
    ['free-form text', { kind: 'text', text: 'Staging' }],
    ['an unknown choice', { kind: 'choice', choice_id: 'Unknown' }],
  ])('rejects %s before answering a fixed requestUserInput choice', async (_label, answer) => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        send({
          id: 'fixed-choice-request',
          method: 'item/tool/requestUserInput',
          params: {
            threadId,
            turnId,
            itemId: 'tool-fixed-choice',
            autoResolutionMs: null,
            questions: [{
              id: 'environment',
              header: 'Environment',
              question: 'Choose an environment.',
              isOther: false,
              isSecret: false,
              options: [
                { label: 'Staging', description: 'Use staging.' },
                { label: 'Production', description: 'Use production.' },
              ],
            }],
          },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = executeAdapter(adapter, executionContext())[Symbol.asyncIterator]();
    const interaction = await iterator.next();

    await expect(deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      answer,
    ))).rejects.toMatchObject({ providerError: { code: 'side_effect_unknown' } });
    expect(server.received.filter(({ id, result }) => (
      id === 'fixed-choice-request' && result !== undefined
    ))).toHaveLength(0);
    await iterator.return();
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
      label: 'tighter free-text length constraints',
      params: {
        mode: 'form',
        message: 'Provide a long environment name.',
        requestedSchema: {
          type: 'object',
          properties: {
            environment: { type: 'string', minLength: 2, maxLength: 20 },
          },
          required: ['environment'],
        },
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
    {
      label: 'unmapped string pattern',
      params: {
        mode: 'form',
        message: 'Provide a production target.',
        requestedSchema: {
          type: 'object',
          properties: {
            environment: { type: 'string', minLength: 1, pattern: '^prod$' },
          },
          required: ['environment'],
        },
        _meta: null,
      },
    },
    {
      label: 'conflicting enum and oneOf choices',
      params: {
        mode: 'form',
        message: 'Choose an environment.',
        requestedSchema: {
          type: 'object',
          properties: {
            environment: {
              type: 'string',
              enum: ['staging'],
              oneOf: [{ const: 'production', title: 'Production' }],
            },
          },
          required: ['environment'],
        },
        _meta: null,
      },
    },
    {
      label: 'astral enum below its code-point minLength',
      params: {
        mode: 'form',
        message: 'Choose a two-code-point value.',
        requestedSchema: {
          type: 'object',
          properties: {
            symbol: { type: 'string', enum: ['😀'], minLength: 2 },
          },
          required: ['symbol'],
        },
        _meta: null,
      },
    },
  ])('fails closed for an unsupported MCP $label', async ({ params }) => {
    const reportProviderFailure = jest.fn(() => ({ status: 'recovering' }));
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

    await expect(executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next())
      .rejects.toMatchObject({ providerError: { code: 'unsupported_capability' } });
    expect(server.received).toContainEqual({
      id: 'unsupported-mcp',
      error: { code: -32601, message: 'Invalid app-server request.' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('rejects an early provider resolved notification before any answer is sent', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'patch-early');
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

    await expect(nextInteraction(
      executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator](),
    ))
      .rejects.toMatchObject({ providerError: { code: 'side_effect_unknown' } });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('rejects reuse of a completed server request ID on the same connection', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'patch-first');
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
    const iterator = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    const interaction = await nextInteraction(iterator);
    await deliverPreparedInteractionAnswer(adapter, handoffDelivery(
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

  test('keeps numeric and string server request IDs distinct across tombstone retention', async () => {
    const server = createFakeAppServer({
      afterTurnStart(details) {
        sendStartedFileChange(details, 'typed-request-first');
        details.send({
          id: 1,
          method: 'item/fileChange/requestApproval',
          params: {
            threadId: details.threadId,
            turnId: details.turnId,
            itemId: 'typed-request-first',
            startedAtMs: 1,
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
    const iterator = executeAdapter(adapter, executionContext())[Symbol.asyncIterator]();
    const first = await nextInteraction(iterator);
    await deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      first.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
      { handoffId: 'handoff-numeric', handoffAttemptId: 'attempt-numeric' },
    ));
    server.send({
      method: 'item/completed',
      params: {
        threadId: 'codex-thread-1',
        turnId: 'codex-turn-1',
        item: {
          type: 'fileChange',
          id: 'typed-request-first',
          status: 'completed',
          changes: [],
        },
      },
    });
    sendStartedFileChange({
      send: server.send,
      threadId: 'codex-thread-1',
      turnId: 'codex-turn-1',
    }, 'typed-request-second');
    server.send({
      id: '1',
      method: 'item/fileChange/requestApproval',
      params: {
        threadId: 'codex-thread-1',
        turnId: 'codex-turn-1',
        itemId: 'typed-request-second',
        startedAtMs: 2,
      },
    });

    const second = await nextInteraction(iterator);
    await deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      second.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'deny' },
      { handoffId: 'handoff-string', handoffAttemptId: 'attempt-string' },
    ));
    server.send({
      method: 'item/completed',
      params: {
        threadId: 'codex-thread-1',
        turnId: 'codex-turn-1',
        item: {
          type: 'fileChange',
          id: 'typed-request-second',
          status: 'declined',
          changes: [],
        },
      },
    });
    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'completed', items: [] },
      },
    });

    await expect(collect({ [Symbol.asyncIterator]: () => iterator })).resolves.toEqual(
      expect.any(Array),
    );
    expect(server.child.kill).not.toHaveBeenCalled();
  });

  test('fails an active turn closed when transport becomes uncertain and reloads on reconnect', async () => {
    const firstServer = createFakeAppServer({ afterTurnStart() {} });
    const secondServer = createFakeAppServer({
      resumeTurns: [{ id: 'codex-turn-historical' }],
      afterThreadResume({ send, threadId }) {
        send({
          method: 'thread/tokenUsage/updated',
          params: {
            threadId,
            turnId: 'codex-turn-historical',
            tokenUsage: {
              total: {
                totalTokens: 1,
                inputTokens: 1,
                cachedInputTokens: 0,
                outputTokens: 0,
                reasoningOutputTokens: 0,
              },
              last: {
                totalTokens: 1,
                inputTokens: 1,
                cachedInputTokens: 0,
                outputTokens: 0,
                reasoningOutputTokens: 0,
              },
              modelContextWindow: 258_400,
            },
          },
        });
      },
      afterTurnStart({ send, threadId, turnId }) {
        send({
          method: 'turn/completed',
          params: { threadId, turn: { id: turnId, status: 'completed', items: [] } },
        });
      },
    });
    const spawnProcess = jest.fn()
      .mockImplementationOnce(() => firstServer.child)
      .mockImplementationOnce(() => secondServer.child);
    const adapter = createCodexAppServerAdapter({ spawnProcess });
    const persistedContext = executionContext({
      lineage: { provider_native_id: 'codex-thread-persisted' },
    });
    const firstExecution = collect(executeAdapter(adapter, persistedContext));
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

    await expect(collect(executeAdapter(adapter, executionContext({
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

  test.each([
    ['error followed by failed terminal', {
      method: 'error',
      params: {
        threadId: 'codex-thread-1',
        turnId: 'codex-turn-1',
        error: { message: 'private provider error' },
      },
    }, {
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'failed', items: [] },
      },
    }],
    ['failed terminal', {
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'failed', items: [] },
      },
    }],
    ['interrupted terminal', {
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    }],
    ['completed terminal with an outstanding request', {
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'completed', items: [] },
      },
    }],
  ])('reports a fenced provider failure for a waiting interaction on %s', async (...testCase) => {
    const [, terminal, followupTerminal = null] = testCase;
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'waiting-patch');
        send({
          id: 'waiting-terminal-request',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'waiting-patch', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    await expect(nextInteraction(iterator)).resolves.toMatchObject({
      done: false,
      value: { kind: 'interaction_requested' },
    });

    server.send(terminal);
    if (followupTerminal) {
      await new Promise((resolve) => setImmediate(resolve));
      expect(reportProviderFailure).not.toHaveBeenCalled();
      server.send(followupTerminal);
    }
    await waitFor(() => reportProviderFailure.mock.calls.length === 1);
    expect(reportProviderFailure).toHaveBeenCalledWith(expect.objectContaining({
      providerError: expect.objectContaining({
        code: 'side_effect_unknown',
        side_effect_status: 'unknown',
      }),
    }));
    await expect(iterator.next()).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
  });

  test.each(['stop', 'steer'])(
    'interrupts only the current %s fence through app-server without killing the shared process',
    async (reason) => {
      const server = createFakeAppServer({ afterTurnStart() {} });
      const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
      const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
      const iterator = executeAdapter(adapter, context)[Symbol.asyncIterator]();
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

  test('rejects a non-empty fixed-version turn interrupt response', async () => {
    const server = createFakeAppServer({
      afterTurnStart() {},
      interruptResult: { accepted: true },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    await expect(adapter.interrupt({
      turn_id: context.turn_id,
      attempt: context.attempt,
      reason: 'stop',
    })).rejects.toMatchObject({ providerError: { code: 'side_effect_unknown' } });
    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test.each(['stop', 'steer'])(
    'maps service %s cancellation to the current fenced app-server turn interrupt',
    async (reason) => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    await expect(adapter.cancel(context, { reason })).resolves.toEqual({
      status: 'interrupt_requested',
      reason,
    });
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

  test('proves abort isolation only after the exact provider turn reaches terminal', async () => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    const aborting = adapter.abort(context);
    await waitFor(() => server.received.some(({ method }) => method === 'turn/interrupt'));
    let settled = false;
    aborting.finally(() => { settled = true; }).catch(() => {});
    await new Promise((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);

    server.send({
      method: 'turn/completed',
      params: {
        threadId: 'codex-thread-1',
        turn: { id: 'codex-turn-1', status: 'interrupted', items: [] },
      },
    });
    await expect(aborting).resolves.toEqual({
      status: 'provider_stopped',
      provider_status: 'interrupted',
    });
    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(server.child.kill).not.toHaveBeenCalled();
  });

  test('proves abort isolation by terminating and observing the full supervised process group', async () => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    server.child.pid = 42_424;
    let processGroupAlive = true;
    const signalProcessGroup = jest.fn((_processGroupId, _child, signal) => {
      if (signal === 'SIGTERM') {
        queueMicrotask(() => server.child.emit('close', 0, signal));
      } else if (signal === 'SIGKILL') {
        processGroupAlive = false;
      }
      return true;
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      interruptConfirmationTimeoutMs: 5,
      processTerminationGraceMs: 5,
      signalProcessGroup,
      isProcessGroupAlive: () => processGroupAlive,
    });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    const waitingFailure = expect(waiting).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
    });
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    await expect(adapter.abort(context)).resolves.toEqual({
      status: 'provider_stopped',
      provider_status: 'process_exited',
    });
    expect(signalProcessGroup.mock.calls.map(([, , signal]) => signal))
      .toEqual(['SIGTERM', 'SIGKILL']);
    await waitingFailure;
  });

  test('waits for the matching provider completion before confirming a timeout interrupt', async () => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
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
    const server = createFakeAppServer({ afterTurnStart() {}, respondToInterrupt: false });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      interruptConfirmationTimeoutMs: 250,
      processTerminationGraceMs: 250,
      setTimeoutFn(callback, delay) {
        expect(delay).toBe(250);
        confirmTimeout = callback;
        return { unref() {} };
      },
      clearTimeoutFn: jest.fn(),
    });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const iterator = executeAdapter(adapter, context)[Symbol.asyncIterator]();
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
        sendStartedFileChange({ send, threadId, turnId }, 'patch-timeout-race');
        send({
          id: 'timeout-race-request',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-timeout-race', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ lineage: { provider_native_id: 'codex-thread-1' } });
    const iterator = executeAdapter(adapter, context)[Symbol.asyncIterator]();
    await nextInteraction(iterator);

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
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
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
        sendStartedFileChange({ send, threadId, turnId }, 'patch-1');
        send({
          id: 91,
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-1', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = executeAdapter(adapter, executionContext())[Symbol.asyncIterator]();
    const interaction = await nextInteraction(iterator);
    const delivery = handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    );
    delivery.handoff.lease_epoch = 4;
    const before = server.received.length;

    await expect(deliverPreparedInteractionAnswer(adapter, delivery)).rejects.toThrow(/runtime fence/);
    expect(server.received).toHaveLength(before);
    await iterator.return();
  });

  test('rechecks the workspace fence immediately before approving a provider write', async () => {
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'patch-stale-workspace');
        send({
          id: 'stale-workspace-approval',
          method: 'item/fileChange/requestApproval',
          params: {
            threadId,
            turnId,
            itemId: 'patch-stale-workspace',
            startedAtMs: 1,
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
    let current = true;
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const controls = workspaceControls({
      assertWorkspaceWrite: jest.fn(() => {
        if (!current) throw staleLease;
        return { status: 'current' };
      }),
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const iterator = executeAdapter(adapter, executionContext(), controls)[Symbol.asyncIterator]();
    const interaction = await nextInteraction(iterator);
    current = false;
    const before = server.received.length;

    await expect(deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ))).rejects.toMatchObject({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: { side_effect_status: 'unknown' },
    });
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(6);
    expect(server.received).toHaveLength(before + 1);
    expect(server.received).toContainEqual({
      id: 'stale-workspace-approval',
      result: { decision: 'decline' },
    });
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
    await iterator.return();
  });

  test('stops a writable app-server run when its workspace fence is stale at tool start', async () => {
    let started;
    const server = createFakeAppServer({
      afterTurnStart(details) {
        started = details;
      },
    });
    let current = true;
    const staleLease = Object.assign(new Error('stale workspace lease'), {
      code: 'stale_workspace_lease',
    });
    const controls = workspaceControls({
      assertWorkspaceWrite: jest.fn(() => {
        if (!current) throw staleLease;
        return { status: 'current' };
      }),
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      cwd: '/workspace',
    });
    const waiting = executeAdapter(
      adapter,
      executionContext(),
      controls,
    )[Symbol.asyncIterator]().next();
    await waitFor(() => started !== undefined);
    current = false;

    started.send({
      method: 'item/started',
      params: {
        threadId: started.threadId,
        turnId: started.turnId,
        item: {
          type: 'commandExecution',
          id: 'stale-workspace-command',
          status: 'inProgress',
        },
      },
    });

    await expect(waiting).rejects.toMatchObject({
      code: 'provider_connection_lost',
      cause: staleLease,
      providerError: { side_effect_status: 'unknown' },
    });
    expect(controls.assertWorkspaceWrite).toHaveBeenCalledTimes(5);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('turns a synchronous provider-answer write failure into a fenced unknown delivery', async () => {
    const reportProviderFailure = jest.fn(() => ({ status: 'handoff_in_progress' }));
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'patch-write-failure');
        send({
          id: 'throwing-answer-write',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-write-failure', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    const interaction = await nextInteraction(iterator);
    server.child.stdin.write = jest.fn(() => {
      throw new Error('forced synchronous EPIPE');
    });

    await expect(deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ))).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown', side_effect_status: 'unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('reports connection loss with an outstanding interaction and clears stale answer state', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({
      afterTurnStart({ send, threadId, turnId }) {
        sendStartedFileChange({ send, threadId, turnId }, 'patch-lost');
        send({
          id: 'lost-interaction',
          method: 'item/fileChange/requestApproval',
          params: { threadId, turnId, itemId: 'patch-lost', startedAtMs: 1 },
        });
      },
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const iterator = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]();
    const interaction = await nextInteraction(iterator);

    server.child.emit('close', 1, null);

    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
    await expect(deliverPreparedInteractionAnswer(adapter, handoffDelivery(
      interaction.value.payload.provider_interaction_ref,
      { kind: 'decision', decision: 'approve' },
    ))).rejects.toThrow(/current provider request/);
  });

  test('turns stdin stream errors into a fenced connection failure', async () => {
    const reportProviderFailure = jest.fn();
    const server = createFakeAppServer({ afterTurnStart() {} });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const waiting = executeAdapter(adapter, executionContext({ reportProviderFailure }))[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    server.child.stdin.emit('error', new Error('EPIPE'));

    await expect(waiting).rejects.toMatchObject({
      providerError: { code: 'side_effect_unknown' },
    });
    expect(reportProviderFailure).toHaveBeenCalledTimes(1);
  });

  test('closes only its supervised app-server child during service shutdown', async () => {
    const server = createFakeAppServer();
    server.child.kill.mockImplementation((signal) => {
      if (signal === 'SIGTERM') queueMicrotask(() => server.child.emit('close', 0, signal));
      return true;
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    await collect(executeAdapter(adapter, executionContext()));

    await expect(adapter.close()).resolves.toEqual([]);
    expect(server.child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  test('reports the conversation whose active provider turn was closed with the service', async () => {
    const server = createFakeAppServer({ afterTurnStart() {} });
    server.child.kill.mockImplementation((signal) => {
      if (signal === 'SIGTERM') queueMicrotask(() => server.child.emit('close', 0, signal));
      return true;
    });
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => server.child });
    const context = executionContext({ conversation_id: 'conversation-active-close' });
    const waiting = executeAdapter(adapter, context)[Symbol.asyncIterator]().next();
    await waitFor(() => server.received.some(({ method }) => method === 'turn/start'));

    await expect(adapter.close()).resolves.toEqual(['conversation-active-close']);
    await expect(waiting).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
    });
  });

  test('escalates supervised shutdown and waits for observed child exit', async () => {
    const server = createFakeAppServer();
    server.child.kill.mockImplementation((signal) => {
      if (signal === 'SIGKILL') queueMicrotask(() => server.child.emit('close', null, signal));
      return true;
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      processTerminationGraceMs: 5,
    });
    await collect(executeAdapter(adapter, executionContext()));

    await expect(adapter.close()).resolves.toEqual([]);
    expect(server.child.kill.mock.calls.map(([signal]) => signal)).toEqual(['SIGTERM', 'SIGKILL']);
  });

  test('fails closed when forced app-server termination is not observed', async () => {
    const server = createFakeAppServer();
    server.child.kill = jest.fn(() => true);
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      processTerminationGraceMs: 5,
    });
    await collect(executeAdapter(adapter, executionContext()));

    await expect(adapter.close()).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
      closedConversationIds: [],
    });
    expect(server.child.kill.mock.calls.map(([signal]) => signal)).toEqual(['SIGTERM', 'SIGKILL']);
  });

  test('fails closed when the app-server process exits but its supervised group survives', async () => {
    const server = createFakeAppServer();
    server.child.pid = 42_425;
    const signalProcessGroup = jest.fn((_processGroupId, _child, signal) => {
      if (signal === 'SIGTERM') queueMicrotask(() => server.child.emit('close', 0, signal));
      return true;
    });
    const adapter = createCodexAppServerAdapter({
      spawnProcess: () => server.child,
      processTerminationGraceMs: 5,
      signalProcessGroup,
      isProcessGroupAlive: () => true,
    });
    await collect(executeAdapter(adapter, executionContext()));

    await expect(adapter.close()).rejects.toMatchObject({
      providerError: { side_effect_status: 'unknown' },
      closedConversationIds: [],
    });
    expect(signalProcessGroup.mock.calls.map(([, , signal]) => signal))
      .toEqual(['SIGTERM', 'SIGKILL']);
  });

  test('keeps the shutdown barrier alive after the detached app-server leader exits', async () => {
    const helperPath = fileURLToPath(new URL(
      './helpers/codex-process-group-barrier-child.js',
      import.meta.url,
    ));
    const result = await new Promise((resolve, reject) => {
      const subprocess = spawnSubprocess(process.execPath, [helperPath], {
        cwd: process.cwd(),
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let stdout = '';
      let stderr = '';
      subprocess.stdout.on('data', (chunk) => { stdout += chunk.toString('utf8'); });
      subprocess.stderr.on('data', (chunk) => { stderr += chunk.toString('utf8'); });
      subprocess.once('error', reject);
      subprocess.once('close', (code, signal) => resolve({ code, signal, stderr, stdout }));
    });

    expect(result).toMatchObject({ code: 0, signal: null, stderr: '' });
    expect(result.stdout).toContain('PROCESS_GROUP_BARRIER_COMPLETED');
  });
});
