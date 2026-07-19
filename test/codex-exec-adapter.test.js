import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

import { describe, expect, jest, test } from '@jest/globals';

import { createCodexExecAdapter } from '../runtime/providers/codex-exec-adapter.js';

function fakeChild(lines, { exitCode = 0, pid = 4102 } = {}) {
  const child = new EventEmitter();
  child.pid = pid;
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  queueMicrotask(() => {
    for (const line of lines) child.stdout.write(`${JSON.stringify(line)}\n`);
    child.stdout.end();
    child.stderr.end();
    child.emit('close', exitCode, null);
  });
  return child;
}

function controlledChild(pid = 4102) {
  const child = new EventEmitter();
  child.pid = pid;
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  return {
    child,
    close(exitCode = null, signal = 'SIGTERM') {
      child.stdout.end();
      child.stderr.end();
      child.emit('close', exitCode, signal);
    },
  };
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
    attempt: { attempt_id: 'attempt-1', attempt_no: 1, lease_epoch: 3 },
    ...overrides,
  };
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}

describe('Codex exec provider adapter', () => {
  test('starts a new lineage with exec --json and binds its first thread ID before output', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      {
        type: 'item.completed',
        item: { id: 'message-1', type: 'agent_message', text: 'Hello from Codex' },
      },
      { type: 'turn.completed', usage: { input_tokens: 4, output_tokens: 3 } },
    ]);
    const spawnProcess = jest.fn(() => child);
    const context = executionContext();
    const adapter = createCodexExecAdapter({ spawnProcess });

    await expect(collect(adapter.execute(context))).resolves.toEqual([
      {
        kind: 'text_snapshot',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Hello from Codex', end_offset: 16 },
      },
    ]);

    expect(spawnProcess).toHaveBeenCalledWith(
      'codex',
      ['exec', '--json', 'Hello Codex'],
      expect.objectContaining({
        detached: true,
        shell: false,
        stdio: ['ignore', 'pipe', 'pipe'],
      }),
    );
    expect(context.bindProviderNativeId).toHaveBeenCalledTimes(1);
    expect(context.bindProviderNativeId).toHaveBeenCalledWith('codex-thread-1');
    expect(context.bindProviderNativeId.mock.invocationCallOrder[0])
      .toBeLessThan(spawnProcess.mock.invocationCallOrder[0] + 2);
  });

  test('resumes the persisted provider lineage without rebinding it', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      {
        type: 'item.completed',
        item: { id: 'message-2', type: 'agent_message', text: 'Resumed answer' },
      },
      { type: 'turn.completed', usage: { input_tokens: 8, output_tokens: 2 } },
    ]);
    const spawnProcess = jest.fn(() => child);
    const context = executionContext({
      input: { kind: 'text', text: 'Follow up', attachments: [] },
      lineage: { provider_native_id: 'codex-thread-1' },
    });
    const adapter = createCodexExecAdapter({
      spawnProcess,
      execOptions: ['--sandbox', 'read-only'],
      resumeOptions: ['--model', 'gpt-test'],
    });

    await expect(collect(adapter.execute(context))).resolves.toEqual([
      {
        kind: 'text_snapshot',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Resumed answer', end_offset: 14 },
      },
    ]);

    expect(spawnProcess).toHaveBeenCalledWith(
      'codex',
      [
        'exec',
        '--sandbox',
        'read-only',
        'resume',
        '--model',
        'gpt-test',
        '--json',
        'codex-thread-1',
        'Follow up',
      ],
      expect.any(Object),
    );
    expect(context.bindProviderNativeId).not.toHaveBeenCalled();
  });

  test('normalizes cumulative text and tool lifecycle without exposing Codex event names', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      {
        type: 'item.completed',
        item: { id: 'message-1', type: 'agent_message', text: 'Working' },
      },
      {
        type: 'item.started',
        item: {
          id: 'command-1',
          type: 'command_execution',
          command: 'printf private',
          status: 'in_progress',
        },
      },
      {
        type: 'item.updated',
        item: {
          id: 'command-1',
          type: 'command_execution',
          command: 'printf private',
          aggregated_output: 'private output',
          status: 'in_progress',
        },
      },
      {
        type: 'item.completed',
        item: {
          id: 'command-1',
          type: 'command_execution',
          command: 'printf private',
          aggregated_output: 'private output',
          exit_code: 0,
          status: 'completed',
        },
      },
      {
        type: 'item.completed',
        item: { id: 'message-2', type: 'agent_message', text: 'Done' },
      },
      { type: 'turn.completed', usage: { input_tokens: 8, output_tokens: 2 } },
    ]);
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });

    const events = await collect(adapter.execute(executionContext()));

    expect(events).toEqual([
      {
        kind: 'text_snapshot',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Working', end_offset: 7 },
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
        payload: { text: 'Working\n\nDone', end_offset: 13 },
      },
    ]);
    expect(JSON.stringify(events)).not.toMatch(/item\.completed|command_execution|private output/);
  });

  test('keeps a non-fatal Codex error item private when the turn still completes', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      {
        type: 'item.completed',
        item: { id: 'warning-1', type: 'error', message: 'private warning detail' },
      },
      {
        type: 'item.completed',
        item: { id: 'message-1', type: 'agent_message', text: 'Safe result' },
      },
      { type: 'turn.completed', usage: { input_tokens: 4, output_tokens: 2 } },
    ]);
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });

    await expect(collect(adapter.execute(executionContext()))).resolves.toEqual([
      {
        kind: 'text_snapshot',
        provider_native_id: 'codex-thread-1',
        payload: { text: 'Safe result', end_offset: 11 },
      },
    ]);
  });

  test.each([
    [
      'turn.failed',
      { type: 'turn.failed', error: { message: 'private failure detail' } },
    ],
    [
      'error',
      { type: 'error', message: 'private top-level failure detail' },
    ],
  ])('converts %s into safe provider error metadata', async (_name, failureEvent) => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      failureEvent,
    ], { exitCode: 1 });
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });

    await expect(collect(adapter.execute(executionContext()))).rejects.toMatchObject({
      code: 'provider_execution_failed',
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider execution failed after side effects may have occurred.',
      },
    });
  });

  test('rejects a resumed thread mismatch as a provider context error', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'different-thread' },
    ], { exitCode: 1 });
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });
    const context = executionContext({
      lineage: { provider_native_id: 'codex-thread-1' },
    });

    await expect(collect(adapter.execute(context))).rejects.toMatchObject({
      code: 'provider_context_invalid',
      providerError: {
        code: 'provider_context_invalid',
        category: 'provider',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'The provider conversation could not be resumed.',
      },
    });
  });

  test('fails closed when an unexposed interaction event appears', async () => {
    const child = fakeChild([
      { type: 'thread.started', thread_id: 'codex-thread-1' },
      { type: 'turn.started' },
      { type: 'interaction.requested', interaction_id: 'private-interaction-1' },
    ], { exitCode: 1 });
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });

    await expect(collect(adapter.execute(executionContext()))).rejects.toMatchObject({
      code: 'provider_interaction_unavailable',
      providerError: {
        code: 'unsupported_capability',
        category: 'provider',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'The provider requested an interaction that this transport cannot expose.',
      },
    });
  });

  test.each([
    [
      'missing thread start',
      [
        { type: 'turn.started' },
        { type: 'turn.completed', usage: {} },
      ],
    ],
    [
      'output after completion',
      [
        { type: 'thread.started', thread_id: 'codex-thread-1' },
        { type: 'turn.started' },
        { type: 'turn.completed', usage: {} },
        {
          type: 'item.completed',
          item: { id: 'message-late', type: 'agent_message', text: 'late output' },
        },
      ],
    ],
  ])('rejects invalid JSONL ordering: %s', async (_name, lines) => {
    const child = fakeChild(lines);
    const adapter = createCodexExecAdapter({ spawnProcess: () => child });

    await expect(collect(adapter.execute(executionContext()))).rejects.toMatchObject({
      code: 'provider_protocol_invalid',
    });
  });

  test.each(['stop', 'timeout', 'steer'])(
    '%s terminates only the exact current process group and preserves lineage',
    async (reason) => {
      const process = controlledChild();
      const signalProcessGroup = jest.fn();
      const context = executionContext({
        lineage: { provider_native_id: 'codex-thread-1' },
      });
      const adapter = createCodexExecAdapter({
        spawnProcess: () => process.child,
        signalProcessGroup,
      });
      const execution = collect(adapter.execute(context));
      await new Promise((resolve) => setImmediate(resolve));

      await expect(adapter.terminateAttempt({
        attempt: context.attempt,
        reason,
      })).resolves.toEqual({ status: 'signalled', reason });
      expect(signalProcessGroup).toHaveBeenCalledTimes(1);
      expect(signalProcessGroup).toHaveBeenCalledWith(4102, 'SIGTERM');
      expect(context.bindProviderNativeId).not.toHaveBeenCalled();

      await expect(adapter.terminateAttempt({
        attempt: { ...context.attempt, lease_epoch: 4 },
        reason,
      })).resolves.toEqual({ status: 'not_current', reason });
      expect(signalProcessGroup).toHaveBeenCalledTimes(1);

      process.close();
      await expect(execution).rejects.toMatchObject({
        code: 'provider_attempt_terminated',
      });
    },
  );
});
