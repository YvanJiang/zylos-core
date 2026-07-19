import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';

export class CodexExecAdapterError extends Error {
  constructor(code, message, {
    exitCode = null,
    signal = null,
    providerError = null,
  } = {}) {
    super(message);
    this.name = 'CodexExecAdapterError';
    this.code = code;
    this.exitCode = exitCode;
    this.signal = signal;
    this.providerError = Object.freeze(providerError ?? providerErrorFor(code));
  }
}

function providerErrorFor(code) {
  if (code === 'provider_context_invalid') {
    return {
      code: 'provider_context_invalid',
      category: 'provider',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The provider conversation could not be resumed.',
    };
  }
  if (code === 'provider_interaction_unavailable') {
    return {
      code: 'unsupported_capability',
      category: 'provider',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The provider requested an interaction that this transport cannot expose.',
    };
  }
  return {
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    side_effect_status: 'unknown',
    user_message: 'The provider execution failed after side effects may have occurred.',
  };
}

function requireExecutionContext(context) {
  if (!context || typeof context !== 'object') {
    throw new TypeError('execution context must be an object');
  }
  if (!context.lineage || typeof context.lineage !== 'object') {
    throw new TypeError('execution context.lineage must be an object');
  }
  if (
    context.lineage.provider_native_id !== null
    && (
      typeof context.lineage.provider_native_id !== 'string'
      || context.lineage.provider_native_id.length === 0
    )
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
    || !Number.isInteger(context.attempt.attempt_no)
    || context.attempt.attempt_no < 1
    || !Number.isInteger(context.attempt.lease_epoch)
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

function sameAttempt(left, right) {
  return left.attempt_id === right.attempt_id
    && left.attempt_no === right.attempt_no
    && left.lease_epoch === right.lease_epoch;
}

function defaultSignalProcessGroup(pid, signal) {
  process.kill(-pid, signal);
}

function requireStringArray(name, values) {
  if (!Array.isArray(values) || values.some((value) => typeof value !== 'string')) {
    throw new TypeError(`${name} must be an array of strings`);
  }
  return Object.freeze([...values]);
}

function buildArguments(context, execOptions, resumeOptions) {
  const nativeId = context.lineage.provider_native_id;
  if (nativeId === null) return ['exec', ...execOptions, '--json', context.input.text];
  return [
    'exec',
    ...execOptions,
    'resume',
    ...resumeOptions,
    '--json',
    nativeId,
    context.input.text,
  ];
}

function parseJsonLine(line) {
  try {
    const value = JSON.parse(line);
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
    return value;
  } catch {
    throw new CodexExecAdapterError(
      'provider_protocol_invalid',
      'Codex emitted an invalid JSONL event.',
    );
  }
}

const TOOL_ITEMS = Object.freeze({
  command_execution: Object.freeze({ name: 'command', label: 'Command', sideEffect: 'unknown' }),
  file_change: Object.freeze({ name: 'file_change', label: 'File change', sideEffect: 'unknown' }),
  mcp_tool_call: Object.freeze({ name: 'external_tool', label: 'External tool', sideEffect: 'unknown' }),
  collab_tool_call: Object.freeze({ name: 'collaboration', label: 'Collaboration', sideEffect: 'unknown' }),
  web_search: Object.freeze({ name: 'web_search', label: 'Web search', sideEffect: 'none' }),
  todo_list: Object.freeze({ name: 'plan', label: 'Plan', sideEffect: 'none' }),
});

const TOOL_EVENT_KINDS = Object.freeze({
  'item.started': Object.freeze({ kind: 'tool_started', verb: 'started' }),
  'item.updated': Object.freeze({ kind: 'tool_progress', verb: 'running' }),
  'item.completed': Object.freeze({ kind: 'tool_finished', verb: 'completed' }),
});

function normalizeToolEvent(event, providerNativeId) {
  const specification = TOOL_ITEMS[event.item?.type];
  const lifecycle = TOOL_EVENT_KINDS[event.type];
  if (!specification || !lifecycle) return null;
  if (typeof event.item.id !== 'string' || event.item.id.length === 0) {
    throw new CodexExecAdapterError(
      'provider_protocol_invalid',
      'Codex emitted a tool item without an ID.',
    );
  }
  return {
    kind: lifecycle.kind,
    provider_native_id: providerNativeId,
    payload: {
      tool_use_id: event.item.id,
      tool_name: specification.name,
      summary: `${specification.label} ${lifecycle.verb}.`,
      side_effect_status: specification.sideEffect,
    },
  };
}

export function createCodexExecAdapter({
  codexExecutable = 'codex',
  spawnProcess = spawn,
  signalProcessGroup = defaultSignalProcessGroup,
  execOptions = [],
  resumeOptions = [],
  cwd,
  env = process.env,
} = {}) {
  if (typeof codexExecutable !== 'string' || codexExecutable.length === 0) {
    throw new TypeError('codexExecutable must be a non-empty string');
  }
  if (typeof spawnProcess !== 'function') throw new TypeError('spawnProcess must be a function');
  if (typeof signalProcessGroup !== 'function') {
    throw new TypeError('signalProcessGroup must be a function');
  }
  const validatedExecOptions = requireStringArray('execOptions', execOptions);
  const validatedResumeOptions = requireStringArray('resumeOptions', resumeOptions);
  const activeAttempts = new Map();

  async function* execute(context) {
    requireExecutionContext(context);
    if (activeAttempts.has(context.attempt.attempt_id)) {
      throw new CodexExecAdapterError(
        'provider_context_invalid',
        'A process is already active for this provider attempt.',
      );
    }
    const child = spawnProcess(
      codexExecutable,
      buildArguments(context, validatedExecOptions, validatedResumeOptions),
      {
      cwd,
      env,
      detached: true,
      shell: false,
      stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    if (!Number.isInteger(child.pid) || child.pid < 1 || !child.stdout) {
      throw new CodexExecAdapterError(
        'provider_execution_failed',
        'Codex process did not expose the required process handle.',
      );
    }
    child.stderr?.resume();
    const handle = {
      attempt: Object.freeze({ ...context.attempt }),
      child,
      processClosed: false,
      terminationReason: null,
    };
    activeAttempts.set(context.attempt.attempt_id, handle);
    const closed = new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (exitCode, signal) => {
        handle.processClosed = true;
        resolve({ exitCode, signal });
      });
    });
    const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
    let providerNativeId = context.lineage.provider_native_id;
    let threadStarted = false;
    let turnStarted = false;
    let turnCompleted = false;
    let textSnapshot = '';

    try {
      for await (const line of lines) {
        if (line.trim().length === 0) continue;
        const event = parseJsonLine(line);
        if (turnCompleted) {
          throw new CodexExecAdapterError(
            'provider_protocol_invalid',
            'Codex emitted JSONL output after turn completion.',
          );
        }
        if (event.type === 'thread.started') {
          if (threadStarted || turnStarted) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted thread.started out of order.',
            );
          }
          if (typeof event.thread_id !== 'string' || event.thread_id.length === 0) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted thread.started without a thread ID.',
            );
          }
          if (providerNativeId !== null && providerNativeId !== event.thread_id) {
            throw new CodexExecAdapterError(
              'provider_context_invalid',
              'Codex resumed a different provider lineage.',
            );
          }
          if (providerNativeId === null) {
            await context.bindProviderNativeId(event.thread_id);
            providerNativeId = event.thread_id;
          }
          threadStarted = true;
        } else if (event.type === 'turn.started') {
          if (!threadStarted || turnStarted) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted turn.started out of order.',
            );
          }
          turnStarted = true;
        } else if (event.type === 'item.completed' && event.item?.type === 'agent_message') {
          const text = event.item.text;
          if (
            !turnStarted
            || typeof text !== 'string'
            || text.length === 0
            || providerNativeId === null
          ) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted an invalid agent message.',
            );
          }
          textSnapshot = textSnapshot.length === 0 ? text : `${textSnapshot}\n\n${text}`;
          yield {
            kind: 'text_snapshot',
            provider_native_id: providerNativeId,
            payload: { text: textSnapshot, end_offset: textSnapshot.length },
          };
        } else if (event.type === 'turn.completed') {
          if (!turnStarted) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted turn.completed before turn.started.',
            );
          }
          turnCompleted = true;
        } else if (event.type in TOOL_EVENT_KINDS && providerNativeId !== null) {
          if (!turnStarted) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted a tool item before turn.started.',
            );
          }
          const toolEvent = normalizeToolEvent(event, providerNativeId);
          if (toolEvent) {
            yield toolEvent;
          } else if (!['reasoning', 'error', 'agent_message'].includes(event.item?.type)) {
            throw new CodexExecAdapterError(
              'provider_protocol_invalid',
              'Codex emitted an unsupported item type.',
            );
          }
        } else if (event.type === 'turn.failed' || event.type === 'error') {
          throw new CodexExecAdapterError(
            'provider_execution_failed',
            'Codex reported that execution failed.',
          );
        } else if (event.type === 'interaction.requested') {
          throw new CodexExecAdapterError(
            'provider_interaction_unavailable',
            'Codex exec JSONL cannot expose this interaction safely.',
          );
        } else {
          throw new CodexExecAdapterError(
            'provider_protocol_invalid',
            'Codex emitted an unsupported JSONL event type.',
          );
        }
      }

      const { exitCode, signal } = await closed;
      if (handle.terminationReason !== null) {
        throw new CodexExecAdapterError(
          'provider_attempt_terminated',
          `Codex process group was terminated for ${handle.terminationReason}.`,
          { exitCode, signal },
        );
      }
      if (exitCode !== 0 || !threadStarted || !turnStarted || !turnCompleted) {
        throw new CodexExecAdapterError(
          'provider_execution_failed',
          'Codex execution did not complete successfully.',
          { exitCode, signal },
        );
      }
    } catch (error) {
      if (!handle.processClosed && handle.terminationReason === null) {
        try {
          await signalProcessGroup(child.pid, 'SIGTERM');
        } catch {
          // The process may have exited between the stream failure and the group signal.
        }
      }
      throw error;
    } finally {
      if (activeAttempts.get(context.attempt.attempt_id) === handle) {
        activeAttempts.delete(context.attempt.attempt_id);
      }
    }
  }

  async function terminateAttempt({ attempt, reason }) {
    if (!attempt || typeof attempt !== 'object') throw new TypeError('attempt must be an object');
    if (!['stop', 'timeout', 'steer'].includes(reason)) {
      throw new TypeError('reason must be stop, timeout, or steer');
    }
    const handle = activeAttempts.get(attempt.attempt_id);
    if (!handle || !sameAttempt(handle.attempt, attempt) || handle.processClosed) {
      return Object.freeze({ status: 'not_current', reason });
    }
    if (handle.terminationReason === null) {
      handle.terminationReason = reason;
      await signalProcessGroup(handle.child.pid, 'SIGTERM');
    }
    return Object.freeze({ status: 'signalled', reason: handle.terminationReason });
  }

  return Object.freeze({ execute, terminateAttempt });
}
