import fs from 'node:fs';
import { EventEmitter } from 'node:events';
import os from 'node:os';
import path from 'node:path';
import { PassThrough } from 'node:stream';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateNormalizedEvent,
} from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { createCodexAppServerAdapter } from '../runtime/providers/codex-app-server-adapter.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-executor-service-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function normalEnvelope(suffix = 'canonical') {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = JSON.parse(JSON.stringify(fixture));
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function acceptQueuedTurn(database, suffix = 'canonical') {
  return acceptNormalInbound(database, normalEnvelope(suffix), {
    now: () => '2026-07-19T07:00:00Z',
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

function stopActiveConversation(service, accepted, suffix = accepted.turn_id) {
  return service.stop({
    conversation_id: accepted.conversation_id,
    stop_id: `stop-${suffix}`,
  });
}

function malformedTurnStartAppServer() {
  const child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.kill = (signal) => {
    if (signal === 'SIGTERM') queueMicrotask(() => child.emit('close', 0, signal));
    return true;
  };
  let buffer = '';
  const send = (message) => child.stdout.write(`${JSON.stringify(message)}\n`);
  child.stdin.on('data', (chunk) => {
    buffer += chunk.toString('utf8');
    while (buffer.includes('\n')) {
      const newline = buffer.indexOf('\n');
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.length === 0) continue;
      const message = JSON.parse(line);
      if (message.method === 'initialize') {
        send({ id: message.id, result: { userAgent: 'codex-test' } });
      } else if (message.method === 'thread/start') {
        send({ id: message.id, result: { thread: { id: 'codex-thread-malformed' } } });
      } else if (message.method === 'turn/start') {
        send({ id: message.id, result: { turn: { status: 'inProgress', items: [] } } });
      }
    }
  });
  return child;
}

function readEvents(database, turnId) {
  return database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence ASC
  `).all(turnId).map(({ event_json: eventJson }) => JSON.parse(eventJson));
}

function readAuthority(database, turnId, conversationId) {
  return {
    turn: database.prepare(`
      SELECT state, turn_version, attempt_id, attempt_no, lease_epoch
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(turnId),
    queue: database.prepare(`
      SELECT status
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(turnId),
    lease: database.prepare(`
      SELECT lease_owner, lease_epoch, turn_id, attempt_id, attempt_no
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(conversationId) ?? null,
    event_count: database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_normalized_events
      WHERE turn_id = ?
    `).get(turnId).count,
    outbox_count: database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_outbox
      WHERE turn_id = ?
    `).get(turnId).count,
    outbox: database.prepare(`
      SELECT aggregate_type, aggregate_version, status, command_json
      FROM runtime_outbox
      WHERE turn_id = ?
      ORDER BY aggregate_type ASC, aggregate_version ASC
    `).all(turnId),
    projections: database.prepare(`
      SELECT aggregate_version, event_sequence_through, critical, terminal,
        status, render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version ASC
    `).all(turnId),
  };
}

function createTestStore(database, namespace = 'store') {
  return createExecutorStore({
    database,
    provider: 'claude',
    serviceInstanceId: `executor-service-${namespace}`,
    now: () => '2026-07-19T07:02:00Z',
    generateId: deterministicIds(namespace),
  });
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('runtime executor service', () => {
  test('atomically binds a provider-native lineage before persisting provider output', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'native-lineage');
    const adapter = {
      async *execute(context) {
        expect(context.lineage).toEqual({ provider_native_id: null });
        await context.bindProviderNativeId('native-thread-1');
        yield {
          kind: 'text_snapshot',
          payload: { text: 'bound output', end_offset: 12 },
          provider_native_id: 'native-thread-1',
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-native-lineage',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('native-lineage'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'completed' });

    expect(database.prepare(`
      SELECT provider, provider_native_id, provider_native_id_bound_at
      FROM runtime_lineages
      WHERE lineage_id = ?
    `).get(accepted.lineage_id)).toEqual({
      provider: 'codex',
      provider_native_id: 'native-thread-1',
      provider_native_id_bound_at: '2026-07-19T07:01:00Z',
    });
    expect(readEvents(database, accepted.turn_id)
      .filter(({ kind }) => kind === 'text_snapshot'))
      .toEqual([
        expect.objectContaining({ provider_native_id: 'native-thread-1' }),
      ]);
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      kind: 'turn_state_changed',
      phase: 'completed',
      provider_native_id: 'native-thread-1',
    });

    database.close();
  });

  test('supplies the persisted provider-native ID to the next turn on the same lineage', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'persisted-resume-first');
    const second = acceptQueuedTurn(database, 'persisted-resume-second');
    const observedLineages = [];
    const adapter = {
      async *execute(context) {
        observedLineages.push(context.lineage);
        if (context.lineage.provider_native_id === null) {
          await context.bindProviderNativeId('native-thread-resume');
        }
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-persisted-resume',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('persisted-resume'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(observedLineages).toEqual([
      { provider_native_id: null },
      { provider_native_id: 'native-thread-resume' },
    ]);
    expect(first.lineage_id).toBe(second.lineage_id);

    database.close();
  });

  test('moves a waiting interaction to fenced recovery when its provider transport is lost', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'waiting-transport-loss');
    let reportProviderFailure;
    const adapter = {
      async *execute(context) {
        reportProviderFailure = context.reportProviderFailure;
        await context.bindProviderNativeId('native-thread-lost');
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-interaction-lost',
            tool_use_id: 'tool-lost',
            kind: 'tool_approval',
            prompt: 'Allow the pending provider action?',
            choices: [],
            authorized_subjects: context.interaction.authorized_subjects,
            allowed_sources: context.interaction.allowed_sources,
          },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-waiting-transport-loss',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('waiting-transport-loss'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    expect(reportProviderFailure).toEqual(expect.any(Function));
    expect(reportProviderFailure({
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The app-server connection was lost.',
      },
    })).toMatchObject({ status: 'recovering', turn_id: accepted.turn_id });

    const interactionRow = database.prepare(`
      SELECT state, handoff_state, request_json
      FROM runtime_interactions
      WHERE turn_id = ?
    `).get(accepted.turn_id);
    expect(interactionRow).toMatchObject({ state: 'cancelled', handoff_state: 'not_started' });
    expect(JSON.parse(interactionRow.request_json)).toMatchObject({
      state: 'cancelled',
      terminal_reason: 'provider_connection_lost',
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(readEvents(database, accepted.turn_id).slice(-3).map(({ kind, phase }) => ({ kind, phase })))
      .toEqual([
        { kind: 'interaction_cancelled', phase: 'waiting_user' },
        { kind: 'turn_state_changed', phase: 'recovering' },
        { kind: 'recovery_started', phase: 'recovering' },
      ]);

    database.close();
  });

  test('rejects an interaction descriptor when transport loss wins before durable persistence', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'interaction-transport-race');
    const providerFailure = {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The app-server connection was lost.',
      },
    };
    const adapter = {
      execute(context) {
        let delivered = false;
        return {
          [Symbol.asyncIterator]() { return this; },
          async next() {
            if (delivered) return { done: true, value: undefined };
            delivered = true;
            await context.bindProviderNativeId('native-thread-race');
            context.reportProviderFailure(providerFailure);
            return {
              done: false,
              value: {
                kind: 'interaction_requested',
                payload: {
                  provider_interaction_ref: 'provider-interaction-race',
                  tool_use_id: 'tool-race',
                  kind: 'tool_approval',
                  prompt: 'This descriptor must not become durable.',
                  choices: [],
                  authorized_subjects: context.interaction.authorized_subjects,
                  allowed_sources: context.interaction.allowed_sources,
                },
              },
            };
          },
          async return() { return { done: true, value: undefined }; },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-interaction-transport-race',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('interaction-transport-race'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_interactions WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-interaction-transport-race',
      turn_id: accepted.turn_id,
    });

    database.close();
  });

  test('retains the writer lease when a started provider loses an uncertain transport', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'running-transport-loss');
    const providerFailure = Object.assign(new Error('private connection failure'), {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The app-server connection was lost.',
      },
    });
    const adapter = {
      execute(context) {
        return {
          [Symbol.asyncIterator]() { return this; },
          async next() {
            context.reportProviderState({ state: 'started', provider_native_id: null });
            context.reportProviderFailure(providerFailure);
            throw providerFailure;
          },
          async return() { return { done: true, value: undefined }; },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-running-transport-loss',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('running-transport-loss'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id, attempt_id
      FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual(expect.objectContaining({
      lease_owner: 'executor-service-running-transport-loss',
      turn_id: accepted.turn_id,
      attempt_id: expect.any(String),
    }));
    expect(readEvents(database, accepted.turn_id).slice(-2)).toEqual([
      expect.objectContaining({
        kind: 'turn_state_changed',
        phase: 'recovering',
        payload: expect.objectContaining({ reason_code: 'provider_connection_lost' }),
      }),
      expect.objectContaining({
        kind: 'recovery_started',
        phase: 'recovering',
        error: expect.objectContaining({ side_effect_status: 'unknown' }),
      }),
    ]);

    database.close();
  });

  test('retains authority when durable provider-loss recovery cannot be persisted', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'transport-loss-persistence-failure');
    const providerFailure = Object.assign(new Error('private connection failure'), {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The app-server connection was lost.',
      },
    });
    const adapter = {
      execute(context) {
        return {
          [Symbol.asyncIterator]() { return this; },
          async next() {
            database.exec(`
              CREATE TRIGGER fail_provider_loss_recovery_projection
              BEFORE INSERT ON runtime_projection_snapshots
              WHEN json_extract(NEW.render_model_json, '$.phase') = 'recovering'
              BEGIN
                SELECT RAISE(ABORT, 'forced provider loss recovery failure');
              END;
            `);
            try {
              context.reportProviderFailure(providerFailure);
            } catch {
              // The transport then closes its iterator with the original provider error.
            }
            throw providerFailure;
          },
          async return() { return { done: true, value: undefined }; },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-transport-loss-persistence-failure',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('transport-loss-persistence-failure'),
    });

    await expect(service.runNext()).rejects.toThrow(/forced provider loss recovery failure/);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'starting' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-transport-loss-persistence-failure',
      turn_id: accepted.turn_id,
    });

    database.close();
  });

  test('retains authority when app-server accepts turn/start without a usable turn ID', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'malformed-turn-start-response');
    const child = malformedTurnStartAppServer();
    const adapter = createCodexAppServerAdapter({ spawnProcess: () => child });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-malformed-turn-start-response',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('malformed-turn-start-response'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-malformed-turn-start-response',
      turn_id: accepted.turn_id,
    });

    database.close();
  });

  test('maps a confirmed Codex cancellation terminal to stopped and preserves lineage', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'codex-cancel-terminal');
    let rejectTerminal;
    const terminal = new Promise((resolve, reject) => { rejectTerminal = reject; });
    terminal.catch(() => {});
    const interrupted = Object.assign(new Error('provider turn interrupted'), {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider turn was interrupted.',
      },
    });
    const adapter = {
      async *execute(context) {
        await context.bindProviderNativeId('codex-thread-cancel-terminal');
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-cancel-terminal',
            tool_use_id: 'tool-cancel-terminal',
            kind: 'tool_approval',
            prompt: 'Allow the pending action?',
            choices: [],
            authorized_subjects: context.interaction.authorized_subjects,
            allowed_sources: context.interaction.allowed_sources,
          },
        };
        await terminal;
      },
      async cancel() {
        rejectTerminal(interrupted);
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-codex-cancel-terminal',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('codex-cancel-terminal'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    await expect(stopActiveConversation(service, accepted)).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: accepted.turn_id },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'stopped' });
    expect(database.prepare(`
      SELECT state FROM runtime_interactions WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'cancelled' });
    expect(database.prepare(`
      SELECT provider, provider_native_id
      FROM runtime_lineages
      WHERE lineage_id = ?
    `).get(accepted.lineage_id)).toEqual({
      provider: 'codex',
      provider_native_id: 'codex-thread-cancel-terminal',
    });

    database.close();
  });

  test('rolls back a failed first lineage binding and rejects stale or conflicting bindings', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'native-binding-fence');
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId: 'executor-service-native-binding-fence',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('native-binding-fence'),
    });
    const turnContext = store.claimNextQueuedTurn();
    store.transitionTurn(turnContext, 'starting', 'running');
    const staleContext = {
      ...turnContext,
      attempt: { ...turnContext.attempt, lease_epoch: turnContext.attempt.lease_epoch + 1 },
    };
    const readBinding = () => database.prepare(`
      SELECT provider, provider_native_id, provider_native_id_bound_at
      FROM runtime_lineages
      WHERE lineage_id = ?
    `).get(accepted.lineage_id);

    expect(() => store.bindProviderNativeId(staleContext, 'native-thread-stale'))
      .toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readBinding()).toEqual({
      provider: null,
      provider_native_id: null,
      provider_native_id_bound_at: null,
    });

    database.exec(`
      CREATE TRIGGER force_native_lineage_binding_failure
      AFTER UPDATE OF provider_native_id ON runtime_lineages
      BEGIN
        SELECT RAISE(ABORT, 'forced native lineage binding failure');
      END;
    `);
    expect(() => store.bindProviderNativeId(turnContext, 'native-thread-1'))
      .toThrow(/forced native lineage binding failure/);
    expect(readBinding()).toEqual({
      provider: null,
      provider_native_id: null,
      provider_native_id_bound_at: null,
    });

    database.exec('DROP TRIGGER force_native_lineage_binding_failure');
    expect(store.bindProviderNativeId(turnContext, 'native-thread-1')).toEqual({
      provider: 'codex',
      provider_native_id: 'native-thread-1',
      newly_bound: true,
    });
    expect(store.bindProviderNativeId(turnContext, 'native-thread-1')).toEqual({
      provider: 'codex',
      provider_native_id: 'native-thread-1',
      newly_bound: false,
    });
    expect(() => store.bindProviderNativeId(turnContext, 'native-thread-other'))
      .toThrow(expect.objectContaining({ code: 'provider_context_invalid' }));
    expect(readBinding()).toEqual({
      provider: 'codex',
      provider_native_id: 'native-thread-1',
      provider_native_id_bound_at: '2026-07-19T07:02:00Z',
    });
    const authorityAfterBinding = readAuthority(
      database,
      accepted.turn_id,
      accepted.conversation_id,
    );
    for (const providerNativeId of [null, 'native-thread-other']) {
      expect(() => store.appendAdapterEvent(turnContext, {
        kind: 'text_snapshot',
        payload: { text: 'wrong lineage output', end_offset: 20 },
        provider_native_id: providerNativeId,
      })).toThrow(expect.objectContaining({ code: 'provider_context_invalid' }));
      expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
        .toEqual(authorityAfterBinding);
    }

    database.close();
  });

  test('normalizes provider failure into a fenced terminal state and public error', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'provider-failure');
    const adapter = {
      async *execute() {
        const error = new Error('private provider stderr must not escape');
        error.providerError = {
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider execution failed after side effects may have occurred.',
        };
        throw error;
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-provider-failure',
      now: () => '2026-07-19T07:05:00Z',
      generateId: deterministicIds('provider-failure'),
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
    expect(database.prepare(`
      SELECT lease_owner, turn_id, attempt_id
      FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: null,
      turn_id: null,
      attempt_id: null,
    });
    const terminalEvent = readEvents(database, accepted.turn_id).at(-1);
    expect(terminalEvent).toMatchObject({
      kind: 'turn_state_changed',
      phase: 'failed',
      payload: {
        from_state: 'starting',
        to_state: 'failed',
        reason_code: 'executor_failed',
      },
      error: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider execution failed after side effects may have occurred.',
        occurred_at: '2026-07-19T07:05:00Z',
      },
    });
    expect(JSON.stringify(terminalEvent)).not.toContain('private provider stderr');
    const failureProjection = database.prepare(`
      SELECT critical, terminal, status, render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(accepted.turn_id);
    expect(failureProjection).toMatchObject({
      critical: 1,
      terminal: 1,
      status: 'staged',
    });
    expect(JSON.parse(failureProjection.render_model_json)).toMatchObject({
      phase: 'failed',
      terminal: true,
      error: terminalEvent.error,
    });

    database.close();
  });

  test('keeps the turn starting until a provider-neutral started signal is persisted', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'provider-start-confirmation');
    const observedStates = [];
    const adapter = {
      async *execute(context) {
        observedStates.push(database.prepare(`
          SELECT state FROM runtime_turns WHERE turn_id = ?
        `).get(accepted.turn_id).state);
        context.reportProviderState({ state: 'started', provider_native_id: null });
        observedStates.push(database.prepare(`
          SELECT state FROM runtime_turns WHERE turn_id = ?
        `).get(accepted.turn_id).state);
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-provider-start-confirmation',
      now: () => '2026-07-19T07:05:15Z',
      generateId: deterministicIds('provider-start-confirmation'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: accepted.turn_id,
    });
    expect(observedStates).toEqual(['starting', 'running']);
    expect(readEvents(database, accepted.turn_id).filter(({ kind }) => (
      kind === 'turn_state_changed'
    )).slice(-2)).toEqual([
      expect.objectContaining({
        phase: 'running',
        payload: expect.objectContaining({ reason_code: 'provider_started' }),
      }),
      expect.objectContaining({ phase: 'completed' }),
    ]);

    database.close();
  });

  test('propagates adapter-event persistence failure without committing provider failure', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'adapter-persistence-failure');
    const adapter = {
      async *execute() {
        database.exec(`
          CREATE TRIGGER force_service_adapter_projection_failure
          BEFORE INSERT ON runtime_projection_snapshots
          WHEN json_extract(NEW.render_model_json, '$.phase') = 'running'
            AND json_extract(NEW.render_model_json, '$.text') = 'must roll back'
          BEGIN
            SELECT RAISE(ABORT, 'forced service adapter projection failure');
          END
        `);
        yield {
          kind: 'text_snapshot',
          provider_native_id: null,
          payload: { text: 'must roll back', end_offset: 14 },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-adapter-persistence-failure',
      now: () => '2026-07-19T07:05:30Z',
      generateId: deterministicIds('adapter-persistence-failure'),
    });

    await expect(service.runNext()).rejects.toMatchObject({
      message: expect.stringMatching(/forced service adapter projection failure/),
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'running' });
    expect(database.prepare(`
      SELECT status FROM runtime_turn_queue WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'claimed' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id, attempt_id
      FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual(expect.objectContaining({
      lease_owner: 'executor-service-adapter-persistence-failure',
      turn_id: accepted.turn_id,
    }));
    expect(readEvents(database, accepted.turn_id))
      .not.toEqual(expect.arrayContaining([
        expect.objectContaining({ kind: 'text_snapshot' }),
      ]));

    database.close();
  });

  test('runs one durable queued turn through a provider-neutral adapter to completion', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database);
    const adapterCalls = [];
    const adapter = {
      async *execute(context) {
        adapterCalls.push(context);
        yield {
          kind: 'text_snapshot',
          payload: {
            text: 'provider-neutral result',
            end_offset: 23,
          },
          provider_native_id: null,
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-A',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('executor'),
    });

    const initialSnapshot = service.start();
    expect(initialSnapshot.executors).toEqual([
      {
        conversation_id: accepted.conversation_id,
        active_turn_id: null,
        queued_turn_ids: [accepted.turn_id],
        wait_reason: null,
      },
    ]);

    await expect(service.runNext()).resolves.toEqual({
      status: 'completed',
      conversation_id: accepted.conversation_id,
      turn_id: accepted.turn_id,
      attempt_id: 'attempt-executor-1',
      attempt_no: 1,
      lease_epoch: 1,
    });

    expect(adapterCalls).toEqual([
      {
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
        lineage_id: accepted.lineage_id,
        provider_native_id: null,
        trace_id: 'trace-canonical',
        input: normalEnvelope().content,
        interaction: {
          authorized_subjects: [{ type: 'actor', actor_id: 'user-A' }],
          allowed_sources: ['main_card_reply', 'card_action'],
        },
        lineage: { provider_native_id: null },
        bindProviderNativeId: expect.any(Function),
        reportProviderFailure: expect.any(Function),
        reportProviderState: expect.any(Function),
        attempt: {
          attempt_id: 'attempt-executor-1',
          attempt_no: 1,
          lease_epoch: 1,
        },
      },
    ]);
    expect(service.snapshot().executors).toEqual([]);

    expect(database.prepare(`
      SELECT state, turn_version, attempt_id, attempt_no, lease_epoch
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({
      state: 'completed',
      turn_version: 6,
      attempt_id: 'attempt-executor-1',
      attempt_no: 1,
      lease_epoch: 1,
    });
    expect(database.prepare(`
      SELECT status
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'completed' });
    expect(database.prepare(`
      SELECT lease_owner, lease_epoch, turn_id, attempt_id, attempt_no
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: null,
      lease_epoch: 1,
      turn_id: null,
      attempt_id: null,
      attempt_no: null,
    });

    const events = readEvents(database, accepted.turn_id);
    expect(events.map((event) => event.event_sequence)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(events.map((event) => event.turn_version)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(events.map((event) => event.phase)).toEqual([
      'received',
      'queued',
      'starting',
      'running',
      'running',
      'completed',
    ]);
    expect(events.map((event) => event.kind)).toEqual([
      'turn_state_changed',
      'turn_state_changed',
      'turn_state_changed',
      'turn_state_changed',
      'text_snapshot',
      'turn_state_changed',
    ]);
    for (const event of events) {
      expect(validateNormalizedEvent(event).forwarded).toEqual(event);
    }
    expect(events.slice(2).map((event) => ({
      attempt_id: event.attempt_id,
      attempt_no: event.attempt_no,
      lease_epoch: event.lease_epoch,
    }))).toEqual(Array.from({ length: 4 }, () => ({
      attempt_id: 'attempt-executor-1',
      attempt_no: 1,
      lease_epoch: 1,
    })));

    const turnOutbox = database.prepare(`
      SELECT aggregate_version, command_json
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
    `).get(accepted.turn_id);
    expect(turnOutbox.aggregate_version).toBe(1);
    const deliveryCommand = JSON.parse(turnOutbox.command_json);
    expect(validateDeliveryCommand(deliveryCommand).forwarded).toEqual(deliveryCommand);
    expect(deliveryCommand).toEqual(expect.objectContaining({
      aggregate_type: 'turn_main',
      aggregate_id: accepted.turn_id,
      operation: 'create_main',
      aggregate_version: 1,
      event_sequence_through: 1,
      render_model: expect.objectContaining({
        phase: 'received',
        terminal: false,
      }),
    }));
    const projections = database.prepare(`
      SELECT aggregate_version, event_sequence_through, critical, terminal,
        status, render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version
    `).all(accepted.turn_id);
    expect(projections.map((projection) => ({
      aggregate_version: projection.aggregate_version,
      status: projection.status,
      critical: projection.critical,
      terminal: projection.terminal,
    }))).toEqual([
      { aggregate_version: 2, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 3, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 4, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 5, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 6, status: 'staged', critical: 1, terminal: 1 },
    ]);
    expect(JSON.parse(projections.at(-1).render_model_json)).toMatchObject({
      phase: 'completed',
      text: 'provider-neutral result',
      terminal: true,
    });

    database.close();
  });

  test('keeps a turn durably queued with a visible capacity reason without duplicate execution', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'capacity-first');
    const secondEnvelope = normalEnvelope('capacity-second');
    secondEnvelope.chat_id = 'chat-capacity-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T07:00:01Z',
      generateId: deterministicIds('inbound-capacity-second'),
    });
    const thirdEnvelope = normalEnvelope('capacity-third');
    thirdEnvelope.chat_id = 'chat-capacity-third';
    const third = acceptNormalInbound(database, thirdEnvelope, {
      now: () => '2026-07-19T07:00:02Z',
      generateId: deterministicIds('inbound-capacity-third'),
    });
    const adapterCalls = [];
    let markFirstStarted;
    const firstStarted = new Promise((resolve) => { markFirstStarted = resolve; });
    let releaseFirst;
    const firstCanFinish = new Promise((resolve) => { releaseFirst = resolve; });
    const adapter = {
      async *execute(context) {
        adapterCalls.push(context.turn_id);
        if (context.turn_id === first.turn_id) {
          markFirstStarted();
          await firstCanFinish;
        }
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-capacity',
      now: () => '2026-07-19T07:01:00Z',
      generateId: deterministicIds('capacity'),
      maxResidentExecutorsPerBot: 1,
    });
    const competingService = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-capacity-competing',
      now: () => '2026-07-19T07:01:01Z',
      generateId: deterministicIds('capacity-competing'),
      maxResidentExecutorsPerBot: 1,
    });

    const firstRun = service.runNext();
    await firstStarted;
    await expect(competingService.runNext()).resolves.toEqual({
      status: 'capacity_wait',
      conversation_id: second.conversation_id,
      turn_id: second.turn_id,
      wait_reason: 'executor_capacity',
    });
    expect(adapterCalls).toEqual([first.turn_id]);
    expect(database.prepare(`
      SELECT turn.state, turn.attempt_id, queue.status, queue.wait_reason
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(second.turn_id)).toEqual({
      state: 'queued',
      attempt_id: null,
      status: 'queued',
      wait_reason: 'executor_capacity',
    });
    expect(database.prepare(`
      SELECT turn_id, wait_reason
      FROM runtime_turn_queue
      WHERE turn_id IN (?, ?)
      ORDER BY turn_id ASC
    `).all(second.turn_id, third.turn_id)).toEqual([
      { turn_id: second.turn_id, wait_reason: 'executor_capacity' },
      { turn_id: third.turn_id, wait_reason: 'executor_capacity' },
    ]);
    const capacityEvents = readEvents(database, second.turn_id);
    expect(capacityEvents).toHaveLength(3);
    expect(validateNormalizedEvent(capacityEvents[2]).forwarded).toEqual(capacityEvents[2]);
    expect(capacityEvents[2]).toMatchObject({
      attempt_id: null,
      attempt_no: null,
      lease_epoch: null,
      provider: null,
      phase: 'queued',
      payload: {
        from_state: 'queued',
        to_state: 'queued',
        reason_code: 'executor_capacity',
      },
    });
    const capacityProjection = database.prepare(`
      SELECT aggregate_version, event_sequence_through, render_model_json, status
      FROM runtime_projection_snapshots
      WHERE turn_id = ? AND aggregate_version = 3
    `).get(second.turn_id);
    expect(capacityProjection).toMatchObject({
      aggregate_version: 3,
      event_sequence_through: 3,
      status: 'staged',
    });
    expect(JSON.parse(capacityProjection.render_model_json)).toMatchObject({
      phase: 'queued',
      text: 'Waiting for executor capacity.',
      terminal: false,
    });

    const deliveryService = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-capacity',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('delivery-capacity'),
      throttleMs: 0,
    });
    let capacityDelivery = null;
    while (capacityDelivery === null) {
      const command = deliveryService.claimNext();
      expect(command).not.toBeNull();
      if (!command) break;
      expect(deliveryService.recordResult(deliveredResult(
        command,
        '2026-07-19T07:02:00Z',
      ))).toEqual({
        status: 'applied',
        outbox_status: 'delivered',
      });
      if (
        command.operation === 'update_main'
        && command.mapping.turn_id === second.turn_id
      ) capacityDelivery = command;
    }
    expect(capacityDelivery).toBeDefined();
    expect(validateDeliveryCommand(capacityDelivery).forwarded).toEqual(capacityDelivery);
    expect(capacityDelivery).toMatchObject({
      operation: 'update_main',
      aggregate_version: 3,
      event_sequence_through: 3,
      render_model: {
        phase: 'queued',
        text: 'Waiting for executor capacity.',
        terminal: false,
      },
    });
    expect(service.snapshot().executors).toEqual([
      {
        conversation_id: first.conversation_id,
        active_turn_id: first.turn_id,
        queued_turn_ids: [],
        wait_reason: null,
      },
      {
        conversation_id: second.conversation_id,
        active_turn_id: null,
        queued_turn_ids: [second.turn_id],
        wait_reason: 'executor_capacity',
      },
      {
        conversation_id: third.conversation_id,
        active_turn_id: null,
        queued_turn_ids: [third.turn_id],
        wait_reason: 'executor_capacity',
      },
    ]);
    expect(database.prepare(`
      SELECT conversation_id, bot_id, provider
      FROM runtime_executor_residents
    `).all()).toEqual([{
      conversation_id: first.conversation_id,
      bot_id: normalEnvelope('capacity-first').bot_id,
      provider: 'claude',
    }]);

    const duplicate = structuredClone(secondEnvelope);
    duplicate.trace_id = 'trace-capacity-second-duplicate';
    duplicate.received_at = '2026-07-19T07:01:01Z';
    const replayed = acceptNormalInbound(database, duplicate, {
      now: () => {
        throw new Error('a duplicate capacity-wait webhook must not commit again');
      },
      generateId: () => {
        throw new Error('a duplicate capacity-wait webhook must not reserve capacity');
      },
    });
    expect(replayed).toEqual({
      ...second,
      trace_id: duplicate.trace_id,
      deduplicated: true,
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count).toBe(3);
    expect(adapterCalls).toEqual([first.turn_id]);

    releaseFirst();
    await expect(firstRun).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    await expect(competingService.runNext()).resolves.toMatchObject({
      status: 'capacity_wait',
      turn_id: second.turn_id,
    });
    expect(adapterCalls).toEqual([first.turn_id]);
    expect(readEvents(database, second.turn_id)).toHaveLength(3);
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_executor_residents
      WHERE conversation_id = ?
    `).get(first.conversation_id).count).toBe(1);
    expect(database.prepare(`
      SELECT status, wait_reason
      FROM runtime_turn_queue
      WHERE turn_id = ?
    `).get(second.turn_id)).toEqual({
      status: 'queued',
      wait_reason: 'executor_capacity',
    });

    database.close();
  });

  test('evicts a truly idle resident and retries capacity admission in the same run', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'resident-eviction-first');
    const adapterCalls = [];
    const adapter = {
      async *execute(context) {
        adapterCalls.push(context.turn_id);
      },
      async evictIdle({ canEvict }) {
        return await canEvict(first.conversation_id) ? [first.conversation_id] : [];
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-eviction',
      now: () => '2026-07-19T07:06:00Z',
      generateId: deterministicIds('resident-eviction'),
      maxResidentExecutorsPerBot: 1,
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    const secondEnvelope = normalEnvelope('resident-eviction-second');
    secondEnvelope.chat_id = 'chat-resident-eviction-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T07:06:01Z',
      generateId: deterministicIds('inbound-resident-eviction-second'),
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(adapterCalls).toEqual([first.turn_id, second.turn_id]);
    expect(database.prepare(`
      SELECT conversation_id FROM runtime_executor_residents
    `).all()).toEqual([{ conversation_id: second.conversation_id }]);
    expect(readEvents(database, second.turn_id).some(
      (event) => event.payload?.reason_code === 'executor_capacity',
    )).toBe(false);

    await service.close();
    database.close();
  });

  test('reconciles an expired resident owner after an unclean process restart', async () => {
    const firstDatabase = openTestDatabase();
    const first = acceptQueuedTurn(firstDatabase, 'resident-crash-first');
    const secondEnvelope = normalEnvelope('resident-crash-second');
    secondEnvelope.chat_id = 'chat-resident-crash-second';
    const second = acceptNormalInbound(firstDatabase, secondEnvelope, {
      now: () => '2026-07-19T07:06:01Z',
      generateId: deterministicIds('inbound-resident-crash-second'),
    });
    const databasePath = firstDatabase.name;
    const firstAdapter = {
      resident: new Set(),
      async *execute(context) { this.resident.add(context.conversation_id); },
      hasResident(conversationId) { return this.resident.has(conversationId); },
    };
    const firstService = createExecutorService({
      database: firstDatabase,
      adapter: firstAdapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-crash-A',
      now: () => '2026-07-19T07:06:00Z',
      generateId: deterministicIds('resident-crash-A'),
      maxResidentExecutorsPerBot: 1,
      leaseDurationMs: 10_000,
      residentLeaseDurationMs: 10_000,
      residentHeartbeatIntervalMs: 3_000,
    });
    await expect(firstService.runNext()).resolves.toMatchObject({ turn_id: first.turn_id });
    firstDatabase.close();

    const restartedDatabase = new Database(databasePath);
    const secondCalls = [];
    const restartedService = createExecutorService({
      database: restartedDatabase,
      adapter: {
        async *execute(context) { secondCalls.push(context.turn_id); },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-crash-B',
      now: () => '2026-07-19T07:06:11Z',
      generateId: deterministicIds('resident-crash-B'),
      maxResidentExecutorsPerBot: 1,
      leaseDurationMs: 10_000,
      residentLeaseDurationMs: 10_000,
      residentHeartbeatIntervalMs: 3_000,
    });
    await expect(restartedService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
    });
    expect(secondCalls).toEqual([second.turn_id]);
    expect(restartedDatabase.prepare(`
      SELECT conversation_id, owner_service_instance_id, owner_epoch
      FROM runtime_executor_residents
    `).all()).toEqual([{
      conversation_id: second.conversation_id,
      owner_service_instance_id: 'executor-service-resident-crash-B',
      owner_epoch: 1,
    }]);

    restartedDatabase.close();
  });

  test('heartbeats idle resident ownership so another live service cannot take capacity', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'resident-heartbeat-first');
    const secondEnvelope = normalEnvelope('resident-heartbeat-second');
    secondEnvelope.chat_id = 'chat-resident-heartbeat-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T07:06:01Z',
      generateId: deterministicIds('inbound-resident-heartbeat-second'),
    });
    let currentTime = '2026-07-19T07:06:00Z';
    let heartbeat;
    const firstAdapter = {
      resident: new Set(),
      async *execute(context) { this.resident.add(context.conversation_id); },
      hasResident(conversationId) { return this.resident.has(conversationId); },
    };
    const firstService = createExecutorService({
      database,
      adapter: firstAdapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-heartbeat-A',
      now: () => currentTime,
      generateId: deterministicIds('resident-heartbeat-A'),
      maxResidentExecutorsPerBot: 1,
      residentLeaseDurationMs: 60_000,
      residentHeartbeatIntervalMs: 20_000,
      scheduleResidentHeartbeat(callback) {
        heartbeat = callback;
        return { unref() {} };
      },
      cancelResidentHeartbeat() {},
    });
    await expect(firstService.runNext()).resolves.toMatchObject({ turn_id: first.turn_id });

    currentTime = '2026-07-19T07:06:50Z';
    heartbeat();
    currentTime = '2026-07-19T07:07:10Z';
    const competingService = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-heartbeat-B',
      now: () => currentTime,
      generateId: deterministicIds('resident-heartbeat-B'),
      maxResidentExecutorsPerBot: 1,
      residentLeaseDurationMs: 60_000,
      residentHeartbeatIntervalMs: 20_000,
    });
    await expect(competingService.runNext()).resolves.toMatchObject({
      status: 'capacity_wait',
      turn_id: second.turn_id,
    });
    expect(database.prepare(`
      SELECT owner_service_instance_id, owner_epoch, owner_expires_at
      FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(first.conversation_id)).toEqual({
      owner_service_instance_id: 'executor-service-resident-heartbeat-A',
      owner_epoch: 1,
      owner_expires_at: '2026-07-19T07:07:50.000Z',
    });

    await competingService.close();
    await firstService.close();
    database.close();
  });

  test('rejects provider output after the resident owner epoch changes', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'resident-epoch-fence');
    const store = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-resident-epoch',
      now: () => '2026-07-19T07:08:00Z',
      generateId: deterministicIds('resident-epoch'),
    });
    expect(store.reserveNextExecutor({ maxResidentExecutorsPerBot: 1 }))
      .toMatchObject({ status: 'ready', conversation_id: accepted.conversation_id });
    const turnContext = store.claimNextQueuedTurn({ conversationId: accepted.conversation_id });
    store.transitionTurn(turnContext, 'starting', 'running');
    database.prepare(`
      UPDATE runtime_executor_residents SET owner_epoch = owner_epoch + 1
      WHERE conversation_id = ?
    `).run(accepted.conversation_id);

    expect(() => store.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'stale resident output', end_offset: 21 },
      provider_native_id: null,
    })).toThrow(/resident executor owner no longer matches/);
    expect(readEvents(database, accepted.turn_id).some(
      (event) => event.payload?.text === 'stale resident output',
    )).toBe(false);

    database.close();
  });

  test('does not claim a turn when shutdown races the capacity-eviction await', async () => {
    const database = openTestDatabase();
    const first = acceptQueuedTurn(database, 'admission-close-first');
    let releaseEviction;
    const evictionCanFinish = new Promise((resolve) => { releaseEviction = resolve; });
    let markEvictionStarted;
    const evictionStarted = new Promise((resolve) => { markEvictionStarted = resolve; });
    const adapter = {
      async *execute() {},
      async evictIdle() {
        markEvictionStarted();
        await evictionCanFinish;
        return [];
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-admission-close',
      now: () => '2026-07-19T07:06:30Z',
      generateId: deterministicIds('admission-close'),
      maxResidentExecutorsPerBot: 1,
    });
    await service.runNext();
    const secondEnvelope = normalEnvelope('admission-close-second');
    secondEnvelope.chat_id = 'chat-admission-close-second';
    const second = acceptNormalInbound(database, secondEnvelope, {
      now: () => '2026-07-19T07:06:31Z',
      generateId: deterministicIds('inbound-admission-close-second'),
    });

    const admission = service.runNext();
    await evictionStarted;
    const closing = service.close();
    releaseEviction();
    await expect(admission).rejects.toThrow(/closing|closed/);
    await expect(closing).resolves.toBeUndefined();
    expect(database.prepare(`
      SELECT state, attempt_id FROM runtime_turns WHERE turn_id = ?
    `).get(second.turn_id)).toEqual({ state: 'queued', attempt_id: null });
    expect(first.turn_id).toBeDefined();

    database.close();
  });

  test('executes one conversation FIFO across lineages without two active turns', async () => {
    const database = openTestDatabase();
    const firstEnvelope = normalEnvelope('fifo-first');
    const first = acceptNormalInbound(database, firstEnvelope, {
      now: () => '2026-07-19T07:10:00Z',
      generateId: deterministicIds('inbound-fifo-first'),
    });
    const alternateLineageId = 'lineage-fifo-alternate';
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES (?, ?, 'normal', 0, ?)
    `).run(alternateLineageId, first.conversation_id, first.committed_at);
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
      'model-message-fifo-alternate',
      first.conversation_id,
      first.turn_id,
      alternateLineageId,
      'mapping-fifo-alternate',
      first.committed_at,
    );
    const replyEnvelope = normalEnvelope('fifo-reply');
    replyEnvelope.reply = {
      root_message_id: 'model-message-fifo-root',
      parent_message_id: 'model-message-fifo-alternate',
      reply_to_message_id: 'model-message-fifo-alternate',
    };
    const reply = acceptNormalInbound(database, replyEnvelope, {
      now: () => '2026-07-19T07:10:01Z',
      generateId: deterministicIds('inbound-fifo-reply'),
    });
    expect(reply).toMatchObject({
      conversation_id: first.conversation_id,
      lineage_id: alternateLineageId,
    });

    const adapterCalls = [];
    let markFirstStarted;
    const firstStarted = new Promise((resolve) => { markFirstStarted = resolve; });
    let releaseFirst;
    const firstCanFinish = new Promise((resolve) => { releaseFirst = resolve; });
    const adapter = {
      async *execute(context) {
        adapterCalls.push({
          turn_id: context.turn_id,
          lineage_id: context.lineage_id,
        });
        if (context.turn_id === first.turn_id) {
          markFirstStarted();
          await firstCanFinish;
        }
      },
    };
    const firstService = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-fifo-A',
      now: () => '2026-07-19T07:11:00Z',
      generateId: deterministicIds('fifo-A'),
    });
    const competingService = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-fifo-B',
      now: () => '2026-07-19T07:11:01Z',
      generateId: deterministicIds('fifo-B'),
    });

    const firstRun = firstService.runNext();
    await firstStarted;
    await expect(competingService.runNext()).resolves.toEqual({ status: 'idle' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turns
      WHERE conversation_id = ? AND state IN (
        'starting', 'running', 'waiting_user', 'redirecting', 'recovering'
      )
    `).get(first.conversation_id).count).toBe(1);
    expect(database.prepare(`
      SELECT state, attempt_id
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(reply.turn_id)).toEqual({ state: 'queued', attempt_id: null });
    expect(adapterCalls).toEqual([
      { turn_id: first.turn_id, lineage_id: first.lineage_id },
    ]);

    releaseFirst();
    await firstRun;
    await expect(firstService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: reply.turn_id,
    });
    expect(adapterCalls).toEqual([
      { turn_id: first.turn_id, lineage_id: first.lineage_id },
      { turn_id: reply.turn_id, lineage_id: alternateLineageId },
    ]);

    database.close();
  });

  test('never invokes the provider for a queue-full failed turn or its duplicate webhook', async () => {
    const database = openTestDatabase();
    const dependencies = {
      now: () => '2026-07-19T07:20:00Z',
      generateId: deterministicIds('inbound-queue-full'),
      maxQueuedTurns: 1,
    };
    const firstEnvelope = normalEnvelope('queue-full-first');
    const first = acceptNormalInbound(database, firstEnvelope, dependencies);
    const rejectedEnvelope = normalEnvelope('queue-full-rejected');
    const rejected = acceptNormalInbound(database, rejectedEnvelope, dependencies);
    expect(rejected).toMatchObject({
      status: 'rejected',
      error: { code: 'queue_full' },
    });

    const duplicate = structuredClone(rejectedEnvelope);
    duplicate.trace_id = 'trace-queue-full-rejected-duplicate';
    duplicate.received_at = '2026-07-19T07:20:01Z';
    expect(acceptNormalInbound(database, duplicate, dependencies)).toEqual({
      ...rejected,
      trace_id: duplicate.trace_id,
      deduplicated: true,
    });
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count).toBe(2);
    expect(database.prepare('SELECT COUNT(*) AS count FROM runtime_turn_queue').get().count).toBe(1);

    const adapterCalls = [];
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          adapterCalls.push(context.turn_id);
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-queue-full',
      now: () => '2026-07-19T07:21:00Z',
      generateId: deterministicIds('queue-full'),
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: first.turn_id,
    });
    await expect(service.runNext()).resolves.toEqual({ status: 'idle' });
    expect(adapterCalls).toEqual([first.turn_id]);
    expect(database.prepare(`
      SELECT state, attempt_id
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(rejected.turn_id)).toEqual({ state: 'failed', attempt_id: null });

    database.close();
  });

  test('rejects illegal transitions and stale attempt fences without authoritative mutations', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'fencing');
    const store = createTestStore(database, 'fencing');
    const turnContext = store.claimNextQueuedTurn();
    const claimedAuthority = readAuthority(
      database,
      accepted.turn_id,
      accepted.conversation_id,
    );

    expect(() => store.transitionTurn(turnContext, 'starting', 'completed'))
      .toThrow(expect.objectContaining({ code: 'illegal_transition' }));
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
      .toEqual(claimedAuthority);

    const staleContexts = [
      {
        ...turnContext,
        attempt: {
          ...turnContext.attempt,
          attempt_id: 'attempt-from-old-executor',
        },
      },
      {
        ...turnContext,
        attempt: {
          ...turnContext.attempt,
          attempt_no: turnContext.attempt.attempt_no + 1,
        },
      },
      {
        ...turnContext,
        attempt: {
          ...turnContext.attempt,
          lease_epoch: turnContext.attempt.lease_epoch + 1,
        },
      },
    ];
    for (const staleContext of staleContexts) {
      expect(() => store.transitionTurn(staleContext, 'starting', 'running'))
        .toThrow(expect.objectContaining({ code: 'stale_attempt' }));
      expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
        .toEqual(claimedAuthority);
    }

    store.transitionTurn(turnContext, 'starting', 'running');
    const runningAuthority = readAuthority(
      database,
      accepted.turn_id,
      accepted.conversation_id,
    );
    for (const staleContext of staleContexts) {
      expect(() => store.appendAdapterEvent(staleContext, {
        kind: 'text_snapshot',
        payload: { text: 'late output', end_offset: 11 },
        provider_native_id: null,
      })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
      expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
        .toEqual(runningAuthority);
    }

    database.close();
  });

  test('rolls back queue claim, lease, state, event, and projection when the atomic commit fails', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'atomic-claim');
    const store = createTestStore(database, 'atomic-claim');
    const before = readAuthority(database, accepted.turn_id, accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER force_executor_projection_failure
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.aggregate_version = 3
      BEGIN
        SELECT RAISE(ABORT, 'forced executor projection failure');
      END;
    `);

    expect(() => store.claimNextQueuedTurn()).toThrow(/forced executor projection failure/);
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id)).toEqual(before);

    database.close();
  });

  test('rolls back terminal state, queue completion, and lease release as one commit', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'atomic-completion');
    const store = createTestStore(database, 'atomic-completion');
    const turnContext = store.claimNextQueuedTurn();
    store.transitionTurn(turnContext, 'starting', 'running');
    const before = readAuthority(database, accepted.turn_id, accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER force_completion_projection_failure
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.aggregate_version = 5
      BEGIN
        SELECT RAISE(ABORT, 'forced completion projection failure');
      END;
    `);

    expect(() => store.transitionTurn(turnContext, 'running', 'completed'))
      .toThrow(/forced completion projection failure/);
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id)).toEqual(before);

    database.close();
  });

  test('rejects output arriving after terminal lease release as stale', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'late-after-terminal');
    const store = createTestStore(database, 'late-after-terminal');
    const turnContext = store.claimNextQueuedTurn();
    store.transitionTurn(turnContext, 'starting', 'running');
    store.transitionTurn(turnContext, 'running', 'completed');
    const terminalAuthority = readAuthority(
      database,
      accepted.turn_id,
      accepted.conversation_id,
    );

    expect(() => store.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'late terminal output', end_offset: 20 },
      provider_native_id: null,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
      .toEqual(terminalAuthority);

    database.close();
  });

  test('isolates the provider and preserves authority when terminal projection persistence fails', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-projection-failure');
    database.exec(`
      CREATE TRIGGER fail_service_terminal_projection
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.terminal = 1
      BEGIN
        SELECT RAISE(ABORT, 'forced service terminal projection failure');
      END;
    `);
    const aborts = [];
    const adapter = {
      async abort(context) {
        aborts.push(context.turn_id);
      },
      async *execute() {},
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-projection-failure',
      now: () => '2026-07-19T07:01:30Z',
      generateId: deterministicIds('service-projection-failure'),
    });

    await expect(service.runNext()).rejects.toThrow(/forced service terminal projection failure/);
    expect(aborts).toEqual([accepted.turn_id]);
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'running' });
    expect(database.prepare(`SELECT status FROM runtime_turn_queue WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ status: 'claimed' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-projection-failure',
      turn_id: accepted.turn_id,
    });

    await service.close();
    database.close();
  });

  test('rolls back adapter event version, event, and projection as one commit', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'atomic-adapter-event');
    const store = createTestStore(database, 'atomic-adapter-event');
    const turnContext = store.claimNextQueuedTurn();
    store.transitionTurn(turnContext, 'starting', 'running');
    const before = readAuthority(database, accepted.turn_id, accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER force_adapter_event_projection_failure
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.aggregate_version = 5
      BEGIN
        SELECT RAISE(ABORT, 'forced adapter event projection failure');
      END;
    `);

    expect(() => store.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'must roll back', end_offset: 14 },
      provider_native_id: null,
    })).toThrow(/forced adapter event projection failure/);
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id)).toEqual(before);

    database.close();
  });

  test('keeps recovering durable conversations ineligible for idle eviction', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'recovering-eviction');
    const store = createTestStore(database, 'recovering-eviction');
    const turnContext = store.claimNextQueuedTurn();

    expect(store.isConversationEvictable(accepted.conversation_id)).toBe(false);
    store.transitionTurn(turnContext, 'starting', 'running');
    store.transitionTurn(turnContext, 'running', 'recovering');
    expect(store.isConversationEvictable(accepted.conversation_id)).toBe(false);

    store.transitionTurn(turnContext, 'recovering', 'stopped');
    expect(store.isConversationEvictable(accepted.conversation_id)).toBe(true);
    database.close();
  });

  test('rebuilds its cache after process restart and continues the not-started durable queue', async () => {
    const firstDatabase = openTestDatabase();
    const first = acceptQueuedTurn(firstDatabase, 'restart-first');
    const second = acceptQueuedTurn(firstDatabase, 'restart-second');
    const databasePath = firstDatabase.name;
    firstDatabase.close();
    const adapterCalls = [];
    const adapter = {
      async *execute(context) {
        adapterCalls.push(context.turn_id);
      },
    };

    const afterInboundRestart = new Database(databasePath);
    const firstService = createExecutorService({
      database: afterInboundRestart,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-restart-A',
      now: () => '2026-07-19T07:03:00Z',
      generateId: deterministicIds('restart-A'),
    });
    expect(firstService.start().executors).toEqual([
      {
        conversation_id: first.conversation_id,
        active_turn_id: null,
        queued_turn_ids: [first.turn_id, second.turn_id],
        wait_reason: null,
      },
    ]);
    await firstService.runNext();
    afterInboundRestart.close();

    const afterServiceRestart = new Database(databasePath);
    const secondService = createExecutorService({
      database: afterServiceRestart,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-restart-B',
      now: () => '2026-07-19T07:04:00Z',
      generateId: deterministicIds('restart-B'),
    });
    expect(secondService.start().executors).toEqual([
      {
        conversation_id: second.conversation_id,
        active_turn_id: null,
        queued_turn_ids: [second.turn_id],
        wait_reason: null,
      },
    ]);
    await expect(secondService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: second.turn_id,
      attempt_no: 1,
      lease_epoch: 2,
    });
    expect(adapterCalls).toEqual([first.turn_id, second.turn_id]);
    expect(secondService.snapshot().executors).toEqual([]);

    afterServiceRestart.close();
  });
});
