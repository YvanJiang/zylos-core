import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { interactionAnswer } from './helpers/runtime-interaction-fixtures.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-stop-linearization-'));
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
  const envelope = structuredClone(fixture);
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

function acceptTurn(database, suffix) {
  return acceptNormalInbound(database, normalEnvelope(suffix), {
    now: () => '2026-07-20T01:00:00Z',
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => { resolve = resolvePromise; });
  return { promise, resolve };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('runtime /stop linearization', () => {
  test('stops active work, cancels only the cutoff queue, and preserves later ingress', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'cutoff-active');
    const queuedOne = acceptTurn(database, 'cutoff-queued-one');
    const queuedTwo = acceptTurn(database, 'cutoff-queued-two');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-cutoff',
      now: () => '2026-07-20T01:01:00Z',
      generateId: deterministicIds('stop-cutoff'),
    });

    const execution = service.runNext();
    await providerStarted.promise;

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-cutoff-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      stop_id: 'stop-cutoff-1',
      conversation_id: active.conversation_id,
      stop_cutoff_queue_sequence: 3,
      active_turn: {
        turn_id: active.turn_id,
        previous_state: 'running',
        state: 'stopped',
      },
      cancelled_turn_ids: [queuedOne.turn_id, queuedTwo.turn_id],
      deduplicated: false,
    });
    await expect(execution).resolves.toMatchObject({
      status: 'stopped',
      turn_id: active.turn_id,
    });

    const afterStop = acceptTurn(database, 'cutoff-after-stop');
    expect(database.prepare(`
      SELECT turn_id, state, queue_sequence
      FROM runtime_turns
      WHERE conversation_id = ?
      ORDER BY queue_sequence
    `).all(active.conversation_id)).toEqual([
      { turn_id: active.turn_id, state: 'stopped', queue_sequence: 1 },
      { turn_id: queuedOne.turn_id, state: 'cancelled', queue_sequence: 2 },
      { turn_id: queuedTwo.turn_id, state: 'cancelled', queue_sequence: 3 },
      { turn_id: afterStop.turn_id, state: 'queued', queue_sequence: 4 },
    ]);
    expect(afterStop.lineage_id).toBe(active.lineage_id);

    database.close();
  });

  test('replays one stop result and makes a concurrent distinct stop a no-op', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'repeat-active');
    const queued = acceptTurn(database, 'repeat-queued');
    const providerStarted = deferred();
    const cancellationEntered = deferred();
    const allowCancellation = deferred();
    const providerStopped = deferred();
    let cancellationCount = 0;
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        cancellationCount += 1;
        cancellationEntered.resolve();
        await allowCancellation.promise;
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-stop-repeat',
      now: () => '2026-07-20T01:02:00Z',
      generateId: deterministicIds('stop-repeat'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    const firstStop = service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-repeat-1',
    });
    await cancellationEntered.promise;

    const sameStopReplay = service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-repeat-1',
    });
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-repeat-2',
    })).resolves.toMatchObject({
      status: 'noop',
      stop_id: 'stop-repeat-2',
      active_turn: null,
      cancelled_turn_ids: [],
      deduplicated: false,
    });
    expect(cancellationCount).toBe(1);

    allowCancellation.resolve();
    await expect(firstStop).resolves.toMatchObject({ status: 'stopped' });
    await expect(sameStopReplay).resolves.toMatchObject({
      status: 'stopped',
      stop_id: 'stop-repeat-1',
      active_turn: { turn_id: active.turn_id, state: 'stopped' },
      cancelled_turn_ids: [queued.turn_id],
      deduplicated: true,
    });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_normalized_events
      WHERE turn_id = ?
        AND json_extract(event_json, '$.kind') = 'turn_state_changed'
        AND json_extract(event_json, '$.phase') = 'stopped'
    `).get(active.turn_id)).toEqual({ count: 1 });

    database.close();
  });

  test('replays unfinished provider termination after the stop commit crash window', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'replay-provider-stop');
    let cancellationCount = 0;
    let abortCount = 0;
    const serviceInstanceId = 'executor-service-stop-replay-provider';
    const adapter = {
      async *execute() {},
      async cancel() {
        cancellationCount += 1;
      },
      async abort() {
        abortCount += 1;
      },
    };
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:30Z',
      generateId: deterministicIds('stop-replay-provider-store'),
    });
    expect(store.reserveNextExecutor({ maxResidentExecutorsPerBot: 20 }))
      .toMatchObject({ status: 'ready', conversation_id: active.conversation_id });
    const turnContext = store.claimNextQueuedTurn({ conversationId: active.conversation_id });
    store.transitionTurn(turnContext, 'starting', 'running');
    expect(store.stopConversation({
      conversation_id: active.conversation_id,
      stop_id: 'stop-replay-provider-1',
    })).toMatchObject({
      status: 'stopped',
      provider_stop_status: 'pending',
      lease_released: false,
    });
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:30Z',
      generateId: deterministicIds('stop-replay-provider-service'),
    });

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-replay-provider-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      deduplicated: true,
      provider_stop_status: 'isolated',
      lease_released: true,
    });
    expect(cancellationCount).toBe(1);
    expect(abortCount).toBe(1);

    database.close();
  });

  test('bounds missing-local-run isolation and retains the lease without proof', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'replay-abort-hangs');
    const serviceInstanceId = 'executor-service-stop-replay-abort-hangs';
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:40Z',
      generateId: deterministicIds('stop-replay-abort-hangs-store'),
    });
    expect(store.reserveNextExecutor({ maxResidentExecutorsPerBot: 20 }))
      .toMatchObject({ status: 'ready', conversation_id: active.conversation_id });
    const turnContext = store.claimNextQueuedTurn({ conversationId: active.conversation_id });
    store.transitionTurn(turnContext, 'starting', 'running');
    store.stopConversation({
      conversation_id: active.conversation_id,
      stop_id: 'stop-replay-abort-hangs-1',
    });
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {},
        async cancel() {},
        async abort() { await new Promise(() => {}); },
      },
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:40Z',
      providerStopTimeoutMs: 1,
      generateId: deterministicIds('stop-replay-abort-hangs-service'),
    });

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-replay-abort-hangs-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'terminal_unconfirmed',
      lease_released: false,
      incident: { disposition: 'manual_recovery_required' },
    });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: serviceInstanceId,
      turn_id: active.turn_id,
    });

    database.close();
  });

  test('atomically rolls back lease release when the durable outcome CAS fails', () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-outcome-rollback');
    const serviceInstanceId = 'executor-service-stop-outcome-rollback';
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:42Z',
      generateId: deterministicIds('stop-outcome-rollback'),
    });
    expect(store.reserveNextExecutor({ maxResidentExecutorsPerBot: 20 }))
      .toMatchObject({ status: 'ready', conversation_id: active.conversation_id });
    const turnContext = store.claimNextQueuedTurn({ conversationId: active.conversation_id });
    store.transitionTurn(turnContext, 'starting', 'running');
    const stopped = store.stopConversation({
      conversation_id: active.conversation_id,
      stop_id: 'stop-outcome-rollback-1',
    });
    database.exec(`
      CREATE TEMP TRIGGER fail_stop_outcome_update
      BEFORE UPDATE OF result_json ON runtime_stop_controls
      BEGIN
        SELECT RAISE(ABORT, 'forced stop outcome write failure');
      END;
    `);

    let writeFailure;
    try {
      store.recordStopProviderOutcome(stopped, 'isolated');
    } catch (error) {
      writeFailure = error;
    }
    expect(writeFailure).toMatchObject({
      name: 'SqliteError',
      code: 'SQLITE_CONSTRAINT_TRIGGER',
      message: 'forced stop outcome write failure',
    });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: serviceInstanceId,
      turn_id: active.turn_id,
    });
    expect(database.prepare(`
      SELECT json_extract(result_json, '$.provider_stop_status') AS provider_stop_status,
        json_extract(result_json, '$.lease_released') AS lease_released
      FROM runtime_stop_controls WHERE stop_id = ?
    `).get(stopped.stop_id)).toEqual({
      provider_stop_status: 'pending',
      lease_released: 0,
    });

    database.exec('DROP TRIGGER fail_stop_outcome_update');
    expect(store.recordStopProviderOutcome(stopped, 'isolated')).toMatchObject({
      provider_stop_status: 'isolated',
      lease_released: true,
    });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({ lease_owner: null, turn_id: null });

    database.close();
  });

  test('keeps the lease when an unknown outcome wins before late isolation proof', () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-unknown-wins');
    const serviceInstanceId = 'executor-service-stop-unknown-wins';
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:02:44Z',
      generateId: deterministicIds('stop-unknown-wins'),
    });
    expect(store.reserveNextExecutor({ maxResidentExecutorsPerBot: 20 }))
      .toMatchObject({ status: 'ready', conversation_id: active.conversation_id });
    const turnContext = store.claimNextQueuedTurn({ conversationId: active.conversation_id });
    store.transitionTurn(turnContext, 'starting', 'running');
    const stopped = store.stopConversation({
      conversation_id: active.conversation_id,
      stop_id: 'stop-unknown-wins-1',
    });
    expect(store.markProviderStopUnknown(
      { turn_id: active.turn_id, attempt: turnContext.attempt },
      'uncertain',
      stopped,
    )).toMatchObject({
      provider_stop_status: 'uncertain',
      stop_result: { lease_released: false },
    });

    expect(store.recordStopProviderOutcome(stopped, 'isolated')).toMatchObject({
      provider_stop_status: 'uncertain',
      lease_released: false,
    });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: serviceInstanceId,
      turn_id: active.turn_id,
    });

    database.close();
  });

  test('exposes stop as the only public conversation termination control', () => {
    const database = openTestDatabase();
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-public-control',
      now: () => '2026-07-20T01:02:45Z',
      generateId: deterministicIds('stop-public-control'),
    });

    expect(Object.hasOwn(service, 'stop')).toBe(true);
    expect(Object.hasOwn(service, 'cancel')).toBe(false);
    database.close();
  });

  test('returns noop when provider completion wins the terminal CAS', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'completion-wins');
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        yield { type: 'turn_result', outcome: 'completed' };
      },
      async cancel() {
        throw new Error('completion winner must not invoke provider cancellation');
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-completion-wins',
      now: () => '2026-07-20T01:02:30Z',
      generateId: deterministicIds('stop-completion-wins'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: active.turn_id,
    });
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-completion-wins-1',
    })).resolves.toMatchObject({
      status: 'noop',
      active_turn: null,
      cancelled_turn_ids: [],
    });
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(active.turn_id)).toEqual({ state: 'completed' });

    database.close();
  });

  test('does not cancel a committed received turn at the stop cutoff', async () => {
    const database = openTestDatabase();
    const received = acceptTurn(database, 'committed-received');
    database.prepare(`UPDATE runtime_turns SET state = 'received' WHERE turn_id = ?`)
      .run(received.turn_id);
    database.prepare(`UPDATE runtime_turn_queue SET status = 'received' WHERE turn_id = ?`)
      .run(received.turn_id);
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'executor-service-stop-committed-received',
      now: () => '2026-07-20T01:02:45Z',
      generateId: deterministicIds('stop-committed-received'),
    });

    await expect(service.stop({
      conversation_id: received.conversation_id,
      stop_id: 'stop-committed-received-1',
    })).resolves.toMatchObject({
      status: 'noop',
      stop_cutoff_queue_sequence: 1,
      cancelled_turn_ids: [],
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(received.turn_id)).toEqual({ state: 'received' });

    database.close();
  });

  test('fences late provider output and completion into diagnostics after stop wins', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'late-active');
    const providerStarted = deferred();
    const emitLateEvents = deferred();
    let providerContext;
    const adapter = {
      async *execute(context) {
        providerContext = context;
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await emitLateEvents.promise;
        yield {
          type: 'normalized_event',
          event: {
            kind: 'text_snapshot',
            payload: { text: 'late-model-result', end_offset: 17 },
            provider_native_id: null,
          },
        };
        yield { type: 'turn_result', outcome: 'completed' };
      },
      async cancel() {
        emitLateEvents.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-late',
      now: () => '2026-07-20T01:03:00Z',
      generateId: deterministicIds('stop-late'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-late-1',
    })).resolves.toMatchObject({ status: 'stopped' });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    expect(providerContext.reportProviderState({
      state: 'started',
      provider_native_id: null,
    })).toEqual({ status: 'stale_attempt' });
    expect(providerContext.bindProviderNativeId('thread-too-late')).toEqual({
      status: 'stale_attempt',
    });

    const authoritativePayloads = database.prepare(`
      SELECT event_json FROM runtime_normalized_events WHERE turn_id = ?
      UNION ALL
      SELECT command_json FROM runtime_outbox WHERE turn_id = ?
    `).all(active.turn_id, active.turn_id).map((row) => Object.values(row)[0]);
    expect(authoritativePayloads.join('\n')).not.toContain('late-model-result');
    expect(database.prepare(`
      SELECT event_kind, reason_code, descriptor_json
      FROM runtime_provider_event_diagnostics
      WHERE turn_id = ?
      ORDER BY observed_at, diagnostic_id
    `).all(active.turn_id)).toEqual([
      expect.objectContaining({
        event_kind: 'text_snapshot',
        reason_code: 'stale_attempt',
        descriptor_json: expect.stringContaining('late-model-result'),
      }),
      expect.objectContaining({
        event_kind: 'turn_result',
        reason_code: 'stale_attempt',
        descriptor_json: expect.stringContaining('completed'),
      }),
      expect.objectContaining({
        event_kind: 'provider_state',
        reason_code: 'stale_attempt',
      }),
      expect.objectContaining({
        event_kind: 'provider_native_id',
        reason_code: 'stale_attempt',
        descriptor_json: expect.stringContaining('thread-too-late'),
      }),
    ]);
    expect(database.prepare(`
      SELECT provider_native_id FROM runtime_lineages WHERE lineage_id = ?
    `).get(active.lineage_id)).toEqual({ provider_native_id: null });

    database.close();
  });

  test('cancels an unsent committed interaction handoff and preserves provider lineage', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-unsent');
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.bindProviderNativeId('thread-interaction-unsent');
        context.reportProviderState({
          state: 'started',
          provider_native_id: 'thread-interaction-unsent',
        });
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-question-interaction-unsent',
            tool_use_id: 'tool-use-interaction-unsent',
            kind: 'tool_approval',
            prompt: 'Allow the requested workspace write?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-interaction-unsent',
      now: () => '2026-07-20T01:04:00Z',
      generateId: deterministicIds('stop-interaction-unsent'),
    });

    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'stop-interaction-unsent'),
    );

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-interaction-unsent-1',
    })).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`
      SELECT interaction.state, interaction.handoff_state,
        handoff.state AS durable_handoff_state, turn.state AS turn_state,
        lineage.provider_native_id
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      JOIN runtime_lineages AS lineage ON lineage.lineage_id = turn.lineage_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'cancelled',
      handoff_state: 'cancelled',
      durable_handoff_state: 'cancelled',
      turn_state: 'stopped',
      provider_native_id: 'thread-interaction-unsent',
    });
    expect(database.prepare(`
      SELECT json_extract(record_json, '$.reason_code') AS reason_code,
        json_extract(record_json, '$.last_send_started_at') AS last_send_started_at
      FROM runtime_interaction_handoffs WHERE handoff_id = ?
    `).get(answer.handoff_id)).toEqual({
      reason_code: 'parent_stopped',
      last_send_started_at: null,
    });
    await expect(service.expireInteraction({
      interaction_id: waiting.request.interaction_id,
      interaction_version: waiting.request.version,
    })).resolves.toMatchObject({
      status: 'not_pending',
      interaction_state: 'cancelled',
    });

    database.close();
  });

  test('marks a sent handoff unknown and treats its late acknowledgement as diagnostic only', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-sent');
    const handlerEntered = deferred();
    const allowHandlerAck = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-question-interaction-sent',
            tool_use_id: 'tool-use-interaction-sent',
            kind: 'tool_approval',
            prompt: 'Allow the requested workspace write?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async prepareInteractionAnswer() {
        return {
          async send(delivery) {
            handlerEntered.resolve();
            await allowHandlerAck.promise;
            return {
              status: 'accepted',
              handoff_id: delivery.handoff.handoff_id,
              provider_attempt_id: delivery.handoff.provider_attempt_id,
              handoff_attempt_id: delivery.handoff.handoff_attempt_id,
              handoff_attempt_no: delivery.handoff.handoff_attempt_no,
              lease_epoch: delivery.handoff.lease_epoch,
            };
          },
        };
      },
      async cancel() {
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-stop-interaction-sent',
      now: () => '2026-07-20T01:05:00Z',
      generateId: deterministicIds('stop-interaction-sent'),
    });

    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'stop-interaction-sent'),
    );
    const delivery = service.deliverInteractionAnswer(answer.handoff_id);
    await handlerEntered.promise;

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-interaction-sent-1',
    })).resolves.toMatchObject({ status: 'stopped' });
    allowHandlerAck.resolve();
    await expect(delivery).resolves.toMatchObject({
      status: 'stale_acknowledgement',
      turn_state: 'stopped',
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
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
      durable_handoff_state: 'delivery_unknown',
      turn_state: 'stopped',
    });
    expect(database.prepare(`
      SELECT event_kind, reason_code
      FROM runtime_provider_event_diagnostics
      WHERE turn_id = ? AND event_kind = 'interaction_handoff_acknowledgement'
    `).get(active.turn_id)).toEqual({
      event_kind: 'interaction_handoff_acknowledgement',
      reason_code: 'stale_attempt',
    });

    database.close();
  });

  test('returns noop when interaction timeout wins before stop', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'timeout-wins');
    const clock = { now: '2026-07-20T01:06:00Z' };
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-question-timeout-wins',
            tool_use_id: 'tool-use-timeout-wins',
            kind: 'question',
            prompt: 'Will timeout win?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
      },
      async interrupt() {
        return { status: 'provider_stopped', reason: 'timeout' };
      },
      async cancel() {
        throw new Error('timeout winner must not invoke stop cancellation');
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-timeout-wins',
      now: () => clock.now,
      interactionTimeoutMs: 1_000,
      setTimeoutFn: () => ({ unref() {} }),
      clearTimeoutFn: () => {},
      generateId: deterministicIds('stop-timeout-wins'),
    });

    const waiting = await service.runNext();
    clock.now = '2026-07-20T01:06:02Z';
    await expect(service.expireInteraction({
      interaction_id: waiting.request.interaction_id,
      interaction_version: waiting.request.version,
    })).resolves.toMatchObject({
      status: 'expired',
      turn_state: 'timed_out',
      lease_released: true,
    });
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-timeout-wins-1',
    })).resolves.toMatchObject({
      status: 'noop',
      active_turn: null,
    });

    database.close();
  });

  test('stops a provider attempt while its canonical state is starting', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'starting');
    const executeEntered = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute() {
        executeEntered.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-starting',
      now: () => '2026-07-20T01:07:00Z',
      generateId: deterministicIds('stop-starting'),
    });

    const execution = service.runNext();
    await executeEntered.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-starting-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { previous_state: 'starting', state: 'stopped' },
    });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });

    database.close();
  });

  test('stops a recovering turn through the same provider-neutral control seam', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'recovering');
    const cancellationContexts = [];
    const isolationContexts = [];
    const providerFailure = Object.assign(new Error('transport lost'), {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'Provider execution state is uncertain.',
      },
    });
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        context.reportProviderFailure(providerFailure);
        throw providerFailure;
      },
      async cancel(context) {
        cancellationContexts.push(context);
      },
      async abort(context) {
        isolationContexts.push(context);
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-stop-recovering',
      now: () => '2026-07-20T01:08:00Z',
      generateId: deterministicIds('stop-recovering'),
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'recovering',
      turn_id: active.turn_id,
    });
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-recovering-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { previous_state: 'recovering', state: 'stopped' },
      provider_stop_status: 'isolated',
      lease_released: true,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_execution_recoveries WHERE turn_id = ?
    `).get(active.turn_id)).toEqual({ state: 'stopped' });
    expect(cancellationContexts).toEqual([
      expect.objectContaining({
        conversation_id: active.conversation_id,
        turn_id: active.turn_id,
        attempt: expect.objectContaining({ lease_epoch: 1 }),
      }),
    ]);
    expect(isolationContexts).toEqual(cancellationContexts);

    database.close();
  });

  test('retains the fenced lease and notifies when provider termination is uncertain', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-stop-uncertain');
    const providerStarted = deferred();
    const providerEventuallyEnds = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerEventuallyEnds.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        const error = new Error('provider cancellation acknowledgement was lost');
        error.cancellationUncertain = true;
        throw error;
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-uncertain',
      now: () => '2026-07-20T01:09:00Z',
      generateId: deterministicIds('stop-uncertain'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-uncertain-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      lease_released: false,
      provider_stop_status: 'uncertain',
      incident: {
        status: 'manual_recovery_required',
        side_effect_status: 'unknown',
        disposition: 'manual_recovery_required',
      },
    });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: 'executor-service-stop-uncertain',
      turn_id: active.turn_id,
    });
    expect(database.prepare(`
      SELECT provider_stop_status, side_effect_status, disposition
      FROM runtime_provider_stop_incidents WHERE turn_id = ?
    `).get(active.turn_id)).toEqual({
      provider_stop_status: 'uncertain',
      side_effect_status: 'unknown',
      disposition: 'manual_recovery_required',
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'text_notice'
    `).get(active.turn_id)).toEqual({ count: 1 });

    providerEventuallyEnds.resolve();
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    database.close();
  });

  test('does not release the lease when an accepted cancel lacks a provider terminal', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-terminal-missing');
    const providerStarted = deferred();
    const providerEventuallyEnds = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerEventuallyEnds.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {},
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-terminal-missing',
      now: () => '2026-07-20T01:10:00Z',
      providerStopTimeoutMs: 1,
      generateId: deterministicIds('stop-terminal-missing'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-terminal-missing-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      lease_released: false,
      provider_stop_status: 'terminal_unconfirmed',
      incident: { disposition: 'manual_recovery_required' },
    });
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: 'executor-service-stop-terminal-missing',
    });

    providerEventuallyEnds.resolve();
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    database.close();
  });

  test('does not treat a post-cancel stream failure as provider terminal proof', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-stream-fails-after-cancel');
    const providerStarted = deferred();
    const cancelAccepted = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await cancelAccepted.promise;
        throw new Error('provider connection failed after cancel acceptance');
      },
      async cancel() {
        cancelAccepted.resolve();
      },
      async abort() {
        throw new Error('provider isolation unavailable');
      },
    };
    const serviceInstanceId = 'executor-service-stop-stream-failure';
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId,
      now: () => '2026-07-20T01:10:30Z',
      generateId: deterministicIds('stop-stream-failure'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-stream-failure-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'terminal_unconfirmed',
      lease_released: false,
      incident: { disposition: 'manual_recovery_required' },
    });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: serviceInstanceId,
      turn_id: active.turn_id,
    });

    database.close();
  });

  test('bounds a non-settling provider cancel and isolates before releasing the lease', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'provider-cancel-hangs');
    const providerStarted = deferred();
    const providerStopped = deferred();
    let abortCount = 0;
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() {
        await new Promise(() => {});
      },
      async abort() {
        abortCount += 1;
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-cancel-hangs',
      now: () => '2026-07-20T01:11:00Z',
      providerStopTimeoutMs: 1,
      generateId: deterministicIds('stop-cancel-hangs'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-cancel-hangs-1',
    })).resolves.toMatchObject({
      status: 'stopped',
      provider_stop_status: 'isolated',
      lease_released: true,
    });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    expect(abortCount).toBe(1);
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({ lease_owner: null });

    database.close();
  });
});
