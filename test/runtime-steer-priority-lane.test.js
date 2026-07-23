import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { acceptQueuedInbound as acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-steer-priority-'));
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

function normalEnvelope(suffix, text = `message ${suffix}`) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.content = { kind: 'text', text, attachments: [] };
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
    now: () => '2026-07-20T02:00:00Z',
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

describe('runtime /steer priority lane', () => {
  test.each([
    ['/steer', 'missing supplement'],
    ['/steer   ', 'blank supplement'],
    ['please /steer revise', 'leading content'],
    [' /steer revise', 'leading whitespace'],
    ['/steering revise', 'different command'],
  ])('rejects %s because it has %s', async (text, _reason) => {
    const database = openTestDatabase();
    const active = acceptTurn(database, `invalid-${_reason.replaceAll(' ', '-')}`);
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
      serviceInstanceId: `executor-service-invalid-${_reason.replaceAll(' ', '-')}`,
      now: () => '2026-07-20T02:01:00Z',
      generateId: deterministicIds(`invalid-${_reason.replaceAll(' ', '-')}`),
    });
    const execution = service.runNext();
    await providerStarted.promise;

    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: `steer-invalid-${_reason.replaceAll(' ', '-')}`,
      envelope: normalEnvelope(`invalid-command-${_reason.replaceAll(' ', '-')}`, text),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: null,
      old_turn: {
        turn_id: active.turn_id,
        state: 'running',
      },
      priority_turn: { status: 'not_created', turn_id: null },
      provider_stop_status: 'not_applicable',
      error: { code: 'steer_precondition_failed', side_effect_status: 'none' },
    });

    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: `stop-invalid-${_reason.replaceAll(' ', '-')}`,
    });
    await execution;
    await service.close();
    database.close();
  });

  test('rejects a valid command while the canonical turn is queued', async () => {
    const database = openTestDatabase();
    const queued = acceptTurn(database, 'queued-state');
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'executor-service-queued-state',
      now: () => '2026-07-20T02:01:00Z',
      generateId: deterministicIds('queued-state'),
    });

    await expect(service.steer({
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      steer_id: 'steer-queued-state',
      envelope: normalEnvelope('queued-state-command', '/steer revise the plan'),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: null,
      old_turn: { turn_id: queued.turn_id, state: 'queued' },
      priority_turn: { status: 'not_created', turn_id: null },
      provider_stop_status: 'not_applicable',
      lease_released: true,
      error: { code: 'steer_precondition_failed' },
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_steer_controls
    `).get().count).toBe(0);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_steer_requests
    `).get().count).toBe(1);

    database.prepare(`
      UPDATE runtime_turns SET state = 'running' WHERE turn_id = ?
    `).run(queued.turn_id);
    await expect(service.steer({
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      steer_id: 'steer-queued-state',
      envelope: normalEnvelope('queued-state-command', '/steer revise the plan'),
    })).resolves.toMatchObject({
      status: 'rejected',
      deduplicated: true,
      old_turn: { turn_id: queued.turn_id, state: 'queued' },
      error: { code: 'steer_precondition_failed' },
    });

    await expect(service.steer({
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      steer_id: 'steer-queued-state-other-id',
      envelope: normalEnvelope('queued-state-command', '/steer revise the plan'),
    })).resolves.toMatchObject({
      status: 'rejected',
      deduplicated: false,
      error: { code: 'idempotency_conflict' },
    });

    await service.close();
    database.close();
  });

  test.each([
    ['attachment', (envelope) => {
      const fixture = inboundFixture.valid.find(
        ({ name }) => name === 'authenticated_dm_with_attachment',
      ).document;
      envelope.content.attachments = structuredClone(fixture.content.attachments);
    }],
    ['unauthenticated actor', (envelope) => { envelope.actor.authenticated = false; }],
  ])('rejects a /steer message with %s', async (reason, mutateEnvelope) => {
    const database = openTestDatabase();
    const queued = acceptTurn(database, `invalid-shape-${reason.replaceAll(' ', '-')}`);
    const envelope = normalEnvelope(
      `invalid-shape-${reason.replaceAll(' ', '-')}`,
      '/steer revise the plan',
    );
    mutateEnvelope(envelope);
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: `executor-service-invalid-shape-${reason.replaceAll(' ', '-')}`,
      now: () => '2026-07-20T02:01:00Z',
      generateId: deterministicIds(`invalid-shape-${reason.replaceAll(' ', '-')}`),
    });

    await expect(service.steer({
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      steer_id: `steer-invalid-shape-${reason.replaceAll(' ', '-')}`,
      envelope,
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: null,
      priority_turn: { status: 'not_created', turn_id: null },
      error: { code: 'steer_precondition_failed' },
    });

    await service.close();
    database.close();
  });

  test('interrupts the running turn and queues the supplement on the same lineage', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'valid-active');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const providerInputs = [];
    const adapter = {
      async *execute(context) {
        providerInputs.push(context.input);
        context.reportProviderState({ state: 'started', provider_native_id: null });
        if (context.turn_id === active.turn_id) {
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
          return;
        }
        yield { type: 'turn_result', outcome: 'completed' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-valid',
      now: () => '2026-07-20T02:01:00Z',
      generateId: deterministicIds('steer-valid'),
    });

    const execution = service.runNext();
    await providerStarted.promise;
    const result = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-valid-1',
      envelope: normalEnvelope('valid-command', '/steer revise the plan'),
    });

    expect(result).toMatchObject({
      status: 'completed',
      winner: {
        control: 'steer',
        control_id: 'steer-valid-1',
        turn_id: active.turn_id,
      },
      old_turn: {
        turn_id: active.turn_id,
        previous_state: 'running',
        state: 'interrupted',
        side_effect_status: 'none',
      },
      priority_turn: {
        status: 'queued',
        state: 'queued',
        lineage_id: active.lineage_id,
        redirected_from_turn_id: active.turn_id,
      },
      provider_stop_status: 'confirmed',
      lease_released: true,
      error: null,
    });
    await expect(execution).resolves.toMatchObject({
      status: 'interrupted',
      turn_id: active.turn_id,
    });

    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: result.priority_turn.turn_id,
    });
    expect(providerInputs).toEqual([
      expect.objectContaining({ text: 'message valid-active' }),
      { kind: 'text', text: 'revise the plan', attachments: [] },
    ]);
    expect(database.prepare(`
      SELECT turn_id, state, lineage_id, redirected_from_turn_id
      FROM runtime_turns WHERE conversation_id = ? ORDER BY queue_sequence
    `).all(active.conversation_id)).toEqual([
      {
        turn_id: active.turn_id,
        state: 'interrupted',
        lineage_id: active.lineage_id,
        redirected_from_turn_id: null,
      },
      {
        turn_id: result.priority_turn.turn_id,
        state: 'completed',
        lineage_id: active.lineage_id,
        redirected_from_turn_id: active.turn_id,
      },
    ]);

    await service.close();
    database.close();
  });

  test('runs the steering turn before preserved ordinary FIFO work', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'priority-active');
    const ordinaryOne = acceptTurn(database, 'priority-ordinary-one');
    const ordinaryTwo = acceptTurn(database, 'priority-ordinary-two');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const executionOrder = [];
    const adapter = {
      async *execute(context) {
        executionOrder.push(context.turn_id);
        context.reportProviderState({ state: 'started', provider_native_id: null });
        if (context.turn_id === active.turn_id) {
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
          return;
        }
        yield { type: 'turn_result', outcome: 'completed' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-priority-order',
      now: () => '2026-07-20T02:02:00Z',
      generateId: deterministicIds('priority-order'),
    });

    const activeExecution = service.runNext();
    await providerStarted.promise;
    const steer = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-priority-order',
      envelope: normalEnvelope('priority-command', '/steer priority work'),
    });
    await activeExecution;
    await service.runNext();
    await service.runNext();
    await service.runNext();

    expect(executionOrder).toEqual([
      active.turn_id,
      steer.priority_turn.turn_id,
      ordinaryOne.turn_id,
      ordinaryTwo.turn_id,
    ]);
    expect(database.prepare(`
      SELECT turn_id, queue_sequence, priority
      FROM runtime_turn_queue
      WHERE conversation_id = ?
      ORDER BY queue_sequence
    `).all(active.conversation_id)).toEqual([
      { turn_id: active.turn_id, queue_sequence: 1, priority: 0 },
      { turn_id: ordinaryOne.turn_id, queue_sequence: 2, priority: 0 },
      { turn_id: ordinaryTwo.turn_id, queue_sequence: 3, priority: 0 },
      { turn_id: steer.priority_turn.turn_id, queue_sequence: 4, priority: 1 },
    ]);

    await service.close();
    database.close();
  });

  test('admits the single priority lane while the ordinary queue is at capacity', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'capacity-active');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() { providerStopped.resolve(); },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-capacity',
      now: () => '2026-07-20T02:02:15Z',
      generateId: deterministicIds('steer-capacity'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    const ordinary = Array.from({ length: 5 }, (_value, index) => (
      acceptTurn(database, `capacity-ordinary-${index + 1}`)
    ));

    const steer = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-capacity-control',
      envelope: normalEnvelope('steer-capacity-command', '/steer bypass full FIFO'),
    });
    expect(steer).toMatchObject({
      status: 'completed',
      priority_turn: { status: 'queued', priority: 1 },
    });
    expect(database.prepare(`
      SELECT priority, COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued'
      GROUP BY priority ORDER BY priority
    `).all(active.conversation_id)).toEqual([
      { priority: 0, count: 5 },
      { priority: 1, count: 1 },
    ]);
    await execution;
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-capacity-cleanup',
    })).resolves.toMatchObject({
      cancelled_turn_ids: [...ordinary.map(({ turn_id: turnId }) => turnId), steer.priority_turn.turn_id],
    });

    await service.close();
    database.close();
  });

  test('does not count the queued priority lane against ordinary queue capacity', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'ordinary-capacity-active');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async cancel() { providerStopped.resolve(); },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-ordinary-capacity',
      now: () => '2026-07-20T02:02:16Z',
      generateId: deterministicIds('ordinary-capacity'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    const ordinary = Array.from({ length: 4 }, (_value, index) => (
      acceptTurn(database, `ordinary-capacity-${index + 1}`)
    ));
    const steer = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-ordinary-capacity',
      envelope: normalEnvelope('ordinary-capacity-command', '/steer separate lane'),
    });
    await execution;

    const fifthOrdinary = acceptTurn(database, 'ordinary-capacity-5');
    const overflow = acceptTurn(database, 'ordinary-capacity-overflow');
    expect(fifthOrdinary).toMatchObject({ status: 'accepted' });
    expect(overflow).toMatchObject({
      status: 'rejected',
      error: { code: 'queue_full' },
    });
    expect(database.prepare(`
      SELECT priority, COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued'
      GROUP BY priority ORDER BY priority
    `).all(active.conversation_id)).toEqual([
      { priority: 0, count: 5 },
      { priority: 1, count: 1 },
    ]);

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-ordinary-capacity-cleanup',
    })).resolves.toMatchObject({
      cancelled_turn_ids: [
        ...ordinary.map(({ turn_id: turnId }) => turnId),
        steer.priority_turn.turn_id,
        fifthOrdinary.turn_id,
      ],
    });
    await service.close();
    database.close();
  });

  test('rebuilds the priority lane from SQLite after a service and database restart', async () => {
    const database = openTestDatabase();
    const databasePath = database.name;
    const active = acceptTurn(database, 'restart-active');
    const ordinary = acceptTurn(database, 'restart-ordinary');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const firstService = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async cancel() { providerStopped.resolve(); },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-restart-first',
      now: () => '2026-07-20T02:02:20Z',
      generateId: deterministicIds('steer-restart-first'),
    });
    const execution = firstService.runNext();
    await providerStarted.promise;
    const steer = await firstService.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-restart-control',
      envelope: normalEnvelope('steer-restart-command', '/steer survive restart'),
    });
    await execution;
    await firstService.close();
    database.close();

    const reopened = new Database(databasePath);
    const providerInputs = [];
    const secondService = createExecutorService({
      database: reopened,
      adapter: {
        async *execute(context) {
          providerInputs.push({ turn_id: context.turn_id, input: context.input });
          context.reportProviderState({ state: 'started', provider_native_id: null });
          yield { type: 'turn_result', outcome: 'completed' };
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-restart-second',
      now: () => '2026-07-20T02:02:21Z',
      generateId: deterministicIds('steer-restart-second'),
    });
    await expect(secondService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: steer.priority_turn.turn_id,
    });
    await expect(secondService.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: ordinary.turn_id,
    });
    expect(providerInputs).toEqual([
      {
        turn_id: steer.priority_turn.turn_id,
        input: { kind: 'text', text: 'survive restart', attachments: [] },
      },
      {
        turn_id: ordinary.turn_id,
        input: expect.objectContaining({ text: 'message restart-ordinary' }),
      },
    ]);

    await secondService.close();
    reopened.close();
  });

  test('seals already completed tool side effects on the interrupted turn', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'known-side-effect');
    const toolRecorded = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        yield {
          kind: 'tool_finished',
          payload: {
            tool_use_id: 'tool-use-steer-known',
            tool_name: 'workspace_write',
            summary: 'Updated a workspace file.',
            side_effect_status: 'known',
          },
          provider_native_id: null,
        };
        toolRecorded.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() { providerStopped.resolve(); },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-known-side-effect',
      now: () => '2026-07-20T02:02:30Z',
      generateId: deterministicIds('steer-known-side-effect'),
    });
    const execution = service.runNext();
    await toolRecorded.promise;

    const steer = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-known-side-effect',
      envelope: normalEnvelope('known-side-effect-command', '/steer preserve side effects'),
    });
    expect(steer).toMatchObject({
      old_turn: { state: 'interrupted', side_effect_status: 'known' },
      error: null,
    });
    expect(JSON.parse(database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
    `).get(active.turn_id).event_json)).toMatchObject({
      phase: 'interrupted',
      error: { code: 'turn_interrupted', side_effect_status: 'known' },
    });
    await execution;
    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-known-side-effect-cleanup',
    });

    await service.close();
    database.close();
  });

  test('fences provider progress that arrives after the durable steer winner', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'late-progress');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield {
          kind: 'text_snapshot',
          payload: { text: 'late provider text', end_offset: 18 },
          provider_native_id: null,
        };
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() { providerStopped.resolve(); },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-late-progress',
      now: () => '2026-07-20T02:02:40Z',
      generateId: deterministicIds('steer-late-progress'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    const steer = await service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-late-progress',
      envelope: normalEnvelope('late-progress-command', '/steer fence late output'),
    });
    await execution;

    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_normalized_events
      WHERE turn_id = ? AND json_extract(event_json, '$.kind') = 'text_snapshot'
    `).get(active.turn_id).count).toBe(0);
    expect(database.prepare(`
      SELECT event_kind, reason_code
      FROM runtime_provider_event_diagnostics
      WHERE turn_id = ? AND event_kind = 'text_snapshot'
    `).get(active.turn_id)).toEqual({
      event_kind: 'text_snapshot',
      reason_code: 'stale_attempt',
    });
    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: `stop-${steer.priority_turn.turn_id}`,
    });

    await service.close();
    database.close();
  });

  test.each([
    ['known', 'completed', 'queued'],
    ['unknown', 'failed', 'blocked_recovery'],
  ])(
    'seals a late %s tool side effect from the winning attempt before deciding priority work',
    async (sideEffectStatus, expectedStatus, expectedPriorityStatus) => {
      const database = openTestDatabase();
      const active = acceptTurn(database, `late-tool-${sideEffectStatus}`);
      const providerStarted = deferred();
      const providerStopped = deferred();
      const adapter = {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield {
            kind: 'tool_finished',
            payload: {
              tool_use_id: `tool-use-late-${sideEffectStatus}`,
              tool_name: 'workspace_write',
              summary: 'The provider reported this after steering won.',
              side_effect_status: sideEffectStatus,
            },
            provider_native_id: null,
          };
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async cancel() { providerStopped.resolve(); },
      };
      const service = createExecutorService({
        database,
        adapter,
        provider: 'codex',
        serviceInstanceId: `executor-service-late-tool-${sideEffectStatus}`,
        now: () => '2026-07-20T02:02:50Z',
        generateId: deterministicIds(`late-tool-${sideEffectStatus}`),
      });
      const execution = service.runNext();
      await providerStarted.promise;

      const steer = await service.steer({
        conversation_id: active.conversation_id,
        turn_id: active.turn_id,
        steer_id: `steer-late-tool-${sideEffectStatus}`,
        envelope: normalEnvelope(
          `late-tool-${sideEffectStatus}-command`,
          '/steer account for the late tool result',
        ),
      });
      expect(steer).toMatchObject({
        status: expectedStatus,
        old_turn: { state: 'interrupted', side_effect_status: sideEffectStatus },
        priority_turn: { status: expectedPriorityStatus },
      });
      if (sideEffectStatus === 'unknown') {
        expect(steer).toMatchObject({
          lease_released: false,
          error: { code: 'side_effect_unknown', side_effect_status: 'unknown' },
          incident: { status: 'manual_recovery_required' },
        });
      }
      expect(database.prepare(`
        SELECT event_kind, reason_code
        FROM runtime_provider_event_diagnostics
        WHERE turn_id = ? AND event_kind = 'tool_finished'
      `).get(active.turn_id)).toEqual({
        event_kind: 'tool_finished',
        reason_code: 'stale_attempt',
      });
      await execution;

      if (sideEffectStatus === 'known') {
        await service.stop({
          conversation_id: active.conversation_id,
          stop_id: 'stop-late-tool-known-cleanup',
        });
        await service.close();
      }
      database.close();
    },
  );

  test('uses the durable running CAS to select one of two concurrent steers', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'double-active');
    const providerStarted = deferred();
    const releaseCancellation = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        await releaseCancellation.promise;
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-double-steer',
      now: () => '2026-07-20T02:03:00Z',
      generateId: deterministicIds('double-steer'),
    });
    const execution = service.runNext();
    await providerStarted.promise;

    const firstRequest = {
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-double-first',
      envelope: normalEnvelope('double-first', '/steer first wins'),
    };
    const first = service.steer(firstRequest);
    const duplicateFirst = service.steer(structuredClone(firstRequest));
    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-double-first',
      envelope: normalEnvelope('double-first-conflict', '/steer conflicting replay'),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: { control: 'steer', control_id: 'steer-double-first' },
      error: { code: 'idempotency_conflict' },
    });
    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-double-second',
      envelope: normalEnvelope('double-second', '/steer second loses'),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: {
        control: 'steer',
        control_id: 'steer-double-first',
        turn_id: active.turn_id,
      },
      old_turn: { state: 'redirecting' },
      priority_turn: { status: 'not_created', turn_id: null },
      error: { code: 'steer_precondition_failed' },
    });
    releaseCancellation.resolve();
    await expect(first).resolves.toMatchObject({
      status: 'completed',
      winner: { control_id: 'steer-double-first' },
      priority_turn: { status: 'queued' },
    });
    await expect(duplicateFirst).resolves.toMatchObject({
      status: 'completed',
      deduplicated: true,
      winner: { control_id: 'steer-double-first' },
      priority_turn: { status: 'queued' },
    });
    await execution;

    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_steer_controls
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
      WHERE redirected_from_turn_id = ?
    `).get(active.turn_id).count).toBe(1);

    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-double-cleanup',
    });
    await service.close();
    database.close();
  });

  test('rejects steer with the durable stop winner when stop wins running CAS', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'stop-wins-active');
    const providerStarted = deferred();
    const releaseCancellation = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(_context, options = {}) {
        expect(options.reason).toBeUndefined();
        await releaseCancellation.promise;
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-stop-wins',
      now: () => '2026-07-20T02:04:00Z',
      generateId: deterministicIds('stop-wins'),
    });
    const execution = service.runNext();
    await providerStarted.promise;

    const stop = service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-wins-control',
    });
    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-after-stop',
      envelope: normalEnvelope('steer-after-stop', '/steer too late'),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: {
        control: 'stop',
        control_id: 'stop-wins-control',
        turn_id: active.turn_id,
      },
      old_turn: { state: 'stopped' },
      priority_turn: { status: 'not_created', turn_id: null },
      error: { code: 'turn_terminal' },
    });
    releaseCancellation.resolve();
    await stop;
    await execution;
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_steer_controls
    `).get().count).toBe(0);

    await service.close();
    database.close();
  });

  test('applies a stop barrier when steer wins running CAS before priority creation', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'steer-wins-active');
    const ordinary = acceptTurn(database, 'steer-wins-ordinary');
    const providerStarted = deferred();
    const releaseCancellation = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        await releaseCancellation.promise;
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-wins',
      now: () => '2026-07-20T02:05:00Z',
      generateId: deterministicIds('steer-wins'),
    });
    const execution = service.runNext();
    await providerStarted.promise;

    const steer = service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-wins-control',
      envelope: normalEnvelope('steer-wins-command', '/steer won the running CAS'),
    });
    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-after-steer',
    })).resolves.toMatchObject({
      status: 'barrier_applied',
      active_turn: {
        turn_id: active.turn_id,
        previous_state: 'redirecting',
        state: 'redirecting',
      },
      cancelled_turn_ids: [ordinary.turn_id],
      steering: {
        steer_id: 'steer-wins-control',
        priority_turn: { status: 'not_created', turn_id: null },
      },
      provider_stop_status: 'not_applicable',
      lease_released: false,
    });
    const redirectProjection = JSON.parse(database.prepare(`
      SELECT render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(active.turn_id).render_model_json);
    expect(redirectProjection).toMatchObject({
      phase: 'redirecting',
      terminal: false,
      text: expect.stringMatching(/interrupting|stopping/i),
    });
    expect(redirectProjection.text).toMatch(/not (?:be )?rolled back/i);
    releaseCancellation.resolve();
    await expect(steer).resolves.toMatchObject({
      status: 'completed',
      winner: {
        control: 'steer',
        control_id: 'steer-wins-control',
      },
      old_turn: { state: 'interrupted' },
      priority_turn: {
        status: 'not_created',
        turn_id: null,
        stop_barrier_id: 'stop-after-steer',
      },
      provider_stop_status: 'confirmed',
      lease_released: true,
    });
    await expect(execution).resolves.toMatchObject({
      status: 'interrupted',
      turn_id: active.turn_id,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
      WHERE redirected_from_turn_id = ?
    `).get(active.turn_id).count).toBe(0);

    await service.close();
    database.close();
  });

  test('keeps the first steer barrier while a later stop clears its newer cutoff', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'repeat-barrier-active');
    const providerStarted = deferred();
    const releaseCancellation = deferred();
    const providerStopped = deferred();
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async cancel() {
          await releaseCancellation.promise;
          providerStopped.resolve();
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-repeat-barrier',
      now: () => '2026-07-20T02:05:30Z',
      generateId: deterministicIds('repeat-barrier'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    const steer = service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-repeat-barrier',
      envelope: normalEnvelope('repeat-barrier-command', '/steer barrier in progress'),
    });
    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-repeat-barrier-first',
    });
    const laterIngress = acceptTurn(database, 'repeat-barrier-later-ingress');

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-repeat-barrier-second',
    })).resolves.toMatchObject({
      status: 'barrier_applied',
      active_turn: { turn_id: active.turn_id, state: 'redirecting' },
      cancelled_turn_ids: [laterIngress.turn_id],
      steering: {
        steer_id: 'steer-repeat-barrier',
        stop_barrier_id: 'stop-repeat-barrier-first',
        priority_turn: {
          status: 'not_created',
          stop_barrier_id: 'stop-repeat-barrier-first',
        },
      },
    });
    releaseCancellation.resolve();
    await expect(steer).resolves.toMatchObject({
      status: 'completed',
      stop_barrier_id: 'stop-repeat-barrier-first',
      priority_turn: {
        status: 'not_created',
        stop_barrier_id: 'stop-repeat-barrier-first',
      },
    });
    await execution;
    expect(database.prepare(`
      SELECT stop_id FROM runtime_stop_controls
      WHERE conversation_id = ? ORDER BY committed_at, stop_id
    `).all(active.conversation_id)).toEqual([
      { stop_id: 'stop-repeat-barrier-first' },
      { stop_id: 'stop-repeat-barrier-second' },
    ]);
    for (const stopId of [
      'stop-repeat-barrier-first',
      'stop-repeat-barrier-second',
    ]) {
      await expect(service.stop({
        conversation_id: active.conversation_id,
        stop_id: stopId,
      })).resolves.toMatchObject({
        status: 'barrier_completed',
        deduplicated: true,
        active_turn: { turn_id: active.turn_id, state: 'interrupted' },
        steering: {
          steer_id: 'steer-repeat-barrier',
          stop_barrier_id: 'stop-repeat-barrier-first',
          priority_turn: {
            status: 'not_created',
            stop_barrier_id: 'stop-repeat-barrier-first',
          },
        },
        provider_stop_status: 'confirmed',
        lease_released: true,
      });
    }

    await service.close();
    database.close();
  });

  test('cancels an already queued steering turn and reports it on both controls', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'queued-priority-active');
    const ordinary = acceptTurn(database, 'queued-priority-ordinary');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerStopped.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        providerStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-queued-priority-stop',
      now: () => '2026-07-20T02:06:00Z',
      generateId: deterministicIds('queued-priority-stop'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    const steerRequest = {
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-queued-priority',
      envelope: normalEnvelope('queued-priority-command', '/steer queue then stop'),
    };
    const steer = await service.steer(steerRequest);
    await execution;

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-queued-priority',
    })).resolves.toMatchObject({
      status: 'queue_cleared',
      active_turn: null,
      cancelled_turn_ids: [ordinary.turn_id, steer.priority_turn.turn_id],
      steering: {
        steer_id: 'steer-queued-priority',
        winner: { control: 'steer', control_id: 'steer-queued-priority' },
        priority_turn: {
          turn_id: steer.priority_turn.turn_id,
          status: 'cancelled',
          state: 'cancelled',
          stop_barrier_id: 'stop-queued-priority',
        },
      },
    });
    await expect(service.steer(steerRequest)).resolves.toMatchObject({
      status: 'completed',
      deduplicated: true,
      winner: { control: 'steer', control_id: 'steer-queued-priority' },
      priority_turn: {
        turn_id: steer.priority_turn.turn_id,
        status: 'cancelled',
        state: 'cancelled',
        stop_barrier_id: 'stop-queued-priority',
      },
    });

    await service.close();
    database.close();
  });

  test('stops an active steering turn and reports its terminal result to the steer control', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'active-priority-active');
    const oldStarted = deferred();
    const oldStopped = deferred();
    const priorityStarted = deferred();
    const priorityStopped = deferred();
    let priorityTurnId;
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        if (context.turn_id === active.turn_id) {
          oldStarted.resolve();
          await oldStopped.promise;
        } else {
          priorityTurnId = context.turn_id;
          priorityStarted.resolve();
          await priorityStopped.promise;
        }
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(context, options = {}) {
        if (options.reason === 'steer') {
          expect(context.turn_id).toBe(active.turn_id);
          oldStopped.resolve();
          return;
        }
        expect(context.turn_id).toBe(priorityTurnId);
        priorityStopped.resolve();
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-active-priority-stop',
      now: () => '2026-07-20T02:07:00Z',
      generateId: deterministicIds('active-priority-stop'),
    });
    const oldExecution = service.runNext();
    await oldStarted.promise;
    const steerRequest = {
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-active-priority',
      envelope: normalEnvelope('active-priority-command', '/steer start then stop'),
    };
    const steer = await service.steer(steerRequest);
    await oldExecution;
    const priorityExecution = service.runNext();
    await priorityStarted.promise;

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-active-priority',
    })).resolves.toMatchObject({
      status: 'stopped',
      active_turn: {
        turn_id: steer.priority_turn.turn_id,
        previous_state: 'running',
        state: 'stopped',
      },
      steering: {
        steer_id: 'steer-active-priority',
        winner: { control: 'steer', control_id: 'steer-active-priority' },
        priority_turn: {
          turn_id: steer.priority_turn.turn_id,
          status: 'stopped',
          state: 'stopped',
          stop_barrier_id: 'stop-active-priority',
        },
      },
    });
    await priorityExecution;
    await expect(service.steer(steerRequest)).resolves.toMatchObject({
      deduplicated: true,
      priority_turn: {
        turn_id: steer.priority_turn.turn_id,
        status: 'stopped',
        state: 'stopped',
        stop_barrier_id: 'stop-active-priority',
      },
    });

    await service.close();
    database.close();
  });

  test('blocks priority work and retains the lease when provider isolation is uncertain', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'uncertain-active');
    const providerStarted = deferred();
    const providerEventuallyEnds = deferred();
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        providerStarted.resolve();
        await providerEventuallyEnds.promise;
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel(_context, { reason } = {}) {
        expect(reason).toBe('steer');
        const error = new Error('provider interruption acknowledgement was lost');
        error.cancellationUncertain = true;
        throw error;
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-uncertain',
      now: () => '2026-07-20T02:08:00Z',
      generateId: deterministicIds('steer-uncertain'),
    });
    const execution = service.runNext();
    await providerStarted.promise;

    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-uncertain-control',
      envelope: normalEnvelope('steer-uncertain-command', '/steer unsafe to continue'),
    })).resolves.toMatchObject({
      status: 'failed',
      winner: { control: 'steer', control_id: 'steer-uncertain-control' },
      old_turn: {
        turn_id: active.turn_id,
        state: 'interrupted',
        side_effect_status: 'unknown',
      },
      priority_turn: {
        status: 'blocked_recovery',
        turn_id: null,
        state: null,
      },
      provider_stop_status: 'uncertain',
      lease_released: false,
      error: { code: 'side_effect_unknown', side_effect_status: 'unknown' },
      incident: {
        status: 'manual_recovery_required',
        side_effect_status: 'unknown',
      },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(active.turn_id)).toEqual({ state: 'interrupted' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: 'executor-service-steer-uncertain',
      turn_id: active.turn_id,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
      WHERE redirected_from_turn_id = ?
    `).get(active.turn_id).count).toBe(0);

    providerEventuallyEnds.resolve();
    await expect(execution).resolves.toMatchObject({ status: 'interrupted' });
    database.close();
  });

  test('rejects steer without cancelling a durable waiting-user interaction', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'waiting-interaction');
    let cancelCalls = 0;
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-steer-waiting-question',
            tool_use_id: 'tool-steer-waiting-question',
            kind: 'question',
            prompt: 'Choose before continuing.',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
        yield { type: 'turn_result', outcome: 'cancelled' };
      },
      async cancel() { cancelCalls += 1; },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-waiting',
      now: () => '2026-07-20T02:09:00Z',
      generateId: deterministicIds('steer-waiting'),
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'waiting_user',
      turn_id: active.turn_id,
    });

    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-waiting-control',
      envelope: normalEnvelope('steer-waiting-command', '/steer do not bypass question'),
    })).resolves.toMatchObject({
      status: 'rejected',
      winner: null,
      old_turn: { state: 'waiting_user' },
      error: { code: 'steer_precondition_failed' },
    });
    expect(cancelCalls).toBe(0);
    expect(database.prepare(`
      SELECT state FROM runtime_interactions WHERE turn_id = ?
    `).get(active.turn_id)).toEqual({ state: 'pending' });

    await service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-waiting-cleanup',
    });
    expect(cancelCalls).toBe(1);
    await service.close();
    database.close();
  });

  test.each([
    ['pending', 'not_started'],
    ['answer_committed', 'pending'],
    ['answer_delivering', 'delivering'],
    ['delivery_unknown', 'delivery_unknown'],
  ])(
    'moves an impossible running + %s handoff projection into fenced reconciliation',
    async (interactionState, handoffState) => {
      const database = openTestDatabase();
      const active = acceptTurn(database, `interaction-inconsistent-${interactionState}`);
      const providerStarted = deferred();
      const providerStopped = deferred();
      let abortCalls = 0;
      const service = createExecutorService({
        database,
        adapter: {
          async *execute(context) {
            context.reportProviderState({ state: 'started', provider_native_id: null });
            providerStarted.resolve();
            await providerStopped.promise;
            yield { type: 'turn_result', outcome: 'cancelled' };
          },
          async abort() {
            abortCalls += 1;
            providerStopped.resolve();
          },
        },
        provider: 'codex',
        serviceInstanceId: `executor-service-inconsistent-${interactionState}`,
        now: () => '2026-07-20T02:09:30Z',
        generateId: deterministicIds(`interaction-inconsistent-${interactionState}`),
      });
      const execution = service.runNext();
      await providerStarted.promise;
      database.prepare(`
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id,
          parent_type, parent_id, ordinal, state, version,
          handoff_state, handoff_version, request_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 99, ?, 1, ?, 1, ?, ?, ?)
      `).run(
        `interaction-inconsistent-${interactionState}`,
        active.conversation_id,
        active.turn_id,
        active.lineage_id,
        active.turn_id,
        interactionState,
        handoffState,
        JSON.stringify({ state: interactionState, handoff_state: handoffState }),
        '2026-07-20T02:09:30Z',
        '2026-07-20T02:09:30Z',
      );

      const steerRequest = {
        conversation_id: active.conversation_id,
        turn_id: active.turn_id,
        steer_id: `steer-inconsistent-${interactionState}`,
        envelope: normalEnvelope(
          `interaction-inconsistent-${interactionState}-command`,
          '/steer cannot bypass durable interaction ownership',
        ),
      };
      await expect(service.steer(steerRequest)).resolves.toMatchObject({
        status: 'rejected',
        reconciliation_required: true,
        old_turn: { turn_id: active.turn_id, state: 'recovering' },
        priority_turn: { status: 'not_created', turn_id: null },
        lease_released: true,
        error: { code: 'steer_reconciliation_required' },
        reconciliation: { provider_isolated: true },
      });
      await expect(service.steer(steerRequest)).resolves.toMatchObject({
        status: 'rejected',
        deduplicated: true,
        reconciliation_required: true,
        reconciliation: { provider_isolated: true },
      });
      expect(abortCalls).toBe(1);
      expect(database.prepare(`
        SELECT state FROM runtime_turns WHERE turn_id = ?
      `).get(active.turn_id)).toEqual({ state: 'recovering' });
      expect(database.prepare(`
        SELECT lease_owner, turn_id FROM runtime_executor_leases
        WHERE conversation_id = ?
      `).get(active.conversation_id)).toEqual({ lease_owner: null, turn_id: null });
      expect(database.prepare(`
        SELECT state, handoff_state FROM runtime_interactions WHERE turn_id = ?
      `).get(active.turn_id)).toEqual({
        state: interactionState,
        handoff_state: handoffState,
      });
      await execution;
      database.close();
    },
  );

  test('keeps failed reconciliation isolation under shutdown supervision', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-reconciliation-supervision');
    const providerStarted = deferred();
    const providerStopped = deferred();
    let abortCalls = 0;
    let closeCalls = 0;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async abort() {
          abortCalls += 1;
          throw new Error('provider isolation is not yet proven');
        },
        async close() {
          closeCalls += 1;
          providerStopped.resolve();
          return [active.conversation_id];
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-reconciliation-supervision',
      now: () => '2026-07-20T02:09:40Z',
      generateId: deterministicIds('interaction-reconciliation-supervision'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id,
        parent_type, parent_id, ordinal, state, version,
        handoff_state, handoff_version, request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 99, 'delivery_unknown', 1,
        'delivery_unknown', 1, ?, ?, ?)
    `).run(
      'interaction-reconciliation-supervision',
      active.conversation_id,
      active.turn_id,
      active.lineage_id,
      active.turn_id,
      JSON.stringify({ state: 'delivery_unknown', handoff_state: 'delivery_unknown' }),
      '2026-07-20T02:09:40Z',
      '2026-07-20T02:09:40Z',
    );
    const steerRequest = {
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-reconciliation-supervision',
      envelope: normalEnvelope(
        'interaction-reconciliation-supervision-command',
        '/steer preserve unproven provider supervision',
      ),
    };

    await expect(service.steer(steerRequest)).resolves.toMatchObject({
      status: 'rejected',
      reconciliation_required: true,
      lease_released: false,
      reconciliation: {
        status: 'provider_isolation_unproven',
        provider_isolated: false,
      },
    });
    expect(abortCalls).toBe(1);
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({
      lease_owner: 'executor-service-reconciliation-supervision',
      turn_id: active.turn_id,
    });

    await expect(service.close()).resolves.toBeUndefined();
    await expect(execution).resolves.toMatchObject({ status: 'recovering' });
    expect(closeCalls).toBe(1);
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({ lease_owner: null, turn_id: null });
    await expect(service.steer(steerRequest)).resolves.toMatchObject({
      status: 'rejected',
      deduplicated: true,
      lease_released: true,
      reconciliation: {
        status: 'provider_isolated_manual_recovery_required',
        provider_isolated: true,
      },
    });
    database.close();
  });

  test('uses a provider terminal proof that wins before abort rejects', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-reconciliation-terminal-race');
    const providerStarted = deferred();
    const releaseProvider = deferred();
    let execution;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await releaseProvider.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async abort() {
          releaseProvider.resolve();
          await execution;
          throw new Error('abort acknowledgement lost after provider terminal');
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-reconciliation-terminal-race',
      now: () => '2026-07-20T02:09:45Z',
      generateId: deterministicIds('interaction-reconciliation-terminal-race'),
    });
    execution = service.runNext();
    await providerStarted.promise;
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id,
        parent_type, parent_id, ordinal, state, version,
        handoff_state, handoff_version, request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 99, 'delivery_unknown', 1,
        'delivery_unknown', 1, ?, ?, ?)
    `).run(
      'interaction-reconciliation-terminal-race',
      active.conversation_id,
      active.turn_id,
      active.lineage_id,
      active.turn_id,
      JSON.stringify({ state: 'delivery_unknown', handoff_state: 'delivery_unknown' }),
      '2026-07-20T02:09:45Z',
      '2026-07-20T02:09:45Z',
    );

    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-reconciliation-terminal-race',
      envelope: normalEnvelope(
        'interaction-reconciliation-terminal-race-command',
        '/steer preserve terminal isolation proof',
      ),
    })).resolves.toMatchObject({
      status: 'rejected',
      reconciliation_required: true,
      lease_released: true,
      reconciliation: {
        status: 'provider_isolated_manual_recovery_required',
        provider_isolated: true,
      },
    });
    await expect(execution).resolves.toMatchObject({ status: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({ lease_owner: null, turn_id: null });

    await service.close();
    database.close();
  });

  test('deduplicates ownership release when shutdown proves isolation before abort returns', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-reconciliation-close-race');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const abortStarted = deferred();
    const releaseAbort = deferred();
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async abort() {
          abortStarted.resolve();
          await releaseAbort.promise;
        },
        async close() {
          providerStopped.resolve();
          return [active.conversation_id];
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-reconciliation-close-race',
      now: () => '2026-07-20T02:09:50Z',
      generateId: deterministicIds('interaction-reconciliation-close-race'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id,
        parent_type, parent_id, ordinal, state, version,
        handoff_state, handoff_version, request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 99, 'delivery_unknown', 1,
        'delivery_unknown', 1, ?, ?, ?)
    `).run(
      'interaction-reconciliation-close-race',
      active.conversation_id,
      active.turn_id,
      active.lineage_id,
      active.turn_id,
      JSON.stringify({ state: 'delivery_unknown', handoff_state: 'delivery_unknown' }),
      '2026-07-20T02:09:50Z',
      '2026-07-20T02:09:50Z',
    );
    const steering = service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-reconciliation-close-race',
      envelope: normalEnvelope(
        'interaction-reconciliation-close-race-command',
        '/steer coordinate shutdown isolation proof',
      ),
    });
    await abortStarted.promise;

    await expect(service.close()).resolves.toBeUndefined();
    await expect(execution).resolves.toMatchObject({ status: 'recovering' });
    releaseAbort.resolve();
    await expect(steering).resolves.toMatchObject({
      lease_released: true,
      reconciliation: { provider_isolated: true },
    });
    await expect(service.runNext()).rejects.toThrow(/service is closed/i);
    expect(database.prepare(`
      SELECT lease_owner, turn_id FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(active.conversation_id)).toEqual({ lease_owner: null, turn_id: null });
    database.close();
  });

  test('lets stop supersede pending reconciliation supervision and result projection', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'interaction-reconciliation-stop-race');
    const providerStarted = deferred();
    const providerStopped = deferred();
    const abortStarted = deferred();
    const releaseAbort = deferred();
    const service = createExecutorService({
      database,
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerStopped.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async abort() {
          abortStarted.resolve();
          await releaseAbort.promise;
        },
        async cancel() {
          providerStopped.resolve();
        },
      },
      provider: 'codex',
      serviceInstanceId: 'executor-service-reconciliation-stop-race',
      now: () => '2026-07-20T02:09:55Z',
      generateId: deterministicIds('interaction-reconciliation-stop-race'),
    });
    const execution = service.runNext();
    await providerStarted.promise;
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id,
        parent_type, parent_id, ordinal, state, version,
        handoff_state, handoff_version, request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 99, 'delivery_unknown', 1,
        'delivery_unknown', 1, ?, ?, ?)
    `).run(
      'interaction-reconciliation-stop-race',
      active.conversation_id,
      active.turn_id,
      active.lineage_id,
      active.turn_id,
      JSON.stringify({ state: 'delivery_unknown', handoff_state: 'delivery_unknown' }),
      '2026-07-20T02:09:55Z',
      '2026-07-20T02:09:55Z',
    );
    const steerRequest = {
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-reconciliation-stop-race',
      envelope: normalEnvelope(
        'interaction-reconciliation-stop-race-command',
        '/steer let durable stop supersede this reconciliation',
      ),
    };
    const steering = service.steer(steerRequest);
    await abortStarted.promise;

    await expect(service.stop({
      conversation_id: active.conversation_id,
      stop_id: 'stop-reconciliation-stop-race',
    })).resolves.toMatchObject({
      status: 'stopped',
      active_turn: { turn_id: active.turn_id, state: 'stopped' },
      provider_stop_status: 'confirmed',
      lease_released: true,
    });
    await expect(execution).resolves.toMatchObject({ status: 'stopped' });
    releaseAbort.resolve();
    await expect(steering).resolves.toMatchObject({
      status: 'rejected',
      winner: {
        control: 'stop',
        control_id: 'stop-reconciliation-stop-race',
        turn_id: active.turn_id,
      },
      old_turn: { state: 'stopped' },
      lease_released: true,
      reconciliation: {
        status: 'superseded_by_stop',
        provider_isolated: true,
      },
    });
    await expect(service.steer(steerRequest)).resolves.toMatchObject({
      deduplicated: true,
      winner: { control: 'stop', control_id: 'stop-reconciliation-stop-race' },
      lease_released: true,
      reconciliation: { status: 'superseded_by_stop' },
    });
    await expect(service.close()).resolves.toBeUndefined();
    database.close();
  });

  test('rejects steer while starting and again after the turn is terminal', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'starting-terminal');
    const providerEntered = deferred();
    const releaseStart = deferred();
    const adapter = {
      async *execute(context) {
        providerEntered.resolve();
        await releaseStart.promise;
        context.reportProviderState({ state: 'started', provider_native_id: null });
        yield { type: 'turn_result', outcome: 'completed' };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-starting-terminal',
      now: () => '2026-07-20T02:10:00Z',
      generateId: deterministicIds('steer-starting-terminal'),
    });
    const execution = service.runNext();
    await providerEntered.promise;

    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-starting-control',
      envelope: normalEnvelope('steer-starting-command', '/steer not while starting'),
    })).resolves.toMatchObject({
      status: 'rejected',
      old_turn: { state: 'starting' },
      error: { code: 'steer_precondition_failed' },
    });
    releaseStart.resolve();
    await expect(execution).resolves.toMatchObject({ status: 'completed' });
    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-terminal-control',
      envelope: normalEnvelope('steer-terminal-command', '/steer not after completion'),
    })).resolves.toMatchObject({
      status: 'rejected',
      old_turn: { state: 'completed' },
      lease_released: true,
      error: { code: 'turn_terminal' },
    });

    await service.close();
    database.close();
  });

  test('rejects steer while provider recovery owns the fenced turn', async () => {
    const database = openTestDatabase();
    const active = acceptTurn(database, 'recovering-state');
    const providerFailure = Object.assign(new Error('provider connection lost'), {
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The provider connection was lost.',
      },
    });
    const adapter = {
      async *execute(context) {
        context.reportProviderState({ state: 'started', provider_native_id: null });
        context.reportProviderFailure(providerFailure);
        throw providerFailure;
      },
      async abort() {},
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-steer-recovering',
      now: () => '2026-07-20T02:11:00Z',
      generateId: deterministicIds('steer-recovering'),
    });
    await expect(service.runNext()).resolves.toMatchObject({ status: 'recovering' });
    await expect(service.steer({
      conversation_id: active.conversation_id,
      turn_id: active.turn_id,
      steer_id: 'steer-recovering-control',
      envelope: normalEnvelope('steer-recovering-command', '/steer not during recovery'),
    })).resolves.toMatchObject({
      status: 'rejected',
      old_turn: { state: 'recovering' },
      lease_released: false,
      error: { code: 'steer_precondition_failed' },
    });

    database.close();
  });
});
