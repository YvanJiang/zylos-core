import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateNormalizedEvent,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';

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
        from_state: 'running',
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
    const delivery = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main'
    `).get(accepted.turn_id).command_json);
    expect(delivery.render_model).toMatchObject({
      phase: 'failed',
      terminal: true,
      error: terminalEvent.error,
    });

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
        trace_id: 'trace-canonical',
        input: normalEnvelope().content,
        lineage: { provider_native_id: null },
        bindProviderNativeId: expect.any(Function),
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
    expect(turnOutbox.aggregate_version).toBe(6);
    const deliveryCommand = JSON.parse(turnOutbox.command_json);
    expect(validateDeliveryCommand(deliveryCommand).forwarded).toEqual(deliveryCommand);
    expect(deliveryCommand).toEqual(expect.objectContaining({
      aggregate_type: 'turn_main',
      aggregate_id: accepted.turn_id,
      operation: 'create_main',
      aggregate_version: 6,
      event_sequence_through: 6,
      render_model: expect.objectContaining({
        phase: 'completed',
        text: 'provider-neutral result',
        terminal: true,
      }),
    }));

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

  test('rolls back queue claim, lease, state, event, and outbox when the atomic commit fails', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'atomic-claim');
    const store = createTestStore(database, 'atomic-claim');
    const before = readAuthority(database, accepted.turn_id, accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER force_executor_outbox_failure
      BEFORE UPDATE ON runtime_outbox
      WHEN OLD.aggregate_type = 'turn_main' AND NEW.aggregate_version = 3
      BEGIN
        SELECT RAISE(ABORT, 'forced executor outbox failure');
      END;
    `);

    expect(() => store.claimNextQueuedTurn()).toThrow(/forced executor outbox failure/);
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
      CREATE TRIGGER force_completion_outbox_failure
      BEFORE UPDATE ON runtime_outbox
      WHEN OLD.aggregate_type = 'turn_main' AND NEW.aggregate_version = 5
      BEGIN
        SELECT RAISE(ABORT, 'forced completion outbox failure');
      END;
    `);

    expect(() => store.transitionTurn(turnContext, 'running', 'completed'))
      .toThrow(/forced completion outbox failure/);
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

  test('rolls back adapter event version, event, and outbox projection as one commit', () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'atomic-adapter-event');
    const store = createTestStore(database, 'atomic-adapter-event');
    const turnContext = store.claimNextQueuedTurn();
    store.transitionTurn(turnContext, 'starting', 'running');
    const before = readAuthority(database, accepted.turn_id, accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER force_adapter_event_outbox_failure
      BEFORE UPDATE ON runtime_outbox
      WHEN OLD.aggregate_type = 'turn_main' AND NEW.aggregate_version = 5
      BEGIN
        SELECT RAISE(ABORT, 'forced adapter event outbox failure');
      END;
    `);

    expect(() => store.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'must roll back', end_offset: 14 },
      provider_native_id: null,
    })).toThrow(/forced adapter event outbox failure/);
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id)).toEqual(before);

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
