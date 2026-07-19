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
        provider_native_id: null,
        trace_id: 'trace-canonical',
        input: normalEnvelope().content,
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
    expect(() => store.appendAdapterEvent(staleContexts[0], {
      kind: 'text_snapshot',
      payload: { text: 'late output', end_offset: 11 },
      provider_native_id: null,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
      .toEqual(runningAuthority);

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
