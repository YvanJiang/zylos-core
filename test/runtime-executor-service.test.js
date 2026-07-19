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
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
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

    await service.close();
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
    await expect(competingService.runNext()).resolves.toMatchObject({
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
    expect(() => store.appendAdapterEvent(staleContexts[0], {
      kind: 'text_snapshot',
      payload: { text: 'late output', end_offset: 11 },
      provider_native_id: null,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readAuthority(database, accepted.turn_id, accepted.conversation_id))
      .toEqual(runningAuthority);

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
