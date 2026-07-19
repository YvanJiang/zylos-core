import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { stageMainProjection } from '../runtime/persistence/main-projection.js';
import { initializeRuntimePersistence } from '../runtime/persistence/schema.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-outbox-service-'));
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

function deliveredResult(command, resultAt) {
  return {
    contract: 'zylos.delivery-result',
    contract_version: '1.0',
    trace_id: command.trace_id,
    outbox_id: command.outbox_id,
    delivery_id: command.delivery_id,
    idempotency_key: command.idempotency_key,
    delivery_attempt_id: command.delivery_attempt_id,
    delivery_attempt_no: command.delivery_attempt_no,
    outbox_lease_epoch: command.outbox_lease_epoch,
    mapping_id: command.mapping.mapping_id,
    operation: command.operation,
    aggregate_version: command.aggregate_version,
    status: 'delivered',
    platform_message_id: command.operation === 'update_main'
      ? command.target_platform_message_id
      : `platform-${command.delivery_attempt_id}`,
    applied_platform_version: null,
    delivered_at: resultAt,
    error: null,
    renderer_capabilities: {
      supports_update: true,
      supports_actions: true,
      supports_platform_idempotency: true,
      supports_platform_version: false,
    },
    result_at: resultAt,
  };
}

function retryableFailure(command, resultAt) {
  return {
    contract: 'zylos.delivery-result',
    contract_version: '1.0',
    trace_id: command.trace_id,
    outbox_id: command.outbox_id,
    delivery_id: command.delivery_id,
    idempotency_key: command.idempotency_key,
    delivery_attempt_id: command.delivery_attempt_id,
    delivery_attempt_no: command.delivery_attempt_no,
    outbox_lease_epoch: command.outbox_lease_epoch,
    mapping_id: command.mapping.mapping_id,
    operation: command.operation,
    aggregate_version: command.aggregate_version,
    status: 'retryable_failure',
    platform_message_id: command.operation === 'update_main'
      ? command.target_platform_message_id
      : null,
    applied_platform_version: null,
    delivered_at: null,
    error: {
      code: 'delivery_transient',
      category: 'channel',
      retryable: true,
      side_effect_status: 'none',
      user_message: 'The channel is temporarily unavailable.',
      occurred_at: resultAt,
    },
    renderer_capabilities: {
      supports_update: true,
      supports_actions: true,
      supports_platform_idempotency: true,
      supports_platform_version: false,
    },
    result_at: resultAt,
  };
}

function readDeliveryAuthority(database, outboxId) {
  return {
    outbox: database.prepare(`
      SELECT status, attempt_count, delivery_attempt_id, delivery_attempt_no,
        outbox_lease_epoch, lease_owner, result_json
      FROM runtime_outbox
      WHERE outbox_id = ?
    `).get(outboxId),
    mappings: database.prepare(`
      SELECT mapping_id, platform_message_id, conversation_id, turn_id,
        lineage_id, binding_state, mapping_version
      FROM runtime_message_mappings
      ORDER BY mapping_id
    `).all(),
  };
}

function projectionEvent(accepted, {
  version,
  kind,
  phase,
  payload,
  error = null,
  persistedAt,
}) {
  return {
    event_id: `event-projection-${version}`,
    trace_id: `trace-projection-${version}`,
    conversation_id: accepted.conversation_id,
    turn_id: accepted.turn_id,
    lineage_id: accepted.lineage_id,
    event_sequence: version,
    turn_version: version,
    kind,
    phase,
    payload,
    error,
    persisted_at: persistedAt,
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('durable outbox service', () => {
  test('migrates the issue 07 outbox without losing rows or blocking fallback versions', () => {
    const database = openTestDatabase();
    database.exec(`
      CREATE TABLE runtime_outbox (
        outbox_id TEXT PRIMARY KEY,
        delivery_id TEXT NOT NULL UNIQUE,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        turn_id TEXT,
        control_id TEXT,
        aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
        status TEXT NOT NULL,
        command_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE (aggregate_type, aggregate_id, aggregate_version)
      );
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id,
        aggregate_version, status, command_json, created_at
      ) VALUES (
        'outbox-legacy', 'delivery-legacy', 'turn_main', 'turn-legacy',
        7, 'dead_letter', '{}', '2026-07-19T07:00:00Z'
      );
    `);

    initializeRuntimePersistence(database);

    expect(database.prepare(`
      SELECT outbox_id, delivery_id, aggregate_version, status, attempt_count,
        outbox_lease_epoch, supersedable, terminal
      FROM runtime_outbox
      WHERE outbox_id = 'outbox-legacy'
    `).get()).toEqual({
      outbox_id: 'outbox-legacy',
      delivery_id: 'delivery-legacy',
      aggregate_version: 7,
      status: 'dead_letter',
      attempt_count: 0,
      outbox_lease_epoch: 0,
      supersedable: 0,
      terminal: 0,
    });
    expect(() => database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id,
        aggregate_version, status, command_json, created_at
      ) VALUES (
        'outbox-fallback', 'delivery-fallback', 'turn_main', 'turn-legacy',
        7, 'pending', '{}', '2026-07-19T07:00:01Z'
      )
    `).run()).not.toThrow();

    database.close();
  });

  test('backfills a delivery lane for an issue 07 pending create before execution resumes', () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('lane-backfill'), {
      now: () => '2026-07-19T07:30:00Z',
      generateId: deterministicIds('inbound-lane-backfill'),
    });
    database.exec(`
      DROP TABLE runtime_projection_snapshots;
      DROP TABLE runtime_delivery_lanes;
      UPDATE runtime_outbox
      SET lane_key = NULL, predecessor_delivery_id = NULL,
        priority = 0, terminal = 0, updated_at = NULL;
    `);

    initializeRuntimePersistence(database);

    expect(database.prepare(`
      SELECT lane.turn_id, lane.aggregate_type, outbox.lane_key
      FROM runtime_delivery_lanes AS lane
      JOIN runtime_outbox AS outbox ON outbox.lane_key = lane.lane_key
      WHERE lane.turn_id = ?
    `).get(accepted.turn_id)).toMatchObject({
      turn_id: accepted.turn_id,
      aggregate_type: 'turn_main',
      lane_key: expect.any(String),
    });
    const executor = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-lane-backfill',
      now: () => '2026-07-19T07:30:01Z',
      generateId: deterministicIds('executor-lane-backfill'),
    });
    expect(executor.claimNextQueuedTurn()).toMatchObject({
      turn_id: accepted.turn_id,
    });

    database.close();
  });

  test('fences delivery claims and applies only the current result exactly once', () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('fencing'), {
      now: () => '2026-07-19T08:00:00Z',
      generateId: deterministicIds('inbound-fencing'),
    });
    let currentTime = '2026-07-19T08:00:01Z';
    const firstService = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-A',
      now: () => currentTime,
      generateId: deterministicIds('delivery-A'),
      leaseDurationMs: 10_000,
    });
    const firstClaim = firstService.claimNext();
    expect(firstClaim).toMatchObject({
      operation: 'create_main',
      delivery_attempt_no: 1,
      outbox_lease_epoch: 1,
    });

    currentTime = '2026-07-19T08:00:12Z';
    const secondService = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-B',
      now: () => currentTime,
      generateId: deterministicIds('delivery-B'),
      leaseDurationMs: 10_000,
    });
    const currentClaim = secondService.claimNext();
    expect(currentClaim).toMatchObject({
      outbox_id: firstClaim.outbox_id,
      delivery_id: firstClaim.delivery_id,
      operation: 'create_main',
      delivery_attempt_no: 2,
      outbox_lease_epoch: 2,
    });
    expect(currentClaim.delivery_attempt_id).not.toBe(firstClaim.delivery_attempt_id);

    const beforeStale = readDeliveryAuthority(database, currentClaim.outbox_id);
    expect(firstService.recordResult(deliveredResult(firstClaim, currentTime)))
      .toEqual({ status: 'stale' });
    expect(readDeliveryAuthority(database, currentClaim.outbox_id)).toEqual(beforeStale);

    const currentResult = deliveredResult(currentClaim, currentTime);
    expect(secondService.recordResult({
      ...currentResult,
      mapping_id: 'mapping-from-another-delivery',
    })).toEqual({ status: 'stale' });
    expect(readDeliveryAuthority(database, currentClaim.outbox_id)).toEqual(beforeStale);
    expect(secondService.recordResult(currentResult)).toEqual({
      status: 'applied',
      outbox_status: 'delivered',
    });
    const applied = readDeliveryAuthority(database, currentClaim.outbox_id);
    expect(applied).toEqual({
      outbox: {
        status: 'delivered',
        attempt_count: 2,
        delivery_attempt_id: currentClaim.delivery_attempt_id,
        delivery_attempt_no: 2,
        outbox_lease_epoch: 2,
        lease_owner: null,
        result_json: JSON.stringify(currentResult),
      },
      mappings: [{
        mapping_id: currentClaim.mapping.mapping_id,
        platform_message_id: currentResult.platform_message_id,
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
        lineage_id: accepted.lineage_id,
        binding_state: 'bound',
        mapping_version: 1,
      }],
    });

    expect(secondService.recordResult(currentResult)).toEqual({
      status: 'duplicate',
      outbox_status: 'delivered',
    });
    expect(readDeliveryAuthority(database, currentClaim.outbox_id)).toEqual(applied);

    database.close();
  });

  test('orders create before update, throttles ordinary progress, and never loses terminal state', () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('ordered'), {
      now: () => '2026-07-19T09:00:00Z',
      generateId: deterministicIds('inbound-ordered'),
    });
    let currentTime = '2026-07-19T09:00:00Z';
    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-ordered',
      now: () => currentTime,
      generateId: deterministicIds('delivery-ordered'),
    });
    const createCommand = outbox.claimNext();
    expect(outbox.recordResult(deliveredResult(createCommand, currentTime)))
      .toMatchObject({ status: 'applied' });

    currentTime = '2026-07-19T09:00:00.100Z';
    const executor = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-ordered',
      now: () => currentTime,
      generateId: deterministicIds('executor-ordered'),
    });
    const turnContext = executor.claimNextQueuedTurn();
    executor.transitionTurn(turnContext, 'starting', 'running');
    executor.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'first', end_offset: 5 },
      provider_native_id: null,
    });
    executor.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'latest progress', end_offset: 15 },
      provider_native_id: null,
    });

    const beforeTerminal = database.prepare(`
      SELECT status, aggregate_version, supersedable, terminal, next_attempt_at,
        predecessor_delivery_id, command_json
      FROM runtime_outbox
      WHERE turn_id = ?
      ORDER BY aggregate_version
    `).all(accepted.turn_id);
    expect(beforeTerminal).toHaveLength(2);
    expect(beforeTerminal[0]).toMatchObject({
      status: 'delivered',
      aggregate_version: 1,
      supersedable: 0,
      terminal: 0,
      predecessor_delivery_id: null,
    });
    expect(beforeTerminal[1]).toMatchObject({
      status: 'pending',
      aggregate_version: 6,
      supersedable: 1,
      terminal: 0,
      next_attempt_at: '2026-07-19T09:00:01.500Z',
      predecessor_delivery_id: createCommand.delivery_id,
    });
    expect(JSON.parse(beforeTerminal[1].command_json)).toMatchObject({
      operation: 'update_main',
      aggregate_version: 6,
      event_sequence_through: 6,
      target_platform_message_id: `platform-${createCommand.delivery_attempt_id}`,
      predecessor_delivery_id: createCommand.delivery_id,
      render_model: {
        phase: 'running',
        text: 'latest progress',
        terminal: false,
      },
    });
    expect(outbox.claimNext()).toBeNull();

    executor.transitionTurn(turnContext, 'running', 'completed');
    const afterTerminal = database.prepare(`
      SELECT status, aggregate_version, supersedable, terminal, next_attempt_at,
        predecessor_delivery_id, command_json
      FROM runtime_outbox
      WHERE turn_id = ?
      ORDER BY aggregate_version
    `).all(accepted.turn_id);
    expect(afterTerminal.map((row) => ({
      status: row.status,
      aggregate_version: row.aggregate_version,
      supersedable: row.supersedable,
      terminal: row.terminal,
    }))).toEqual([
      { status: 'delivered', aggregate_version: 1, supersedable: 0, terminal: 0 },
      { status: 'superseded', aggregate_version: 6, supersedable: 1, terminal: 0 },
      { status: 'pending', aggregate_version: 7, supersedable: 0, terminal: 1 },
    ]);

    const terminalCommand = outbox.claimNext();
    expect(terminalCommand).toMatchObject({
      operation: 'update_main',
      aggregate_version: 7,
      event_sequence_through: 7,
      predecessor_delivery_id: createCommand.delivery_id,
      render_model: {
        phase: 'completed',
        text: 'latest progress',
        terminal: true,
      },
    });

    database.close();
  });

  test('does not overtake an ordinary update that is already being rendered', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('in-flight-order'), {
      now: () => '2026-07-19T09:30:00Z',
      generateId: deterministicIds('inbound-in-flight-order'),
    });
    let currentTime = '2026-07-19T09:30:00Z';
    let releaseOrdinary;
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push(command);
        if (command.operation === 'update_main' && !command.render_model.terminal) {
          return new Promise((resolve) => {
            releaseOrdinary = () => resolve(deliveredResult(command, currentTime));
          });
        }
        return deliveredResult(command, currentTime);
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-in-flight-order',
      now: () => currentTime,
      generateId: deterministicIds('delivery-in-flight-order'),
      throttleMs: 0,
    });
    await outbox.dispatchNext();

    currentTime = '2026-07-19T09:30:00.100Z';
    const executor = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-in-flight-order',
      now: () => currentTime,
      generateId: deterministicIds('executor-in-flight-order'),
    });
    const turnContext = executor.claimNextQueuedTurn();
    executor.transitionTurn(turnContext, 'starting', 'running');
    currentTime = '2026-07-19T09:30:02.000Z';
    const ordinaryDispatch = outbox.dispatchNext();
    await Promise.resolve();
    expect(releaseOrdinary).toEqual(expect.any(Function));

    executor.transitionTurn(turnContext, 'running', 'completed');
    expect(database.prepare(`
      SELECT status FROM runtime_outbox
      WHERE outbox_id = ?
    `).get(rendered[1].outbox_id)).toEqual({ status: 'delivering' });
    expect(outbox.claimNext()).toBeNull();

    releaseOrdinary();
    await expect(ordinaryDispatch).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });
    await expect(outbox.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });
    expect(rendered.map((command) => ({
      operation: command.operation,
      terminal: command.render_model.terminal,
    }))).toEqual([
      { operation: 'create_main', terminal: false },
      { operation: 'update_main', terminal: false },
      { operation: 'update_main', terminal: true },
    ]);

    database.close();
  });

  test('immediately delivers a durable text acknowledgement when initial create fails', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('initial-fallback'), {
      now: () => '2026-07-19T10:00:00Z',
      generateId: deterministicIds('inbound-initial-fallback'),
    });
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push(command);
        if (command.operation === 'create_main') {
          return retryableFailure(command, '2026-07-19T10:00:00Z');
        }
        return deliveredResult(command, '2026-07-19T10:00:00Z');
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-initial-fallback',
      now: () => '2026-07-19T10:00:00Z',
      generateId: deterministicIds('delivery-initial-fallback'),
    });

    await expect(outbox.dispatchNext()).resolves.toEqual({
      status: 'applied',
      outbox_status: 'retry_wait',
    });
    await expect(outbox.dispatchNext()).resolves.toEqual({
      status: 'applied',
      outbox_status: 'delivered',
    });
    expect(rendered.map((command) => command.operation)).toEqual([
      'create_main',
      'send_text',
    ]);
    expect(rendered[1]).toMatchObject({
      aggregate_type: 'text_notice',
      event_sequence_through: 1,
      render_model: {
        phase: 'received',
        text: 'Message received. Rich delivery is temporarily unavailable.',
        terminal: false,
      },
      mapping: {
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
        lineage_id: accepted.lineage_id,
        binding_state: 'bound',
      },
    });
    expect(database.prepare(`
      SELECT status, next_attempt_at
      FROM runtime_outbox
      WHERE outbox_id = ?
    `).get(rendered[0].outbox_id)).toEqual({
      status: 'retry_wait',
      next_attempt_at: '2026-07-19T10:00:02.000Z',
    });

    database.close();
  });

  test('retries a final update after 2/4/8 seconds then uses a new delivery and mapping', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('final-fallback'), {
      now: () => '2026-07-19T11:00:00Z',
      generateId: deterministicIds('inbound-final-fallback'),
    });
    let currentTime = '2026-07-19T11:00:00Z';
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push({ at: currentTime, command });
        if (command.operation === 'update_main') {
          return retryableFailure(command, currentTime);
        }
        return deliveredResult(command, currentTime);
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-final-fallback',
      now: () => currentTime,
      generateId: deterministicIds('delivery-final-fallback'),
    });
    await expect(outbox.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });

    currentTime = '2026-07-19T11:00:00.100Z';
    const executor = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-final-fallback',
      now: () => currentTime,
      generateId: deterministicIds('executor-final-fallback'),
    });
    const turnContext = executor.claimNextQueuedTurn();
    executor.transitionTurn(turnContext, 'starting', 'running');
    executor.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'durable final result', end_offset: 20 },
      provider_native_id: null,
    });
    executor.transitionTurn(turnContext, 'running', 'completed');

    await expect(outbox.dispatchNext()).resolves.toEqual({
      status: 'applied',
      outbox_status: 'retry_wait',
    });
    currentTime = '2026-07-19T11:00:02.099Z';
    await expect(outbox.dispatchNext()).resolves.toEqual({ status: 'idle' });

    for (const timestamp of [
      '2026-07-19T11:00:02.100Z',
      '2026-07-19T11:00:06.100Z',
    ]) {
      currentTime = timestamp;
      await expect(outbox.dispatchNext()).resolves.toEqual({
        status: 'applied',
        outbox_status: 'retry_wait',
      });
    }
    currentTime = '2026-07-19T11:00:14.100Z';
    await expect(outbox.dispatchNext()).resolves.toEqual({
      status: 'applied',
      outbox_status: 'dead_letter',
    });
    await expect(outbox.dispatchNext()).resolves.toEqual({
      status: 'applied',
      outbox_status: 'delivered',
    });

    const updateAttempts = rendered.filter(({ command }) => command.operation === 'update_main');
    expect(updateAttempts.map(({ at }) => at)).toEqual([
      '2026-07-19T11:00:00.100Z',
      '2026-07-19T11:00:02.100Z',
      '2026-07-19T11:00:06.100Z',
      '2026-07-19T11:00:14.100Z',
    ]);
    expect(updateAttempts.map(({ command }) => ({
      delivery_id: command.delivery_id,
      attempt_no: command.delivery_attempt_no,
      lease_epoch: command.outbox_lease_epoch,
    }))).toEqual([1, 2, 3, 4].map((attempt) => ({
      delivery_id: updateAttempts[0].command.delivery_id,
      attempt_no: attempt,
      lease_epoch: attempt,
    })));

    const fallback = rendered.at(-1).command;
    expect(fallback).toMatchObject({
      operation: 'send_fallback',
      aggregate_type: 'turn_main',
      aggregate_id: accepted.turn_id,
      predecessor_delivery_id: updateAttempts[0].command.delivery_id,
      render_model: {
        phase: 'completed',
        text: 'durable final result',
        terminal: true,
      },
    });
    expect(fallback.delivery_id).not.toBe(updateAttempts[0].command.delivery_id);
    expect(fallback.mapping.mapping_id).not.toBe(updateAttempts[0].command.mapping.mapping_id);
    expect(database.prepare(`
      SELECT state
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'completed' });
    expect(database.prepare(`
      SELECT mapping_id, platform_message_id
      FROM runtime_message_mappings
      WHERE turn_id = ?
      ORDER BY created_at
    `).all(accepted.turn_id)).toEqual([
      {
        mapping_id: updateAttempts[0].command.mapping.mapping_id,
        platform_message_id: updateAttempts[0].command.target_platform_message_id,
      },
      {
        mapping_id: fallback.mapping.mapping_id,
        platform_message_id: `platform-${fallback.delivery_attempt_id}`,
      },
    ]);

    database.close();
  });

  test('falls back to the accumulated terminal projection when initial create exhausts retries', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('create-terminal-fallback'), {
      now: () => '2026-07-19T11:30:00Z',
      generateId: deterministicIds('inbound-create-terminal-fallback'),
    });
    let currentTime = '2026-07-19T11:30:00Z';
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push({ at: currentTime, command });
        if (command.operation === 'create_main') {
          return retryableFailure(command, currentTime);
        }
        return deliveredResult(command, currentTime);
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-create-terminal-fallback',
      now: () => currentTime,
      generateId: deterministicIds('delivery-create-terminal-fallback'),
    });
    await outbox.dispatchNext();
    await outbox.dispatchNext();

    currentTime = '2026-07-19T11:30:00.100Z';
    const executor = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-service-create-terminal-fallback',
      now: () => currentTime,
      generateId: deterministicIds('executor-create-terminal-fallback'),
    });
    const turnContext = executor.claimNextQueuedTurn();
    executor.transitionTurn(turnContext, 'starting', 'running');
    executor.appendAdapterEvent(turnContext, {
      kind: 'text_snapshot',
      payload: { text: 'result after failed create', end_offset: 26 },
      provider_native_id: null,
    });
    executor.transitionTurn(turnContext, 'running', 'completed');

    for (const timestamp of [
      '2026-07-19T11:30:02.000Z',
      '2026-07-19T11:30:06.000Z',
      '2026-07-19T11:30:14.000Z',
    ]) {
      currentTime = timestamp;
      await outbox.dispatchNext();
    }
    await expect(outbox.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });

    const fallback = rendered.at(-1).command;
    expect(rendered.filter(({ command }) => command.operation === 'create_main'))
      .toHaveLength(4);
    expect(fallback).toMatchObject({
      operation: 'send_fallback',
      aggregate_type: 'turn_main',
      aggregate_id: accepted.turn_id,
      render_model: {
        phase: 'completed',
        text: 'result after failed create',
        terminal: true,
      },
    });
    expect(fallback.predecessor_delivery_id)
      .toBe(rendered[0].command.delivery_id);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'completed' });
    expect(database.prepare(`
      SELECT aggregate_version, status, materialized_outbox_id
      FROM runtime_projection_snapshots
      WHERE turn_id = ? AND critical = 1
      ORDER BY aggregate_version
    `).all(accepted.turn_id)).toEqual([
      {
        aggregate_version: 6,
        status: 'materialized',
        materialized_outbox_id: fallback.outbox_id,
      },
    ]);

    database.close();
  });

  test('establishes a fallback lane for waiting-user projections after create exhausts', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('create-interaction-fallback'), {
      now: () => '2026-07-19T11:45:00Z',
      generateId: deterministicIds('inbound-create-interaction-fallback'),
    });
    let currentTime = '2026-07-19T11:45:00Z';
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push(command);
        if (command.operation === 'create_main') {
          return retryableFailure(command, currentTime);
        }
        return deliveredResult(command, currentTime);
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-create-interaction-fallback',
      now: () => currentTime,
      generateId: deterministicIds('delivery-create-interaction-fallback'),
    });
    await outbox.dispatchNext();
    await outbox.dispatchNext();

    stageMainProjection(database, { turn_id: accepted.turn_id }, projectionEvent(accepted, {
      version: 3,
      kind: 'interaction_requested',
      phase: 'waiting_user',
      payload: {
        interaction_id: 'interaction-after-create-failure',
        ordinal: 1,
        interaction_version: 1,
        handoff_version: null,
      },
      persistedAt: '2026-07-19T11:45:00.100Z',
    }), {
      generateId: deterministicIds('projection-create-interaction-fallback'),
    });

    for (const timestamp of [
      '2026-07-19T11:45:02.000Z',
      '2026-07-19T11:45:06.000Z',
      '2026-07-19T11:45:14.000Z',
    ]) {
      currentTime = timestamp;
      await outbox.dispatchNext();
    }
    await expect(outbox.dispatchNext()).resolves.toMatchObject({
      status: 'applied',
      outbox_status: 'delivered',
    });

    expect(rendered.at(-1)).toMatchObject({
      operation: 'send_fallback',
      aggregate_version: 3,
      render_model: {
        phase: 'waiting_user',
        interactions: [{
          interaction_id: 'interaction-after-create-failure',
        }],
        terminal: false,
        user_action_required: true,
      },
    });
    expect(database.prepare(`
      SELECT status, materialized_outbox_id
      FROM runtime_projection_snapshots
      WHERE turn_id = ? AND aggregate_version = 3
    `).get(accepted.turn_id)).toEqual({
      status: 'materialized',
      materialized_outbox_id: rendered.at(-1).outbox_id,
    });

    database.close();
  });

  test('preserves interaction, safety, uncertainty, and terminal projections in lane order', async () => {
    const database = openTestDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('critical-projections'), {
      now: () => '2026-07-19T12:00:00Z',
      generateId: deterministicIds('inbound-critical-projections'),
    });
    let currentTime = '2026-07-19T12:00:00Z';
    const rendered = [];
    const renderer = {
      async deliver(command) {
        rendered.push(command);
        return deliveredResult(command, currentTime);
      },
    };
    const outbox = createOutboxService({
      database,
      renderer,
      serviceInstanceId: 'delivery-service-critical-projections',
      now: () => currentTime,
      generateId: deterministicIds('delivery-critical-projections'),
    });
    await outbox.dispatchNext();

    const generateId = deterministicIds('projection-critical');
    for (const event of [
      projectionEvent(accepted, {
        version: 3,
        kind: 'interaction_requested',
        phase: 'waiting_user',
        payload: {
          interaction_id: 'interaction-critical',
          ordinal: 1,
          interaction_version: 1,
          handoff_version: null,
        },
        persistedAt: '2026-07-19T12:00:00.100Z',
      }),
      projectionEvent(accepted, {
        version: 4,
        kind: 'permission_changed',
        phase: 'running',
        payload: {
          scope: { mode: 'safe' },
          actor_id: 'actor-critical',
          audit_id: 'audit-critical',
        },
        persistedAt: '2026-07-19T12:00:00.200Z',
      }),
      projectionEvent(accepted, {
        version: 5,
        kind: 'tool_finished',
        phase: 'running',
        payload: {
          tool_use_id: 'tool-uncertain',
          tool_name: 'external-write',
          summary: 'Write acknowledgement unavailable.',
          side_effect_status: 'unknown',
        },
        persistedAt: '2026-07-19T12:00:00.300Z',
      }),
      projectionEvent(accepted, {
        version: 6,
        kind: 'recovery_waiting_decision',
        phase: 'recovering',
        payload: {
          recovery_id: 'recovery-critical',
          recovery_of_turn_id: accepted.turn_id,
          recovery_of_lineage_id: accepted.lineage_id,
          side_effect_status: 'unknown',
        },
        error: {
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'Recovery requires an authorized user decision.',
          occurred_at: '2026-07-19T12:00:00.300Z',
        },
        persistedAt: '2026-07-19T12:00:00.400Z',
      }),
      projectionEvent(accepted, {
        version: 7,
        kind: 'text_snapshot',
        phase: 'running',
        payload: { text: 'ordinary progress', end_offset: 17 },
        persistedAt: '2026-07-19T12:00:00.500Z',
      }),
      projectionEvent(accepted, {
        version: 8,
        kind: 'turn_state_changed',
        phase: 'completed',
        payload: {
          from_state: 'running',
          to_state: 'completed',
          reason_code: 'provider_completed',
        },
        persistedAt: '2026-07-19T12:00:00.600Z',
      }),
    ]) {
      stageMainProjection(database, { turn_id: accepted.turn_id }, event, { generateId });
    }

    expect(database.prepare(`
      SELECT aggregate_version, status, critical, terminal
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version
    `).all(accepted.turn_id)).toEqual([
      { aggregate_version: 2, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 3, status: 'materialized', critical: 1, terminal: 0 },
      { aggregate_version: 4, status: 'staged', critical: 1, terminal: 0 },
      { aggregate_version: 5, status: 'staged', critical: 1, terminal: 0 },
      { aggregate_version: 6, status: 'staged', critical: 1, terminal: 0 },
      { aggregate_version: 7, status: 'superseded', critical: 0, terminal: 0 },
      { aggregate_version: 8, status: 'staged', critical: 1, terminal: 1 },
    ]);

    currentTime = '2026-07-19T12:00:01Z';
    for (let index = 0; index < 5; index += 1) {
      await expect(outbox.dispatchNext()).resolves.toMatchObject({
        status: 'applied',
        outbox_status: 'delivered',
      });
    }
    expect(rendered.slice(1).map((command) => command.aggregate_version))
      .toEqual([3, 4, 5, 6, 8]);
    expect(rendered.slice(1).map((command) => command.render_model.phase)).toEqual([
      'waiting_user',
      'running',
      'running',
      'recovering',
      'completed',
    ]);
    expect(rendered[2].render_model.permissions).toEqual([{
      kind: 'permission_changed',
      payload: {
        scope: { mode: 'safe' },
        actor_id: 'actor-critical',
        audit_id: 'audit-critical',
      },
    }]);
    expect(rendered[3].render_model.tools).toContainEqual(expect.objectContaining({
      side_effect_status: 'unknown',
    }));
    expect(database.prepare(`
      SELECT aggregate_version, status, supersedable, terminal
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_version > 1
      ORDER BY aggregate_version
    `).all(accepted.turn_id)).toEqual([
      { aggregate_version: 2, status: 'superseded', supersedable: 1, terminal: 0 },
      { aggregate_version: 3, status: 'delivered', supersedable: 0, terminal: 0 },
      { aggregate_version: 4, status: 'delivered', supersedable: 0, terminal: 0 },
      { aggregate_version: 5, status: 'delivered', supersedable: 0, terminal: 0 },
      { aggregate_version: 6, status: 'delivered', supersedable: 0, terminal: 0 },
      { aggregate_version: 8, status: 'delivered', supersedable: 0, terminal: 1 },
    ]);

    database.close();
  });
});
