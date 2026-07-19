import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateInteractionAnswerResult,
  validateInteractionHandoff,
  validateInteractionRequest,
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
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-interaction-happy-path-'));
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

function acceptQueuedTurn(database, suffix) {
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
  return acceptNormalInbound(database, envelope, {
    now: () => '2026-07-19T07:00:00Z',
    generateId: deterministicIds(`inbound-${suffix}`),
  });
}

function createRunningTurn(database, suffix = 'request') {
  const accepted = acceptQueuedTurn(database, suffix);
  const store = createExecutorStore({
    database,
    provider: 'claude',
    serviceInstanceId: `executor-service-${suffix}`,
    now: () => '2026-07-19T07:02:00Z',
    generateId: deterministicIds(suffix),
  });
  const turnContext = store.claimNextQueuedTurn();
  store.transitionTurn(turnContext, 'starting', 'running');
  return { accepted, store, turnContext };
}

function readEvents(database, turnId) {
  return database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence
  `).all(turnId).map(({ event_json: eventJson }) => JSON.parse(eventJson));
}

function readInteractionAuthority(database, turnId) {
  return {
    turn: database.prepare(`
      SELECT state, turn_version, attempt_id, attempt_no, lease_epoch
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(turnId),
    interactions: database.prepare(`
      SELECT interaction_id, ordinal, state, version, handoff_state,
        handoff_version, request_json
      FROM runtime_interactions
      WHERE turn_id = ?
      ORDER BY ordinal
    `).all(turnId),
    answers: database.prepare(`
      SELECT answer_id, interaction_id, idempotency_key, answer_json, result_json
      FROM runtime_interaction_answers
      WHERE interaction_id IN (
        SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
      )
      ORDER BY answer_id
    `).all(turnId),
    handoffs: database.prepare(`
      SELECT handoff_id, interaction_id, state, provider_attempt_id,
        handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json
      FROM runtime_interaction_handoffs
      WHERE interaction_id IN (
        SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
      )
      ORDER BY handoff_id
    `).all(turnId),
    audits: database.prepare(`
      SELECT audit_id, interaction_id, handoff_id, outcome,
        provider_attempt_id, lease_epoch, acknowledgement_json
      FROM runtime_interaction_audit
      WHERE interaction_id IN (
        SELECT interaction_id FROM runtime_interactions WHERE turn_id = ?
      )
      ORDER BY audit_id
    `).all(turnId),
    events: database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_normalized_events
      WHERE turn_id = ?
    `).get(turnId).count,
    projections: database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
    `).get(turnId).count,
  };
}

function interactionAnswer(request, suffix = '1') {
  const sourceEventId = `card-action-${suffix}`;
  return {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-answer-${suffix}`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `answer-${suffix}`,
    source_event_or_action_id: sourceEventId,
    actor: {
      type: 'user',
      actor_id: 'user-123',
      authenticated: true,
      roles: ['member'],
    },
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    },
    source: 'card_action',
    value: { kind: 'decision', decision: 'approve' },
    answered_at: '2026-07-19T07:02:00Z',
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: request.interaction_id,
      source_event_or_action_id: sourceEventId,
    }),
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('runtime interaction happy path', () => {
  test('atomically publishes a provider question with a unique ordinal in the user projection', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database);

    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-1',
      tool_use_id: 'tool-use-1',
      kind: 'tool_approval',
      prompt: 'Allow the requested workspace write?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });

    expect(validateInteractionRequest(request).forwarded).toEqual(request);
    expect(request).toMatchObject({
      interaction_id: 'interaction-request-1',
      conversation_id: accepted.conversation_id,
      turn_id: accepted.turn_id,
      lineage_id: accepted.lineage_id,
      ordinal: 1,
      state: 'pending',
      version: 1,
      handoff_state: 'not_started',
      runtime_fence: {
        provider_attempt_id: 'attempt-request-1',
        lease_epoch: 1,
        provider_interaction_ref: 'provider-question-1',
      },
    });

    expect(database.prepare(`
      SELECT state, turn_version
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'waiting_user', turn_version: 6 });

    const events = readEvents(database, accepted.turn_id);
    expect(events.map(({ event_sequence: sequence }) => sequence)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(events.map(({ turn_version: version }) => version)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(events.slice(-2).map(({ kind, phase }) => ({ kind, phase }))).toEqual([
      { kind: 'turn_state_changed', phase: 'waiting_user' },
      { kind: 'interaction_requested', phase: 'waiting_user' },
    ]);
    for (const event of events) {
      expect(validateNormalizedEvent(event).forwarded).toEqual(event);
    }

    const projection = database.prepare(`
      SELECT render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(accepted.turn_id);
    expect(JSON.parse(projection.render_model_json)).toMatchObject({
      phase: 'waiting_user',
      user_action_required: true,
      interactions: [{
        interaction_id: 'interaction-request-1',
        ordinal: 1,
        interaction_version: 1,
        handoff_version: null,
        prompt: 'Allow the requested workspace write?',
      }],
    });

    database.close();
  });

  test('durably commits a valid answer and pending handoff before returning to the provider', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'answer');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-answer',
      tool_use_id: 'tool-use-answer',
      kind: 'tool_approval',
      prompt: 'Allow the requested workspace write?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });

    const result = store.commitInteractionAnswer(interactionAnswer(request));

    expect(validateInteractionAnswerResult(result).forwarded).toEqual(result);
    expect(result).toMatchObject({
      status: 'accepted',
      interaction_id: request.interaction_id,
      answer_id: 'answer-1',
      interaction_state: 'answer_committed',
      interaction_version: 2,
      handoff_state: 'pending',
      handoff_id: 'handoff-answer-1',
      turn_id: accepted.turn_id,
      turn_version: 7,
      control_id: null,
      error: null,
    });

    const authority = database.prepare(`
      SELECT interaction.state, interaction.version, interaction.handoff_state,
        interaction.handoff_version, answer.result_json, handoff.record_json,
        turn.state AS turn_state, turn.turn_version
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_answers AS answer
        ON answer.interaction_id = interaction.interaction_id
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      WHERE interaction.interaction_id = ?
    `).get(request.interaction_id);
    expect(authority).toMatchObject({
      state: 'answer_committed',
      version: 2,
      handoff_state: 'pending',
      handoff_version: 1,
      turn_state: 'waiting_user',
      turn_version: 7,
    });
    expect(JSON.parse(authority.result_json)).toEqual(result);
    expect(validateInteractionHandoff(JSON.parse(authority.record_json))).toEqual(
      expect.objectContaining({
        state: 'pending',
        provider_attempt_id: 'attempt-answer-1',
        lease_epoch: 1,
      }),
    );

    const lastEvent = readEvents(database, accepted.turn_id).at(-1);
    expect(lastEvent).toMatchObject({
      kind: 'interaction_answer_committed',
      phase: 'waiting_user',
      turn_version: 7,
      payload: {
        interaction_id: request.interaction_id,
        ordinal: 1,
        interaction_version: 2,
        handoff_version: 1,
      },
    });

    database.close();
  });

  test('resumes only after the current fenced handler acknowledges the durable handoff', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'ack');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-ack',
      tool_use_id: 'tool-use-ack',
      kind: 'tool_approval',
      prompt: 'Allow the requested workspace write?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const result = store.commitInteractionAnswer(interactionAnswer(request, 'ack'));

    const delivery = store.claimInteractionHandoff(result.handoff_id);
    expect(delivery).toMatchObject({
      handoff: {
        handoff_id: result.handoff_id,
        state: 'delivering',
        provider_attempt_id: 'attempt-ack-1',
        handoff_attempt_id: 'handoff-attempt-ack-1',
        handoff_attempt_no: 1,
        lease_epoch: 1,
        claimed_by: 'executor-service-ack',
      },
      request: {
        interaction_id: request.interaction_id,
        state: 'answer_delivering',
        version: 3,
        handoff_state: 'delivering',
      },
      answer: {
        answer_id: 'answer-ack',
        value: { kind: 'decision', decision: 'approve' },
      },
    });
    expect(database.prepare(`
      SELECT state, turn_version
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'waiting_user', turn_version: 8 });

    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: delivery.handoff.handoff_id,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch,
    });

    expect(acknowledgement).toEqual({
      status: 'accepted',
      interaction_id: request.interaction_id,
      handoff_id: result.handoff_id,
      audit_id: 'audit-ack-1',
      resumed: true,
      turn_state: 'running',
      turn_version: 10,
    });
    const authority = database.prepare(`
      SELECT interaction.state, interaction.version, interaction.handoff_state,
        interaction.handoff_version, handoff.record_json,
        turn.state AS turn_state, turn.turn_version,
        audit.outcome AS audit_outcome, audit.provider_attempt_id AS audit_attempt_id,
        audit.lease_epoch AS audit_lease_epoch
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      JOIN runtime_interaction_audit AS audit
        ON audit.interaction_id = interaction.interaction_id
      WHERE interaction.interaction_id = ?
    `).get(request.interaction_id);
    expect(authority).toMatchObject({
      state: 'answered',
      version: 4,
      handoff_state: 'accepted',
      handoff_version: 3,
      turn_state: 'running',
      turn_version: 10,
      audit_outcome: 'accepted',
      audit_attempt_id: 'attempt-ack-1',
      audit_lease_epoch: 1,
    });
    expect(validateInteractionHandoff(JSON.parse(authority.record_json))).toEqual(
      expect.objectContaining({
        state: 'accepted',
        provider_attempt_id: 'attempt-ack-1',
        handoff_attempt_id: 'handoff-attempt-ack-1',
        provider_acked_at: '2026-07-19T07:02:00Z',
        side_effect_status: 'known',
      }),
    );
    expect(readEvents(database, accepted.turn_id).slice(-3).map(({ kind, phase }) => ({
      kind,
      phase,
    }))).toEqual([
      { kind: 'interaction_answer_handoff_started', phase: 'waiting_user' },
      { kind: 'turn_state_changed', phase: 'running' },
      { kind: 'interaction_answered', phase: 'running' },
    ]);

    database.close();
  });

  test('records a handler deny acknowledgement as a completed denied decision', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'deny');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-deny',
      tool_use_id: 'tool-use-deny',
      kind: 'tool_approval',
      prompt: 'Allow the requested workspace write?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const answer = interactionAnswer(request, 'deny');
    answer.value.decision = 'deny';
    const result = store.commitInteractionAnswer(answer);
    const delivery = store.claimInteractionHandoff(result.handoff_id);

    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'deny',
      handoff_id: delivery.handoff.handoff_id,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch,
    });

    expect(acknowledgement).toMatchObject({
      status: 'deny',
      interaction_id: request.interaction_id,
      resumed: true,
      turn_state: 'running',
    });
    expect(database.prepare(`
      SELECT interaction.state, interaction.handoff_state,
        handoff.state AS handoff_record_state, audit.outcome
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_interaction_audit AS audit
        ON audit.interaction_id = interaction.interaction_id
      WHERE interaction.interaction_id = ?
    `).get(request.interaction_id)).toEqual({
      state: 'answered',
      handoff_state: 'accepted',
      handoff_record_state: 'accepted',
      outcome: 'deny',
    });

    database.close();
  });

  test('keeps the turn waiting when the acknowledged answer has a next blocking interaction', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'next-blocking');
    const first = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-first',
      tool_use_id: 'tool-use-first',
      kind: 'question',
      prompt: 'What should happen first?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    });
    const second = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-second',
      tool_use_id: 'tool-use-second',
      kind: 'question',
      prompt: 'What should happen next?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    });
    expect([first.ordinal, second.ordinal]).toEqual([1, 2]);

    const result = store.commitInteractionAnswer(interactionAnswer(first, 'first'));
    const delivery = store.claimInteractionHandoff(result.handoff_id);
    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: delivery.handoff.handoff_id,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch,
    });

    expect(acknowledgement).toMatchObject({
      resumed: false,
      turn_state: 'waiting_user',
      turn_version: 10,
    });
    expect(database.prepare(`
      SELECT state, turn_version
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'waiting_user', turn_version: 10 });
    expect(database.prepare(`
      SELECT ordinal, state
      FROM runtime_interactions
      WHERE turn_id = ?
      ORDER BY ordinal
    `).all(accepted.turn_id)).toEqual([
      { ordinal: 1, state: 'answered' },
      { ordinal: 2, state: 'pending' },
    ]);

    database.close();
  });

  test('runs the durable question-answer-handler loop through the executor service', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-loop');
    const handlerCalls = [];
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-question-service-loop',
            tool_use_id: 'tool-use-service-loop',
            kind: 'tool_approval',
            prompt: 'Allow the requested workspace write?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
        yield {
          kind: 'text_snapshot',
          payload: { text: 'Continued after the answer.', end_offset: 27 },
          provider_native_id: null,
        };
      },
      async handleInteractionAnswer(delivery) {
        const durable = database.prepare(`
          SELECT result_json
          FROM runtime_interaction_answers
          WHERE answer_id = ?
        `).get(delivery.answer.answer_id);
        handlerCalls.push({
          delivery,
          result: JSON.parse(durable.result_json),
        });
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
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-service-loop',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('service-loop'),
    });

    const waiting = await service.runNext();
    expect(waiting).toMatchObject({
      status: 'waiting_user',
      turn_id: accepted.turn_id,
      request: {
        ordinal: 1,
        state: 'pending',
      },
    });
    const answerResult = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'service-loop'),
    );
    expect(answerResult).toMatchObject({
      status: 'accepted',
      interaction_state: 'answer_committed',
      handoff_state: 'pending',
      turn_version: 7,
    });
    expect(handlerCalls).toEqual([]);

    const handled = await service.deliverInteractionAnswer(answerResult.handoff_id);

    expect(handled).toMatchObject({
      acknowledgement: {
        status: 'accepted',
        resumed: true,
        turn_state: 'running',
        turn_version: 10,
      },
      execution: {
        status: 'completed',
        turn_id: accepted.turn_id,
      },
    });
    expect(handlerCalls).toHaveLength(1);
    expect(handlerCalls[0].result).toEqual(answerResult);
    expect(database.prepare(`
      SELECT state, turn_version
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'completed', turn_version: 12 });

    database.close();
  });

  test('rolls back the complete request transaction when its user projection cannot persist', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'request-rollback');
    const before = readInteractionAuthority(database, accepted.turn_id);
    database.exec(`
      CREATE TRIGGER force_interaction_request_projection_failure
      BEFORE INSERT ON runtime_projection_snapshots
      WHEN NEW.aggregate_version = 6
      BEGIN
        SELECT RAISE(ABORT, 'forced interaction request projection failure');
      END;
    `);

    expect(() => store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-request-rollback',
      tool_use_id: 'tool-use-request-rollback',
      kind: 'question',
      prompt: 'Should this transaction roll back?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    })).toThrow(/forced interaction request projection failure/);
    expect(readInteractionAuthority(database, accepted.turn_id)).toEqual(before);

    database.close();
  });

  test('rolls back answer state, handoff, event, and projection when result persistence fails', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'answer-rollback');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-answer-rollback',
      tool_use_id: 'tool-use-answer-rollback',
      kind: 'question',
      prompt: 'Should this answer transaction roll back?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    });
    const before = readInteractionAuthority(database, accepted.turn_id);
    database.exec(`
      CREATE TRIGGER force_interaction_answer_result_failure
      BEFORE INSERT ON runtime_interaction_answers
      BEGIN
        SELECT RAISE(ABORT, 'forced interaction answer result failure');
      END;
    `);

    expect(() => store.commitInteractionAnswer(interactionAnswer(request, 'rollback')))
      .toThrow(/forced interaction answer result failure/);
    expect(readInteractionAuthority(database, accepted.turn_id)).toEqual(before);

    database.close();
  });

  test('rolls back interaction, handoff, audit, state, and events when acknowledgement commit fails', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'ack-rollback');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-ack-rollback',
      tool_use_id: 'tool-use-ack-rollback',
      kind: 'question',
      prompt: 'Should this acknowledgement transaction roll back?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    });
    const result = store.commitInteractionAnswer(interactionAnswer(request, 'ack-rollback'));
    const delivery = store.claimInteractionHandoff(result.handoff_id);
    const before = readInteractionAuthority(database, accepted.turn_id);
    database.exec(`
      CREATE TRIGGER force_interaction_audit_failure
      BEFORE INSERT ON runtime_interaction_audit
      BEGIN
        SELECT RAISE(ABORT, 'forced interaction audit failure');
      END;
    `);

    expect(() => store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: delivery.handoff.handoff_id,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch,
    })).toThrow(/forced interaction audit failure/);
    expect(readInteractionAuthority(database, accepted.turn_id)).toEqual(before);

    database.close();
  });

  test('rejects a stale handler acknowledgement without authoritative mutations', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(database, 'stale-ack');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-stale-ack',
      tool_use_id: 'tool-use-stale-ack',
      kind: 'question',
      prompt: 'Should this stale acknowledgement be ignored?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply'],
    });
    const result = store.commitInteractionAnswer(interactionAnswer(request, 'stale-ack'));
    const delivery = store.claimInteractionHandoff(result.handoff_id);
    const before = readInteractionAuthority(database, accepted.turn_id);

    expect(() => store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: delivery.handoff.handoff_id,
      provider_attempt_id: delivery.handoff.provider_attempt_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: delivery.handoff.handoff_attempt_no,
      lease_epoch: delivery.handoff.lease_epoch + 1,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readInteractionAuthority(database, accepted.turn_id)).toEqual(before);

    database.close();
  });
});
