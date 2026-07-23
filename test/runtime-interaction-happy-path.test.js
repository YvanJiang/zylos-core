import { afterEach, describe, expect, test } from '@jest/globals';

import {
  createContractError,
  validateInteractionAnswerResult,
  validateInteractionHandoff,
  validateInteractionRequest,
  validateNormalizedEvent,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import {
  acceptQueuedInteractionTurn,
  cleanupInteractionTestDatabases,
  createRunningInteractionTurn,
  deterministicIds,
  interactionAnswer,
  openInteractionTestDatabase,
} from './helpers/runtime-interaction-fixtures.js';
import { deliveredResult } from './helpers/delivered-result.js';

function openTestDatabase() {
  return openInteractionTestDatabase('interaction-happy-path');
}

function acceptQueuedTurn(database, suffix) {
  return acceptQueuedInteractionTurn(database, suffix);
}

function createRunningTurn(database, suffix = 'request') {
  return createRunningInteractionTurn(database, suffix);
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

function preparedInteractionAnswer(handler) {
  return async function prepare(delivery) {
    return Object.freeze({
      send(startedDelivery) {
        return handler(startedDelivery, delivery);
      },
    });
  };
}

function markLatestTurnProjectionDelivered(database, turnId) {
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `delivery-service-${turnId}`,
    now: () => '2026-07-19T07:02:00Z',
    generateId: deterministicIds(`delivery-${turnId}`),
    throttleMs: 0,
  });
  for (let count = 0; count < 100; count += 1) {
    const command = outbox.claimNext();
    if (command === null) break;
    expect(outbox.recordResult(deliveredResult(command, '2026-07-19T07:02:00Z')))
      .toMatchObject({ status: 'applied', outbox_status: 'delivered' });
  }
  expect(database.prepare(`
    SELECT COUNT(*) AS count
    FROM runtime_projection_snapshots AS projection
    JOIN runtime_outbox AS outbox
      ON outbox.outbox_id = projection.materialized_outbox_id
    WHERE projection.turn_id = ? AND outbox.status = 'delivered'
  `).get(turnId).count).toBeGreaterThan(0);
}

afterEach(() => {
  cleanupInteractionTestDatabases();
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

  test('resolves the exact delivered channel card to its current interaction scope', () => {
    const database = openTestDatabase();
    const { envelope, store, turnContext } = createRunningTurn(database, 'channel-resolution');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-channel-resolution',
      tool_use_id: 'tool-use-channel-resolution',
      kind: 'tool_approval',
      prompt: 'Allow the requested workspace write?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const outbox = createOutboxService({
      database,
      serviceInstanceId: 'delivery-service-channel-resolution',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('delivery-channel-resolution'),
      throttleMs: 0,
    });
    let platformMessageId = null;
    for (let count = 0; count < 10; count += 1) {
      const command = outbox.claimNext();
      if (command === null) break;
      const result = deliveredResult(command, '2026-07-19T07:02:00Z');
      platformMessageId = result.platform_message_id;
      outbox.recordResult(result);
    }
    const mapping = database.prepare(`
      SELECT mapping_id FROM runtime_message_mappings
      WHERE platform_message_id = ?
    `).get(platformMessageId);

    expect(store.resolveInteractionTarget({
      region: envelope.region,
      tenantId: envelope.tenant_id,
      channel: envelope.channel,
      botId: envelope.bot_id,
      platformMessageId,
      mappingId: mapping.mapping_id,
      interactionId: request.interaction_id,
    })).toEqual({
      platform_message_id: platformMessageId,
      mapping: expect.objectContaining({
        mapping_id: mapping.mapping_id,
        conversation_id: request.conversation_id,
        turn_id: request.turn_id,
        lineage_id: request.lineage_id,
        binding_state: 'bound',
      }),
      request_scope: {
        region: envelope.region,
        tenant_id: envelope.tenant_id,
        channel: envelope.channel,
        bot_id: envelope.bot_id,
        chat_id: envelope.chat_id,
        native_thread_or_topic_id: envelope.native_thread_or_topic_id,
      },
      interactions: [request],
    });
    expect(() => store.resolveInteractionTarget({
      region: envelope.region,
      tenantId: envelope.tenant_id,
      channel: envelope.channel,
      botId: 'another-bot',
      platformMessageId,
    })).toThrow(expect.objectContaining({ code: 'mapping_missing' }));

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

    const sendingDelivery = store.markInteractionHandoffSendStarted(delivery);
    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: sendingDelivery.handoff.handoff_id,
      provider_attempt_id: sendingDelivery.handoff.provider_attempt_id,
      handoff_attempt_id: sendingDelivery.handoff.handoff_attempt_id,
      handoff_attempt_no: sendingDelivery.handoff.handoff_attempt_no,
      lease_epoch: sendingDelivery.handoff.lease_epoch,
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
      handoff_version: 4,
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

  test('claims a handoff without send evidence and fences the later send start', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'send-start-fence');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-send-start-fence',
      tool_use_id: 'tool-use-send-start-fence',
      kind: 'tool_approval',
      prompt: 'Allow this fenced handoff?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const result = store.commitInteractionAnswer(
      interactionAnswer(request, 'send-start-fence'),
    );

    const delivery = store.claimInteractionHandoff(result.handoff_id);
    expect(delivery.handoff).toMatchObject({
      state: 'delivering',
      last_send_started_at: null,
      provider_attempt_id: request.runtime_fence.provider_attempt_id,
      lease_epoch: request.runtime_fence.lease_epoch,
    });

    const staleDelivery = structuredClone(delivery);
    staleDelivery.handoff.handoff_attempt_id = 'stale-handoff-attempt';
    const beforeStaleStart = readInteractionAuthority(database, request.turn_id);
    expect(() => store.markInteractionHandoffSendStarted(staleDelivery))
      .toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readInteractionAuthority(database, request.turn_id)).toEqual(beforeStaleStart);

    const sending = store.markInteractionHandoffSendStarted(delivery);
    expect(sending.handoff).toMatchObject({
      handoff_id: delivery.handoff.handoff_id,
      handoff_attempt_id: delivery.handoff.handoff_attempt_id,
      handoff_attempt_no: 1,
      last_send_started_at: '2026-07-19T07:02:00Z',
    });
    expect(JSON.parse(database.prepare(`
      SELECT record_json
      FROM runtime_interaction_handoffs
      WHERE handoff_id = ?
    `).get(result.handoff_id).record_json)).toEqual(sending.handoff);

    database.close();
  });

  test('retries only a proven pre-send failure with a new fenced handoff attempt', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'pre-send-retry');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-pre-send-retry',
      tool_use_id: 'tool-use-pre-send-retry',
      kind: 'tool_approval',
      prompt: 'Allow the answer retry test?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const result = store.commitInteractionAnswer(
      interactionAnswer(request, 'pre-send-retry'),
    );
    const firstDelivery = store.claimInteractionHandoff(result.handoff_id);
    const retryWait = store.markInteractionHandoffPreSendFailure(
      firstDelivery,
      createContractError({
        code: 'delivery_transient',
        category: 'provider',
        retryable: true,
        sideEffectStatus: 'none',
        userMessage: 'The provider was unavailable before the answer send began.',
        occurredAt: '2026-07-19T07:02:00Z',
      }),
    );

    expect(retryWait).toMatchObject({
      status: 'retry_wait',
      interaction_state: 'answer_committed',
      handoff: {
        state: 'retry_wait',
        handoff_attempt_id: firstDelivery.handoff.handoff_attempt_id,
        handoff_attempt_no: 1,
        last_send_started_at: null,
        side_effect_status: 'none',
      },
    });

    const retryDelivery = store.claimInteractionHandoff(result.handoff_id);
    expect(retryDelivery.handoff).toMatchObject({
      state: 'delivering',
      handoff_attempt_id: 'handoff-attempt-pre-send-retry-2',
      handoff_attempt_no: 2,
      last_send_started_at: null,
      error: null,
      side_effect_status: 'none',
    });
    expect(() => store.markInteractionHandoffSendStarted(firstDelivery))
      .toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    const beforeLateAck = readInteractionAuthority(database, request.turn_id);
    expect(() => store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: firstDelivery.handoff.handoff_id,
      provider_attempt_id: firstDelivery.handoff.provider_attempt_id,
      handoff_attempt_id: firstDelivery.handoff.handoff_attempt_id,
      handoff_attempt_no: firstDelivery.handoff.handoff_attempt_no,
      lease_epoch: firstDelivery.handoff.lease_epoch,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    const afterLateAck = readInteractionAuthority(database, request.turn_id);
    expect({ ...afterLateAck, audits: afterLateAck.audits.slice(0, -1) }).toEqual(beforeLateAck);
    expect(afterLateAck.audits.at(-1)).toMatchObject({
      outcome: 'late_ack_ignored',
      handoff_id: firstDelivery.handoff.handoff_id,
    });

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

    const sendingDelivery = store.markInteractionHandoffSendStarted(delivery);
    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'deny',
      handoff_id: sendingDelivery.handoff.handoff_id,
      provider_attempt_id: sendingDelivery.handoff.provider_attempt_id,
      handoff_attempt_id: sendingDelivery.handoff.handoff_attempt_id,
      handoff_attempt_no: sendingDelivery.handoff.handoff_attempt_no,
      lease_epoch: sendingDelivery.handoff.lease_epoch,
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
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const second = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-second',
      tool_use_id: 'tool-use-second',
      kind: 'question',
      prompt: 'What should happen next?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    expect([first.ordinal, second.ordinal]).toEqual([1, 2]);

    const result = store.commitInteractionAnswer(interactionAnswer(first, 'first'));
    const delivery = store.claimInteractionHandoff(result.handoff_id);
    const sendingDelivery = store.markInteractionHandoffSendStarted(delivery);
    const acknowledgement = store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: sendingDelivery.handoff.handoff_id,
      provider_attempt_id: sendingDelivery.handoff.provider_attempt_id,
      handoff_attempt_id: sendingDelivery.handoff.handoff_attempt_id,
      handoff_attempt_no: sendingDelivery.handoff.handoff_attempt_no,
      lease_epoch: sendingDelivery.handoff.lease_epoch,
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
      prepareInteractionAnswer: preparedInteractionAnswer(async (delivery) => {
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
      }),
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

  test('executor service retries preparation failure before durably starting the send', async () => {
    const database = openTestDatabase();
    acceptQueuedTurn(database, 'service-pre-send-retry');
    let prepareCalls = 0;
    const sent = [];
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-service-pre-send-retry',
            tool_use_id: 'tool-service-pre-send-retry',
            kind: 'tool_approval',
            prompt: 'Allow the prepared answer?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
      },
      async prepareInteractionAnswer(delivery) {
        prepareCalls += 1;
        if (prepareCalls === 1) {
          const error = new Error('provider unavailable before send');
          error.providerError = {
            code: 'delivery_transient',
            category: 'provider',
            retryable: true,
            side_effect_status: 'none',
            user_message: 'The provider was unavailable before answer delivery began.',
          };
          throw error;
        }
        return {
          async send(startedDelivery) {
            sent.push(startedDelivery);
            return {
              status: 'accepted',
              handoff_id: startedDelivery.handoff.handoff_id,
              provider_attempt_id: startedDelivery.handoff.provider_attempt_id,
              handoff_attempt_id: startedDelivery.handoff.handoff_attempt_id,
              handoff_attempt_no: startedDelivery.handoff.handoff_attempt_no,
              lease_epoch: startedDelivery.handoff.lease_epoch,
            };
          },
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-pre-send-retry',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('service-pre-send-retry'),
    });
    const waiting = await service.runNext();
    const committed = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'service-pre-send-retry'),
    );

    await expect(service.deliverInteractionAnswer(committed.handoff_id)).resolves.toMatchObject({
      status: 'retry_wait',
      handoff: {
        state: 'retry_wait',
        last_send_started_at: null,
      },
    });
    expect(sent).toEqual([]);

    await expect(service.deliverInteractionAnswer(committed.handoff_id)).resolves.toMatchObject({
      acknowledgement: { status: 'accepted' },
      execution: { status: 'completed' },
    });
    expect(sent).toHaveLength(1);
    expect(sent[0].handoff).toMatchObject({
      handoff_attempt_no: 2,
      last_send_started_at: '2026-07-19T07:02:00Z',
    });
    expect(database.prepare(`
      SELECT interaction.handoff_version, handoff.state
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      WHERE handoff.handoff_id = ?
    `).get(committed.handoff_id)).toEqual({
      handoff_version: 6,
      state: 'accepted',
    });

    database.close();
  });

  test('does not retry when preparation cannot prove no provider side effect', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-pre-send-terminal');
    let sendCalls = 0;
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-service-pre-send-terminal',
            tool_use_id: 'tool-service-pre-send-terminal',
            kind: 'tool_approval',
            prompt: 'Allow the non-retryable prepared answer?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
      },
      async prepareInteractionAnswer() {
        const error = new Error('provider rejected answer before send');
        error.providerError = {
          code: 'provider_context_invalid',
          category: 'provider',
          retryable: true,
          side_effect_status: 'unknown',
          user_message: 'Preparation may have crossed the provider boundary.',
        };
        throw error;
      },
      async send() {
        sendCalls += 1;
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-pre-send-terminal',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('service-pre-send-terminal'),
    });
    const waiting = await service.runNext();
    const committed = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'service-pre-send-terminal'),
    );

    await expect(service.deliverInteractionAnswer(committed.handoff_id)).resolves.toMatchObject({
      status: 'stopped',
      interaction_state: 'cancelled',
      handoff_state: 'cancelled',
      turn_state: 'stopped',
    });
    expect(sendCalls).toBe(0);
    expect(readInteractionAuthority(database, accepted.turn_id)).toMatchObject({
      turn: { state: 'stopped' },
      interactions: [expect.objectContaining({
        state: 'cancelled',
        handoff_state: 'cancelled',
      })],
      handoffs: [expect.objectContaining({
        state: 'cancelled',
        record_json: expect.stringContaining('"side_effect_status":"unknown"'),
      })],
      audits: expect.arrayContaining([
        expect.objectContaining({ outcome: 'cancelled_pre_send' }),
      ]),
    });
    expect(database.prepare(`
      SELECT state, side_effect_status
      FROM runtime_provider_attempts
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({
      state: 'stopped',
      side_effect_status: 'unknown',
    });

    database.close();
  });

  test('startup recovers an expired sent handoff after a worker crash', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'crashed-sent-handoff');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-crashed-sent-handoff',
      tool_use_id: 'tool-use-crashed-sent-handoff',
      kind: 'tool_approval',
      prompt: 'Allow the action before the worker crashes?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(request, 'crashed-sent-handoff'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    const restarted = createExecutorService({
      database,
      adapter: {
        async *execute() {},
        async prepareInteractionAnswer() {
          throw new Error('The crashed answer must never be resent.');
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-crash-restart',
      now: () => '2026-07-19T07:03:00Z',
      generateId: deterministicIds('crash-restart'),
    });

    restarted.start();
    expect(readInteractionAuthority(database, request.turn_id)).toMatchObject({
      turn: { state: 'recovering' },
      interactions: [expect.objectContaining({
        interaction_id: request.interaction_id,
        state: 'delivery_unknown',
        handoff_state: 'delivery_unknown',
      })],
      handoffs: [expect.objectContaining({ state: 'delivery_unknown' })],
      audits: [expect.objectContaining({
        outcome: 'delivery_unknown',
        acknowledgement_json: expect.stringContaining('expired_writer_lease'),
      })],
    });
    expect(readEvents(database, request.turn_id).at(-1)).toMatchObject({
      kind: 'interaction_answer_delivery_unknown',
      phase: 'recovering',
    });
    expect(() => store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: sending.handoff.handoff_id,
      provider_attempt_id: sending.handoff.provider_attempt_id,
      handoff_attempt_id: sending.handoff.handoff_attempt_id,
      handoff_attempt_no: sending.handoff.handoff_attempt_no,
      lease_epoch: sending.handoff.lease_epoch,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));

    database.close();
  });

  test('completes delivery_unknown only from a read-only idempotent same-handoff proof', async () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(
      database,
      'query-accepted',
    );
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-query-accepted',
      tool_use_id: 'tool-use-query-accepted',
      kind: 'tool_approval',
      prompt: 'Allow the queried answer?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(request, 'query-accepted'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    store.markInteractionHandoffDeliveryUnknown(sending);

    let queryCalls = 0;
    const adapter = {
      async *execute() {},
      async queryInteractionHandoffAcceptance(delivery) {
        queryCalls += 1;
        return {
          status: 'accepted',
          read_only: true,
          idempotent: true,
          handoff_id: delivery.handoff.handoff_id,
          provider_attempt_id: delivery.handoff.provider_attempt_id,
          handoff_attempt_id: delivery.handoff.handoff_attempt_id,
          handoff_attempt_no: delivery.handoff.handoff_attempt_no,
          lease_epoch: delivery.handoff.lease_epoch,
          accepted_at: '2026-07-19T07:02:00Z',
          evidence_ref: 'provider-ack-query-query-accepted',
          reason_code: null,
        };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-query-accepted',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('query-accepted-recovery'),
    });

    await expect(service.reconcileInteractionHandoff(committed.handoff_id))
      .rejects.toMatchObject({ code: 'notification_pending' });
    expect(queryCalls).toBe(0);
    markLatestTurnProjectionDelivered(database, request.turn_id);
    await expect(service.reconcileInteractionHandoff(committed.handoff_id)).resolves.toMatchObject({
      status: 'accepted',
      acknowledgement_source: 'read_only_idempotent_query',
      handoff_id: committed.handoff_id,
      turn_state: 'recovering',
    });
    expect(database.prepare(`
      SELECT interaction.state, interaction.handoff_state,
        handoff.state AS durable_handoff_state, turn.state AS turn_state,
        audit.outcome
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      JOIN runtime_interaction_audit AS audit ON audit.audit_id = (
        SELECT audit_id FROM runtime_interaction_audit
        WHERE handoff_id = handoff.handoff_id
        ORDER BY created_at DESC, audit_id DESC LIMIT 1
      )
      WHERE handoff.handoff_id = ?
    `).get(committed.handoff_id)).toEqual({
      state: 'answered',
      handoff_state: 'accepted',
      durable_handoff_state: 'accepted',
      turn_state: 'recovering',
      outcome: 'accepted_via_query',
    });
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      kind: 'interaction_answered',
      phase: 'recovering',
    });

    database.close();
  });

  test('keeps delivery_unknown without resending when the provider query cannot prove acceptance', async () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'query-unproven');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-query-unproven',
      tool_use_id: 'tool-use-query-unproven',
      kind: 'tool_approval',
      prompt: 'Allow the unproven answer?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(request, 'query-unproven'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    store.markInteractionHandoffDeliveryUnknown(sending);
    let sendPreparations = 0;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {},
        async prepareInteractionAnswer() {
          sendPreparations += 1;
          throw new Error('reconciliation must not prepare another send');
        },
        async queryInteractionHandoffAcceptance(delivery) {
          return {
            status: 'unknown',
            read_only: true,
            idempotent: true,
            handoff_id: delivery.handoff.handoff_id,
            provider_attempt_id: delivery.handoff.provider_attempt_id,
            handoff_attempt_id: delivery.handoff.handoff_attempt_id,
            handoff_attempt_no: delivery.handoff.handoff_attempt_no,
            lease_epoch: delivery.handoff.lease_epoch,
            accepted_at: null,
            evidence_ref: null,
            reason_code: 'provider_acceptance_query_unavailable',
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-query-unproven',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('query-unproven-recovery'),
    });

    markLatestTurnProjectionDelivered(database, request.turn_id);
    await expect(service.reconcileInteractionHandoff(committed.handoff_id)).resolves.toMatchObject({
      status: 'unknown',
      handoff_id: committed.handoff_id,
      turn_state: 'recovering',
    });
    expect(sendPreparations).toBe(0);
    expect(readInteractionAuthority(database, request.turn_id)).toMatchObject({
      turn: { state: 'recovering' },
      interactions: [expect.objectContaining({ state: 'delivery_unknown' })],
      handoffs: [expect.objectContaining({ state: 'delivery_unknown' })],
      audits: expect.arrayContaining([
        expect.objectContaining({ outcome: 'query_unproven' }),
      ]),
    });

    database.close();
  });

  test('requires a trusted authorization decision to terminate delivery_unknown', async () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'authorized-termination');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-authorized-termination',
      tool_use_id: 'tool-use-authorized-termination',
      kind: 'tool_approval',
      prompt: 'Allow the answer that may need recovery?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(request, 'authorized-termination'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    store.markInteractionHandoffDeliveryUnknown(sending);

    let authorized = false;
    let authorizationCalls = 0;
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'executor-service-authorized-termination',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('authorized-termination-disposition'),
      async interactionHandoffDispositionAuthorizer(decision) {
        authorizationCalls += 1;
        if (!authorized) return null;
        return {
          decision_id: 'disposition-decision-authorized-termination',
          authorized: true,
          actor_id: 'operations-user-123',
          capability: decision.capability,
          scope: decision.scope,
          policy_id: 'recovery-policy-1',
          policy_version: 1,
          authorized_at: '2026-07-19T07:02:00Z',
        };
      },
    });

    await expect(service.resolveInteractionHandoff(committed.handoff_id, {
      action: 'terminate',
      replacement_interaction: null,
    })).rejects.toMatchObject({ code: 'notification_pending' });
    expect(authorizationCalls).toBe(0);
    markLatestTurnProjectionDelivered(database, request.turn_id);
    await expect(service.resolveInteractionHandoff(committed.handoff_id, {
      action: 'terminate',
      replacement_interaction: null,
    })).rejects.toMatchObject({ code: 'authorization_denied' });
    expect(readInteractionAuthority(database, request.turn_id)).toMatchObject({
      turn: { state: 'recovering' },
      interactions: [expect.objectContaining({ state: 'delivery_unknown' })],
    });

    authorized = true;
    await expect(service.resolveInteractionHandoff(committed.handoff_id, {
      action: 'terminate',
      replacement_interaction: null,
    })).resolves.toMatchObject({
      status: 'terminated',
      interaction_id: request.interaction_id,
      handoff_id: committed.handoff_id,
      turn_state: 'recovering',
    });
    const authority = readInteractionAuthority(database, request.turn_id);
    const cancelledHandoff = JSON.parse(authority.handoffs[0].record_json);
    expect(authority).toMatchObject({
      turn: { state: 'recovering' },
      interactions: [expect.objectContaining({
        state: 'cancelled',
        handoff_state: 'cancelled',
      })],
      handoffs: [expect.objectContaining({ state: 'cancelled' })],
      audits: expect.arrayContaining([
        expect.objectContaining({ outcome: 'authorized_termination' }),
      ]),
    });
    expect(cancelledHandoff).toMatchObject({
      state: 'cancelled',
      last_send_started_at: sending.handoff.last_send_started_at,
      provider_acked_at: null,
      side_effect_status: 'unknown',
      reason_code: 'authorized_delivery_unknown_termination',
    });
    expect(JSON.parse(authority.audits.at(-1).acknowledgement_json)).toMatchObject({
      disposition: { action: 'terminate', replacement_interaction: null },
      authorization: {
        decision_id: 'disposition-decision-authorized-termination',
        capability: 'interaction.handoff.resolve',
      },
      previous_handoff: {
        state: 'delivery_unknown',
        handoff_attempt_id: sending.handoff.handoff_attempt_id,
      },
    });
    expect(() => store.acknowledgeInteractionHandoff({
      status: 'accepted',
      handoff_id: sending.handoff.handoff_id,
      provider_attempt_id: sending.handoff.provider_attempt_id,
      handoff_attempt_id: sending.handoff.handoff_attempt_id,
      handoff_attempt_no: sending.handoff.handoff_attempt_no,
      lease_epoch: sending.handoff.lease_epoch,
    })).toThrow(expect.objectContaining({ code: 'stale_attempt' }));
    expect(readInteractionAuthority(database, request.turn_id).audits).toEqual(
      expect.arrayContaining([expect.objectContaining({
        outcome: 'late_ack_ignored',
        handoff_id: sending.handoff.handoff_id,
      })]),
    );

    database.close();
  });

  test('authorized recovery establishes an answerable Core-owned interaction', async () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'authorized-supersession');
    const first = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-question-authorized-supersession-old',
      tool_use_id: 'tool-use-authorized-supersession-old',
      kind: 'tool_approval',
      prompt: 'Allow the original answer?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(first, 'authorized-supersession'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    store.markInteractionHandoffDeliveryUnknown(sending);
    markLatestTurnProjectionDelivered(database, first.turn_id);
    store.releaseRecoveringExecutorOwnership(turnContext);
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {},
        async prepareInteractionAnswer() {
          throw new Error('Core recovery control must not call the provider adapter.');
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-authorized-supersession',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('authorized-supersession-disposition'),
      async interactionHandoffDispositionAuthorizer(decision) {
        return {
          decision_id: 'disposition-decision-authorized-supersession',
          authorized: true,
          actor_id: 'operations-user-123',
          capability: decision.capability,
          scope: decision.scope,
          policy_id: 'recovery-policy-1',
          policy_version: 1,
          authorized_at: '2026-07-19T07:02:00Z',
        };
      },
    });

    const resolution = await service.resolveInteractionHandoff(committed.handoff_id, {
      action: 'establish_interaction',
      replacement_interaction: {
        kind: 'recovery_decision',
        prompt: 'Choose how to continue after uncertain delivery.',
        choices: [],
        authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
        allowed_sources: ['card_action'],
      },
    });
    expect(resolution).toMatchObject({
      status: 'interaction_established',
      replacement_interaction_id: expect.any(String),
      turn_state: 'recovering',
    });
    expect(readInteractionAuthority(database, first.turn_id)).toMatchObject({
      turn: { state: 'recovering' },
      interactions: expect.arrayContaining([
        expect.objectContaining({ interaction_id: first.interaction_id, state: 'cancelled' }),
        expect.objectContaining({
          interaction_id: resolution.replacement_interaction_id,
          state: 'pending',
          handoff_state: 'not_started',
        }),
      ]),
    });
    const replacementRow = readInteractionAuthority(database, first.turn_id).interactions
      .find(({ interaction_id: interactionId }) => (
        interactionId === resolution.replacement_interaction_id
      ));
    const replacementRequest = JSON.parse(replacementRow.request_json);
    expect(replacementRequest).toMatchObject({
      interaction_id: resolution.replacement_interaction_id,
      parent_type: 'recovery_control',
      control_id: expect.any(String),
      ordinal: 1,
      kind: 'recovery_decision',
      prompt: 'Choose how to continue after uncertain delivery.',
      state: 'pending',
      handoff_state: 'not_started',
      runtime_fence: null,
    });
    const recoveryAnswer = service.submitInteractionAnswer(
      interactionAnswer(replacementRequest, 'authorized-recovery-control'),
    );
    expect(recoveryAnswer).toMatchObject({
      status: 'accepted',
      control_id: replacementRequest.control_id,
      handoff_state: 'pending',
    });
    await expect(service.deliverInteractionAnswer(recoveryAnswer.handoff_id)).resolves.toMatchObject({
      acknowledgement: {
        status: 'accepted',
        resumed: false,
        turn_state: 'recovering',
      },
      execution: null,
    });
    expect(JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE interaction_id = ?
    `).get(replacementRequest.interaction_id).request_json)).toMatchObject({
      state: 'answered',
      handoff_state: 'accepted',
    });

    database.close();
  });

  test('rolls back a Core-owned recovery claim when its acknowledgement cannot commit', async () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'recovery-control-rollback');
    const original = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-recovery-control-rollback',
      tool_use_id: 'tool-recovery-control-rollback',
      kind: 'tool_approval',
      prompt: 'Allow the original answer?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const committed = store.commitInteractionAnswer(
      interactionAnswer(original, 'recovery-control-rollback-original'),
    );
    const claimed = store.claimInteractionHandoff(committed.handoff_id);
    const sending = store.markInteractionHandoffSendStarted(claimed);
    store.markInteractionHandoffDeliveryUnknown(sending);
    const unknown = store.getInteractionHandoffForRecovery(committed.handoff_id);
    store.releaseRecoveringExecutorOwnership(turnContext);
    const replacementInteraction = {
      kind: 'recovery_decision',
      prompt: 'Choose how to continue after uncertain delivery.',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    };
    const disposition = {
      action: 'establish_interaction',
      replacement_interaction: replacementInteraction,
    };
    const resolution = store.resolveInteractionHandoffDisposition(
      unknown,
      disposition,
      {
        decision_id: 'decision-recovery-control-rollback',
        authorized: true,
        actor_id: 'operations-user-123',
        capability: 'interaction.handoff.resolve',
        scope: {
          conversation_id: original.conversation_id,
          turn_id: original.turn_id,
          handoff_id: committed.handoff_id,
          action: disposition.action,
          replacement_interaction: replacementInteraction,
        },
        policy_id: 'recovery-policy-1',
        policy_version: 1,
        authorized_at: '2026-07-19T07:02:00Z',
      },
    );
    const replacementRequest = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE interaction_id = ?
    `).get(resolution.replacement_interaction_id).request_json);
    const answer = store.commitInteractionAnswer(
      interactionAnswer(replacementRequest, 'recovery-control-rollback'),
    );
    const before = readInteractionAuthority(database, original.turn_id);
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {},
        async prepareInteractionAnswer() {
          throw new Error('Core recovery control must not call the provider adapter.');
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-recovery-control-rollback',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('recovery-control-rollback-service'),
    });
    database.exec(`
      CREATE TRIGGER force_recovery_control_audit_failure
      BEFORE INSERT ON runtime_interaction_audit
      WHEN NEW.interaction_id = '${replacementRequest.interaction_id}'
      BEGIN
        SELECT RAISE(ABORT, 'forced recovery control audit failure');
      END;
    `);

    await expect(service.deliverInteractionAnswer(answer.handoff_id))
      .rejects.toThrow(/forced recovery control audit failure/);
    expect(readInteractionAuthority(database, original.turn_id)).toEqual(before);
    const persistedHandoff = JSON.parse(database.prepare(`
      SELECT record_json FROM runtime_interaction_handoffs WHERE handoff_id = ?
    `).get(answer.handoff_id).record_json);
    expect(persistedHandoff).toMatchObject({
      state: 'pending',
      last_send_started_at: null,
      provider_acked_at: null,
      side_effect_status: 'none',
    });
    database.exec('DROP TRIGGER force_recovery_control_audit_failure;');
    await expect(service.deliverInteractionAnswer(answer.handoff_id)).resolves.toMatchObject({
      acknowledgement: {
        status: 'accepted',
        resumed: false,
        turn_state: 'recovering',
      },
      execution: null,
    });
    expect(JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE interaction_id = ?
    `).get(replacementRequest.interaction_id).request_json)).toMatchObject({
      state: 'answered',
      handoff_state: 'accepted',
    });

    database.close();
  });

  test('uses iterator return to prove isolation when a failed handler has no abort primitive', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-handler-no-abort');
    let iteratorReturnCalls = 0;
    const adapter = {
      async *execute() {
        try {
          yield {
            kind: 'interaction_requested',
            payload: {
              provider_interaction_ref: 'provider-handler-no-abort',
              tool_use_id: 'tool-handler-no-abort',
              kind: 'tool_approval',
              prompt: 'Allow the uncertain handler?',
              choices: [],
              authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
              allowed_sources: ['card_action'],
            },
          };
        } finally {
          iteratorReturnCalls += 1;
        }
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async () => {
        throw new Error('uncertain handler send');
      }),
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-handler-no-abort',
      now: () => '2026-07-19T07:02:01Z',
      generateId: deterministicIds('handler-no-abort'),
    });

    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'handler-no-abort'),
    );
    await expect(service.deliverInteractionAnswer(answer.handoff_id)).rejects.toThrow(
      /uncertain handler send/,
    );

    expect(iteratorReturnCalls).toBe(1);
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null });

    await service.close();
    database.close();
  });

  test('retries atomic delivery-unknown recovery after persistence and abort failures', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-handler-recovery-retry');
    let abortCalls = 0;
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-handler-recovery-retry',
            tool_use_id: 'tool-handler-recovery-retry',
            kind: 'tool_approval',
            prompt: 'Allow the retrying handler?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async () => {
        throw new Error('uncertain retrying send');
      }),
      async abort() {
        abortCalls += 1;
        throw new Error('forced recovery abort failure');
      },
      async close() { return [accepted.conversation_id]; },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-handler-recovery-retry',
      now: () => '2026-07-19T07:02:02Z',
      generateId: deterministicIds('handler-recovery-retry'),
    });
    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'handler-recovery-retry'),
    );
    database.exec(`
      CREATE TRIGGER fail_delivery_unknown_audit
      BEFORE INSERT ON runtime_interaction_audit
      WHEN NEW.outcome = 'delivery_unknown'
      BEGIN
        SELECT RAISE(ABORT, 'forced delivery unknown audit failure');
      END;
    `);

    await expect(service.deliverInteractionAnswer(answer.handoff_id)).rejects.toThrow(
      /uncertain retrying send/,
    );
    expect(abortCalls).toBe(1);
    expect(database.prepare(`
      SELECT interaction.state, handoff.state AS handoff_state, turn.state AS turn_state
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'answer_delivering',
      handoff_state: 'delivering',
      turn_state: 'waiting_user',
    });
    await expect(service.runNext()).rejects.toThrow(/close_failed/);

    database.exec('DROP TRIGGER fail_delivery_unknown_audit');
    await expect(service.close()).resolves.toBeUndefined();
    expect(database.prepare(`
      SELECT interaction.state, handoff.state AS handoff_state, turn.state AS turn_state
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
      turn_state: 'recovering',
    });
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null });
    expect(database.prepare(`
      SELECT owner_service_instance_id FROM runtime_executor_residents WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ owner_service_instance_id: null });

    database.close();
  });

  test('waits for an in-flight handler failure before closing provider ownership', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-handler-close-race');
    let releaseHandler;
    let handlerStarted;
    const handlerStartedPromise = new Promise((resolve) => { handlerStarted = resolve; });
    const handlerReleasePromise = new Promise((resolve) => { releaseHandler = resolve; });
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-handler-close-race',
            tool_use_id: 'tool-handler-close-race',
            kind: 'tool_approval',
            prompt: 'Allow the racing handler?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async () => {
        handlerStarted();
        await handlerReleasePromise;
        throw new Error('deferred uncertain handler send');
      }),
      async abort() {},
      async close() { return [accepted.conversation_id]; },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-handler-close-race',
      now: () => '2026-07-19T07:02:03Z',
      generateId: deterministicIds('handler-close-race'),
    });
    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'handler-close-race'),
    );
    const delivery = service.deliverInteractionAnswer(answer.handoff_id);
    await handlerStartedPromise;

    let closeSettled = false;
    const closing = service.close().finally(() => { closeSettled = true; });
    await new Promise((resolve) => setImmediate(resolve));
    expect(closeSettled).toBe(false);
    releaseHandler();

    await expect(delivery).rejects.toThrow(/deferred uncertain handler send/);
    await expect(closing).resolves.toBeUndefined();
    expect(database.prepare(`
      SELECT interaction.state, handoff.state AS handoff_state, turn.state AS turn_state
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
      turn_state: 'recovering',
    });
    expect(database.prepare(`
      SELECT lease_owner FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({ lease_owner: null });

    database.close();
  });

  test('closes the provider after acknowledgement without waiting for resumed execution', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-ack-close-race');
    let releaseResumedExecution;
    let resumedExecutionStarted;
    const resumedExecutionStartedPromise = new Promise((resolve) => {
      resumedExecutionStarted = resolve;
    });
    const resumedExecutionGate = new Promise((resolve) => {
      releaseResumedExecution = resolve;
    });
    let closeCalls = 0;
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-ack-close-race',
            tool_use_id: 'tool-ack-close-race',
            kind: 'tool_approval',
            prompt: 'Allow the provider close race?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['card_action'],
          },
        };
        resumedExecutionStarted();
        await resumedExecutionGate;
        yield {
          kind: 'text_snapshot',
          payload: { text: 'Resumed execution closed.', end_offset: 25 },
          provider_native_id: null,
        };
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async (delivery) => {
        return {
          status: 'accepted',
          handoff_id: delivery.handoff.handoff_id,
          provider_attempt_id: delivery.handoff.provider_attempt_id,
          handoff_attempt_id: delivery.handoff.handoff_attempt_id,
          handoff_attempt_no: delivery.handoff.handoff_attempt_no,
          lease_epoch: delivery.handoff.lease_epoch,
        };
      }),
      async close() {
        closeCalls += 1;
        releaseResumedExecution();
        return [accepted.conversation_id];
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-ack-close-race',
      now: () => '2026-07-19T07:02:04Z',
      generateId: deterministicIds('ack-close-race'),
    });
    const waiting = await service.runNext();
    const answer = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'ack-close-race'),
    );
    const delivery = service.deliverInteractionAnswer(answer.handoff_id);
    await resumedExecutionStartedPromise;

    const closing = service.close();
    await new Promise((resolve) => setImmediate(resolve));
    const closeCallsBeforeManualRelease = closeCalls;
    if (closeCalls === 0) releaseResumedExecution();
    await expect(delivery).resolves.toMatchObject({
      acknowledgement: { status: 'accepted', resumed: true },
      execution: { status: 'completed' },
    });
    await expect(closing).resolves.toBeUndefined();
    expect(closeCallsBeforeManualRelease).toBe(1);

    database.close();
  });

  test('does not advance an unanswered interaction while closing the service', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-close-waiting');
    let advancedPastQuestion = false;
    const service = createExecutorService({
      database,
      adapter: {
        async *execute() {
          yield {
            kind: 'interaction_requested',
            payload: {
              provider_interaction_ref: 'provider-question-service-close',
              tool_use_id: 'tool-use-service-close',
              kind: 'tool_approval',
              prompt: 'Allow the requested workspace write?',
              choices: [],
              authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
              allowed_sources: ['main_card_reply', 'card_action'],
            },
          };
          advancedPastQuestion = true;
          yield {
            kind: 'text_snapshot',
            payload: { text: 'must not run', end_offset: 12 },
            provider_native_id: null,
          };
        },
      },
      provider: 'claude',
      serviceInstanceId: 'executor-service-close-waiting',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('service-close-waiting'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    await expect(service.close()).resolves.toBeUndefined();
    expect(advancedPastQuestion).toBe(false);
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      kind: 'turn_state_changed',
      payload: { reason_code: 'executor_shutdown_uncertain' },
    });

    database.close();
  });

  test('retains Codex ownership when provider close cannot prove process-group isolation', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'codex-close-group-uncertain');
    let iteratorReturned = false;
    let interactionEmitted = false;
    const adapter = {
      execute() {
        return {
          [Symbol.asyncIterator]() { return this; },
          async next() {
            if (interactionEmitted) return new Promise(() => {});
            interactionEmitted = true;
            return {
              done: false,
              value: {
                kind: 'interaction_requested',
                payload: {
                  provider_interaction_ref: 'provider-codex-close-group-uncertain',
                  tool_use_id: 'tool-codex-close-group-uncertain',
                  kind: 'tool_approval',
                  prompt: 'Allow the uncertain provider action?',
                  choices: [],
                  authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
                  allowed_sources: ['card_action'],
                },
              },
            };
          },
          async return() {
            iteratorReturned = true;
            return { done: true, value: undefined };
          },
        };
      },
      async close() {
        const error = new Error('Codex process group is still alive');
        error.closedConversationIds = [];
        throw error;
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-codex-close-group-uncertain',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('codex-close-group-uncertain'),
    });

    await expect(service.runNext()).resolves.toMatchObject({ status: 'waiting_user' });
    await expect(service.close()).rejects.toBeInstanceOf(Error);
    expect(iteratorReturned).toBe(false);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-codex-close-group-uncertain',
      turn_id: accepted.turn_id,
    });

    database.close();
  });

  test('holds an answered interaction until a parallel permission callback settles', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'service-permission-interleave');
    let resolvePermission;
    const permission = new Promise((resolve) => { resolvePermission = resolve; });
    const adapter = {
      async *execute(context, controls) {
        const permissionResult = controls.requestPermission(
          { tool_name: 'Write', input: { path: '/workspace/file' } },
        );
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-question-permission-interleave',
            tool_use_id: 'tool-use-permission-interleave',
            kind: 'tool_approval',
            prompt: 'Confirm the second blocking interaction.',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
        await permissionResult;
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async (delivery) => {
        return {
          status: 'accepted',
          handoff_id: delivery.handoff.handoff_id,
          provider_attempt_id: delivery.handoff.provider_attempt_id,
          handoff_attempt_id: delivery.handoff.handoff_attempt_id,
          handoff_attempt_no: delivery.handoff.handoff_attempt_no,
          lease_epoch: delivery.handoff.lease_epoch,
        };
      }),
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-permission-interleave',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('permission-interleave'),
      permissionHandler: () => permission,
    });

    const waiting = await service.runNext();
    const answerResult = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'permission-interleave'),
    );
    const handled = await service.deliverInteractionAnswer(answerResult.handoff_id);
    expect(handled).toMatchObject({
      acknowledgement: { resumed: false, turn_state: 'waiting_user' },
      execution: null,
    });
    resolvePermission({ behavior: 'allow' });
    await new Promise((resolve) => setImmediate(resolve));
    expect(database.prepare(`SELECT state FROM runtime_turns WHERE turn_id = ?`)
      .get(accepted.turn_id)).toEqual({ state: 'completed' });

    await service.close();
    database.close();
  });

  test('fails a sent provider answer closed as delivery_unknown when app-server acknowledgement is uncertain', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'app-server-answer-unknown');
    const adapter = {
      async *execute() {
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-answer-unknown',
            tool_use_id: 'tool-answer-unknown',
            kind: 'tool_approval',
            prompt: 'Allow the action?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async () => {
        const error = new Error('connection closed after response write');
        error.providerError = {
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'The provider may have received the answer.',
        };
        throw error;
      }),
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-app-server-answer-unknown',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('app-server-answer-unknown'),
    });
    const waiting = await service.runNext();
    const committed = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'app-server-answer-unknown'),
    );

    await expect(service.deliverInteractionAnswer(committed.handoff_id)).resolves.toMatchObject({
      status: 'delivery_unknown',
      turn_id: accepted.turn_id,
      handoff_id: committed.handoff_id,
    });
    expect(database.prepare(`
      SELECT state
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(database.prepare(`
      SELECT interaction.state, interaction.handoff_state, handoff.state AS handoff_state_record
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id)).toEqual({
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
      handoff_state_record: 'delivery_unknown',
    });
    expect(readEvents(database, accepted.turn_id).at(-1)).toMatchObject({
      kind: 'interaction_answer_delivery_unknown',
      phase: 'recovering',
      error: {
        code: 'side_effect_unknown',
        side_effect_status: 'unknown',
      },
    });

    database.close();
  });

  test('cancels a committed unsent handoff when the provider is no longer waiting', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedTurn(database, 'app-server-unsent-answer-cancel');
    let reportProviderFailure;
    const handlerCalls = [];
    const adapter = {
      async *execute(context) {
        reportProviderFailure = context.reportProviderFailure;
        yield {
          kind: 'interaction_requested',
          payload: {
            provider_interaction_ref: 'provider-unsent-answer-cancel',
            tool_use_id: 'tool-unsent-answer-cancel',
            kind: 'tool_approval',
            prompt: 'Allow the action?',
            choices: [],
            authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
            allowed_sources: ['main_card_reply', 'card_action'],
          },
        };
      },
      prepareInteractionAnswer: preparedInteractionAnswer(async (delivery) => {
        handlerCalls.push(delivery);
        throw new Error('cancelled handoff must never reach the provider');
      }),
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-app-server-unsent-answer-cancel',
      now: () => '2026-07-19T07:02:00Z',
      generateId: deterministicIds('app-server-unsent-answer-cancel'),
    });
    const waiting = await service.runNext();
    const committed = service.submitInteractionAnswer(
      interactionAnswer(waiting.request, 'app-server-unsent-answer-cancel'),
    );
    expect(reportProviderFailure({
      providerError: {
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The app-server is no longer waiting for this answer.',
      },
    })).toMatchObject({
      status: 'recovering',
      cancelled_interaction_ids: [waiting.request.interaction_id],
      cancelled_handoff_ids: [committed.handoff_id],
    });

    const authority = database.prepare(`
      SELECT interaction.state, interaction.handoff_state, interaction.handoff_version,
        interaction.request_json, handoff.state AS durable_handoff_state, handoff.record_json
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      WHERE interaction.interaction_id = ?
    `).get(waiting.request.interaction_id);
    expect(authority).toMatchObject({
      state: 'cancelled',
      handoff_state: 'cancelled',
      handoff_version: 2,
      durable_handoff_state: 'cancelled',
    });
    expect(JSON.parse(authority.request_json)).toMatchObject({
      state: 'cancelled',
      handoff_state: 'cancelled',
      terminal_reason: 'provider_connection_lost',
    });
    expect(JSON.parse(authority.record_json)).toMatchObject({
      state: 'cancelled',
      last_send_started_at: null,
      reason_code: 'provider_connection_lost',
      side_effect_status: 'none',
    });
    expect(database.prepare(`
      SELECT outcome, acknowledgement_json
      FROM runtime_interaction_audit
      WHERE handoff_id = ?
    `).get(committed.handoff_id)).toMatchObject({
      outcome: 'cancelled',
      acknowledgement_json: expect.any(String),
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'recovering' });
    expect(readEvents(database, accepted.turn_id).slice(-4).map(({ kind, phase }) => ({ kind, phase })))
      .toEqual([
        { kind: 'interaction_cancelled', phase: 'waiting_user' },
        { kind: 'turn_state_changed', phase: 'recovering' },
        { kind: 'recovery_started', phase: 'recovering' },
        { kind: 'recovery_waiting_decision', phase: 'recovering' },
      ]);
    const recoveryProjection = database.prepare(`
      SELECT status, render_model_json
      FROM runtime_projection_snapshots
      WHERE turn_id = ?
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(accepted.turn_id);
    expect(recoveryProjection.status).toBe('staged');
    expect(JSON.parse(recoveryProjection.render_model_json)).toMatchObject({
      phase: 'recovering',
    });
    expect(database.prepare(`
      SELECT status FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ status: 'pending' });
    await expect(service.deliverInteractionAnswer(committed.handoff_id))
      .rejects.toMatchObject({ code: 'illegal_transition' });
    expect(handlerCalls).toEqual([]);

    database.close();
  });

  test('rejects an unauthorized provider answer before creating its durable handoff', () => {
    const database = openTestDatabase();
    const { accepted, store, turnContext } = createRunningTurn(
      database,
      'app-server-answer-unauthorized',
    );
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-answer-unauthorized',
      tool_use_id: 'tool-answer-unauthorized',
      kind: 'tool_approval',
      prompt: 'Allow the action?',
      choices: [],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const answer = interactionAnswer(request, 'app-server-answer-unauthorized');
    answer.actor.actor_id = 'attacker';

    expect(store.commitInteractionAnswer(answer)).toMatchObject({
      status: 'rejected',
      interaction_state: 'pending',
      interaction_version: request.version,
      handoff_state: 'not_applicable',
      error: { code: 'interaction_actor_forbidden' },
    });
    expect(database.prepare(`
      SELECT state, handoff_state
      FROM runtime_interactions
      WHERE interaction_id = ?
    `).get(request.interaction_id)).toEqual({ state: 'pending', handoff_state: 'not_started' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_interaction_handoffs
      WHERE interaction_id = ?
    `).get(request.interaction_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT state
      FROM runtime_turns
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'waiting_user' });

    database.close();
  });

  test('rejects an out-of-domain choice before creating durable answer or handoff state', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'choice-domain');
    const request = store.requestInteraction(turnContext, {
      provider_interaction_ref: 'provider-choice-domain',
      tool_use_id: 'tool-choice-domain',
      kind: 'choice',
      prompt: 'Choose a safe path.',
      choices: [{ choice_id: 'safe', label: 'Safe path' }],
      authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
      allowed_sources: ['card_action'],
    });
    const invalidAnswer = interactionAnswer(request, 'choice-domain', {
      value: { kind: 'choice', choice_id: 'unknown' },
    });

    expect(store.commitInteractionAnswer(invalidAnswer)).toMatchObject({
      status: 'rejected',
      interaction_state: 'pending',
      interaction_version: request.version,
      handoff_state: 'not_applicable',
      error: { code: 'validation_error' },
    });
    expect(readInteractionAuthority(database, request.turn_id)).toMatchObject({
      interactions: [{
        interaction_id: request.interaction_id,
        state: 'pending',
        handoff_state: 'not_started',
      }],
      answers: [],
      handoffs: [],
    });

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
      allowed_sources: ['main_card_reply', 'card_action'],
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
      allowed_sources: ['main_card_reply', 'card_action'],
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
      allowed_sources: ['main_card_reply', 'card_action'],
    });
    const result = store.commitInteractionAnswer(interactionAnswer(request, 'ack-rollback'));
    const delivery = store.claimInteractionHandoff(result.handoff_id);
    const sendingDelivery = store.markInteractionHandoffSendStarted(delivery);
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
      handoff_id: sendingDelivery.handoff.handoff_id,
      provider_attempt_id: sendingDelivery.handoff.provider_attempt_id,
      handoff_attempt_id: sendingDelivery.handoff.handoff_attempt_id,
      handoff_attempt_no: sendingDelivery.handoff.handoff_attempt_no,
      lease_epoch: sendingDelivery.handoff.lease_epoch,
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
      allowed_sources: ['main_card_reply', 'card_action'],
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
    const after = readInteractionAuthority(database, accepted.turn_id);
    expect({ ...after, audits: after.audits.slice(0, -1) }).toEqual(before);
    expect(after.audits.at(-1)).toMatchObject({
      outcome: 'late_ack_ignored',
      handoff_id: delivery.handoff.handoff_id,
    });

    database.close();
  });
});
