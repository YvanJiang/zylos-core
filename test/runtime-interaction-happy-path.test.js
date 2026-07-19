import { afterEach, describe, expect, test } from '@jest/globals';

import {
  validateInteractionAnswerResult,
  validateInteractionHandoff,
  validateInteractionRequest,
  validateNormalizedEvent,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import {
  acceptQueuedInteractionTurn,
  cleanupInteractionTestDatabases,
  createRunningInteractionTurn,
  deterministicIds,
  interactionAnswer,
  openInteractionTestDatabase,
} from './helpers/runtime-interaction-fixtures.js';

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
      allowed_sources: ['main_card_reply', 'card_action'],
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
      async handleInteractionAnswer() {
        throw new Error('uncertain handler send');
      },
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
      async handleInteractionAnswer() {
        throw new Error('uncertain retrying send');
      },
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
      async handleInteractionAnswer() {
        handlerStarted();
        await handlerReleasePromise;
        throw new Error('deferred uncertain handler send');
      },
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
      async handleInteractionAnswer(delivery) {
        return {
          status: 'accepted',
          handoff_id: delivery.handoff.handoff_id,
          provider_attempt_id: delivery.handoff.provider_attempt_id,
          handoff_attempt_id: delivery.handoff.handoff_attempt_id,
          handoff_attempt_no: delivery.handoff.handoff_attempt_no,
          lease_epoch: delivery.handoff.lease_epoch,
        };
      },
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
      async handleInteractionAnswer(delivery) {
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
    expect(readInteractionAuthority(database, accepted.turn_id)).toEqual(before);

    database.close();
  });
});
