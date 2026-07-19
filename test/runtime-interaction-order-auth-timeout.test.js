import { afterEach, describe, expect, test } from '@jest/globals';
import { createExecutorService } from '../runtime/executor/service.js';
import {
  acceptQueuedInteractionTurn,
  cleanupInteractionTestDatabases,
  createRunningInteractionTurn,
  deterministicIds,
  interactionAnswer as buildInteractionAnswer,
  openInteractionTestDatabase,
} from './helpers/runtime-interaction-fixtures.js';

function openTestDatabase() {
  return openInteractionTestDatabase('interaction-order');
}

function createRunningTurn(database, suffix = 'order') {
  return createRunningInteractionTurn(database, suffix);
}

function requestInteraction(store, turnContext, suffix, overrides = {}) {
  return store.requestInteraction(turnContext, {
    provider_interaction_ref: `provider-question-${suffix}`,
    tool_use_id: `tool-use-${suffix}`,
    kind: 'question',
    prompt: `Question ${suffix}?`,
    choices: [],
    authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
    allowed_sources: ['card_action', 'main_card_reply'],
    ...overrides,
  });
}

function interactionAnswer(request, suffix = 'answer', overrides = {}) {
  return buildInteractionAnswer(request, suffix, {
    value: { kind: 'text', text: `answer ${suffix}` },
    ...overrides,
  });
}

function mainCardReply(request, suffix = 'reply', overrides = {}) {
  const sourceEventId = `message-reply-${suffix}`;
  return interactionAnswer(request, suffix, {
    source_event_or_action_id: sourceEventId,
    source: 'main_card_reply',
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    },
    ...overrides,
  });
}

function insertBoundMapping(database, accepted, platformMessageId, suffix = 'main') {
  database.prepare(`
    INSERT INTO runtime_message_mappings (
      region, tenant_id, channel, bot_id, platform_message_id,
      conversation_id, turn_id, lineage_id, binding_state, reason,
      mapping_id, mapping_version, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'bound', NULL, ?, 1, ?)
  `).run(
    'cn',
    'tenant-A',
    'feishu',
    'bot-A',
    platformMessageId,
    accepted.conversation_id,
    accepted.turn_id,
    accepted.lineage_id,
    `mapping-${suffix}`,
    '2026-07-19T07:01:00Z',
  );
}

function readInteraction(database, interactionId) {
  const row = database.prepare(`
    SELECT state, version, handoff_state, request_json
    FROM runtime_interactions
    WHERE interaction_id = ?
  `).get(interactionId);
  return { ...row, request: JSON.parse(row.request_json) };
}

afterEach(() => {
  cleanupInteractionTestDatabases();
});

describe('runtime interaction order, authorization, and timeout', () => {
  test('rejects an answer before its smallest blocking ordinal without mutation', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database);
    const first = requestInteraction(store, turnContext, 'first');
    const second = requestInteraction(store, turnContext, 'second');
    const before = readInteraction(database, second.interaction_id);

    expect(store.commitInteractionAnswer(interactionAnswer(second, 'early'))).toMatchObject({
      status: 'conflict',
      interaction_state: 'pending',
      interaction_version: second.version,
      handoff_state: 'not_applicable',
      error: { code: 'interaction_out_of_order' },
    });
    expect(readInteraction(database, second.interaction_id)).toEqual(before);
    expect(readInteraction(database, first.interaction_id)).toMatchObject({
      state: 'pending',
      version: 1,
      handoff_state: 'not_started',
    });

    database.close();
  });

  test('requires the authenticated actor, allowed source, scope, and interaction version', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'authorization');
    const request = requestInteraction(store, turnContext, 'authorization', {
      allowed_sources: ['card_action'],
    });
    const before = readInteraction(database, request.interaction_id);

    const forbiddenActor = interactionAnswer(request, 'display-name-cannot-authorize', {
      actor: {
        type: 'user',
        actor_id: 'someone-named-user-123',
        authenticated: true,
        roles: ['member'],
      },
      value: { kind: 'text', text: 'I am user-123' },
    });
    expect(store.commitInteractionAnswer(forbiddenActor)).toMatchObject({
      status: 'rejected',
      error: { code: 'interaction_actor_forbidden' },
    });

    expect(store.commitInteractionAnswer(interactionAnswer(request, 'unauthenticated', {
      actor: {
        type: 'user',
        actor_id: 'user-123',
        authenticated: false,
        roles: ['member'],
      },
    }))).toMatchObject({
      status: 'rejected',
      interaction_state: 'pending',
      interaction_version: request.version,
      error: { code: 'validation_error' },
    });

    const disallowedSource = mainCardReply(request, 'disallowed-source');
    expect(store.commitInteractionAnswer(disallowedSource)).toMatchObject({
      status: 'rejected',
      error: { code: 'validation_error' },
    });

    for (const [field, value] of [
      ['region', 'global'],
      ['tenant_id', 'tenant-B'],
      ['channel', 'lark'],
      ['bot_id', 'bot-B'],
      ['chat_id', 'chat-dm-B'],
      ['native_thread_or_topic_id', 'thread-other'],
    ]) {
      const answer = interactionAnswer(request, `wrong-${field}`);
      answer.source_context = { ...answer.source_context, [field]: value };
      expect(store.commitInteractionAnswer(answer)).toMatchObject({
        status: 'rejected',
        error: { code: 'validation_error' },
      });
    }

    expect(store.commitInteractionAnswer(interactionAnswer(request, 'stale-version', {
      interaction_version: request.version + 1,
    }))).toMatchObject({
      status: 'conflict',
      error: { code: 'version_conflict' },
    });
    expect(readInteraction(database, request.interaction_id)).toEqual(before);

    database.close();
  });

  test('requires a main-card reply to carry a trusted bound mapping to the same turn', () => {
    const database = openTestDatabase();
    const {
      accepted,
      store,
      turnContext,
    } = createRunningTurn(database, 'reply-mapping');
    const request = requestInteraction(store, turnContext, 'reply-mapping', {
      allowed_sources: ['main_card_reply'],
    });
    const answer = mainCardReply(request, 'reply-mapping');
    const before = readInteraction(database, request.interaction_id);

    expect(store.commitInteractionAnswer(answer, {
      replyToMessageId: 'unmapped-main-card',
    })).toMatchObject({
      status: 'rejected',
      error: { code: 'mapping_missing' },
    });
    expect(readInteraction(database, request.interaction_id)).toEqual(before);

    const foreignTurn = acceptQueuedInteractionTurn(database, 'foreign-mapping', {
      acceptedAt: '2026-07-19T07:03:00Z',
    });
    insertBoundMapping(database, foreignTurn, 'foreign-main-card', 'foreign-turn');
    expect(store.commitInteractionAnswer(answer, {
      replyToMessageId: 'foreign-main-card',
    })).toMatchObject({
      status: 'rejected',
      error: { code: 'mapping_missing' },
    });
    expect(readInteraction(database, request.interaction_id)).toEqual(before);

    insertBoundMapping(database, accepted, 'mapped-main-card', 'reply-mapping');
    expect(store.commitInteractionAnswer(answer, {
      replyToMessageId: 'mapped-main-card',
    })).toMatchObject({
      status: 'accepted',
      interaction_id: request.interaction_id,
      answer_id: answer.answer_id,
    });

    database.close();
  });

  test('deduplicates the same answer and never lets a competing value overwrite it', () => {
    const database = openTestDatabase();
    const { store, turnContext } = createRunningTurn(database, 'answer-race');
    const request = requestInteraction(store, turnContext, 'answer-race', {
      allowed_sources: ['card_action'],
    });
    const firstAnswer = interactionAnswer(request, 'winner');
    const accepted = store.commitInteractionAnswer(firstAnswer);
    const countEvents = () => database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_normalized_events WHERE turn_id = ?
    `).get(request.turn_id).count;
    const eventCount = countEvents();

    const duplicate = store.commitInteractionAnswer({
      ...structuredClone(firstAnswer),
      trace_id: 'trace-answer-winner-retry',
    });
    expect(duplicate).toEqual({
      ...accepted,
      trace_id: 'trace-answer-winner-retry',
      status: 'duplicate',
    });
    expect(countEvents()).toBe(eventCount);

    const conflictingReplay = {
      ...structuredClone(firstAnswer),
      value: { kind: 'text', text: 'different value under the same key' },
    };
    expect(store.commitInteractionAnswer(conflictingReplay)).toMatchObject({
      status: 'conflict',
      error: { code: 'idempotency_conflict' },
    });

    const competingAnswer = interactionAnswer(request, 'loser');
    expect(store.commitInteractionAnswer(competingAnswer)).toMatchObject({
      status: 'conflict',
      error: { code: 'interaction_already_answered' },
    });

    const stored = database.prepare(`
      SELECT answer_json FROM runtime_interaction_answers WHERE interaction_id = ?
    `).get(request.interaction_id);
    expect(JSON.parse(stored.answer_json)).toEqual(firstAnswer);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_interaction_answers WHERE interaction_id = ?
    `).get(request.interaction_id).count).toBe(1);

    database.close();
  });

  test('expires only a due pending version and rejects a late answer explicitly', () => {
    const database = openTestDatabase();
    const { clock, store, turnContext } = createRunningTurn(database, 'timeout-wins');
    const request = requestInteraction(store, turnContext, 'timeout-wins');
    const sibling = requestInteraction(store, turnContext, 'timeout-sibling');

    expect(store.expireInteraction({
      interaction_id: request.interaction_id,
      interaction_version: request.version,
    })).toMatchObject({
      status: 'not_due',
      interaction_id: request.interaction_id,
      interaction_version: request.version,
    });

    clock.now = '2026-07-19T07:12:01Z';
    const expired = store.expireInteraction({
      interaction_id: request.interaction_id,
      interaction_version: request.version,
    });
    expect(expired).toMatchObject({
      status: 'expired',
      newly_expired: true,
      interaction_id: request.interaction_id,
      interaction_version: request.version + 1,
      turn_id: request.turn_id,
      turn_state: 'timed_out',
    });
    expect(readInteraction(database, request.interaction_id)).toMatchObject({
      state: 'expired',
      version: request.version + 1,
      handoff_state: 'not_started',
      request: {
        state: 'expired',
        version: request.version + 1,
      },
    });
    expect(readInteraction(database, sibling.interaction_id)).toMatchObject({
      state: 'cancelled',
      version: sibling.version + 1,
      handoff_state: 'not_started',
      request: {
        state: 'cancelled',
        version: sibling.version + 1,
        terminal_reason: 'parent_timed_out',
      },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(request.turn_id)).toEqual({ state: 'timed_out' });
    expect(database.prepare(`
      SELECT status FROM runtime_turn_queue WHERE turn_id = ?
    `).get(request.turn_id)).toEqual({ status: 'timed_out' });
    const events = database.prepare(`
      SELECT event_json FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 3
    `).all(request.turn_id).map(({ event_json: eventJson }) => JSON.parse(eventJson)).reverse();
    expect(events.map(({ kind, phase }) => ({ kind, phase }))).toEqual([
      { kind: 'interaction_expired', phase: 'waiting_user' },
      { kind: 'interaction_cancelled', phase: 'waiting_user' },
      { kind: 'turn_state_changed', phase: 'timed_out' },
    ]);
    expect(events[0]).toMatchObject({
      payload: {
        interaction_id: request.interaction_id,
        ordinal: request.ordinal,
        interaction_version: request.version + 1,
        handoff_version: null,
      },
      error: { code: 'interaction_expired', side_effect_status: 'none' },
    });
    expect(database.prepare(`
      SELECT terminal FROM runtime_projection_snapshots
      WHERE turn_id = ? ORDER BY aggregate_version DESC LIMIT 1
    `).get(request.turn_id)).toEqual({ terminal: 1 });

    expect(store.commitInteractionAnswer(interactionAnswer(request, 'late'))).toMatchObject({
      status: 'rejected',
      interaction_state: 'expired',
      interaction_version: request.version + 1,
      error: { code: 'interaction_expired' },
    });
    expect(store.commitInteractionAnswer(interactionAnswer(sibling, 'late-sibling'))).toMatchObject({
      status: 'conflict',
      interaction_state: 'cancelled',
      interaction_version: sibling.version + 1,
      error: { code: 'turn_terminal' },
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_interaction_answers WHERE interaction_id = ?
    `).get(request.interaction_id).count).toBe(0);

    database.close();
  });

  test('does not let a deadline overwrite an answer that committed first', () => {
    const database = openTestDatabase();
    const { clock, store, turnContext } = createRunningTurn(database, 'answer-wins');
    const request = requestInteraction(store, turnContext, 'answer-wins');
    const answer = interactionAnswer(request, 'answer-wins');
    const result = store.commitInteractionAnswer(answer);
    const before = readInteraction(database, request.interaction_id);
    const turnBefore = database.prepare(`
      SELECT state, turn_version FROM runtime_turns WHERE turn_id = ?
    `).get(request.turn_id);

    clock.now = '2026-07-19T07:12:01Z';
    expect(store.expireInteraction({
      interaction_id: request.interaction_id,
      interaction_version: request.version,
    })).toMatchObject({
      status: 'not_pending',
      interaction_id: request.interaction_id,
      interaction_state: 'answer_committed',
      interaction_version: result.interaction_version,
    });
    expect(readInteraction(database, request.interaction_id)).toEqual(before);
    expect(database.prepare(`
      SELECT state, turn_version FROM runtime_turns WHERE turn_id = ?
    `).get(request.turn_id)).toEqual(turnBefore);

    database.close();
  });

  test('does not let a later ordinal timeout strand an earlier committed handoff', () => {
    const database = openTestDatabase();
    const { clock, store, turnContext } = createRunningTurn(database, 'timeout-order-race');
    const first = requestInteraction(store, turnContext, 'timeout-order-first');
    const second = requestInteraction(store, turnContext, 'timeout-order-second');
    expect(store.listPendingInteractionDeadlines()).toEqual([{
      interaction_id: first.interaction_id,
      interaction_version: first.version,
      expires_at: first.expires_at,
    }]);
    const committed = store.commitInteractionAnswer(interactionAnswer(first, 'timeout-order'));
    expect(store.listPendingInteractionDeadlines()).toEqual([]);

    clock.now = '2026-07-19T07:12:01Z';
    expect(store.expireInteraction({
      interaction_id: second.interaction_id,
      interaction_version: second.version,
    })).toMatchObject({
      status: 'not_current',
      interaction_id: second.interaction_id,
      interaction_version: second.version,
      blocking_interaction_id: first.interaction_id,
    });
    expect(readInteraction(database, first.interaction_id)).toMatchObject({
      state: 'answer_committed',
      version: first.version + 1,
      handoff_state: 'pending',
    });
    expect(readInteraction(database, second.interaction_id)).toMatchObject({
      state: 'pending',
      version: second.version,
    });
    expect(database.prepare(`
      SELECT state FROM runtime_interaction_handoffs WHERE handoff_id = ?
    `).get(committed.handoff_id)).toEqual({ state: 'pending' });
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(second.turn_id)).toEqual({ state: 'waiting_user' });

    database.close();
  });

  test('rolls back interaction, terminal events, projection, and queue when timeout commit fails', () => {
    const database = openTestDatabase();
    const { clock, store, turnContext } = createRunningTurn(database, 'timeout-rollback');
    const request = requestInteraction(store, turnContext, 'timeout-rollback');
    const readAuthority = () => ({
      interaction: readInteraction(database, request.interaction_id),
      turn: database.prepare(`
        SELECT state, turn_version FROM runtime_turns WHERE turn_id = ?
      `).get(request.turn_id),
      queue: database.prepare(`
        SELECT status, wait_reason FROM runtime_turn_queue WHERE turn_id = ?
      `).get(request.turn_id),
      events: database.prepare(`
        SELECT event_json FROM runtime_normalized_events
        WHERE turn_id = ? ORDER BY event_sequence
      `).all(request.turn_id),
      projections: database.prepare(`
        SELECT aggregate_version, status, terminal, render_model_json
        FROM runtime_projection_snapshots
        WHERE turn_id = ? ORDER BY aggregate_version
      `).all(request.turn_id),
    });
    const before = readAuthority();
    database.exec(`
      CREATE TRIGGER force_timeout_queue_failure
      BEFORE UPDATE ON runtime_turn_queue
      WHEN NEW.status = 'timed_out'
      BEGIN
        SELECT RAISE(ABORT, 'forced timeout queue failure');
      END;
    `);

    clock.now = '2026-07-19T07:12:01Z';
    expect(() => store.expireInteraction({
      interaction_id: request.interaction_id,
      interaction_version: request.version,
    })).toThrow(/forced timeout queue failure/);
    expect(readAuthority()).toEqual(before);

    database.close();
  });

  test('executor service stops the suspended provider before releasing its timeout lease', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedInteractionTurn(database, 'service-timeout');
    const clock = { now: '2026-07-19T07:01:00Z' };
    let providerStopped = false;
    let interruptObservedProviderStopped;
    let deadlineCallback;
    let deadlineDelay;
    const adapter = {
      async *execute() {
        try {
          yield {
            kind: 'interaction_requested',
            payload: {
              provider_interaction_ref: 'provider-question-service-timeout',
              tool_use_id: 'tool-use-service-timeout',
              kind: 'question',
              prompt: 'Will this provider be stopped?',
              choices: [],
              authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
              allowed_sources: ['card_action'],
            },
          };
        } finally {
          providerStopped = true;
        }
      },
      async interrupt({ turn_id: turnId, attempt, reason }) {
        expect(turnId).toBe(accepted.turn_id);
        expect(attempt).toMatchObject({ attempt_id: expect.any(String), lease_epoch: 1 });
        expect(reason).toBe('timeout');
        interruptObservedProviderStopped = providerStopped;
        return { status: 'provider_stopped', reason, provider_status: 'interrupted' };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'executor-service-timeout',
      now: () => clock.now,
      generateId: deterministicIds('service-timeout'),
      interactionTimeoutMs: 1_000,
      setTimeoutFn: (callback, delay) => {
        deadlineCallback = callback;
        deadlineDelay = delay;
        return { unref() {} };
      },
      clearTimeoutFn: () => {},
    });
    const waiting = await service.runNext();
    expect(deadlineDelay).toBe(1_000);
    expect(deadlineCallback).toEqual(expect.any(Function));

    clock.now = '2026-07-19T07:01:02Z';
    await deadlineCallback();
    expect(interruptObservedProviderStopped).toBe(false);
    expect(providerStopped).toBe(true);
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ state: 'timed_out' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id, attempt_id, attempt_no, lease_expires_at
      FROM runtime_executor_leases WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: null,
      turn_id: null,
      attempt_id: null,
      attempt_no: null,
      lease_expires_at: null,
    });
    expect(service.snapshot().executors).toEqual([]);

    database.close();
  });

  test('executor service retains the timeout lease when app-server cannot confirm provider stop', async () => {
    const database = openTestDatabase();
    const accepted = acceptQueuedInteractionTurn(database, 'service-timeout-uncertain');
    const clock = { now: '2026-07-19T07:01:00Z' };
    let providerStopped = false;
    const adapter = {
      async *execute() {
        try {
          yield {
            kind: 'interaction_requested',
            payload: {
              provider_interaction_ref: 'provider-question-service-timeout-uncertain',
              tool_use_id: 'tool-use-service-timeout-uncertain',
              kind: 'question',
              prompt: 'Will the provider stop be confirmed?',
              choices: [],
              authorized_subjects: [{ type: 'actor', actor_id: 'user-123' }],
              allowed_sources: ['card_action'],
            },
          };
        } finally {
          providerStopped = true;
        }
      },
      async interrupt() {
        return { status: 'not_current', reason: 'timeout' };
      },
    };
    const service = createExecutorService({
      database,
      adapter,
      provider: 'codex',
      serviceInstanceId: 'executor-service-timeout-uncertain',
      now: () => clock.now,
      generateId: deterministicIds('service-timeout-uncertain'),
      interactionTimeoutMs: 1_000,
      setTimeoutFn: () => ({ unref() {} }),
      clearTimeoutFn: () => {},
    });
    const waiting = await service.runNext();
    clock.now = '2026-07-19T07:01:02Z';

    await expect(service.expireInteraction({
      interaction_id: waiting.request.interaction_id,
      interaction_version: waiting.request.version,
    })).resolves.toMatchObject({
      status: 'expired',
      turn_state: 'timed_out',
      lease_released: false,
      provider_stop_status: 'not_current',
    });
    expect(providerStopped).toBe(false);
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(accepted.conversation_id)).toEqual({
      lease_owner: 'executor-service-timeout-uncertain',
      turn_id: accepted.turn_id,
    });
    expect(database.prepare(`
      SELECT provider_stop_status, side_effect_status, disposition
      FROM runtime_provider_stop_incidents
      WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({
      provider_stop_status: 'not_current',
      side_effect_status: 'unknown',
      disposition: 'manual_recovery_required',
    });
    const notice = database.prepare(`
      SELECT command_json
      FROM runtime_outbox
      WHERE aggregate_type = 'text_notice' AND aggregate_id LIKE ?
    `).get(`${accepted.turn_id}-provider-stop-%`);
    expect(JSON.parse(notice.command_json)).toMatchObject({
      render_model: {
        phase: 'timed_out',
        terminal: true,
        user_action_required: true,
        error: {
          code: 'side_effect_unknown',
          side_effect_status: 'unknown',
        },
      },
    });

    database.close();
  });

  test('retains the timeout lease when this service cannot prove the writer stopped', async () => {
    const database = openTestDatabase();
    const { clock, store, turnContext } = createRunningTurn(database, 'timeout-no-writer');
    const request = requestInteraction(store, turnContext, 'timeout-no-writer');
    let deadlineCallback;
    let deadlineError;
    const restartedService = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'executor-service-timeout-no-writer',
      now: () => clock.now,
      generateId: deterministicIds('timeout-no-writer-restart'),
      setTimeoutFn: (callback) => {
        deadlineCallback = callback;
        return { unref() {} };
      },
      clearTimeoutFn: () => {},
      onDeadlineError: (error) => { deadlineError = error; },
    });
    restartedService.start();
    expect(deadlineCallback).toEqual(expect.any(Function));

    clock.now = '2026-07-19T07:12:01Z';
    await deadlineCallback();
    expect(deadlineError).toBeUndefined();
    expect(database.prepare(`
      SELECT state FROM runtime_turns WHERE turn_id = ?
    `).get(request.turn_id)).toEqual({ state: 'timed_out' });
    expect(database.prepare(`
      SELECT lease_owner, turn_id
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(request.conversation_id)).toEqual({
      lease_owner: 'executor-service-timeout-no-writer',
      turn_id: request.turn_id,
    });
    expect(database.prepare(`
      SELECT provider_stop_status, side_effect_status, disposition
      FROM runtime_provider_stop_incidents
      WHERE turn_id = ?
    `).get(request.turn_id)).toEqual({
      provider_stop_status: 'not_current',
      side_effect_status: 'unknown',
      disposition: 'manual_recovery_required',
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_outbox
      WHERE aggregate_type = 'text_notice' AND aggregate_id LIKE ?
    `).get(`${request.turn_id}-provider-stop-%`)).toEqual({ count: 1 });

    database.close();
  });
});
