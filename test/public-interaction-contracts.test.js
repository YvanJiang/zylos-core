import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  ContractKernelError,
  createIdempotencyKey,
  INTERACTION_ANSWER_SCHEMA_V1,
  INTERACTION_ANSWER_RESULT_SCHEMA_V1,
  INTERACTION_REQUEST_SCHEMA_V1,
  INTERACTION_HANDOFF_SCHEMA_V1,
  INTERACTION_HANDOFF_TRANSITIONS_V1,
  INTERACTION_TRANSITIONS_V1,
  validateInteractionAnswer,
  validateInteractionAnswerAgainstRequest,
  validateInteractionAnswerResult,
  validateInteractionAnswerResultReplay,
  validateInteractionHandoff,
  validateInteractionHandoffTransition,
  validateInteractionRequest,
  validateInteractionRequestSequence,
  validateInteractionTransition,
  validatePublicFixtureSafety,
} from '../contracts/public/index.js';

const CREATED_AT = '2026-07-19T05:00:00Z';
const EXPIRES_AT = '2026-07-19T05:10:00Z';
const REQUEST_SCOPE = Object.freeze({
  region: 'cn',
  tenant_id: 'tenant-A',
  channel: 'feishu',
  bot_id: 'bot-A',
  chat_id: 'chat-A',
  native_thread_or_topic_id: null,
});

function providerRequest(overrides = {}) {
  return {
    contract: 'zylos.interaction-request',
    contract_version: '1.0',
    trace_id: 'trace-provider-question',
    interaction_id: 'interaction-provider-1',
    conversation_id: 'conversation-A',
    turn_id: 'turn-A',
    lineage_id: 'lineage-A',
    control_id: null,
    parent_type: 'provider_turn',
    tool_use_id: 'tool-use-A',
    ordinal: 1,
    kind: 'tool_approval',
    prompt: 'Allow the requested workspace write?',
    choices: [],
    authorized_subjects: [{ type: 'actor', actor_id: 'user-A' }],
    allowed_sources: ['main_card_reply', 'card_action'],
    runtime_fence: {
      provider_attempt_id: 'provider-attempt-A',
      lease_epoch: 7,
      provider_interaction_ref: 'provider-interaction-A',
    },
    state: 'pending',
    version: 1,
    handoff_state: 'not_started',
    created_at: CREATED_AT,
    expires_at: EXPIRES_AT,
    card_delivery_id: null,
    ...overrides,
  };
}

function answer(source, sourceEventId, overrides = {}) {
  const interactionId = overrides.interaction_id ?? 'interaction-provider-1';
  const sourceContext = source === 'operations_control'
    ? {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'operations',
      bot_id: 'bot-A',
      chat_id: null,
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    }
    : {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceEventId,
    };
  return {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-${sourceEventId}`,
    interaction_id: interactionId,
    interaction_version: 1,
    answer_id: `answer-${sourceEventId}`,
    source_event_or_action_id: sourceEventId,
    actor: source === 'operations_control'
      ? { type: 'service', actor_id: 'dashboard-operator-A', authenticated: true, roles: [] }
      : { type: 'user', actor_id: 'user-A', authenticated: true, roles: ['member'] },
    source_context: sourceContext,
    source,
    value: { kind: 'decision', decision: 'approve' },
    answered_at: '2026-07-19T05:04:00Z',
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: interactionId,
      source_event_or_action_id: sourceEventId,
    }),
    ...overrides,
  };
}

function answerResult(overrides = {}) {
  return {
    contract: 'zylos.interaction-answer-result',
    contract_version: '1.0',
    trace_id: 'trace-answer-result-A',
    interaction_id: 'interaction-provider-1',
    answer_id: 'answer-card-action-A',
    idempotency_key: createIdempotencyKey('interaction', {
      interaction_id: 'interaction-provider-1',
      source_event_or_action_id: 'card-action-A',
    }),
    status: 'accepted',
    interaction_state: 'answer_committed',
    interaction_version: 2,
    handoff_state: 'pending',
    handoff_id: 'handoff-A',
    turn_id: 'turn-A',
    turn_version: 6,
    control_id: null,
    error: null,
    received_at: '2026-07-19T05:04:01Z',
    committed_at: '2026-07-19T05:04:01Z',
    ...overrides,
  };
}

function handoff(overrides = {}) {
  return {
    handoff_id: 'handoff-A',
    interaction_id: 'interaction-provider-1',
    answer_id: 'answer-card-action-A',
    parent_type: 'provider_turn',
    state: 'pending',
    provider_attempt_id: 'provider-attempt-A',
    handoff_attempt_id: null,
    handoff_attempt_no: null,
    lease_epoch: 7,
    claimed_by: null,
    claimed_at: null,
    last_send_started_at: null,
    provider_acked_at: null,
    handoff_deadline_at: '2026-07-19T05:05:00Z',
    reason_code: null,
    error: null,
    side_effect_status: 'none',
    ...overrides,
  };
}

function loadInteractionFixture() {
  return JSON.parse(readFileSync(
    new URL('../contracts/public/fixtures/interaction-handoff-v1.json', import.meta.url),
    'utf8',
  ));
}

describe('interaction request v1 public contract', () => {
  test('validates provider-turn identity, ordering, authorization, sources, and runtime fencing', () => {
    const request = providerRequest({ future_optional: { display_hint: 'compact' } });

    expect(INTERACTION_REQUEST_SCHEMA_V1.requiredFields).toContain('runtime_fence');
    expect(validateInteractionRequest(request)).toEqual({
      header: {
        contract: 'zylos.interaction-request',
        contractVersion: '1.0',
        major: 1,
        minor: 0,
      },
      known: expect.objectContaining({
        interaction_id: 'interaction-provider-1',
        parent_type: 'provider_turn',
        ordinal: 1,
        authorized_subjects: [{ type: 'actor', actor_id: 'user-A' }],
        allowed_sources: ['main_card_reply', 'card_action'],
        runtime_fence: {
          provider_attempt_id: 'provider-attempt-A',
          lease_epoch: 7,
          provider_interaction_ref: 'provider-interaction-A',
        },
      }),
      extensions: { future_optional: { display_hint: 'compact' } },
      forwarded: request,
    });
  });

  test('rejects a missing provider fence or a non-positive ordinal', () => {
    expect(() => validateInteractionRequest(providerRequest({ runtime_fence: null })))
      .toThrow(ContractKernelError);
    expect(() => validateInteractionRequest(providerRequest({ ordinal: 0 })))
      .toThrow(ContractKernelError);
    expect(() => validateInteractionRequest(providerRequest({
      state: 'answered',
      handoff_state: 'pending',
    }))).toThrow(ContractKernelError);
  });

  test('requires unique, contiguous ordinals within the same parent', () => {
    const first = providerRequest();
    const second = providerRequest({
      interaction_id: 'interaction-provider-2',
      tool_use_id: 'tool-use-B',
      ordinal: 2,
    });

    expect(validateInteractionRequestSequence([first, second])).toBe(true);
    expect(() => validateInteractionRequestSequence([
      first,
      { ...second, ordinal: 1 },
    ])).toThrow(ContractKernelError);
    expect(() => validateInteractionRequestSequence([
      first,
      { ...second, ordinal: 3 },
    ])).toThrow(ContractKernelError);
  });

  test('validates security and recovery control parents with capability subjects', () => {
    const securityControl = providerRequest({
      trace_id: 'trace-security-control',
      interaction_id: 'interaction-security-1',
      turn_id: null,
      lineage_id: null,
      control_id: 'control-security-1',
      parent_type: 'security_control',
      tool_use_id: null,
      kind: 'permission_approval',
      prompt: 'Enable trusted mode for this bot?',
      authorized_subjects: [{
        type: 'capability',
        capability: 'permission.bot.manage',
        scope: {
          scope_type: 'bot',
          region: 'cn',
          tenant_id: 'tenant-A',
          bot_id: 'bot-A',
          conversation_id: null,
          service_instance_id: null,
          recovery_id: null,
        },
      }],
      allowed_sources: ['card_action', 'magic_command_repeat'],
      runtime_fence: null,
    });
    const recoveryControl = providerRequest({
      trace_id: 'trace-recovery-control',
      interaction_id: 'interaction-recovery-1',
      lineage_id: null,
      control_id: 'control-recovery-1',
      parent_type: 'recovery_control',
      tool_use_id: null,
      kind: 'recovery_decision',
      prompt: 'Continue recovery with a new lineage?',
      authorized_subjects: [{
        type: 'capability',
        capability: 'recovery.decide',
        scope: {
          scope_type: 'recovery',
          region: 'cn',
          tenant_id: 'tenant-A',
          bot_id: 'bot-A',
          conversation_id: 'conversation-A',
          service_instance_id: null,
          recovery_id: 'recovery-A',
        },
      }],
      allowed_sources: ['operations_control'],
      runtime_fence: null,
    });

    expect(validateInteractionRequest(securityControl).known.parent_type)
      .toBe('security_control');
    expect(validateInteractionRequest(recoveryControl).known).toEqual(expect.objectContaining({
      parent_type: 'recovery_control',
      turn_id: 'turn-A',
      lineage_id: null,
      ordinal: 1,
    }));
  });
});

describe('interaction answer v1 public contract', () => {
  test('validates every allowed source and the provider-neutral answer value variants', () => {
    const answers = [
      answer('main_card_reply', 'message-reply-A', {
        value: { kind: 'text', text: 'Use the safer path.' },
      }),
      answer('card_action', 'card-action-A', {
        value: { kind: 'choice', choice_id: 'choice-safe' },
      }),
      answer('magic_command_repeat', 'inbound-event-repeat-A'),
      answer('operations_control', 'operations-action-A', {
        value: { kind: 'decision', decision: 'deny' },
        future_optional: { operator_ui: 'dashboard' },
      }),
    ];

    expect(INTERACTION_ANSWER_SCHEMA_V1.allowedSources).toEqual([
      'main_card_reply',
      'card_action',
      'magic_command_repeat',
      'operations_control',
    ]);
    for (const candidate of answers) {
      expect(validateInteractionAnswer(candidate).known).toEqual(expect.objectContaining({
        interaction_id: 'interaction-provider-1',
        source: candidate.source,
        value: candidate.value,
      }));
    }
    expect(validateInteractionAnswer(answers[3]).extensions)
      .toEqual({ future_optional: { operator_ui: 'dashboard' } });
  });

  test('rejects an unknown source, unauthenticated actor, or mismatched idempotency key', () => {
    expect(() => validateInteractionAnswer(answer('quoted_reply', 'quoted-A')))
      .toThrow(ContractKernelError);
    expect(() => validateInteractionAnswer(answer('card_action', 'action-unauthenticated', {
      actor: { type: 'user', actor_id: 'user-A', authenticated: false, roles: ['member'] },
    }))).toThrow(ContractKernelError);
    expect(() => validateInteractionAnswer(answer('card_action', 'action-wrong-key', {
      idempotency_key: createIdempotencyKey('interaction', {
        interaction_id: 'interaction-provider-1',
        source_event_or_action_id: 'different-action',
      }),
    }))).toThrow(ContractKernelError);
    expect(() => validateInteractionAnswer(answer('card_action', 'action-context-mismatch', {
      source_context: {
        region: 'cn',
        tenant_id: 'tenant-A',
        channel: 'feishu',
        bot_id: 'bot-A',
        chat_id: 'chat-A',
        native_thread_or_topic_id: null,
        platform_message_or_action_id: 'different-action',
      },
    }))).toThrow(ContractKernelError);
  });

  test('enforces request sources and accepts only the smallest blocking ordinal', () => {
    const first = providerRequest();
    const second = providerRequest({
      interaction_id: 'interaction-provider-2',
      tool_use_id: 'tool-use-B',
      ordinal: 2,
    });
    const secondAnswer = answer('main_card_reply', 'message-reply-B', {
      interaction_id: 'interaction-provider-2',
    });
    const prohibitedProviderCommand = answer(
      'magic_command_repeat',
      'provider-command-repeat-A',
    );

    expect(() => validateInteractionAnswerAgainstRequest(
      prohibitedProviderCommand,
      first,
      { interactions: [first, second], requestScope: REQUEST_SCOPE },
    )).toThrow(ContractKernelError);
    expect(() => validateInteractionAnswerAgainstRequest(
      secondAnswer,
      second,
      { interactions: [first, second], requestScope: REQUEST_SCOPE },
    )).toThrow(expect.objectContaining({
      contractError: expect.objectContaining({ code: 'interaction_out_of_order' }),
    }));

    const unauthorizedAnswer = answer('card_action', 'unauthorized-action-A', {
      actor: { type: 'user', actor_id: 'user-B', authenticated: true, roles: ['member'] },
    });
    expect(() => validateInteractionAnswerAgainstRequest(
      unauthorizedAnswer,
      first,
      { interactions: [first, second], requestScope: REQUEST_SCOPE },
    )).toThrow(expect.objectContaining({
      contractError: expect.objectContaining({ code: 'interaction_actor_forbidden' }),
    }));

    const wrongChatAnswer = answer('card_action', 'wrong-chat-action-A', {
      source_context: {
        ...answer('card_action', 'wrong-chat-action-A').source_context,
        chat_id: 'chat-B',
      },
    });
    expect(() => validateInteractionAnswerAgainstRequest(
      wrongChatAnswer,
      first,
      { interactions: [first, second], requestScope: REQUEST_SCOPE },
    )).toThrow(ContractKernelError);

    const firstAnswered = {
      ...first,
      state: 'answered',
      handoff_state: 'accepted',
      version: 2,
    };
    expect(validateInteractionAnswerAgainstRequest(
      secondAnswer,
      second,
      { interactions: [firstAnswered, second], requestScope: REQUEST_SCOPE },
    )).toBe(true);

    const firstAnswer = answer('main_card_reply', 'message-reply-current-A');
    expect(validateInteractionAnswerAgainstRequest(
      firstAnswer,
      first,
      {
        interactions: [firstAnswered, second],
        requestScope: REQUEST_SCOPE,
      },
    )).toBe(true);
  });
});

describe('interaction answer result v1 public contract', () => {
  test('distinguishes durable accepted and duplicate results from provider acknowledgement', () => {
    const accepted = answerResult();
    const duplicate = answerResult({
      trace_id: 'trace-answer-result-replay',
      status: 'duplicate',
    });

    expect(INTERACTION_ANSWER_RESULT_SCHEMA_V1.statuses).toEqual([
      'accepted',
      'duplicate',
      'rejected',
      'conflict',
    ]);
    expect(validateInteractionAnswerResult(accepted).known).toEqual(expect.objectContaining({
      status: 'accepted',
      interaction_state: 'answer_committed',
      handoff_state: 'pending',
      handoff_id: 'handoff-A',
    }));
    expect(validateInteractionAnswerResult(duplicate).known.status).toBe('duplicate');
    expect(validateInteractionAnswerResultReplay(accepted, duplicate)).toBe(true);
  });

  test('validates rejected/conflict nullability and preserves the first immutable business result', () => {
    const rejected = answerResult({
      status: 'rejected',
      interaction_state: 'pending',
      interaction_version: 1,
      handoff_state: 'not_applicable',
      handoff_id: null,
      turn_version: 5,
      error: {
        code: 'interaction_actor_forbidden',
        category: 'authorization',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'This actor is not authorized to answer the interaction.',
        occurred_at: '2026-07-19T05:04:02Z',
      },
      received_at: null,
      committed_at: null,
    });
    const conflict = answerResult({
      status: 'conflict',
      handoff_state: 'not_applicable',
      handoff_id: null,
      error: {
        code: 'idempotency_conflict',
        category: 'conflict',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'The answer key was already used with different content.',
        occurred_at: '2026-07-19T05:04:03Z',
      },
      received_at: null,
      committed_at: null,
    });

    expect(validateInteractionAnswerResult(rejected).known.status).toBe('rejected');
    expect(validateInteractionAnswerResult(conflict).known.status).toBe('conflict');
    expect(() => validateInteractionAnswerResult(answerResult({
      interaction_state: 'answered',
      handoff_state: 'accepted',
    }))).toThrow(ContractKernelError);
    expect(() => validateInteractionAnswerResultReplay(answerResult(), answerResult({
      trace_id: 'trace-replay-mutated',
      status: 'duplicate',
      committed_at: '2026-07-19T05:04:09Z',
    }))).toThrow(ContractKernelError);
  });
});

describe('durable interaction handoff v1 record contract', () => {
  test('validates pending, acknowledged, rejected, and cancelled lifecycle records', () => {
    const records = [
      handoff(),
      handoff({
        state: 'accepted',
        handoff_attempt_id: 'handoff-attempt-1',
        handoff_attempt_no: 1,
        claimed_by: 'executor-A',
        claimed_at: '2026-07-19T05:04:02Z',
        last_send_started_at: '2026-07-19T05:04:03Z',
        provider_acked_at: '2026-07-19T05:04:04Z',
        reason_code: 'handler_accepted',
        side_effect_status: 'known',
      }),
      handoff({
        state: 'rejected',
        handoff_attempt_id: 'handoff-attempt-1',
        handoff_attempt_no: 1,
        claimed_by: 'executor-A',
        claimed_at: '2026-07-19T05:04:02Z',
        last_send_started_at: '2026-07-19T05:04:03Z',
        provider_acked_at: '2026-07-19T05:04:04Z',
        reason_code: 'provider_no_longer_waiting',
        error: {
          code: 'provider_context_invalid',
          category: 'provider',
          retryable: false,
          side_effect_status: 'known',
          user_message: 'The provider no longer accepts this interaction answer.',
          occurred_at: '2026-07-19T05:04:04Z',
        },
        side_effect_status: 'known',
      }),
      handoff({ state: 'cancelled', reason_code: 'parent_stopped' }),
    ];

    expect(INTERACTION_HANDOFF_SCHEMA_V1.states).toEqual([
      'pending',
      'delivering',
      'retry_wait',
      'accepted',
      'delivery_unknown',
      'rejected',
      'cancelled',
    ]);
    expect(records.map((record) => validateInteractionHandoff(record).state)).toEqual([
      'pending',
      'accepted',
      'rejected',
      'cancelled',
    ]);
  });

  test('requires send evidence and unknown side effects when provider acknowledgement is unknown', () => {
    const unknown = handoff({
      state: 'delivery_unknown',
      handoff_attempt_id: 'handoff-attempt-1',
      handoff_attempt_no: 1,
      claimed_by: 'executor-A',
      claimed_at: '2026-07-19T05:04:02Z',
      last_send_started_at: '2026-07-19T05:04:03Z',
      reason_code: 'send_started_ack_missing',
      error: {
        code: 'interaction_answer_delivery_unknown',
        category: 'provider',
        retryable: false,
        side_effect_status: 'unknown',
        user_message: 'The answer may have reached the provider, but acknowledgement is unknown.',
        occurred_at: '2026-07-19T05:04:05Z',
      },
      side_effect_status: 'unknown',
    });

    expect(validateInteractionHandoff(unknown).state).toBe('delivery_unknown');
    expect(() => validateInteractionHandoff({ ...unknown, last_send_started_at: null }))
      .toThrow(ContractKernelError);
    expect(() => validateInteractionHandoff({ ...unknown, side_effect_status: 'none' }))
      .toThrow(ContractKernelError);
  });

  test('only permits retry_wait before answer delivery starts', () => {
    const retryWait = handoff({
      state: 'retry_wait',
      handoff_attempt_id: 'handoff-attempt-1',
      handoff_attempt_no: 1,
      claimed_by: 'executor-A',
      claimed_at: '2026-07-19T05:04:02Z',
      reason_code: 'pre_send_transient_failure',
      error: {
        code: 'provider_context_invalid',
        category: 'provider',
        retryable: true,
        side_effect_status: 'none',
        user_message: 'The provider handler was unavailable before answer delivery started.',
        occurred_at: '2026-07-19T05:04:03Z',
      },
    });

    expect(validateInteractionHandoff(retryWait).state).toBe('retry_wait');
    expect(() => validateInteractionHandoff({
      ...retryWait,
      last_send_started_at: '2026-07-19T05:04:03Z',
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionHandoff({
      ...retryWait,
      error: { ...retryWait.error, side_effect_status: 'unknown' },
      side_effect_status: 'unknown',
    })).toThrow(ContractKernelError);
  });

  test('requires null provider fencing for security and recovery control handoffs', () => {
    const controlHandoff = handoff({
      parent_type: 'recovery_control',
      provider_attempt_id: null,
      lease_epoch: null,
    });

    expect(validateInteractionHandoff(controlHandoff).parent_type).toBe('recovery_control');
    expect(() => validateInteractionHandoff({
      ...controlHandoff,
      provider_attempt_id: 'stale-provider-attempt',
    })).toThrow(ContractKernelError);
  });
});

describe('interaction and handoff transition contracts', () => {
  const interactionAllowed = new Set([
    'pending->answer_committed',
    'pending->expired',
    'pending->cancelled',
    'answer_committed->answer_delivering',
    'answer_committed->cancelled',
    'answer_delivering->answered',
    'answer_delivering->rejected',
    'answer_delivering->delivery_unknown',
    'answer_delivering->answer_committed',
    'answer_delivering->cancelled',
    'delivery_unknown->answered',
    'delivery_unknown->rejected',
    'delivery_unknown->cancelled',
  ]);
  const handoffAllowed = new Set([
    'pending->delivering',
    'pending->cancelled',
    'delivering->accepted',
    'delivering->rejected',
    'delivering->retry_wait',
    'delivering->delivery_unknown',
    'delivering->cancelled',
    'retry_wait->delivering',
    'retry_wait->cancelled',
    'delivery_unknown->accepted',
    'delivery_unknown->rejected',
    'delivery_unknown->cancelled',
  ]);

  test('enumerates every allowed and prohibited interaction transition', () => {
    for (const from of INTERACTION_REQUEST_SCHEMA_V1.states) {
      for (const to of INTERACTION_REQUEST_SCHEMA_V1.states) {
        const edge = `${from}->${to}`;
        const transition = {
          from,
          to,
          sendStarted: [
            'answer_delivering->answered',
            'answer_delivering->rejected',
            'answer_delivering->delivery_unknown',
          ].includes(edge),
          acknowledgementProven: edge === 'delivery_unknown->answered',
        };
        if (interactionAllowed.has(edge)) {
          expect(validateInteractionTransition(transition)).toBe(true);
        } else {
          expect(() => validateInteractionTransition(transition)).toThrow(ContractKernelError);
        }
      }
    }
  });

  test('enforces send-before-ack, delivery-unknown proof, cancel, and late-ack guards', () => {
    expect(() => validateInteractionTransition({
      from: 'answer_delivering',
      to: 'answer_committed',
      sendStarted: true,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionTransition({
      from: 'answer_delivering',
      to: 'cancelled',
      sendStarted: true,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionTransition({
      from: 'answer_delivering',
      to: 'delivery_unknown',
      sendStarted: false,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionTransition({
      from: 'answer_delivering',
      to: 'answered',
      sendStarted: false,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionTransition({
      from: 'delivery_unknown',
      to: 'answered',
      acknowledgementProven: false,
    })).toThrow(ContractKernelError);
    for (const terminal of ['answered', 'rejected', 'expired', 'cancelled']) {
      expect(() => validateInteractionTransition({
        from: terminal,
        to: 'answered',
        acknowledgementProven: true,
      })).toThrow(ContractKernelError);
    }
  });

  test('enumerates every allowed and prohibited durable handoff transition', () => {
    for (const from of INTERACTION_HANDOFF_SCHEMA_V1.states) {
      for (const to of INTERACTION_HANDOFF_SCHEMA_V1.states) {
        const edge = `${from}->${to}`;
        const transition = {
          from,
          to,
          sendStarted: [
            'delivering->accepted',
            'delivering->rejected',
            'delivering->delivery_unknown',
          ].includes(edge),
          safeToRetry: edge === 'delivering->retry_wait',
          acknowledgementProven: edge === 'delivery_unknown->accepted',
        };
        if (handoffAllowed.has(edge)) {
          expect(validateInteractionHandoffTransition(transition)).toBe(true);
        } else {
          expect(() => validateInteractionHandoffTransition(transition))
            .toThrow(ContractKernelError);
        }
      }
    }
  });

  test('does not retry or cancel after send, infer missing sends, or apply late handoff ack', () => {
    expect(() => validateInteractionHandoffTransition({
      from: 'delivering',
      to: 'retry_wait',
      safeToRetry: false,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionHandoffTransition({
      from: 'delivering',
      to: 'retry_wait',
      sendStarted: true,
      safeToRetry: true,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionHandoffTransition({
      from: 'delivering',
      to: 'cancelled',
      sendStarted: true,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionHandoffTransition({
      from: 'delivering',
      to: 'delivery_unknown',
      sendStarted: false,
    })).toThrow(ContractKernelError);
    expect(() => validateInteractionHandoffTransition({
      from: 'delivery_unknown',
      to: 'accepted',
      acknowledgementProven: false,
    })).toThrow(ContractKernelError);
    for (const terminal of INTERACTION_HANDOFF_SCHEMA_V1.terminalStates) {
      expect(() => validateInteractionHandoffTransition({
        from: terminal,
        to: 'accepted',
        acknowledgementProven: true,
      })).toThrow(ContractKernelError);
    }
  });
});

describe('shared interaction and durable handoff v1 fixtures', () => {
  test('publishes every parent, source, answer-result status, and handoff state safely', () => {
    const fixture = loadInteractionFixture();

    expect(fixture.fixture_version).toBe('1.0');
    expect(validatePublicFixtureSafety(fixture)).toBe(true);
    expect(fixture.requests.map(({ name }) => name)).toEqual([
      'provider_turn',
      'provider_turn_ordinal_2',
      'security_control',
      'recovery_control',
    ]);
    for (const { document } of fixture.requests) validateInteractionRequest(document);
    expect(validateInteractionRequestSequence(
      fixture.requests.map(({ document }) => document),
    )).toBe(true);
    for (const { document } of fixture.answers) validateInteractionAnswer(document);
    for (const { document } of fixture.answer_results) validateInteractionAnswerResult(document);
    for (const { record } of fixture.handoffs) validateInteractionHandoff(record);

    expect(new Set(fixture.requests.map(({ document }) => document.parent_type)))
      .toEqual(new Set(INTERACTION_REQUEST_SCHEMA_V1.parentTypes));
    expect(new Set(fixture.answers.map(({ document }) => document.source)))
      .toEqual(new Set(INTERACTION_ANSWER_SCHEMA_V1.allowedSources));
    expect(new Set(fixture.answer_results.map(({ document }) => document.status)))
      .toEqual(new Set(INTERACTION_ANSWER_RESULT_SCHEMA_V1.statuses));
    expect(new Set(fixture.handoffs.map(({ record }) => record.state)))
      .toEqual(new Set(INTERACTION_HANDOFF_SCHEMA_V1.states));
  });

  test('publishes executable request-source and ordered-question adjudication examples', () => {
    const fixture = loadInteractionFixture();
    const requestByName = Object.fromEntries(
      fixture.requests.map(({ name, document }) => [name, document]),
    );
    const answerByName = Object.fromEntries(
      fixture.answers.map(({ name, document }) => [name, document]),
    );

    expect(fixture.answer_validation_examples.map(({ name }) => name)).toEqual([
      'provider_reply_current_ordinal',
      'provider_magic_command_prohibited',
      'unauthorized_actor_prohibited',
      'wrong_chat_prohibited',
      'later_ordinal_blocked',
      'later_ordinal_after_first_answer',
      'permission_command_repeat',
      'recovery_operations_control',
    ]);
    for (const example of fixture.answer_validation_examples) {
      const interactions = example.interactions.map((name) => ({
        ...requestByName[name],
        ...(example.interaction_overrides?.[name] ?? {}),
      }));
      const request = interactions.find(
        ({ interaction_id: interactionId }) => (
          interactionId === requestByName[example.request].interaction_id
        ),
      );
      const validate = () => validateInteractionAnswerAgainstRequest(
        answerByName[example.answer],
        request,
        {
          interactions,
          requestScope: fixture.request_scopes[example.request],
          actorCapabilities: example.actor_capabilities ?? [],
        },
      );
      if (example.expected === 'allowed') {
        expect(validate()).toBe(true);
      } else {
        expect(validate).toThrow(expect.objectContaining({
          contractError: expect.objectContaining({ code: example.error_code }),
        }));
      }
    }
  });

  test('keeps duplicate business results immutable and transition rules authoritative', () => {
    const fixture = loadInteractionFixture();
    const resultByName = Object.fromEntries(
      fixture.answer_results.map(({ name, document }) => [name, document]),
    );

    expect(validateInteractionAnswerResultReplay(
      resultByName.accepted,
      resultByName.duplicate,
    )).toBe(true);
    expect(fixture.transition_rules.interaction).toEqual(INTERACTION_TRANSITIONS_V1);
    expect(fixture.transition_rules.handoff).toEqual(INTERACTION_HANDOFF_TRANSITIONS_V1);
  });

  test('keeps the shared interaction idempotency golden vector valid under the answer schema', () => {
    const idempotencyFixture = JSON.parse(readFileSync(
      new URL('../contracts/public/fixtures/idempotency-v1.json', import.meta.url),
      'utf8',
    ));
    const interactionVector = idempotencyFixture.vectors.find(
      ({ scope }) => scope === 'interaction',
    );

    expect(validateInteractionAnswer(interactionVector.payload).known).toEqual(
      expect.objectContaining({
        interaction_id: 'interaction-A',
        interaction_version: 1,
        source: 'card_action',
      }),
    );
  });

  test('contains explicit send-before-ack, unknown delivery, cancel/reject, and late-ack examples', () => {
    const fixture = loadInteractionFixture();

    expect(fixture.transition_examples.map(({ name }) => name)).toEqual(expect.arrayContaining([
      'send_before_ack_crash',
      'unknown_ack_proven',
      'post_send_cancel_prohibited',
      'post_send_retry_prohibited',
      'provider_rejected',
      'late_ack_after_cancel_prohibited',
    ]));
    for (const example of fixture.transition_examples) {
      const validate = example.machine === 'interaction'
        ? validateInteractionTransition
        : validateInteractionHandoffTransition;
      if (example.expected === 'allowed') {
        expect(validate(example.transition)).toBe(true);
      } else {
        expect(() => validate(example.transition)).toThrow(ContractKernelError);
      }
    }
  });
});
