import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  ContractKernelError,
  createIdempotencyKey,
  DELIVERY_COMMAND_VERSIONS,
  DELIVERY_OPERATIONS,
  DELIVERY_RESULT_STATUSES,
  DELIVERY_TARGET_FIELDS_V1_0,
  DELIVERY_TARGET_FIELDS_V1_1,
  MAPPING_BINDING_AUTHORITIES,
  MAPPING_BINDING_STATES,
  MAPPING_RECOVERY_REASONS,
  resolveProvisionalMappingBinding,
  validateDeliveryCommand,
  validateDeliveryMapping,
  validateDeliveryResult,
  validatePublicFixtureSafety,
} from '../contracts/public/index.js';

describe('delivery/mapping v1 vocabulary', () => {
  test('publishes every safety-critical operation, result and binding state', () => {
    expect(DELIVERY_COMMAND_VERSIONS).toEqual(['1.0', '1.1']);
    expect(DELIVERY_TARGET_FIELDS_V1_0).toEqual([
      'region',
      'tenant_id',
      'channel',
      'bot_id',
      'chat_type',
      'chat_id',
      'native_thread_or_topic_id',
    ]);
    expect(DELIVERY_TARGET_FIELDS_V1_1).toEqual([
      ...DELIVERY_TARGET_FIELDS_V1_0,
      'native_thread_root_message_id',
      'native_thread_reply_target_message_id',
    ]);
    expect(DELIVERY_OPERATIONS).toEqual([
      'create_main',
      'update_main',
      'send_text',
      'send_fallback',
    ]);
    expect(DELIVERY_RESULT_STATUSES).toEqual([
      'delivered',
      'retryable_failure',
      'permanent_failure',
      'obsolete',
    ]);
    expect(MAPPING_BINDING_AUTHORITIES).toEqual(['core', 'channel']);
    expect(MAPPING_BINDING_STATES).toEqual(['pending', 'bound', 'not_applicable']);
    expect(MAPPING_RECOVERY_REASONS).toEqual([
      'mapping_missing',
      'mapping_corrupt',
      'mapping_unbound',
      'provider_lineage_invalid',
    ]);
  });
});

describe('provisional mapping binding v1', () => {
  test('allows only Core null-to-one binding, same-value replay and immutable bound state', () => {
    const pending = {
      mapping_id: 'mapping-A',
      conversation_id: 'conversation-A',
      turn_id: 'turn-A',
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
      reason: 'mapping_missing',
    };
    const request = {
      authority: 'core',
      mapping_id: 'mapping-A',
      expected_mapping_version: 1,
      lineage_id: 'lineage-A',
    };

    const applied = resolveProvisionalMappingBinding(pending, request, {
      occurredAt: '2026-07-19T04:00:02Z',
    });
    expect(applied).toEqual({
      status: 'bound',
      mapping: {
        ...pending,
        lineage_id: 'lineage-A',
        binding_state: 'bound',
        mapping_version: 2,
      },
      error: null,
    });

    expect(resolveProvisionalMappingBinding(applied.mapping, request).status).toBe('duplicate');
    const conflict = resolveProvisionalMappingBinding(applied.mapping, {
      ...request,
      lineage_id: 'lineage-B',
    }, { occurredAt: '2026-07-19T04:00:03Z' });
    expect(conflict.status).toBe('conflict');
    expect(conflict.mapping).toEqual(applied.mapping);
    expect(conflict.error.code).toBe('version_conflict');

    const channelAttempt = resolveProvisionalMappingBinding(pending, {
      ...request,
      authority: 'channel',
    }, { occurredAt: '2026-07-19T04:00:04Z' });
    expect(channelAttempt.status).toBe('forbidden');
    expect(channelAttempt.mapping).toEqual(pending);
    expect(channelAttempt.error.code).toBe('forbidden');

    expectContractFailure(
      () => resolveProvisionalMappingBinding(pending, {
        ...request,
        authority: 'dashboard',
      }),
      'unsupported_capability',
    );
  });
});

function deliveryKey(target, deliveryId) {
  return createIdempotencyKey('delivery', {
    channel: target.channel,
    target,
    delivery_id: deliveryId,
  });
}

function createMainCommand() {
  const target = {
    region: 'cn',
    tenant_id: 'tenant-A',
    channel: 'feishu',
    bot_id: 'bot-A',
    chat_type: 'group',
    chat_id: 'chat-A',
    native_thread_or_topic_id: null,
  };
  const deliveryId = 'delivery-create-A';
  return {
    contract: 'zylos.delivery-command',
    contract_version: '1.0',
    outbox_id: 'outbox-create-A',
    delivery_id: deliveryId,
    trace_id: 'trace-create-A',
    delivery_attempt_id: 'delivery-attempt-create-1',
    delivery_attempt_no: 1,
    outbox_lease_epoch: 7,
    target,
    aggregate_type: 'turn_main',
    aggregate_id: 'turn-A',
    operation: 'create_main',
    aggregate_version: 1,
    event_sequence_through: 1,
    idempotency_key: deliveryKey(target, deliveryId),
    render_model: {
      title: 'Zylos',
      phase: 'received',
      text: 'Message received.',
      error: null,
      tools: [],
      interactions: [],
      terminal: false,
      user_action_required: false,
    },
    mapping: {
      mapping_id: 'mapping-A',
      conversation_id: 'conversation-A',
      turn_id: 'turn-A',
      lineage_id: 'lineage-A',
      binding_state: 'bound',
      mapping_version: 1,
    },
    target_platform_message_id: null,
    predecessor_delivery_id: null,
    expected_platform_version: null,
    priority: 10,
    not_before: '2026-07-19T04:00:00Z',
    created_at: '2026-07-19T04:00:00Z',
  };
}

function withDeliveryIdentity(command, deliveryId, fields) {
  return {
    ...command,
    ...fields,
    delivery_id: deliveryId,
    idempotency_key: deliveryKey(command.target, deliveryId),
  };
}

function updateMainCommand() {
  const create = createMainCommand();
  return withDeliveryIdentity(create, 'delivery-update-A', {
    outbox_id: 'outbox-update-A',
    operation: 'update_main',
    aggregate_version: 2,
    event_sequence_through: 3,
    target_platform_message_id: 'platform-message-A',
    predecessor_delivery_id: create.delivery_id,
    expected_platform_version: 4,
  });
}

function sendTextCommand() {
  const create = createMainCommand();
  return withDeliveryIdentity(create, 'delivery-text-A', {
    outbox_id: 'outbox-text-A',
    aggregate_type: 'text_notice',
    aggregate_id: 'notice-A',
    operation: 'send_text',
    mapping: { ...create.mapping, mapping_id: 'mapping-text-A' },
  });
}

function sendFallbackCommand() {
  const create = createMainCommand();
  return withDeliveryIdentity(create, 'delivery-fallback-A', {
    outbox_id: 'outbox-fallback-A',
    operation: 'send_fallback',
    mapping: { ...create.mapping, mapping_id: 'mapping-fallback-A' },
    predecessor_delivery_id: updateMainCommand().delivery_id,
  });
}

function withDeliveryTarget(command, targetFields, contractVersion = command.contract_version) {
  const target = { ...command.target, ...targetFields };
  return {
    ...command,
    contract_version: contractVersion,
    target,
    idempotency_key: deliveryKey(target, command.delivery_id),
  };
}

function nativeThreadCommand(command, contractVersion, {
  includeDeliveryAnchors = contractVersion === '1.1',
} = {}) {
  return withDeliveryTarget(command, {
    chat_type: 'thread',
    native_thread_or_topic_id: 'native-thread-A',
    ...(includeDeliveryAnchors ? {
      native_thread_root_message_id: 'platform-root-message-A',
      native_thread_reply_target_message_id: 'platform-inbound-message-A',
    } : {}),
  }, contractVersion);
}

describe('delivery command v1 schema', () => {
  test('validates create/update/text/fallback target, predecessor and mapping conditions', () => {
    const create = createMainCommand();
    const update = updateMainCommand();
    const sendText = sendTextCommand();
    const fallback = sendFallbackCommand();

    for (const command of [create, update, sendText, fallback]) {
      const withExtension = { ...command, future_minor_field: { preserved: true } };
      const validated = validateDeliveryCommand(withExtension);
      expect(validated.forwarded).toEqual(withExtension);
      expect(validated.extensions).toEqual({ future_minor_field: { preserved: true } });
    }

    for (const invalid of [
      { ...create, target_platform_message_id: 'unexpected-platform-message' },
      { ...update, target_platform_message_id: null },
      { ...sendText, predecessor_delivery_id: create.delivery_id },
      { ...fallback, predecessor_delivery_id: null },
      { ...create, mapping: { ...create.mapping, turn_id: 'turn-B' } },
    ]) {
      expect(() => validateDeliveryCommand(invalid)).toThrow(ContractKernelError);
    }
  });

  test('keeps v1.0 non-thread commands compatible and enforces the v1.1 non-thread null matrix', () => {
    expect(validateDeliveryCommand(createMainCommand()).forwarded)
      .toEqual(createMainCommand());

    const v11 = withDeliveryTarget(createMainCommand(), {
      native_thread_root_message_id: null,
      native_thread_reply_target_message_id: null,
    }, '1.1');
    expect(validateDeliveryCommand(v11).forwarded).toEqual(v11);

    for (const fieldName of [
      'native_thread_root_message_id',
      'native_thread_reply_target_message_id',
    ]) {
      const missing = structuredClone(v11);
      delete missing.target[fieldName];
      expectContractFailure(() => validateDeliveryCommand(missing), 'unsupported_capability');

      const nonNull = withDeliveryTarget(v11, { [fieldName]: `${fieldName}-unexpected` });
      expectContractFailure(() => validateDeliveryCommand(nonNull), 'unsupported_capability');
    }
  });

  test('requires authoritative v1.1 thread anchors for create, update, text and fallback', () => {
    for (const command of [
      createMainCommand(),
      updateMainCommand(),
      sendTextCommand(),
      sendFallbackCommand(),
    ]) {
      const v11 = nativeThreadCommand(command, '1.1');
      expect(validateDeliveryCommand(v11).forwarded).toEqual(v11);

      for (const fieldName of [
        'native_thread_or_topic_id',
        'native_thread_root_message_id',
        'native_thread_reply_target_message_id',
      ]) {
        const missing = structuredClone(v11);
        delete missing.target[fieldName];
        expectContractFailure(() => validateDeliveryCommand(missing), 'unsupported_capability');

        const nullAnchor = withDeliveryTarget(v11, { [fieldName]: null });
        expectContractFailure(() => validateDeliveryCommand(nullAnchor), 'unsupported_capability');
      }
    }

    const threadIdAsReplyTarget = nativeThreadCommand(createMainCommand(), '1.1');
    threadIdAsReplyTarget.target.native_thread_reply_target_message_id
      = threadIdAsReplyTarget.target.native_thread_or_topic_id;
    expectContractFailure(
      () => validateDeliveryCommand(threadIdAsReplyTarget),
      'unsupported_capability',
    );
  });

  test('fails closed for v1.0 native-thread create, text and fallback without blocking exact updates', () => {
    for (const command of [createMainCommand(), sendTextCommand(), sendFallbackCommand()]) {
      expectContractFailure(
        () => validateDeliveryCommand(nativeThreadCommand(command, '1.0')),
        'unsupported_capability',
      );
    }

    const update = nativeThreadCommand(updateMainCommand(), '1.0', {
      includeDeliveryAnchors: false,
    });
    expect(validateDeliveryCommand(update).forwarded).toEqual(update);
  });
});

function deliveredResult(command, fields = {}) {
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
    platform_message_id: command.target_platform_message_id ?? 'platform-message-created-A',
    applied_platform_version: 5,
    delivered_at: '2026-07-19T04:00:01Z',
    error: null,
    renderer_capabilities: {
      supports_update: true,
      supports_actions: true,
      supports_platform_idempotency: true,
      supports_platform_version: true,
    },
    result_at: '2026-07-19T04:00:01Z',
    ...fields,
  };
}

function deliveryFailure(command, status, error) {
  return deliveredResult(command, {
    status,
    platform_message_id: command.operation === 'update_main'
      ? command.target_platform_message_id
      : null,
    applied_platform_version: null,
    delivered_at: null,
    error,
  });
}

const retryableDeliveryError = {
  code: 'delivery_transient',
  category: 'channel',
  retryable: true,
  side_effect_status: 'unknown',
  user_message: 'The channel may have accepted the message; reconciliation is required.',
  occurred_at: '2026-07-19T04:00:01Z',
};
const permanentDeliveryError = {
  code: 'delivery_permanent',
  category: 'channel',
  retryable: false,
  side_effect_status: 'none',
  user_message: 'The channel rejected this delivery permanently.',
  occurred_at: '2026-07-19T04:00:01Z',
};

describe('delivery result v1 schema', () => {
  test('validates fencing, platform IDs, versions, errors and side-effect status by result state', () => {
    const create = createMainCommand();
    const update = updateMainCommand();
    const sendText = sendTextCommand();
    const fallback = sendFallbackCommand();
    const delivered = deliveredResult(create);
    const retryable = deliveryFailure(create, 'retryable_failure', retryableDeliveryError);
    const permanent = deliveryFailure(create, 'permanent_failure', permanentDeliveryError);
    const obsolete = deliveryFailure(update, 'obsolete', {
      code: 'obsolete',
      category: 'conflict',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'A newer aggregate version is already applied.',
      occurred_at: '2026-07-19T04:00:01Z',
    });

    for (const [result, command] of [
      [delivered, create],
      [retryable, create],
      [permanent, create],
      [obsolete, update],
    ]) {
      expect(validateDeliveryResult(result, { command }).forwarded).toEqual(result);
    }
    expect(() => validateDeliveryResult(delivered)).toThrow(ContractKernelError);

    for (const [result, command] of [
      [{ ...delivered, platform_message_id: null }, create],
      [{ ...retryable, error: { ...retryable.error, retryable: false } }, create],
      [{ ...permanent, error: { ...permanent.error, retryable: true } }, create],
      [{ ...obsolete, operation: 'create_main' }, update],
      [{ ...delivered, delivery_attempt_id: 'stale-attempt' }, create],
    ]) {
      expect(() => validateDeliveryResult(result, { command })).toThrow(ContractKernelError);
    }

    for (const command of [create, sendText, fallback]) {
      const uncertainPermanent = deliveryFailure(
        command,
        'permanent_failure',
        { ...permanentDeliveryError, side_effect_status: 'unknown' },
      );
      expect(() => validateDeliveryResult(uncertainPermanent, { command }))
        .toThrow(ContractKernelError);
    }
  });
});

describe('delivery mapping v1 schema', () => {
  test('keeps pending, bound and non-reply mappings structurally distinct', () => {
    const pending = {
      mapping_id: 'mapping-A',
      conversation_id: 'conversation-A',
      turn_id: 'turn-A',
      lineage_id: null,
      binding_state: 'pending',
      mapping_version: 1,
      reason: 'mapping_missing',
    };
    const bound = {
      ...pending,
      lineage_id: 'lineage-A',
      binding_state: 'bound',
      mapping_version: 2,
      reason: null,
    };
    const notApplicable = {
      mapping_id: 'mapping-security-A',
      conversation_id: null,
      turn_id: null,
      lineage_id: null,
      binding_state: 'not_applicable',
      mapping_version: 1,
      reason: null,
    };

    expect(validateDeliveryMapping(pending)).toEqual(pending);
    expect(validateDeliveryMapping(bound)).toEqual(bound);
    expect(validateDeliveryMapping(notApplicable)).toEqual(notApplicable);
    expect(() => validateDeliveryMapping({ ...pending, lineage_id: 'lineage-A' }))
      .toThrow(ContractKernelError);
    expect(() => validateDeliveryMapping({ ...pending, reason: null }))
      .toThrow(ContractKernelError);
    const pendingWithoutReason = { ...pending };
    delete pendingWithoutReason.reason;
    expect(() => validateDeliveryMapping(pendingWithoutReason))
      .toThrow(ContractKernelError);
    expectContractFailure(
      () => validateDeliveryMapping({ ...pending, reason: 'unknown_recovery_reason' }),
      'unsupported_capability',
    );
    expect(() => validateDeliveryMapping({ ...bound, reason: 'mapping_missing' }))
      .not.toThrow();
    expect(() => validateDeliveryMapping({ ...notApplicable, reason: 'mapping_missing' }))
      .toThrow(ContractKernelError);
    expect(() => validateDeliveryMapping({ ...bound, lineage_id: null }))
      .toThrow(ContractKernelError);
    expect(() => validateDeliveryMapping({
      ...notApplicable,
      conversation_id: null,
      turn_id: null,
      lineage_id: 'lineage-A',
    })).toThrow(ContractKernelError);
  });
});

function expectContractFailure(operation, expectedCode) {
  try {
    operation();
    throw new Error(`expected ${expectedCode}`);
  } catch (error) {
    expect(error).toBeInstanceOf(ContractKernelError);
    expect(error.contractError.code).toBe(expectedCode);
  }
}

describe('delivery/mapping cross-repository fixtures', () => {
  test('publish the complete command, result and one-way binding matrices safely', () => {
    const fixture = JSON.parse(readFileSync(
      new URL('../contracts/public/fixtures/delivery-mapping-v1.json', import.meta.url),
      'utf8',
    ));
    expect(fixture.fixture_version).toBe('1.0');
    expect(fixture.contract_versions).toEqual({
      'zylos.delivery-command': '1.0',
      'zylos.delivery-result': '1.0',
    });
    expect(validatePublicFixtureSafety(fixture)).toBe(true);

    const validCommands = new Map();
    for (const vector of fixture.command_vectors) {
      if (vector.valid) {
        expect(validateDeliveryCommand(vector.document).forwarded).toEqual(vector.document);
        validCommands.set(vector.name, vector.document);
      } else {
        expectContractFailure(
          () => validateDeliveryCommand(vector.document),
          vector.expected_error_code,
        );
      }
    }
    expect(new Set([...validCommands.values()].map((document) => document.operation)))
      .toEqual(new Set(DELIVERY_OPERATIONS));
    for (const operation of DELIVERY_OPERATIONS) {
      expect(fixture.command_vectors.some(
        (vector) => !vector.valid && vector.document.operation === operation,
      )).toBe(true);
    }

    for (const vector of fixture.result_vectors) {
      const command = validCommands.get(vector.command);
      expect(command).toBeDefined();
      if (vector.valid) {
        expect(validateDeliveryResult(vector.document, { command }).forwarded)
          .toEqual(vector.document);
      } else {
        expectContractFailure(
          () => validateDeliveryResult(vector.document, { command }),
          vector.expected_error_code,
        );
      }
    }
    expect(new Set(
      fixture.result_vectors
        .filter((vector) => vector.valid)
        .map((vector) => vector.document.status),
    )).toEqual(new Set(DELIVERY_RESULT_STATUSES));
    for (const status of DELIVERY_RESULT_STATUSES) {
      expect(fixture.result_vectors.some(
        (vector) => !vector.valid && vector.document.status === status,
      )).toBe(true);
    }

    for (const vector of fixture.mapping_schema_vectors) {
      if (vector.valid) {
        expect(validateDeliveryMapping(vector.mapping)).toEqual(vector.mapping);
      } else {
        expectContractFailure(
          () => validateDeliveryMapping(vector.mapping),
          vector.expected_error_code,
        );
      }
    }
    expect(new Set(
      fixture.mapping_schema_vectors
        .filter((vector) => vector.valid && vector.mapping.binding_state === 'pending')
        .map((vector) => vector.mapping.reason),
    )).toEqual(new Set(MAPPING_RECOVERY_REASONS));

    for (const vector of fixture.mapping_binding_vectors) {
      const outcome = resolveProvisionalMappingBinding(vector.current, vector.request, {
        occurredAt: vector.occurred_at,
      });
      expect(outcome.status).toBe(vector.expected.status);
      expect(outcome.mapping).toEqual(vector.expected.mapping);
      expect(outcome.error?.code ?? null).toBe(vector.expected.error_code);
    }
  });

  test('publishes the portable v1.1 target schema and native-thread golden matrix', () => {
    const schema = JSON.parse(readFileSync(
      new URL('../contracts/public/schemas/delivery-command-v1.schema.json', import.meta.url),
      'utf8',
    ));
    expect(schema).toMatchObject({
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      $id: 'https://schemas.zylos.ai/public/v1/delivery-command.schema.json',
      type: 'object',
      additionalProperties: true,
    });
    expect(schema.required).toEqual(expect.arrayContaining([
      'contract',
      'contract_version',
      'target',
      'operation',
    ]));
    expect(schema['x-zylos-native-thread-target']).toEqual({
      conversation_identity: 'native_thread_or_topic_id',
      reply_api_target: 'native_thread_reply_target_message_id',
      root_scope: 'native_thread_root_message_id',
      reply_target_must_differ_from_conversation_identity: true,
      target_change: 'version_conflict',
      missing_capability: 'unsupported_capability',
      fallback_inference: false,
    });

    const fixture = JSON.parse(readFileSync(
      new URL('../contracts/public/fixtures/delivery-native-thread-v1.1.json', import.meta.url),
      'utf8',
    ));
    expect(fixture.fixture_version).toBe('1.1');
    expect(fixture.contract_versions).toEqual({
      'zylos.delivery-command': ['1.0', '1.1'],
    });
    expect(validatePublicFixtureSafety(fixture)).toBe(true);

    const validOperations = new Set();
    for (const vector of fixture.command_vectors) {
      if (vector.valid) {
        expect(validateDeliveryCommand(vector.document).forwarded).toEqual(vector.document);
        if (vector.document.target.chat_type === 'thread') {
          validOperations.add(vector.document.operation);
        }
      } else {
        expectContractFailure(
          () => validateDeliveryCommand(vector.document),
          vector.expected_error_code,
        );
      }
    }
    expect(validOperations).toEqual(new Set(DELIVERY_OPERATIONS));
    expect(fixture.command_vectors.some(
      ({ name, valid }) => name === 'v1_0_non_thread_compatible' && valid,
    )).toBe(true);
    expect(fixture.command_vectors.some(({ name, valid, document }) => (
      name === 'v1_1_non_thread_explicit_null_anchors'
      && valid
      && document.target.native_thread_root_message_id === null
      && document.target.native_thread_reply_target_message_id === null
    ))).toBe(true);
    expect(fixture.command_vectors.filter(({ valid }) => !valid).map(({ name }) => name))
      .toEqual(expect.arrayContaining([
        'v1_0_thread_create_requires_v1_1',
        'v1_0_thread_send_text_requires_v1_1',
        'v1_0_thread_send_fallback_requires_v1_1',
      ]));
    expect(fixture.command_vectors.filter(({ valid }) => !valid).map(({ expected_error_code }) =>
      expected_error_code)).toEqual(expect.arrayContaining([
      'unsupported_capability',
      'unsupported_capability',
      'unsupported_capability',
    ]));
  });
});
