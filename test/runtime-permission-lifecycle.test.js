import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, jest, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateInboundResult,
  validateInteractionAnswerResult,
  validateInteractionRequest,
} from '../contracts/public/index.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { createExecutorService } from '../runtime/executor/service.js';
import {
  createPermissionService,
  parsePermissionCommand,
} from '../runtime/permissions/permission-service.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const temporaryDirectories = [];

function openDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-permission-'));
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

function envelope(suffix, text, {
  fixtureName = 'authenticated_dm_with_attachment',
  actorId,
  roles,
  chatId,
  contentKind = 'text',
  sourceKind = 'platform_original',
} = {}) {
  const fixture = inboundFixture.valid.find(({ name }) => name === fixtureName).document;
  const value = structuredClone(fixture);
  value.inbound_event_id = `evt-permission-${suffix}`;
  value.trace_id = `trace-permission-${suffix}`;
  value.message_id = `message-permission-${suffix}`;
  value.actor.actor_id = actorId ?? value.actor.actor_id;
  value.actor.roles = roles ?? value.actor.roles;
  value.chat_id = chatId ?? value.chat_id;
  value.content = { kind: contentKind, text, attachments: [] };
  value.source = { kind: sourceKind, source_ref: null };
  value.idempotency_key = createIdempotencyKey('inbound', {
    region: value.region,
    tenant_id: value.tenant_id,
    channel: value.channel,
    bot_id: value.bot_id,
    inbound_event_id: value.inbound_event_id,
  });
  return value;
}

function confirmationAnswer(request, suffix, {
  actorId,
  roles = ['bot_owner'],
  decision = 'approve',
} = {}) {
  const sourceId = `permission-action-${suffix}`;
  const answer = {
    contract: 'zylos.interaction-answer',
    contract_version: '1.0',
    trace_id: `trace-permission-answer-${suffix}`,
    interaction_id: request.interaction_id,
    interaction_version: request.version,
    answer_id: `permission-answer-${suffix}`,
    source_event_or_action_id: sourceId,
    actor: {
      type: 'user',
      actor_id: actorId,
      authenticated: true,
      roles,
    },
    source_context: {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      chat_id: 'chat-dm-A',
      native_thread_or_topic_id: null,
      platform_message_or_action_id: sourceId,
    },
    source: 'card_action',
    value: { kind: 'decision', decision },
    answered_at: '2026-07-20T01:05:00Z',
  };
  answer.idempotency_key = createIdempotencyKey('interaction', {
    interaction_id: answer.interaction_id,
    source_event_or_action_id: answer.source_event_or_action_id,
  });
  return answer;
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('permission command recognition', () => {
  test.each([
    ['/permission trusted', { kind: 'next_turn', duration_ms: null }],
    ['/permission trusted 15m', { kind: 'timed_conversation', duration_ms: 900_000 }],
    ['/permission trusted --bot', { kind: 'persistent_bot', duration_ms: null }],
    ['/permission safe', { kind: 'safe', duration_ms: null }],
    ['  /permission trusted 2h\n', { kind: 'timed_conversation', duration_ms: 7_200_000 }],
  ])('recognizes exact authenticated platform-original text %s', (text, expected) => {
    expect(parsePermissionCommand(envelope('parse', text))).toMatchObject(expected);
  });

  test.each([
    ['/permission trusted 0s', 'text', 'platform_original'],
    ['/permission trusted 1.5h', 'text', 'platform_original'],
    ['/permission trusted 1h30m', 'text', 'platform_original'],
    ['/permission trusted 1H', 'text', 'platform_original'],
    ['/permission trusted --bot ', 'rich_text', 'platform_original'],
    ['/permission safe', 'mixed', 'platform_original'],
    ['/permission trusted', 'text', 'scheduler'],
  ])('does not recognize invalid or non-original command input', (text, kind, source) => {
    expect(parsePermissionCommand(envelope('excluded', text, {
      contentKind: kind,
      sourceKind: source,
    }))).toBeNull();
  });

  test('persists an over-policy duration as a rejected control without granting trust', () => {
    const database = openDatabase();
    const result = acceptNormalInbound(
      database,
      envelope('over-policy', '/permission trusted 2h'),
      {
        now: () => '2026-07-20T01:00:00Z',
        generateId: deterministicIds('over-policy'),
        permissionMaxTimedDurationMs: 60 * 60 * 1_000,
      },
    );
    expect(result).toMatchObject({
      status: 'rejected',
      control_id: expect.any(String),
      error: { code: 'permission_duration_exceeds_policy' },
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_grants
    `).get().count).toBe(0);
    expect(database.prepare(`
      SELECT status FROM runtime_permission_controls WHERE control_id = ?
    `).get(result.control_id)).toEqual({ status: 'rejected' });
    database.close();
  });

  test('keeps security notices on the explicit delivery v1.1 native-thread anchors', () => {
    const database = openDatabase();
    const result = acceptNormalInbound(database, envelope(
      'native-thread-safe',
      '/permission safe',
      { fixtureName: 'native_thread_or_topic' },
    ), {
      now: () => '2026-07-20T01:00:00Z',
      generateId: deterministicIds('native-thread-safe'),
    });
    const command = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE control_id = ?
    `).get(result.control_id).command_json);
    expect(validateDeliveryCommand(command).forwarded).toEqual(command);
    expect(command).toMatchObject({
      contract_version: '1.1',
      aggregate_type: 'security_notice',
      operation: 'send_text',
      target: {
        chat_type: 'thread',
        native_thread_or_topic_id: 'thread-native-001',
        native_thread_root_message_id: 'message-thread-root-001',
        native_thread_reply_target_message_id: 'message-permission-native-thread-safe',
      },
      mapping: {
        conversation_id: result.conversation_id,
        turn_id: null,
        lineage_id: null,
        binding_state: 'not_applicable',
      },
    });
    database.close();
  });
});

describe('durable next-turn permission', () => {
  test('is consumed atomically by the issuing actor first accepted executable turn', () => {
    const database = openDatabase();
    const now = () => '2026-07-20T01:00:00Z';
    const generateId = deterministicIds('next');
    const options = { now, generateId, maxQueuedTurns: 3 };

    const command = acceptNormalInbound(
      database,
      envelope('next-command', '/permission trusted', { actorId: 'actor-owner' }),
      options,
    );
    expect(validateInboundResult(command).forwarded).toEqual(command);
    expect(command).toMatchObject({
      status: 'accepted',
      turn_id: null,
      lineage_id: null,
      turn_version: null,
      lineage_resolution_state: 'not_applicable',
    });
    expect(command.control_id).not.toBeNull();

    const otherActorTurn = acceptNormalInbound(
      database,
      envelope('other-turn', 'do something', { actorId: 'actor-other' }),
      options,
    );
    const ownerTurn = acceptNormalInbound(
      database,
      envelope('owner-turn', 'do protected work', { actorId: 'actor-owner' }),
      options,
    );
    const laterOwnerTurn = acceptNormalInbound(
      database,
      envelope('owner-later', 'do more protected work', { actorId: 'actor-owner' }),
      options,
    );

    const service = createPermissionService({ database, now, generateId });
    expect(service.authorizeProtectedAction({
      turn_id: otherActorTurn.turn_id,
      action_ref: 'tool-other',
      action_kind: 'filesystem_write',
    })).toMatchObject({ trusted: false, basis_kind: 'default_safe' });
    expect(service.authorizeProtectedAction({
      turn_id: ownerTurn.turn_id,
      action_ref: 'tool-owner',
      action_kind: 'filesystem_write',
    })).toMatchObject({ trusted: true, basis_kind: 'next_turn' });
    expect(service.authorizeProtectedAction({
      turn_id: laterOwnerTurn.turn_id,
      action_ref: 'tool-later',
      action_kind: 'filesystem_write',
    })).toMatchObject({ trusted: false, basis_kind: 'default_safe' });

    expect(database.prepare(`
      SELECT state, consumed_by_turn_id
      FROM runtime_permission_grants
      WHERE grant_kind = 'next_turn'
    `).get()).toEqual({ state: 'consumed', consumed_by_turn_id: ownerTurn.turn_id });
    expect(database.prepare(`
      SELECT action, turn_id, grant_id, policy_revision
      FROM runtime_permission_audit
      WHERE action = 'permission_consumed'
    `).get()).toMatchObject({
      action: 'permission_consumed',
      turn_id: ownerTurn.turn_id,
      grant_id: expect.any(String),
      policy_revision: 2,
    });
    database.close();
  });

  test('does not consume the grant when admission fails at queue capacity', () => {
    const database = openDatabase();
    const now = () => '2026-07-20T01:00:00Z';
    const generateId = deterministicIds('queue-full');
    const options = { now, generateId, maxQueuedTurns: 1 };

    acceptNormalInbound(database, envelope('queue-fill', 'fill queue'), options);
    acceptNormalInbound(
      database,
      envelope('queue-command', '/permission trusted', { actorId: 'actor-owner' }),
      options,
    );
    const rejected = acceptNormalInbound(
      database,
      envelope('queue-rejected', 'protected work', { actorId: 'actor-owner' }),
      options,
    );
    expect(rejected).toMatchObject({ status: 'rejected', error: { code: 'queue_full' } });
    expect(database.prepare(`
      SELECT state, consumed_by_turn_id
      FROM runtime_permission_grants
      WHERE grant_kind = 'next_turn'
    `).get()).toEqual({ state: 'active', consumed_by_turn_id: null });
    database.close();
  });

  test('replays a permission command idempotently without minting another revision', () => {
    const database = openDatabase();
    const commandEnvelope = envelope('next-replay', '/permission trusted');
    const options = {
      now: () => '2026-07-20T01:00:00Z',
      generateId: deterministicIds('next-replay'),
    };
    const first = acceptNormalInbound(database, commandEnvelope, options);
    const duplicateEnvelope = structuredClone(commandEnvelope);
    duplicateEnvelope.trace_id = 'trace-permission-next-replay-duplicate';
    const duplicate = acceptNormalInbound(database, duplicateEnvelope, options);

    expect(duplicate).toEqual({ ...first, trace_id: duplicateEnvelope.trace_id, deduplicated: true });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_grants
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT current_revision FROM runtime_permission_revision_sequence WHERE singleton_id = 1
    `).get()).toEqual({ current_revision: 1 });
    database.close();
  });

  test('rolls back grant, revision, control, inbound, audit, and outbox together', () => {
    const database = openDatabase();
    createPermissionService({ database });
    database.exec(`
      CREATE TRIGGER fail_permission_audit
      BEFORE INSERT ON runtime_permission_audit
      BEGIN
        SELECT RAISE(ABORT, 'injected permission audit failure');
      END;
    `);
    expect(() => acceptNormalInbound(
      database,
      envelope('rollback', '/permission trusted'),
      {
        now: () => '2026-07-20T01:00:00Z',
        generateId: deterministicIds('rollback'),
      },
    )).toThrow('injected permission audit failure');
    for (const table of [
      'runtime_permission_grants',
      'runtime_permission_controls',
      'runtime_inbound_events',
      'runtime_permission_audit',
      'runtime_outbox',
    ]) {
      expect(database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count).toBe(0);
    }
    expect(database.prepare(`
      SELECT current_revision FROM runtime_permission_revision_sequence WHERE singleton_id = 1
    `).get()).toEqual({ current_revision: 0 });
    database.close();
  });

  test('survives a real database reopen and still honors a later durable barrier', () => {
    const database = openDatabase();
    const file = database.prepare('PRAGMA database_list').all()
      .find(({ name }) => name === 'main').file;
    const now = () => '2026-07-20T01:00:00Z';
    const options = { now, generateId: deterministicIds('reopen') };
    acceptNormalInbound(database, envelope(
      'reopen-command', '/permission trusted', { actorId: 'reopen-actor' },
    ), options);
    const turn = acceptNormalInbound(database, envelope(
      'reopen-turn', 'protected work', { actorId: 'reopen-actor' },
    ), options);
    database.close();

    const reopened = new Database(file);
    const service = createPermissionService({
      database: reopened,
      now,
      generateId: deterministicIds('reopened-service'),
    });
    expect(service.authorizeProtectedAction({
      turn_id: turn.turn_id,
      action_ref: 'after-reopen',
      action_kind: 'shell',
    })).toMatchObject({ trusted: true, basis_kind: 'next_turn' });
    acceptNormalInbound(reopened, envelope(
      'reopen-safe', '/permission safe', { actorId: 'other-actor' },
    ), { ...options, generateId: deterministicIds('reopen-safe') });
    expect(service.authorizeProtectedAction({
      turn_id: turn.turn_id,
      action_ref: 'after-reopen-safe',
      action_kind: 'shell',
    })).toMatchObject({ trusted: false, reason: 'revocation_barrier' });
    reopened.close();
  });
});

describe('timed permission and safe revocation barrier', () => {
  test('enforces role scope and invalidates already-admitted protected work immediately', () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('timed-safe');
    const options = { now, generateId };
    const fixtureName = 'group_main_conversation';

    const forbidden = acceptNormalInbound(database, envelope(
      'timed-member',
      '/permission trusted 10m',
      { fixtureName, actorId: 'group-member', roles: ['member'] },
    ), options);
    expect(forbidden).toMatchObject({
      status: 'rejected',
      error: { code: 'forbidden' },
    });

    const granted = acceptNormalInbound(database, envelope(
      'timed-owner',
      '/permission trusted 10m',
      { fixtureName, actorId: 'group-owner', roles: ['member', 'group_owner'] },
    ), options);
    expect(granted.status).toBe('accepted');
    const turn = acceptNormalInbound(database, envelope(
      'timed-turn',
      'perform protected work',
      { fixtureName, actorId: 'group-member', roles: ['member'] },
    ), options);
    const service = createPermissionService({ database, now, generateId });
    expect(service.authorizeProtectedAction({
      turn_id: turn.turn_id,
      action_ref: 'before-safe',
      action_kind: 'shell',
    })).toMatchObject({ trusted: true, basis_kind: 'timed_conversation' });

    clock.value = '2026-07-20T01:01:00Z';
    const safe = acceptNormalInbound(database, envelope(
      'safe-member',
      '/permission safe',
      { fixtureName, actorId: 'group-member', roles: ['member'] },
    ), options);
    expect(safe.status).toBe('accepted');
    expect(service.authorizeProtectedAction({
      turn_id: turn.turn_id,
      action_ref: 'after-safe',
      action_kind: 'shell',
    })).toMatchObject({ trusted: false, reason: 'revocation_barrier' });
    expect(database.prepare(`
      SELECT scope_kind, actor_id, reason
      FROM runtime_permission_revocations
    `).get()).toEqual({
      scope_kind: 'conversation',
      actor_id: 'group-member',
      reason: 'safe_command',
    });
    database.close();
  });

  test('expires timed grants durably with audit, normalized event, and security notice', () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('expiry');
    const options = { now, generateId };

    acceptNormalInbound(database, envelope('expiry-command', '/permission trusted 1m'), options);
    const turn = acceptNormalInbound(database, envelope('expiry-turn', 'protected work'), options);
    const service = createPermissionService({ database, now, generateId });
    clock.value = '2026-07-20T01:02:00Z';

    expect(service.expireDue()).toMatchObject({ grants_expired: 1 });
    expect(service.authorizeProtectedAction({
      turn_id: turn.turn_id,
      action_ref: 'after-expiry',
      action_kind: 'shell',
    })).toMatchObject({ trusted: false, reason: 'grant_expired' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_normalized_events
      WHERE event_json LIKE '%permission_expired%'
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_permission_audit
      WHERE action = 'permission_expired'
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_outbox
      WHERE aggregate_type = 'security_notice'
    `).get().count).toBe(2);
    database.close();
  });
});

describe('persistent bot permission confirmation', () => {
  test('requires the same manager to confirm and supports both identical command and signed button', async () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('bot-confirm');
    const options = { now, generateId };
    const manager = { actorId: 'bot-owner', roles: ['member', 'bot_owner'] };

    const pendingGrant = acceptNormalInbound(database, envelope(
      'bot-first',
      '/permission trusted --bot',
      manager,
    ), options);
    expect(pendingGrant.status).toBe('accepted');
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_grants
      WHERE grant_kind = 'persistent_bot'
    `).get().count).toBe(0);
    const grantRequest = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions
      WHERE parent_type = 'security_control'
      ORDER BY created_at DESC
      LIMIT 1
    `).get().request_json);
    expect(validateInteractionRequest(grantRequest)).toBeDefined();

    clock.value = '2026-07-20T01:01:00Z';
    const confirmedGrant = acceptNormalInbound(database, envelope(
      'bot-repeat',
      '/permission trusted --bot',
      manager,
    ), options);
    expect(confirmedGrant.status).toBe('accepted');
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'active' });

    const botTrustedTurn = acceptNormalInbound(database, envelope(
      'bot-trusted-turn',
      'bot-wide protected work',
      { actorId: 'another-user', roles: ['member'] },
    ), options);
    const permissionPolicy = createPermissionService({ database, now, generateId });
    expect(permissionPolicy.authorizeProtectedAction({
      turn_id: botTrustedTurn.turn_id,
      action_ref: 'bot-before-safe',
      action_kind: 'network',
    })).toMatchObject({ trusted: true, basis_kind: 'persistent_bot' });

    clock.value = '2026-07-20T01:02:00Z';
    const safe = acceptNormalInbound(database, envelope(
      'bot-safe',
      '/permission safe',
      manager,
    ), options);
    expect(safe.status).toBe('accepted');
    expect(permissionPolicy.authorizeProtectedAction({
      turn_id: botTrustedTurn.turn_id,
      action_ref: 'bot-after-conversation-safe',
      action_kind: 'network',
    })).toMatchObject({ trusted: false, reason: 'revocation_barrier' });
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'active' });

    const otherConversationTurn = acceptNormalInbound(database, envelope(
      'bot-other-conversation',
      'bot policy remains active elsewhere',
      { actorId: 'another-user', roles: ['member'], chatId: 'chat-dm-B' },
    ), options);
    expect(permissionPolicy.authorizeProtectedAction({
      turn_id: otherConversationTurn.turn_id,
      action_ref: 'bot-other-conversation',
      action_kind: 'network',
    })).toMatchObject({ trusted: true, basis_kind: 'persistent_bot' });

    const revokeRequest = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions
      WHERE parent_id = ?
    `).get(safe.control_id).request_json);
    const runtimeService = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'permission-confirmation-router',
      now,
      generateId,
    });
    const wrongActor = runtimeService.submitInteractionAnswer(confirmationAnswer(
      revokeRequest,
      'wrong',
      { actorId: 'different-manager', roles: ['bot_admin'] },
    ));
    expect(validateInteractionAnswerResult(wrongActor)).toBeDefined();
    expect(wrongActor).toMatchObject({
      status: 'rejected',
      error: { code: 'interaction_actor_forbidden' },
    });
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'active' });

    clock.value = '2026-07-20T01:05:00Z';
    const approved = runtimeService.submitInteractionAnswer(confirmationAnswer(
      revokeRequest,
      'approved',
      { actorId: 'bot-owner' },
    ));
    expect(validateInteractionAnswerResult(approved)).toBeDefined();
    expect(approved.status).toBe('accepted');
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'revoked' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_revocations WHERE scope_kind = 'bot'
    `).get().count).toBe(1);
    expect(JSON.parse(database.prepare(`
      SELECT final_result_json FROM runtime_permission_controls WHERE control_id = ?
    `).get(safe.control_id).final_result_json)).toMatchObject({
      record: 'zylos.permission-control-outcome',
      status: 'approved',
      action_kind: 'revoke_persistent_bot',
      source: 'card_action',
      grant_id: expect.any(String),
    });
    expect(database.prepare(`
      SELECT final_result_json FROM runtime_permission_controls WHERE control_id = ?
    `).get(pendingGrant.control_id).final_result_json).not.toBeNull();
    expect(database.prepare("PRAGMA foreign_key_list('runtime_permission_controls')").all()
      .some(({ table, from }) => table === 'runtime_interactions' && from === 'interaction_id'))
      .toBe(true);
    expect(database.prepare("PRAGMA foreign_key_list('runtime_permission_confirmations')").all()
      .some(({ table, from }) => table === 'runtime_interactions' && from === 'interaction_id'))
      .toBe(true);
    expect(() => database.prepare(`
      UPDATE runtime_permission_confirmations
      SET interaction_id = 'missing-security-interaction'
      WHERE control_id = ?
    `).run(safe.control_id)).toThrow(/FOREIGN KEY constraint failed/);
    expect(database.prepare('PRAGMA foreign_key_check').all()).toEqual([]);
    await runtimeService.close();
    database.close();
  });

  test('retains bot-wide trusted policy when revoke confirmation is denied or expires', () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('bot-retain');
    const options = { now, generateId };
    const manager = { actorId: 'bot-owner', roles: ['member', 'bot_owner'] };

    acceptNormalInbound(database, envelope(
      'retain-grant-first', '/permission trusted --bot', manager,
    ), options);
    clock.value = '2026-07-20T01:01:00Z';
    acceptNormalInbound(database, envelope(
      'retain-grant-repeat', '/permission trusted --bot', manager,
    ), options);
    clock.value = '2026-07-20T01:02:00Z';
    const firstSafe = acceptNormalInbound(database, envelope(
      'retain-safe-deny', '/permission safe', manager,
    ), options);
    const firstRequest = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE parent_id = ?
    `).get(firstSafe.control_id).request_json);
    const service = createPermissionService({ database, now, generateId });
    expect(service.submitConfirmation(confirmationAnswer(
      firstRequest,
      'deny-revoke',
      { actorId: 'bot-owner', decision: 'deny' },
    )).status).toBe('accepted');
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'active' });

    clock.value = '2026-07-20T01:06:00Z';
    acceptNormalInbound(database, envelope(
      'retain-safe-expire', '/permission safe', manager,
    ), options);
    clock.value = '2026-07-20T01:17:00Z';
    expect(service.expireDue()).toMatchObject({ confirmations_expired: 1 });
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'persistent_bot'
    `).get()).toEqual({ state: 'active' });
    expect(database.prepare(`
      SELECT status, COUNT(*) AS count
      FROM runtime_permission_confirmations
      WHERE action_kind = 'revoke_persistent_bot'
      GROUP BY status
      ORDER BY status
    `).all()).toEqual([
      { status: 'denied', count: 1 },
      { status: 'expired', count: 1 },
    ]);
    database.close();
  });

  test('invalidates a pending confirmation after any intervening policy revision', () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('stale-confirmation');
    const options = { now, generateId };
    const manager = { actorId: 'bot-owner', roles: ['member', 'bot_owner'] };

    const pending = acceptNormalInbound(database, envelope(
      'stale-bot-first', '/permission trusted --bot', manager,
    ), options);
    const request = JSON.parse(database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE parent_id = ?
    `).get(pending.control_id).request_json);
    clock.value = '2026-07-20T01:01:00Z';
    acceptNormalInbound(database, envelope(
      'stale-intervening-policy',
      '/permission trusted 5m',
      { actorId: 'other-user', chatId: 'chat-dm-policy-change' },
    ), options);

    const permissionPolicy = createPermissionService({ database, now, generateId });
    const rejected = permissionPolicy.submitConfirmation(confirmationAnswer(
      request,
      'stale-policy',
      { actorId: 'bot-owner' },
    ));
    expect(rejected).toMatchObject({
      status: 'rejected',
      error: { code: 'version_conflict' },
    });
    expect(database.prepare(`
      SELECT status FROM runtime_permission_confirmations WHERE control_id = ?
    `).get(pending.control_id)).toEqual({ status: 'expired' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_grants
      WHERE grant_kind = 'persistent_bot'
    `).get().count).toBe(0);
    expect(JSON.parse(database.prepare(`
      SELECT final_result_json FROM runtime_permission_controls WHERE control_id = ?
    `).get(pending.control_id).final_result_json)).toMatchObject({
      status: 'invalidated',
      reason: 'policy_revision_changed',
      error: { code: 'version_conflict' },
    });
    database.close();
  });

  test('rejects an identical-command confirmation after its policy fence becomes stale', () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('stale-repeat');
    const options = { now, generateId };
    const manager = { actorId: 'bot-owner', roles: ['member', 'bot_owner'] };
    acceptNormalInbound(database, envelope(
      'stale-repeat-first', '/permission trusted --bot', manager,
    ), options);
    clock.value = '2026-07-20T01:01:00Z';
    acceptNormalInbound(database, envelope(
      'stale-repeat-policy', '/permission trusted 5m', { chatId: 'chat-dm-C' },
    ), options);
    const rejected = acceptNormalInbound(database, envelope(
      'stale-repeat-confirm', '/permission trusted --bot', manager,
    ), options);
    expect(rejected).toMatchObject({
      status: 'rejected',
      error: { code: 'version_conflict' },
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_grants
      WHERE grant_kind = 'persistent_bot'
    `).get().count).toBe(0);

    clock.value = '2026-07-20T01:02:00Z';
    const fresh = acceptNormalInbound(database, envelope(
      'stale-repeat-fresh', '/permission trusted --bot', manager,
    ), options);
    expect(fresh.status).toBe('accepted');
    expect(database.prepare(`
      SELECT status FROM runtime_permission_confirmations
      WHERE control_id = ?
    `).get(fresh.control_id)).toEqual({ status: 'pending' });
    database.close();
  });

  test('runs expiry reconciliation automatically from the production service sweep', async () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('automatic-expiry');
    const options = { now, generateId };
    acceptNormalInbound(database, envelope(
      'automatic-timed', '/permission trusted 1m',
    ), options);
    const pending = acceptNormalInbound(database, envelope(
      'automatic-confirmation',
      '/permission trusted --bot',
      { actorId: 'bot-owner', roles: ['member', 'bot_owner'] },
    ), options);
    let sweep = null;
    const cancelPermissionSweep = jest.fn();
    const runtimeService = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'automatic-permission-expiry',
      now,
      generateId,
      schedulePermissionSweep(callback) {
        sweep = callback;
        return { unref() {} };
      },
      cancelPermissionSweep,
    });
    runtimeService.start();
    expect(sweep).toEqual(expect.any(Function));

    clock.value = '2026-07-20T01:11:00Z';
    sweep();
    expect(database.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'timed_conversation'
    `).get()).toEqual({ state: 'expired' });
    expect(database.prepare(`
      SELECT status FROM runtime_permission_confirmations WHERE control_id = ?
    `).get(pending.control_id)).toEqual({ status: 'expired' });
    expect(JSON.parse(database.prepare(`
      SELECT final_result_json FROM runtime_permission_controls WHERE control_id = ?
    `).get(pending.control_id).final_result_json)).toMatchObject({
      status: 'expired',
      reason: 'confirmation_deadline',
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_audit
      WHERE action IN ('permission_expired', 'permission_confirmation_expired')
    `).get().count).toBe(2);

    await runtimeService.close();
    expect(cancelPermissionSweep).toHaveBeenCalledTimes(1);
    database.close();
  });

  test('reconciles already-expired permission state immediately on service restart', async () => {
    const database = openDatabase();
    const file = database.prepare('PRAGMA database_list').all()
      .find(({ name }) => name === 'main').file;
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const options = {
      now,
      generateId: deterministicIds('restart-expiry-inbound'),
    };
    acceptNormalInbound(database, envelope(
      'restart-expiry-timed', '/permission trusted 1m',
    ), options);
    const pending = acceptNormalInbound(database, envelope(
      'restart-expiry-confirmation',
      '/permission trusted --bot',
      { actorId: 'bot-owner', roles: ['member', 'bot_owner'] },
    ), options);
    database.close();

    clock.value = '2026-07-20T01:11:00Z';
    const reopened = new Database(file);
    const runtimeService = createExecutorService({
      database: reopened,
      adapter: { async *execute() {} },
      provider: 'claude',
      serviceInstanceId: 'restart-permission-expiry',
      now,
      generateId: deterministicIds('restart-expiry-service'),
      schedulePermissionSweep() {
        return { unref() {} };
      },
      cancelPermissionSweep() {},
    });
    runtimeService.start();

    expect(reopened.prepare(`
      SELECT state FROM runtime_permission_grants WHERE grant_kind = 'timed_conversation'
    `).get()).toEqual({ state: 'expired' });
    expect(reopened.prepare(`
      SELECT status FROM runtime_permission_confirmations WHERE control_id = ?
    `).get(pending.control_id)).toEqual({ status: 'expired' });
    expect(JSON.parse(reopened.prepare(`
      SELECT final_result_json FROM runtime_permission_controls WHERE control_id = ?
    `).get(pending.control_id).final_result_json)).toMatchObject({
      status: 'expired',
      reason: 'confirmation_deadline',
    });
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_audit
      WHERE action IN ('permission_expired', 'permission_confirmation_expired')
    `).get().count).toBe(2);
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox WHERE aggregate_type = 'security_notice'
    `).get().count).toBe(4);

    await runtimeService.close();
    reopened.close();
  });
});

describe('executor protected-action integration', () => {
  test('auto-allows trusted actions and routes safe actions through the interactive handler', async () => {
    const database = openDatabase();
    const clock = { value: '2026-07-20T01:00:00Z' };
    const now = () => clock.value;
    const generateId = deterministicIds('executor-policy');
    const options = { now, generateId };
    const decisions = [];
    const permissionHandler = jest.fn(async () => ({
      behavior: 'deny',
      message: 'Interactive approval denied.',
      interrupt: false,
    }));
    const adapter = {
      async *execute(_context, controls) {
        decisions.push(await controls.requestPermission({
          tool_name: 'shell',
          input: { command: 'protected' },
        }));
        yield { type: 'turn_result', outcome: 'completed' };
      },
    };

    acceptNormalInbound(database, envelope(
      'executor-command',
      '/permission trusted',
      { actorId: 'executor-actor' },
    ), options);
    const trustedTurn = acceptNormalInbound(database, envelope(
      'executor-trusted',
      'trusted work',
      { actorId: 'executor-actor' },
    ), options);
    clock.value = '2026-07-20T01:01:00Z';
    const service = createExecutorService({
      database,
      adapter,
      provider: 'claude',
      serviceInstanceId: 'permission-executor-service',
      now,
      generateId,
      permissionHandler,
    });
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: trustedTurn.turn_id,
    });
    expect(decisions[0]).toMatchObject({
      behavior: 'allow',
      permission_basis: 'next_turn',
    });
    expect(permissionHandler).not.toHaveBeenCalled();

    acceptNormalInbound(database, envelope(
      'executor-safe-command',
      '/permission safe',
      { actorId: 'executor-actor' },
    ), options);
    const safeTurn = acceptNormalInbound(database, envelope(
      'executor-safe',
      'safe work',
      { actorId: 'executor-actor' },
    ), options);
    clock.value = '2026-07-20T01:02:00Z';
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'completed',
      turn_id: safeTurn.turn_id,
    });
    expect(decisions[1]).toMatchObject({ behavior: 'deny' });
    expect(permissionHandler).toHaveBeenCalledTimes(1);

    await service.close();
    expect(database.prepare(`
      SELECT outcome, COUNT(*) AS count
      FROM runtime_permission_action_decisions
      GROUP BY outcome
      ORDER BY outcome
    `).all()).toEqual([
      { outcome: 'requires_approval', count: 1 },
      { outcome: 'trusted', count: 1 },
    ]);
    database.close();
  });
});
