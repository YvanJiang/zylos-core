import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import {
  acceptScheduledOccurrence,
  decideScheduledOccurrence,
} from '../runtime/scheduler/scheduler-queue.js';

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-scheduler-queue-'));
  temporaryDirectories.push(directory);
  return { database: new Database(path.join(directory, 'c4.db')), directory };
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function manualEnvelope(suffix) {
  const inboundEventId = `manual-event-${suffix}`;
  return {
    contract: 'zylos.inbound-envelope',
    contract_version: '1.0',
    inbound_event_id: inboundEventId,
    idempotency_key: createIdempotencyKey('inbound', {
      region: 'global', tenant_id: 'tenant-1', channel: 'telegram', bot_id: 'bot-1',
      inbound_event_id: inboundEventId,
    }),
    trace_id: `manual-trace-${suffix}`,
    occurred_at: '2026-07-20T00:00:00.000Z',
    received_at: '2026-07-20T00:00:01.000Z',
    region: 'global', tenant_id: 'tenant-1', channel: 'telegram', bot_id: 'bot-1',
    chat_type: 'group', chat_id: 'group-1', native_thread_or_topic_id: null,
    message_id: `manual-message-${suffix}`,
    actor: { type: 'user', actor_id: 'user-1', authenticated: true, roles: ['member'] },
    content: { kind: 'text', text: 'Human message', attachments: [] },
    reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
    source: { kind: 'platform_original', source_ref: null },
  };
}

function occurrence(suffix, overrides = {}) {
  return {
    schedule_id: 'schedule-1',
    task_id: 'task-1',
    occurrence_id: `occurrence-${suffix}`,
    prompt: `Run scheduled work ${suffix}.`,
    occurred_at: '2026-07-20T00:01:00.000Z',
    received_at: '2026-07-20T00:01:01.000Z',
    region: 'global', tenant_id: 'tenant-1', bot_id: 'bot-1',
    bound_conversation: {
      channel: 'telegram', chat_type: 'group', chat_id: 'group-1',
      native_thread_or_topic_id: null, message_id: 'schedule-anchor-1',
    },
    ...overrides,
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('scheduler conversation queue admission', () => {
  test('places a chat-bound occurrence after human work in the current conversation lineage and replays safely after SQLite reopen', () => {
    const { database, directory } = openTestDatabase();
    const human = acceptNormalInbound(database, manualEnvelope('first'), {
      now: () => '2026-07-20T00:00:02.000Z', generateId: deterministicIds('human'),
    });
    const accepted = acceptScheduledOccurrence(database, occurrence('chat-bound'), {
      now: () => '2026-07-20T00:01:02.000Z', generateId: deterministicIds('scheduled'),
    });

    expect(accepted).toMatchObject({ status: 'accepted', deduplicated: false });
    expect(accepted.conversation_id).toBe(human.conversation_id);
    expect(accepted.lineage_id).toBe(human.lineage_id);
    expect(database.prepare(`
      SELECT queue_sequence FROM runtime_turns WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ queue_sequence: 2 });
    expect(database.prepare(`
      SELECT actor_id, basis_kind FROM runtime_turn_permissions WHERE turn_id = ?
    `).get(accepted.turn_id)).toEqual({ actor_id: 'schedule-1', basis_kind: 'default_safe' });
    expect(database.prepare(`
      SELECT status, bound_conversation, turn_id FROM runtime_scheduler_occurrences
      WHERE schedule_id = ? AND occurrence_id = ?
    `).get('schedule-1', 'occurrence-chat-bound')).toEqual({
      status: 'accepted', bound_conversation: 1, turn_id: accepted.turn_id,
    });

    database.close();
    const restarted = new Database(path.join(directory, 'c4.db'));
    const replayed = acceptScheduledOccurrence(restarted, occurrence('chat-bound'), {
      now: () => { throw new Error('a duplicate occurrence must not commit'); },
      generateId: () => { throw new Error('a duplicate occurrence must not allocate IDs'); },
    });
    expect(replayed).toEqual({ ...accepted, deduplicated: true });
    expect(restarted.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count).toBe(2);
    restarted.close();
  });

  test('uses one stable synthetic conversation and independent lineage per system task while retaining region, tenant, permission, and audit facts', () => {
    const { database } = openTestDatabase();
    const first = acceptScheduledOccurrence(database, occurrence('system-first', {
      task_id: 'nightly-report', bound_conversation: null,
    }), { now: () => '2026-07-20T01:00:00.000Z', generateId: deterministicIds('system-first') });
    const second = acceptScheduledOccurrence(database, occurrence('system-second', {
      task_id: 'nightly-report', bound_conversation: null,
    }), { now: () => '2026-07-20T01:01:00.000Z', generateId: deterministicIds('system-second') });
    const other = acceptScheduledOccurrence(database, occurrence('system-other', {
      task_id: 'weekly-report', bound_conversation: null,
    }), { now: () => '2026-07-20T01:02:00.000Z', generateId: deterministicIds('system-other') });

    expect(second.conversation_id).toBe(first.conversation_id);
    expect(second.lineage_id).toBe(first.lineage_id);
    expect(other.conversation_id).not.toBe(first.conversation_id);
    expect(other.lineage_id).not.toBe(first.lineage_id);
    expect(database.prepare(`
      SELECT region, tenant_id, chat_type, chat_id FROM runtime_conversations
      WHERE conversation_id = ?
    `).get(first.conversation_id)).toEqual({
      region: 'global', tenant_id: 'tenant-1', chat_type: 'synthetic',
      chat_id: 'scheduler:bot-1:nightly-report',
    });
    expect(database.prepare(`
      SELECT mode, basis_kind FROM runtime_turn_permissions WHERE turn_id = ?
    `).get(first.turn_id)).toEqual({ mode: 'safe', basis_kind: 'default_safe' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_scheduler_occurrences
      WHERE schedule_id = 'schedule-1'
    `).get().count).toBe(3);
    database.close();
  });

  test('persists queue-full scheduled work and its visible explanation atomically', () => {
    const { database } = openTestDatabase();
    acceptScheduledOccurrence(database, occurrence('first'), {
      now: () => '2026-07-20T02:00:00.000Z', generateId: deterministicIds('capacity-first'),
      maxQueuedTurns: 1,
    });
    const rejected = acceptScheduledOccurrence(database, occurrence('full'), {
      now: () => '2026-07-20T02:01:00.000Z', generateId: deterministicIds('capacity-full'),
      maxQueuedTurns: 1,
    });
    expect(rejected).toMatchObject({ status: 'rejected', error: { code: 'queue_full' } });
    const notification = JSON.parse(database.prepare(`
      SELECT command_json FROM runtime_outbox WHERE turn_id = ?
    `).get(rejected.turn_id).command_json);
    expect(notification.render_model).toMatchObject({
      phase: 'failed', text: 'The conversation queue is full.', terminal: true,
    });
    expect(database.prepare(`
      SELECT status, turn_id FROM runtime_scheduler_occurrences
      WHERE occurrence_id = 'occurrence-full'
    `).get()).toEqual({ status: 'rejected', turn_id: rejected.turn_id });
    database.close();
  });

  test('rolls back scheduler audit together with the inbound turn and notification when audit persistence fails', () => {
    const { database } = openTestDatabase();
    acceptNormalInbound(database, manualEnvelope('schema'), {
      now: () => '2026-07-20T02:30:00.000Z', generateId: deterministicIds('schema'),
    });
    database.exec(`
      CREATE TRIGGER reject_scheduler_audit
      BEFORE INSERT ON runtime_scheduler_occurrences
      BEGIN SELECT RAISE(ABORT, 'scheduler audit unavailable'); END;
    `);
    expect(() => acceptScheduledOccurrence(database, occurrence('audit-rollback'), {
      now: () => '2026-07-20T02:31:00.000Z', generateId: deterministicIds('audit-rollback'),
    })).toThrow('scheduler audit unavailable');
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turns
    `).get().count).toBe(1);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_inbound_events
      WHERE inbound_event_id = 'occurrence-audit-rollback'
    `).get().count).toBe(0);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox
      WHERE aggregate_id != (SELECT turn_id FROM runtime_turns LIMIT 1)
    `).get().count).toBe(0);
    database.close();
  });

  test('does not turn a missed recurring schedule into catch-up occurrences across UTC boundaries', () => {
    expect(decideScheduledOccurrence({
      schedule_type: 'recurring', scheduled_for: '2026-07-19T23:00:00.000Z',
      now: '2026-07-20T01:00:00.000Z', miss_threshold_ms: 300_000,
    })).toEqual({ status: 'skipped', reason: 'missed_occurrence' });
    expect(decideScheduledOccurrence({
      schedule_type: 'recurring', scheduled_for: '2026-07-20T00:59:00.000Z',
      now: '2026-07-20T01:00:00.000Z', miss_threshold_ms: 300_000,
    })).toEqual({ status: 'enqueue', reason: null });
  });
});
