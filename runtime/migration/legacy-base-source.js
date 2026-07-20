/** One-time exact-base source extraction and execution fencing. */

const FENCE_PREFIX = 'zylos_executor_migration_fence_';

function tableExists(database, table) {
  return database.prepare(`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?
  `).get(table) !== undefined;
}

function safeTime(value, fallback) {
  if (typeof value !== 'string') return fallback;
  const parsed = Date.parse(value.includes('T') ? value : `${value.replace(' ', 'T')}Z`);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : fallback;
}

function parseLegacyRoute(row) {
  if (typeof row.channel !== 'string' || row.channel.length === 0
    || typeof row.endpoint_id !== 'string' || row.endpoint_id.length === 0) return null;
  const endpoint = row.endpoint_id;
  if (!['feishu', 'lark'].includes(row.channel)) {
    return {
      chat_type: 'dm', chat_id: endpoint, native_thread_or_topic_id: null,
      native_thread_root_message_id: null, native_thread_reply_target_message_id: null,
      message_id: `legacy-c4-message:${row.id}`,
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
    };
  }
  const parts = endpoint.split('|');
  if (parts.length === 1 || parts[0].length === 0) return null;
  const facts = {};
  const allowed = new Set(['type', 'root', 'parent', 'msg', 'thread']);
  for (const encoded of parts.slice(1)) {
    const separator = encoded.indexOf(':');
    const key = separator > 0 ? encoded.slice(0, separator) : '';
    const value = separator > 0 ? encoded.slice(separator + 1) : '';
    if (!allowed.has(key) || value.length === 0 || Object.hasOwn(facts, key)) return null;
    facts[key] = value;
  }
  const legacyType = facts.type;
  if (!['p2p', 'group'].includes(legacyType) || typeof facts.msg !== 'string') return null;
  const hasThreadFacts = facts.root !== undefined || facts.parent !== undefined
    || facts.thread !== undefined;
  if (hasThreadFacts) {
    if (legacyType !== 'group' || typeof facts.root !== 'string'
      || typeof facts.thread !== 'string') return null;
    return {
      chat_type: 'thread', chat_id: parts[0], native_thread_or_topic_id: facts.thread,
      native_thread_root_message_id: facts.root,
      native_thread_reply_target_message_id: facts.msg,
      message_id: facts.msg,
      reply: {
        root_message_id: facts.root,
        parent_message_id: facts.parent ?? facts.root,
        reply_to_message_id: facts.parent ?? facts.root,
      },
    };
  }
  return {
    chat_type: legacyType === 'p2p' ? 'dm' : 'group',
    chat_id: parts[0], native_thread_or_topic_id: null,
    native_thread_root_message_id: null, native_thread_reply_target_message_id: null,
    message_id: facts.msg,
    reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
  };
}

function notificationTarget(row) {
  const route = parseLegacyRoute(row);
  if (route === null) return null;
  return {
    region: 'global', tenant_id: 'legacy-exact-base', channel: row.channel,
    bot_id: 'legacy-exact-base', chat_type: route.chat_type, chat_id: route.chat_id,
    native_thread_or_topic_id: route.native_thread_or_topic_id,
    native_thread_root_message_id: route.native_thread_root_message_id,
    native_thread_reply_target_message_id: route.native_thread_reply_target_message_id,
  };
}

function pendingEnvelope(row, route, batchId, recordId, observedAt) {
  return {
    contract: 'zylos.inbound-envelope', contract_version: '1.0',
    inbound_event_id: `legacy-c4-event:${row.id}`,
    idempotency_key: `legacy-c4:${recordId}`,
    trace_id: `legacy-c4-trace:${row.id}`,
    occurred_at: safeTime(row.timestamp, observedAt), received_at: observedAt,
    region: 'global', tenant_id: 'legacy-exact-base',
    channel: row.channel, bot_id: 'legacy-exact-base',
    chat_type: route.chat_type, chat_id: route.chat_id,
    native_thread_or_topic_id: route.native_thread_or_topic_id,
    message_id: route.message_id,
    actor: { type: 'system', actor_id: 'legacy-c4-migrator', authenticated: true, roles: [] },
    content: { kind: 'text', text: row.content, attachments: [] },
    reply: route.reply,
    source: { kind: 'legacy_compat', source_ref: `migration:${batchId}:${recordId}` },
    legacy: { legacy_record_id: recordId, legacy_state: 'pending', migration_batch_id: batchId },
  };
}

function installFence(database) {
  for (const table of ['conversations', 'control_queue']) {
    if (!tableExists(database, table)) continue;
    for (const operation of ['INSERT', 'UPDATE', 'DELETE']) {
      const name = `${FENCE_PREFIX}${table}_${operation.toLowerCase()}`;
      database.exec(`
        CREATE TRIGGER IF NOT EXISTS ${name}
        BEFORE ${operation} ON ${table}
        BEGIN SELECT RAISE(ABORT, 'legacy source fenced by executor migration'); END
      `);
    }
  }
}

export function dropLegacyBaseIngressFence(database) {
  const triggers = database.prepare(`
    SELECT name FROM sqlite_master WHERE type = 'trigger' AND name GLOB ? ORDER BY name
  `).all(`${FENCE_PREFIX}*`);
  for (const { name } of triggers) database.exec(`DROP TRIGGER "${name.replaceAll('"', '""')}"`);
  return Object.freeze({ removed_fences: triggers.map(({ name }) => name) });
}

export function extractAndFenceLegacyBaseBatch({
  database,
  batchId,
  provider,
  providerActive = false,
  observedAt = new Date().toISOString(),
}) {
  if (typeof batchId !== 'string' || batchId.length === 0) throw new TypeError('batchId is required');
  if (!['claude', 'codex'].includes(provider)) throw new TypeError('provider is invalid');
  return database.transaction(() => {
    installFence(database);
    const conversations = tableExists(database, 'conversations')
      ? database.prepare(`
        SELECT id, timestamp, direction, channel, endpoint_id, content, status
        FROM conversations ORDER BY id
      `).all()
      : [];
    const controls = tableExists(database, 'control_queue')
      ? database.prepare(`
        SELECT id, content, status, created_at, updated_at FROM control_queue ORDER BY id
      `).all()
      : [];
    let sequence = 0;
    const records = [];
    for (const row of conversations) {
      const recordId = `conversation:${row.id}`;
      const state = ['pending', 'running', 'delivered', 'failed'].includes(row.status)
        ? row.status : 'failed';
      if (row.direction === 'in' && state === 'pending') {
        const target = parseLegacyRoute(row);
        const route = target === null ? 'ambiguous' : 'unique';
        if (route === 'unique') sequence += 1;
        records.push({
          kind: 'c4', legacy_record_id: recordId, legacy_state: 'pending', route,
          ...(route === 'unique' ? {
            legacy_queue_sequence: sequence,
            envelope: pendingEnvelope(row, target, batchId, recordId, observedAt),
          } : {}),
        });
      } else if (row.direction === 'in' && state === 'running') {
        const target = notificationTarget(row);
        if (target === null) {
          throw new Error(`Running legacy C4 record ${row.id} has no durable notification target.`);
        }
        records.push({
          kind: 'c4', legacy_record_id: recordId, legacy_state: 'running',
          notification_target: target,
        });
      } else {
        records.push({
          kind: 'c4', legacy_record_id: recordId,
          legacy_state: state === 'running' ? 'delivered' : state,
          history: {
            direction: row.direction, channel: row.channel,
            endpoint_id: row.endpoint_id, content: row.content,
            occurred_at: safeTime(row.timestamp, observedAt), final_status: state,
          },
        });
      }
    }
    for (const row of controls) {
      records.push({
        kind: 'runtime_control', legacy_record_id: `control:${row.id}`,
        legacy_state: typeof row.status === 'string' ? row.status : 'failed',
        audit: { content: row.content, created_at: row.created_at, updated_at: row.updated_at },
      });
    }
    const recentTargetRow = [...conversations].reverse().find((row) => notificationTarget(row) !== null);
    if (providerActive && recentTargetRow === undefined) {
      throw new Error('Active legacy provider has no durable user notification target.');
    }
    if (providerActive) {
      records.push({
        kind: 'c4', legacy_record_id: `provider-session:${provider}`,
        legacy_state: 'running', notification_target: notificationTarget(recentTargetRow),
      });
    }
    return Object.freeze({ batch_id: batchId, records: Object.freeze(records) });
  }).immediate();
}

export function reconcileLegacyBaseRollback({ database, rollbackBatch }) {
  if (!rollbackBatch || !Array.isArray(rollbackBatch.records)) {
    throw new TypeError('rollbackBatch must contain records');
  }
  return database.transaction(() => {
    dropLegacyBaseIngressFence(database);
    if (tableExists(database, 'conversations')) {
      database.prepare(`
        UPDATE conversations SET status = 'failed'
        WHERE direction = 'in' AND status IN ('pending', 'running')
      `).run();
      const restore = database.prepare(`
        UPDATE conversations SET status = 'pending'
        WHERE id = ? AND direction = 'in' AND status = 'failed'
      `);
      for (const record of rollbackBatch.records) {
        if (record.kind !== 'c4' || record.legacy_state !== 'pending'
          || !record.legacy_record_id.startsWith('conversation:')) continue;
        const id = Number(record.legacy_record_id.slice('conversation:'.length));
        if (Number.isSafeInteger(id) && id > 0) restore.run(id);
      }
    }
    if (tableExists(database, 'control_queue')) {
      database.prepare(`
        UPDATE control_queue SET status = 'failed', last_error = 'executor_migration_rollback_invalidated'
        WHERE status IN ('pending', 'running')
      `).run();
    }
    return Object.freeze({ rollback_reconciled: true });
  }).immediate();
}
