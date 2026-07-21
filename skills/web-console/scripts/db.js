import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';

const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
const WC_DATA_DIR = path.join(ZYLOS_DIR, 'web-console');
const DB_PATH = path.join(WC_DATA_DIR, 'web-console.db');

const SESSION_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const UPLOAD_TTL_MS = 30 * 60 * 1000;

function openDb(dbPath = DB_PATH) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      created_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS uploads (
      id TEXT PRIMARY KEY,
      session_token TEXT,
      path TEXT NOT NULL,
      name TEXT NOT NULL,
      size INTEGER NOT NULL,
      size_label TEXT,
      mime TEXT,
      kind TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      consumed INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS scoped_uploads (
      id TEXT PRIMARY KEY,
      session_token TEXT,
      path TEXT NOT NULL,
      name TEXT NOT NULL,
      size INTEGER NOT NULL,
      size_label TEXT,
      mime TEXT,
      kind TEXT NOT NULL,
      region TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      bot_id TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      consumed INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS delivery_mailbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_key TEXT NOT NULL UNIQUE,
      delivery_id TEXT UNIQUE,
      direction TEXT NOT NULL CHECK(direction IN ('in', 'out')),
      channel TEXT NOT NULL,
      endpoint_id TEXT NOT NULL,
      region TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      bot_id TEXT NOT NULL,
      content TEXT NOT NULL,
      attachments_json TEXT NOT NULL DEFAULT '[]',
      timestamp TEXT NOT NULL
    );
  `);
  const mailboxColumns = new Set(
    db.prepare('PRAGMA table_info(delivery_mailbox)').all().map(({ name }) => name),
  );
  if (!mailboxColumns.has('attachments_json')) {
    db.exec("ALTER TABLE delivery_mailbox ADD COLUMN attachments_json TEXT NOT NULL DEFAULT '[]'");
  }
  for (const column of ['region', 'tenant_id', 'bot_id']) {
    if (!mailboxColumns.has(column)) {
      db.exec(`ALTER TABLE delivery_mailbox ADD COLUMN ${column} TEXT`);
    }
  }
  db.exec(`
    CREATE INDEX IF NOT EXISTS delivery_mailbox_scope_cursor
    ON delivery_mailbox (region, tenant_id, bot_id, channel, endpoint_id, id)
  `);
  const uploadColumns = new Set(
    db.prepare('PRAGMA table_info(uploads)').all().map(({ name }) => name),
  );
  const candidateScopeColumns = ['region', 'tenant_id', 'bot_id'];
  if (candidateScopeColumns.some((column) => uploadColumns.has(column))) {
    db.transaction(() => {
      if (candidateScopeColumns.every((column) => uploadColumns.has(column))) {
        db.exec(`INSERT OR IGNORE INTO scoped_uploads (
          id, session_token, path, name, size, size_label, mime, kind,
          region, tenant_id, bot_id, created_at, consumed
        )
        SELECT id, session_token, path, name, size, size_label, mime, kind,
          region, tenant_id, bot_id, created_at, consumed
        FROM uploads
        WHERE region IS NOT NULL AND tenant_id IS NOT NULL AND bot_id IS NOT NULL`);
      }
      db.exec(`CREATE TABLE uploads_rollback_compatible (
          id TEXT PRIMARY KEY,
          session_token TEXT,
          path TEXT NOT NULL,
          name TEXT NOT NULL,
          size INTEGER NOT NULL,
          size_label TEXT,
          mime TEXT,
          kind TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          consumed INTEGER NOT NULL DEFAULT 0
        )`);
      const legacyPredicate = candidateScopeColumns.every((column) => uploadColumns.has(column))
        ? 'WHERE region IS NULL OR tenant_id IS NULL OR bot_id IS NULL' : '';
      db.exec(`INSERT OR IGNORE INTO uploads_rollback_compatible (
          id, session_token, path, name, size, size_label, mime, kind, created_at, consumed
        )
        SELECT id, session_token, path, name, size, size_label, mime, kind, created_at, consumed
        FROM uploads
        ${legacyPredicate};
        DROP TABLE uploads;
        ALTER TABLE uploads_rollback_compatible RENAME TO uploads`);
    })();
  }
  db.exec(`
    CREATE INDEX IF NOT EXISTS scoped_uploads_scope_capability
    ON scoped_uploads (region, tenant_id, bot_id, id, session_token, consumed);
    CREATE INDEX IF NOT EXISTS scoped_uploads_scope_path
    ON scoped_uploads (region, tenant_id, bot_id, path)
  `);
  return db;
}

const MAILBOX_ATTACHMENT_FIELDS = Object.freeze([
  'attachment_id', 'kind', 'name', 'media_type', 'size_bytes', 'size_label', 'href',
]);

function serializeMailboxAttachments(attachments) {
  if (!Array.isArray(attachments)) throw new TypeError('attachments must be an array');
  const normalized = attachments.map((attachment) => {
    if (!attachment || typeof attachment !== 'object' || Array.isArray(attachment)
      || Object.keys(attachment).some((field) => !MAILBOX_ATTACHMENT_FIELDS.includes(field))
      || MAILBOX_ATTACHMENT_FIELDS.some((field) => !Object.hasOwn(attachment, field))) {
      throw new TypeError('mailbox attachment has invalid fields');
    }
    if (typeof attachment.attachment_id !== 'string' || attachment.attachment_id.length === 0
      || !['image', 'file'].includes(attachment.kind)
      || typeof attachment.name !== 'string' || attachment.name.length === 0
      || typeof attachment.media_type !== 'string' || attachment.media_type.length === 0
      || !Number.isSafeInteger(attachment.size_bytes) || attachment.size_bytes < 0
      || typeof attachment.size_label !== 'string'
      || !/^\/api\/inbound-media\/wc-[A-Za-z0-9._-]+$/.test(attachment.href)) {
      throw new TypeError('mailbox attachment metadata is invalid');
    }
    return Object.fromEntries(MAILBOX_ATTACHMENT_FIELDS.map((field) => [field, attachment[field]]));
  });
  return JSON.stringify(normalized);
}

export class DeliveryMailbox {
  constructor(db, { region, tenantId, botId }) {
    for (const [fieldName, value] of [
      ['region', region], ['tenantId', tenantId], ['botId', botId],
    ]) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${fieldName} must be a non-empty string`);
      }
    }
    this.db = db;
    this.region = region;
    this.tenantId = tenantId;
    this.botId = botId;
    this.channel = 'web-console';
    this.endpointId = 'console';
    this.scopeKey = crypto.createHash('sha256').update(JSON.stringify([
      region, tenantId, botId, this.channel, this.endpointId,
    ])).digest('hex');
    this.cursorScope = `web-console-mailbox-v1:${this.scopeKey}`;
    this._insert = db.prepare(`
      INSERT OR IGNORE INTO delivery_mailbox (
        source_key, delivery_id, direction, channel, endpoint_id,
        region, tenant_id, bot_id, content, attachments_json, timestamp
      ) VALUES (?, ?, ?, 'web-console', 'console', ?, ?, ?, ?, ?, ?)
    `);
    this._bySource = db.prepare(`
      SELECT * FROM delivery_mailbox
      WHERE source_key = ? AND channel = 'web-console' AND endpoint_id = 'console'
        AND region = ? AND tenant_id = ? AND bot_id = ?
    `);
    this._hasInboundAttachment = db.prepare(`
      SELECT 1
      FROM delivery_mailbox AS mailbox
      JOIN json_each(mailbox.attachments_json) AS attachment
      WHERE mailbox.direction = 'in'
        AND mailbox.channel = 'web-console' AND mailbox.endpoint_id = 'console'
        AND mailbox.region = ? AND mailbox.tenant_id = ? AND mailbox.bot_id = ?
        AND json_extract(attachment.value, '$.attachment_id') = ?
        AND json_extract(attachment.value, '$.href') = ?
      LIMIT 1
    `);
  }

  _store({
    sourceKey, deliveryId = null, direction, endpointId, content, attachments = [], timestamp,
  }) {
    if (typeof sourceKey !== 'string' || sourceKey.length === 0) {
      throw new TypeError('sourceKey must be a non-empty string');
    }
    if (!['in', 'out'].includes(direction)) throw new TypeError('direction must be in or out');
    if (typeof endpointId !== 'string' || endpointId.length === 0) {
      throw new TypeError('endpointId must be a non-empty string');
    }
    if (endpointId !== this.endpointId) {
      throw new TypeError('endpointId does not match this Web Console mailbox owner');
    }
    if (typeof content !== 'string') throw new TypeError('content must be a string');
    if (typeof timestamp !== 'string' || Number.isNaN(Date.parse(timestamp))) {
      throw new TypeError('timestamp must be an ISO timestamp');
    }
    const attachmentsJson = serializeMailboxAttachments(attachments);
    const scopedSourceKey = `scope:${this.scopeKey}:${sourceKey}`;
    const scopedDeliveryId = deliveryId === null
      ? null : `scope:${this.scopeKey}:${deliveryId}`;
    this._insert.run(
      scopedSourceKey, scopedDeliveryId, direction,
      this.region, this.tenantId, this.botId, content, attachmentsJson, timestamp,
    );
    const row = this._bySource.get(
      scopedSourceKey, this.region, this.tenantId, this.botId,
    );
    if (!row || row.delivery_id !== scopedDeliveryId || row.direction !== direction
      || row.endpoint_id !== endpointId || row.content !== content
      || row.attachments_json !== attachmentsJson) {
      throw new Error('A Web Console mailbox source conflicts with its durable projection.');
    }
    return Object.freeze({
      id: row.id,
      direction: row.direction,
      channel: row.channel,
      endpoint_id: row.endpoint_id,
      content: row.content,
      attachments: JSON.parse(row.attachments_json),
      timestamp: row.timestamp,
      platform_message_id: `web-console-mailbox:${row.id}`,
    });
  }

  projectInbound({ inboundEventId, endpointId, content, attachments = [], timestamp }) {
    return this._store({
      sourceKey: `inbound:${inboundEventId}`,
      direction: 'in', endpointId, content, attachments, timestamp,
    });
  }

  hasInboundEvent(inboundEventId) {
    if (typeof inboundEventId !== 'string' || inboundEventId.length === 0) return false;
    const row = this._bySource.get(
      `scope:${this.scopeKey}:inbound:${inboundEventId}`,
      this.region, this.tenantId, this.botId,
    );
    return row?.direction === 'in';
  }

  deliver({ deliveryId, endpointId, content, timestamp }) {
    if (typeof deliveryId !== 'string' || deliveryId.length === 0) {
      throw new TypeError('deliveryId must be a non-empty string');
    }
    return this._store({
      sourceKey: `delivery:${deliveryId}`,
      deliveryId, direction: 'out', endpointId, content, timestamp,
    });
  }

  hasInboundAttachment({ attachmentId, href }) {
    if (typeof attachmentId !== 'string' || attachmentId.length === 0
      || typeof href !== 'string' || href.length === 0) return false;
    return Boolean(this._hasInboundAttachment.get(
      this.region, this.tenantId, this.botId, attachmentId, href,
    ));
  }

  assertCursorScope(cursorScope) {
    if (typeof cursorScope !== 'string' || cursorScope.length === 0) {
      const error = new Error('The durable mailbox cursor scope is required.');
      error.code = 'mailbox_cursor_scope_required';
      error.status = 409;
      error.cursorScope = this.cursorScope;
      throw error;
    }
    if (cursorScope !== this.cursorScope) {
      const error = new Error('The durable mailbox cursor scope does not match this owner.');
      error.code = 'mailbox_cursor_scope_mismatch';
      error.status = 409;
      error.cursorScope = this.cursorScope;
      throw error;
    }
  }

  list({ sinceId = 0, limit = 100, latest = false, cursorScope = null } = {}) {
    if (!Number.isSafeInteger(sinceId) || sinceId < 0) {
      throw new TypeError('sinceId must be a non-negative safe integer');
    }
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new TypeError('limit must be an integer from 1 to 1000');
    }
    if (cursorScope !== null && cursorScope !== this.cursorScope) {
      const error = new Error('The durable mailbox cursor scope does not match this owner.');
      error.code = 'mailbox_cursor_scope_mismatch';
      error.status = 409;
      error.cursorScope = this.cursorScope;
      throw error;
    }
    if (sinceId > 0 && cursorScope === null) {
      const error = new Error('A nonzero durable mailbox cursor requires its cursor scope.');
      error.code = 'mailbox_cursor_scope_required';
      error.status = 409;
      error.cursorScope = this.cursorScope;
      throw error;
    }
    const rows = this.db.prepare(`
      SELECT id, direction, channel, endpoint_id, content, attachments_json, timestamp
      FROM delivery_mailbox
      WHERE id > ? AND channel = 'web-console' AND endpoint_id = 'console'
        AND region = ? AND tenant_id = ? AND bot_id = ?
      ORDER BY id ${latest ? 'DESC' : 'ASC'}
      LIMIT ?
    `).all(sinceId, this.region, this.tenantId, this.botId, limit);
    const projected = rows.map(({ attachments_json: attachmentsJson, ...row }) => ({
      ...row,
      attachments: JSON.parse(attachmentsJson),
    }));
    return latest ? projected.reverse() : projected;
  }
}

export class SessionStore {
  constructor(db, { maxAgeMs = SESSION_MAX_AGE_MS } = {}) {
    this.db = db;
    this.maxAgeMs = maxAgeMs;
    this._stmts = {
      has: db.prepare('SELECT 1 FROM sessions WHERE token = ? AND last_seen_at >= ?'),
      add: db.prepare('INSERT OR REPLACE INTO sessions (token, created_at, last_seen_at) VALUES (?, ?, ?)'),
      del: db.prepare('DELETE FROM sessions WHERE token = ?'),
      delStale: db.prepare('DELETE FROM sessions WHERE token = ? AND last_seen_at < ?'),
      touch: db.prepare('UPDATE sessions SET last_seen_at = ? WHERE token = ?'),
      cleanup: db.prepare('DELETE FROM sessions WHERE last_seen_at < ?'),
    };
    this.cleanup();
  }

  has(token) {
    const cutoff = Date.now() - this.maxAgeMs;
    if (this._stmts.has.get(token, cutoff)) return true;
    this._stmts.delStale.run(token, cutoff);
    return false;
  }

  add(token) {
    const now = Date.now();
    this._stmts.add.run(token, now, now);
  }

  create() {
    const token = crypto.randomBytes(32).toString('hex');
    this.add(token);
    return token;
  }

  delete(token) {
    this._stmts.del.run(token);
  }

  touch(token) {
    this._stmts.touch.run(Date.now(), token);
  }

  cleanup() {
    this._stmts.cleanup.run(Date.now() - this.maxAgeMs);
  }

  get maxAgeSec() {
    return Math.floor(this.maxAgeMs / 1000);
  }
}

export class PersistentUploadRegistry {
  constructor(db, {
    ttlMs = UPLOAD_TTL_MS, region, tenantId, botId,
  } = {}) {
    for (const [fieldName, value] of [
      ['region', region], ['tenantId', tenantId], ['botId', botId],
    ]) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${fieldName} must be a non-empty string`);
      }
    }
    this.db = db;
    this.ttlMs = ttlMs;
    this.region = region;
    this.tenantId = tenantId;
    this.botId = botId;
    this._stmts = {
      add: db.prepare(`INSERT INTO scoped_uploads (
        id, session_token, path, name, size, size_label, mime, kind,
        region, tenant_id, bot_id, created_at, consumed
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`),
      get: db.prepare(`SELECT * FROM scoped_uploads
        WHERE id = ? AND region = ? AND tenant_id = ? AND bot_id = ? AND consumed = 0`),
      getStored: db.prepare(`SELECT * FROM scoped_uploads
        WHERE id = ? AND region = ? AND tenant_id = ? AND bot_id = ?`),
      getMedia: db.prepare(`SELECT * FROM scoped_uploads
        WHERE path = ? AND region = ? AND tenant_id = ? AND bot_id = ?`),
      consume: db.prepare(`UPDATE scoped_uploads SET consumed = 1
        WHERE id = ? AND region = ? AND tenant_id = ? AND bot_id = ? AND consumed = 0`),
      restore: db.prepare(`UPDATE scoped_uploads SET consumed = 0
        WHERE id = ? AND region = ? AND tenant_id = ? AND bot_id = ?`),
      cleanup: db.prepare(`DELETE FROM scoped_uploads
        WHERE region = ? AND tenant_id = ? AND bot_id = ? AND consumed = 0 AND created_at < ?`),
    };
  }

  add(entry) {
    const id = crypto.randomUUID();
    const now = Date.now();
    this._stmts.add.run(
      id, entry.sessionId || null, entry.path, entry.name,
      entry.size, entry.sizeLabel || null, entry.mime || null,
      entry.kind, this.region, this.tenantId, this.botId, now
    );
    return { ...entry, id };
  }

  cleanup() {
    this._stmts.cleanup.run(
      this.region, this.tenantId, this.botId, Date.now() - this.ttlMs,
    );
  }

  getMany(ids, sessionId) {
    this.cleanup();
    if (!Array.isArray(ids)) return [];
    if (new Set(ids).size !== ids.length) return [];
    const results = [];
    for (const id of ids) {
      const row = this._stmts.get.get(id, this.region, this.tenantId, this.botId);
      if (!row || row.session_token !== sessionId) return [];
      results.push({
        id: row.id,
        sessionId: row.session_token,
        path: row.path,
        name: row.name,
        size: row.size,
        sizeLabel: row.size_label,
        mime: row.mime,
        kind: row.kind,
      });
    }
    return results;
  }

  consumeMany(ids, sessionId) {
    const entries = this.getMany(ids, sessionId);
    if (entries.length !== ids.length) return null;
    for (const id of ids) {
      this._stmts.consume.run(id, this.region, this.tenantId, this.botId);
    }
    return entries;
  }

  restoreMany(entries) {
    for (const entry of entries || []) {
      if (entry?.id) {
        this._stmts.restore.run(entry.id, this.region, this.tenantId, this.botId);
      }
    }
  }

  getForProjection(id) {
    const row = this._stmts.getStored.get(id, this.region, this.tenantId, this.botId);
    if (!row) return null;
    return {
      id: row.id,
      path: row.path,
      name: row.name,
      size: row.size,
      sizeLabel: row.size_label,
      mime: row.mime,
      kind: row.kind,
      consumed: row.consumed === 1,
    };
  }

  getForMediaPath(mediaPath) {
    if (typeof mediaPath !== 'string' || mediaPath.length === 0) return null;
    const row = this._stmts.getMedia.get(
      mediaPath, this.region, this.tenantId, this.botId,
    );
    if (!row) return null;
    return { id: row.id, path: row.path, consumed: row.consumed === 1 };
  }
}

export { openDb, DB_PATH, SESSION_MAX_AGE_MS, UPLOAD_TTL_MS };
