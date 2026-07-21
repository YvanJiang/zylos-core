import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/web-console/node_modules/better-sqlite3/lib/index.js';
import {
  DeliveryMailbox,
  openDb,
  SessionStore,
  PersistentUploadRegistry,
} from '../skills/web-console/scripts/db.js';
import { createDrainBarrier } from '../skills/web-console/scripts/core-outbox-owner.js';

let tempDir;
let db;
const TEST_MAILBOX_SCOPE = Object.freeze({
  region: 'global', tenantId: 'test-tenant', botId: 'test-bot',
});

beforeEach(() => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wc-db-test-'));
  db = openDb(path.join(tempDir, 'test.db'));
});

afterEach(() => {
  if (db) db.close();
  if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true });
});

describe('SessionStore', () => {
  test('creates, checks, touches, and deletes sessions', () => {
    const store = new SessionStore(db);
    const token = store.create();

    expect(typeof token).toBe('string');
    expect(token.length).toBe(64);
    expect(store.has(token)).toBe(true);
    expect(store.has('nonexistent')).toBe(false);

    store.touch(token);
    expect(store.has(token)).toBe(true);

    store.delete(token);
    expect(store.has(token)).toBe(false);
  });

  test('persists sessions across store instances', () => {
    const store1 = new SessionStore(db);
    const token = store1.create();
    expect(store1.has(token)).toBe(true);

    const store2 = new SessionStore(db);
    expect(store2.has(token)).toBe(true);
  });

  test('rejects stale tokens at auth time without explicit cleanup', () => {
    const store = new SessionStore(db, { maxAgeMs: 100 });
    const token = store.create();
    expect(store.has(token)).toBe(true);

    db.prepare('UPDATE sessions SET last_seen_at = ? WHERE token = ?')
      .run(Date.now() - 200, token);

    expect(store.has(token)).toBe(false);
    expect(db.prepare('SELECT 1 FROM sessions WHERE token = ?').get(token)).toBeUndefined();
  });

  test('cleanup removes all expired sessions in bulk', () => {
    const store = new SessionStore(db, { maxAgeMs: 100 });
    const token = store.create();
    expect(store.has(token)).toBe(true);

    db.prepare('UPDATE sessions SET last_seen_at = ? WHERE token = ?')
      .run(Date.now() - 200, token);

    store.cleanup();
    expect(store.has(token)).toBe(false);
  });

  test('maxAgeSec returns seconds', () => {
    const store = new SessionStore(db, { maxAgeMs: 7 * 24 * 60 * 60 * 1000 });
    expect(store.maxAgeSec).toBe(604800);
  });
});

describe('PersistentUploadRegistry', () => {
  test('add, getMany, consumeMany work like in-memory registry', () => {
    const registry = new PersistentUploadRegistry(db, { ttlMs: 30000 });
    const entry = registry.add({
      sessionId: 's1',
      path: '/tmp/a.txt',
      name: 'a.txt',
      size: 100,
      sizeLabel: '100B',
      mime: 'text/plain',
      kind: 'file'
    });

    expect(entry.id).toBeDefined();
    expect(registry.getMany([entry.id], 's1')).toHaveLength(1);
    expect(registry.getMany([entry.id], 's2')).toHaveLength(0);
    expect(registry.getMany([entry.id, entry.id], 's1')).toHaveLength(0);

    const consumed = registry.consumeMany([entry.id], 's1');
    expect(consumed).toHaveLength(1);
    expect(consumed[0].path).toBe('/tmp/a.txt');

    expect(registry.consumeMany([entry.id], 's1')).toBeNull();
  });

  test('restoreMany re-enables consumed entries', () => {
    const registry = new PersistentUploadRegistry(db, { ttlMs: 30000 });
    const entry = registry.add({ sessionId: 's1', path: '/tmp/b.txt', name: 'b.txt', size: 50, kind: 'file' });
    const consumed = registry.consumeMany([entry.id], 's1');

    expect(registry.consumeMany([entry.id], 's1')).toBeNull();
    registry.restoreMany(consumed);
    expect(registry.getMany([entry.id], 's1')).toHaveLength(1);
  });

  test('expired entries are cleaned up', () => {
    const registry = new PersistentUploadRegistry(db, { ttlMs: 100 });
    const entry = registry.add({ sessionId: 's1', path: '/tmp/c.txt', name: 'c.txt', size: 10, kind: 'file' });

    db.prepare('UPDATE uploads SET created_at = ? WHERE id = ?')
      .run(Date.now() - 200, entry.id);

    expect(registry.consumeMany([entry.id], 's1')).toBeNull();
  });

  test('persists uploads across registry instances', () => {
    const reg1 = new PersistentUploadRegistry(db, { ttlMs: 30000 });
    const entry = reg1.add({ sessionId: 's1', path: '/tmp/d.txt', name: 'd.txt', size: 5, kind: 'file' });

    const reg2 = new PersistentUploadRegistry(db, { ttlMs: 30000 });
    expect(reg2.getMany([entry.id], 's1')).toHaveLength(1);
  });
});

describe('DeliveryMailbox', () => {
  test('migrates legacy unscoped rows as hidden records and admits a scoped reprojection', () => {
    const legacyPath = path.join(tempDir, 'legacy-mailbox.db');
    const legacyDb = new Database(legacyPath);
    legacyDb.exec(`
      CREATE TABLE delivery_mailbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_key TEXT NOT NULL UNIQUE,
        delivery_id TEXT UNIQUE,
        direction TEXT NOT NULL,
        channel TEXT NOT NULL,
        endpoint_id TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp TEXT NOT NULL
      );
      INSERT INTO delivery_mailbox (
        source_key, delivery_id, direction, channel, endpoint_id, content, timestamp
      ) VALUES (
        'inbound:legacy-unscoped', NULL, 'in', 'web-console', 'console',
        'legacy scope unknown', '2026-07-21T00:00:00.000Z'
      );
    `);
    legacyDb.close();

    const migrated = openDb(legacyPath);
    const mailbox = new DeliveryMailbox(migrated, TEST_MAILBOX_SCOPE);
    expect(mailbox.list()).toEqual([]);
    mailbox.projectInbound({
      inboundEventId: 'legacy-unscoped', endpointId: 'console', content: 'scoped reprojection',
      timestamp: '2026-07-21T00:00:01.000Z',
    });
    expect(mailbox.list().map(({ content }) => content)).toEqual(['scoped reprojection']);
    expect(migrated.prepare(`
      SELECT region, tenant_id, bot_id FROM delivery_mailbox WHERE content = ?
    `).get('legacy scope unknown')).toEqual({ region: null, tenant_id: null, bot_id: null });
    migrated.close();
  });

  test('isolates inbound and outbound rows across scope reconfiguration and reopen', () => {
    const dbPath = path.join(tempDir, 'test.db');
    const scopeA = { region: 'global', tenantId: 'tenant-a', botId: 'bot-a' };
    const scopeB = { region: 'global', tenantId: 'tenant-b', botId: 'bot-b' };
    const first = new DeliveryMailbox(db, scopeA);
    first.projectInbound({
      inboundEventId: 'shared-inbound-id', endpointId: 'console', content: 'tenant A inbound',
      timestamp: '2026-07-21T00:00:00.000Z',
    });
    first.deliver({
      deliveryId: 'shared-delivery-id', endpointId: 'console', content: 'tenant A outbound',
      timestamp: '2026-07-21T00:00:01.000Z',
    });
    db.close();
    db = openDb(dbPath);

    const second = new DeliveryMailbox(db, scopeB);
    expect(second.list()).toEqual([]);
    second.projectInbound({
      inboundEventId: 'shared-inbound-id', endpointId: 'console', content: 'tenant B inbound',
      timestamp: '2026-07-21T00:00:02.000Z',
    });
    second.deliver({
      deliveryId: 'shared-delivery-id', endpointId: 'console', content: 'tenant B outbound',
      timestamp: '2026-07-21T00:00:03.000Z',
    });
    expect(second.list().map(({ content }) => content)).toEqual([
      'tenant B inbound', 'tenant B outbound',
    ]);
    expect(new DeliveryMailbox(db, scopeA).list().map(({ content }) => content)).toEqual([
      'tenant A inbound', 'tenant A outbound',
    ]);
  });

  test('rejects a nonzero visibility cursor from another mailbox scope', () => {
    const scopeA = { region: 'global', tenantId: 'tenant-a', botId: 'bot-a' };
    const scopeB = { region: 'global', tenantId: 'tenant-b', botId: 'bot-b' };
    const mailboxB = new DeliveryMailbox(db, scopeB);
    mailboxB.projectInbound({
      inboundEventId: 'b-old-inbound', endpointId: 'console', content: 'B inbound',
      timestamp: '2026-07-21T00:00:00.000Z',
    });
    const mailboxA = new DeliveryMailbox(db, scopeA);
    const newestA = mailboxA.deliver({
      deliveryId: 'a-new-outbound', endpointId: 'console', content: 'A outbound',
      timestamp: '2026-07-21T00:00:01.000Z',
    });

    expect(() => mailboxB.list({
      sinceId: newestA.id,
      cursorScope: mailboxA.cursorScope,
    })).toThrow(/cursor scope/i);
    expect(mailboxB.list({ sinceId: 0, cursorScope: mailboxB.cursorScope }))
      .toEqual([expect.objectContaining({ content: 'B inbound' })]);
  });

  test('persists canonical attachment metadata across reopen and fences conflicting replay', () => {
    const dbPath = path.join(tempDir, 'test.db');
    const mailbox = new DeliveryMailbox(db, TEST_MAILBOX_SCOPE);
    const attachment = {
      attachment_id: 'upload-attachment-1',
      kind: 'file',
      name: 'report.txt',
      media_type: 'text/plain',
      size_bytes: 12,
      size_label: '12B',
      href: '/api/inbound-media/wc-2026-07-21-abcdef12.txt',
    };
    mailbox.projectInbound({
      inboundEventId: 'inbound-attachment', endpointId: 'console', content: 'report',
      attachments: [attachment], timestamp: '2026-07-21T00:00:00.000Z',
    });
    db.close();
    db = openDb(dbPath);
    const reopened = new DeliveryMailbox(db, TEST_MAILBOX_SCOPE);
    expect(reopened.list()).toEqual([
      expect.objectContaining({ content: 'report', attachments: [attachment] }),
    ]);
    expect(() => reopened.projectInbound({
      inboundEventId: 'inbound-attachment', endpointId: 'console', content: 'report',
      attachments: [{ ...attachment, href: '/api/inbound-media/wc-other-abcdef12.txt' }],
      timestamp: '2026-07-21T00:00:00.000Z',
    })).toThrow(/conflicts with its durable projection/);
  });

  test('assigns durable monotonic visibility cursors in actual mailbox order', () => {
    const mailbox = new DeliveryMailbox(db, TEST_MAILBOX_SCOPE);
    const firstInbound = mailbox.projectInbound({
      inboundEventId: 'inbound-1', endpointId: 'console', content: 'first',
      timestamp: '2026-07-21T00:00:00.000Z',
    });
    const secondInbound = mailbox.projectInbound({
      inboundEventId: 'inbound-2', endpointId: 'console', content: 'second',
      timestamp: '2026-07-21T00:00:01.000Z',
    });
    const delayedFirstReply = mailbox.deliver({
      deliveryId: 'delivery-1', endpointId: 'console', content: 'first reply',
      timestamp: '2026-07-21T00:00:02.000Z',
    });

    expect([firstInbound.id, secondInbound.id, delayedFirstReply.id]).toEqual([1, 2, 3]);
    expect(mailbox.list({
      sinceId: secondInbound.id, cursorScope: mailbox.cursorScope,
    })).toEqual([
      expect.objectContaining({ id: delayedFirstReply.id, content: 'first reply' }),
    ]);
  });

  test('makes renderer retries idempotent and rejects conflicting replay content', () => {
    const mailbox = new DeliveryMailbox(db, TEST_MAILBOX_SCOPE);
    const delivery = {
      deliveryId: 'delivery-retry', endpointId: 'console', content: 'rendered text',
      timestamp: '2026-07-21T00:00:00.000Z',
    };
    const first = mailbox.deliver(delivery);
    const replay = mailbox.deliver(delivery);
    expect(replay).toEqual(first);
    expect(mailbox.list()).toHaveLength(1);
    expect(() => mailbox.deliver({ ...delivery, content: 'conflicting text' }))
      .toThrow(/conflicts with its durable projection/);
  });

  test('paginates more than 100 unseen rows without a later reply skipping backlog', () => {
    const mailbox = new DeliveryMailbox(db, TEST_MAILBOX_SCOPE);
    for (let index = 1; index <= 150; index += 1) {
      mailbox.projectInbound({
        inboundEventId: `backlog-${index}`, endpointId: 'console', content: `message ${index}`,
        timestamp: new Date(Date.UTC(2026, 6, 21, 0, 0, index)).toISOString(),
      });
    }
    const reply = mailbox.deliver({
      deliveryId: 'backlog-reply', endpointId: 'console', content: 'rendered reply',
      timestamp: '2026-07-21T00:10:00.000Z',
    });
    const firstPage = mailbox.list({ sinceId: 0, limit: 100 });
    const secondPage = mailbox.list({
      sinceId: firstPage.at(-1).id, limit: 100, cursorScope: mailbox.cursorScope,
    });

    expect(firstPage).toHaveLength(100);
    expect(secondPage).toHaveLength(51);
    expect(secondPage.at(-1)).toMatchObject({ id: reply.id, content: 'rendered reply' });
    expect(new Set([...firstPage, ...secondPage].map(({ id }) => id)).size).toBe(151);
  });
});

describe('Web Console drain shutdown barrier', () => {
  test('stop awaits the active fenced result and refuses every later claim', async () => {
    let release;
    let calls = 0;
    const blocked = new Promise((resolve) => { release = resolve; });
    const barrier = createDrainBarrier({
      async drain() {
        calls += 1;
        await blocked;
        return { status: 'delivered', delivered: 1 };
      },
    });
    const active = barrier.run();
    let stopped = false;
    const stopping = barrier.stop().then(() => { stopped = true; });
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(calls).toBe(1);
    release();
    await Promise.all([active, stopping]);
    expect(await barrier.run()).toEqual({ status: 'stopped', delivered: 0 });
    expect(calls).toBe(1);
  });
});
