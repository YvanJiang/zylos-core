import fs from 'node:fs';
import crypto from 'node:crypto';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import vm from 'node:vm';
import { afterEach, beforeEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/web-console/node_modules/better-sqlite3/lib/index.js';
import WebSocket from '../skills/web-console/node_modules/ws/wrapper.mjs';
import { createIdempotencyKey } from '../contracts/public/index.js';
import { acceptCompatibilityInbound } from '../runtime/compatibility/c4-channel-fallback.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';

const SERVER_PATH = path.resolve('skills/web-console/scripts/server.js');
const SQLITE_MODULE = path.resolve('skills/web-console/node_modules/better-sqlite3/lib/index.js');
const RECONCILIATION_PATH = path.resolve(
  'skills/web-console/public/message-reconciliation.js',
);

let ctx;

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

function createDb(dbPath) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.exec(`
    CREATE TABLE conversations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      direction TEXT,
      channel TEXT,
      endpoint_id TEXT,
      content TEXT,
      timestamp TEXT
    );
  `);
  db.close();
}

function createFakeC4Receive(skillsDir, dbPath) {
  const scriptDir = path.join(skillsDir, 'comm-bridge', 'scripts');
  fs.mkdirSync(scriptDir, { recursive: true });
  fs.writeFileSync(path.join(scriptDir, 'c4-receive.js'), `
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from ${JSON.stringify(SQLITE_MODULE)};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
function arg(name) {
  const idx = args.indexOf(name);
  return idx === -1 ? null : args[idx + 1];
}
const channel = arg('--channel');
const endpoint = arg('--endpoint');
const content = arg('--content');
if (!channel || !content) process.exit(2);

const suffixBase = 'reply via: node ' + path.join(__dirname, 'c4-send.js') + ' "' + channel + '"';
const suffix = endpoint ? ' ---- ' + suffixBase + ' "' + endpoint + '"' : ' ---- ' + suffixBase;
const fullMessage = content + suffix;
let dbContent = fullMessage;
if (Buffer.byteLength(fullMessage, 'utf8') > 2048) {
  const msgId = 'test-' + Date.now();
  const dir = path.join(path.dirname(${JSON.stringify(dbPath)}), 'attachments', msgId);
  fs.mkdirSync(dir, { recursive: true });
  const filePath = path.join(dir, 'message.txt');
  fs.writeFileSync(filePath, fullMessage, 'utf8');
  dbContent = content.substring(0, 100) + '\\n\\n[C4] Full message at: ' + filePath + suffix;
}
const db = new Database(${JSON.stringify(dbPath)});
db.prepare('INSERT INTO conversations (direction, channel, endpoint_id, content, timestamp) VALUES (?, ?, ?, ?, ?)')
  .run('in', channel, endpoint, dbContent, new Date().toISOString());
db.close();
`);
  fs.writeFileSync(path.join(scriptDir, 'c4-send.js'), '');
}

async function startServer({
  maxUploadMb = 20,
  actualC4Receive = false,
  region = 'global',
  tenantId = 'default',
  botId = 'zylos',
} = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wc-routes-'));
  const dbPath = path.join(root, 'comm-bridge', 'c4.db');
  const skillsDir = path.join(root, 'skills');
  fs.writeFileSync(path.join(root, '.env'), '');
  createDb(dbPath);
  if (actualC4Receive) {
    fs.mkdirSync(skillsDir, { recursive: true });
  } else {
    createFakeC4Receive(skillsDir, dbPath);
  }
  const port = await freePort();

  const child = spawn(process.execPath, [SERVER_PATH], {
    cwd: path.resolve('.'),
    env: {
      ...process.env,
      ZYLOS_DIR: root,
      WEB_CONSOLE_SKILLS_DIR: actualC4Receive ? path.resolve('skills') : skillsDir,
      WEB_CONSOLE_PORT: String(port),
      WEB_CONSOLE_BIND: '127.0.0.1',
      WEB_CONSOLE_MAX_UPLOAD_MB: String(maxUploadMb),
      ZYLOS_REGION: region,
      ZYLOS_TENANT_ID: tenantId,
      ZYLOS_BOT_ID: botId,
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  let output = '';
  child.stdout.on('data', (chunk) => { output += chunk.toString(); });
  child.stderr.on('data', (chunk) => { output += chunk.toString(); });

  const baseUrl = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`server exited early: ${output}`);
    try {
      const res = await fetch(`${baseUrl}/api/health`);
      if (res.ok) return { root, dbPath, skillsDir, port, baseUrl, child };
    } catch {
      // Retry until server is listening.
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  child.kill('SIGTERM');
  throw new Error(`server did not start: ${output}`);
}

async function stopServer(active) {
  if (!active) return;
  if (active.child.exitCode === null) {
    let forced = false;
    active.child.kill('SIGTERM');
    await new Promise((resolve) => {
      const timeout = setTimeout(() => {
        if (active.child.exitCode === null) {
          forced = true;
          active.child.kill('SIGKILL');
        }
      }, 3000);
      active.child.once('close', () => {
        clearTimeout(timeout);
        resolve();
      });
    });
    if (forced) throw new Error('Web Console did not complete its drain shutdown barrier');
  }
  fs.rmSync(active.root, { recursive: true, force: true });
}

function rows(dbPath) {
  const db = new Database(dbPath);
  const result = db.prepare('SELECT id, direction, channel, endpoint_id, content FROM conversations ORDER BY id ASC').all();
  db.close();
  return result;
}

async function uploadFile(active, { name = 'report.txt', type = 'text/plain', content = 'hello' } = {}) {
  const form = new FormData();
  form.append('file', new Blob([content], { type }), name);
  const res = await fetch(`${active.baseUrl}/api/upload`, { method: 'POST', body: form });
  return { res, body: await res.json() };
}

async function sendHttp(active, payload) {
  const res = await fetch(`${active.baseUrl}/api/send`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  return { res, body: await res.json() };
}

beforeEach(() => {
  ctx = null;
});

afterEach(async () => {
  await stopServer(ctx);
});

describe('web-console attachment routes', () => {
  test('actual mailbox and outbox ownership reject the same endpoint from another Core scope', async () => {
    ctx = await startServer({
      actualC4Receive: true,
      region: 'global',
      tenantId: 'tenant-local',
      botId: 'bot-local',
    });
    const coreDb = new Database(ctx.dbPath);
    const foreign = [
      ['region-foreign', 'tenant-local', 'bot-local', 'region'],
      ['global', 'tenant-foreign', 'bot-local', 'tenant'],
      ['global', 'tenant-local', 'bot-foreign', 'bot'],
    ].map(([region, tenantId, botId, suffix]) => acceptCompatibilityInbound(coreDb, {
      inbound_event_id: `foreign-console-${suffix}`,
      trace_id: `foreign-console-${suffix}-trace`,
      occurred_at: '2026-07-21T00:00:00.000Z', received_at: '2026-07-21T00:00:00.000Z',
      region, tenant_id: tenantId, channel: 'web-console', bot_id: botId,
      chat_type: 'dm', chat_id: 'console', native_thread_or_topic_id: null,
      message_id: `foreign-console-${suffix}-message`,
      actor: { type: 'user', actor_id: 'foreign-user', authenticated: true, roles: [] },
      content: {
        kind: 'text', text: `foreign ${suffix} scope must stay hidden`, attachments: [],
      },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: `foreign-console-${suffix}-source`,
    }, { now: () => '2026-07-21T00:00:00.000Z' }));
    coreDb.close();
    const local = await sendHttp(ctx, {
      message: 'local scope is visible', message_id: 'local-console-scope',
    });
    expect(local.res.status).toBe(200);

    const response = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(response.status).toBe(200);
    const messages = await response.json();
    expect(messages.some(({ content }) => content === 'local scope is visible')).toBe(true);
    expect(messages.some(({ content }) => content.includes('must stay hidden'))).toBe(false);

    const reopened = new Database(ctx.dbPath);
    for (const accepted of foreign) {
      expect(reopened.prepare('SELECT status FROM runtime_outbox WHERE turn_id = ?')
        .get(accepted.turn_id).status).toBe('pending');
    }
    reopened.close();
  });

  test('actual Core ingress projects safe attachment metadata through the durable mailbox', async () => {
    ctx = await startServer({ actualC4Receive: true });
    const upload = await uploadFile(ctx, {
      name: 'quarterly report.txt',
      type: 'text/plain',
      content: 'durable attachment bytes',
    });
    const sent = await sendHttp(ctx, {
      message: 'Review this report',
      attachments: [upload.body.id],
      message_id: 'actual-http-attachment',
    });

    expect(sent.res.status).toBe(200);
    const response = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(response.status).toBe(200);
    const mailbox = await response.json();
    const inbound = mailbox.find(({ direction }) => direction === 'in');
    expect(inbound).toMatchObject({
      channel: 'web-console',
      endpoint_id: 'console',
      content: 'Review this report',
      attachments: [{
        attachment_id: upload.body.id,
        kind: 'file',
        name: 'quarterly report.txt',
        media_type: 'text/plain',
        size_bytes: 24,
        size_label: '24B',
      }],
    });
    expect(inbound.attachments[0].href).toMatch(
      /^\/api\/inbound-media\/wc-.*-[0-9a-f]{8}\.txt$/,
    );
    expect(inbound.content).not.toContain('[attachment:');
    const downloaded = await fetch(`${ctx.baseUrl}${inbound.attachments[0].href}`);
    expect(downloaded.status).toBe(200);
    expect(await downloaded.text()).toBe('durable attachment bytes');
    expect(mailbox.filter(({ direction }) => direction === 'out')).toEqual([
      expect.objectContaining({ attachments: [] }),
    ]);

    const coreDb = new Database(ctx.dbPath);
    const envelope = JSON.parse(coreDb.prepare(`
      SELECT envelope_json FROM runtime_inbound_events
      WHERE inbound_event_id = ?
    `).get(`web-console-${crypto.createHash('sha256')
      .update('actual-http-attachment').digest('hex')}`).envelope_json);
    coreDb.close();
    expect(envelope.content.attachments).toEqual([expect.objectContaining({
      attachment_id: upload.body.id,
      name: 'quarterly report.txt',
      media_type: 'text/plain',
    })]);
    expect(envelope.reply).toEqual({
      root_message_id: null,
      parent_message_id: null,
      reply_to_message_id: null,
    });
  });

  test('actual WebSocket ingress reconciles one matching optimistic attachment message', async () => {
    ctx = await startServer({ actualC4Receive: true });
    const upload = await uploadFile(ctx, {
      name: 'diagram.png',
      type: 'image/png',
      content: 'image payload',
    });
    const received = [];
    const acknowledgements = [];
    const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/`);
    await new Promise((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });
    ws.on('message', (raw) => {
      const event = JSON.parse(raw.toString());
      if (event.type === 'messages') received.push(...event.data);
      if (event.type === 'sent') acknowledgements.push(event);
    });
    ws.send(JSON.stringify({ type: 'subscribe', since_id: 0 }));
    ws.send(JSON.stringify({
      type: 'send',
      content: '',
      attachments: [upload.body.id],
      tempId: 'actual-ws-attachment',
    }));

    const deadline = Date.now() + 5000;
    while ((!acknowledgements.some(({ success }) => success)
      || !received.some((message) => message.direction === 'in'
        && message.attachments?.[0]?.attachment_id === upload.body.id))
      && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    ws.close();
    const canonical = received.find((message) => message.direction === 'in'
      && message.attachments?.[0]?.attachment_id === upload.body.id);
    expect(canonical).toMatchObject({
      content: '',
      attachments: [{ attachment_id: upload.body.id, kind: 'image', name: 'diagram.png' }],
    });

    const browser = {};
    vm.runInNewContext(fs.readFileSync(RECONCILIATION_PATH, 'utf8'), browser);
    const optimistic = [
      { tempId: 'unrelated', content: '', attachments: [{ attachment_id: crypto.randomUUID() }] },
      { tempId: 'actual-ws-attachment', content: '', attachments: [{ attachment_id: upload.body.id }] },
    ];
    const reconcile = browser.ZylosMessageReconciliation.reconcileOptimisticMessages;
    const reconcileElements = browser.ZylosMessageReconciliation.reconcileOptimisticElements;
    const safeHref = browser.ZylosMessageReconciliation.safeAttachmentHref;
    expect(safeHref(canonical.attachments[0].href)).toBe(canonical.attachments[0].href);
    expect(safeHref('javascript:alert(1)')).toBeNull();
    expect(safeHref('https://attacker.invalid/file')).toBeNull();
    expect(safeHref('/api/inbound-media/../../secret')).toBeNull();
    expect(reconcile(optimistic, canonical)).toBe(true);
    expect(optimistic.map(({ tempId }) => tempId)).toEqual(['unrelated']);
    expect(reconcile(optimistic, canonical)).toBe(false);
    expect(optimistic.map(({ tempId }) => tempId)).toEqual(['unrelated']);

    const rows = [
      { dataset: { rawContent: '', attachmentKey: JSON.stringify([crypto.randomUUID()]) } },
      { dataset: { rawContent: '', attachmentKey: JSON.stringify([upload.body.id]) } },
    ];
    for (const row of rows) {
      row.remove = () => rows.splice(rows.indexOf(row), 1);
    }
    expect(reconcileElements(rows, canonical)).toBe(true);
    expect(rows).toHaveLength(1);
    expect(reconcileElements(rows, canonical)).toBe(false);
    expect(rows).toHaveLength(1);
  });

  test('Core mailbox projection rejects unowned attachment paths and URLs without blocking safe ingress', async () => {
    ctx = await startServer({ actualC4Receive: true });
    const upload = await uploadFile(ctx, {
      name: 'safe.txt', type: 'text/plain', content: 'safe bytes',
    });
    const coreDb = new Database(ctx.dbPath);
    acceptCompatibilityInbound(coreDb, {
      inbound_event_id: 'unsafe-web-attachment', trace_id: 'unsafe-web-attachment-trace',
      occurred_at: '2026-07-21T00:00:00.000Z', received_at: '2026-07-21T00:00:00.000Z',
      region: 'global', tenant_id: 'default', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null, message_id: 'unsafe-web-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: {
        kind: 'mixed',
        text: 'never display\n[attachment:file /private/etc/passwd name="safe.txt" 10B]',
        attachments: [{
          attachment_id: upload.body.id,
          media_type: 'text/plain',
          name: 'safe.txt',
          content_ref: 'https://attacker.invalid/secret',
          size_bytes: 10,
          href: 'javascript:alert(1)',
        }],
      },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'unsafe-web-source',
    }, { now: () => '2026-07-21T00:00:00.000Z' });
    coreDb.close();

    const firstPoll = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(firstPoll.status).toBe(200);
    expect((await firstPoll.json()).some(({ content }) => content.includes('never display'))).toBe(false);

    const sent = await sendHttp(ctx, {
      message: 'safe display',
      attachments: [upload.body.id],
      message_id: 'safe-after-unsafe-attachment',
    });
    expect(sent.res.status).toBe(200);
    const safePoll = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(safePoll.status).toBe(200);
    const messages = await safePoll.json();
    expect(messages.some(({ content }) => content.includes('never display'))).toBe(false);
    expect(messages).toEqual(expect.arrayContaining([
      expect.objectContaining({
        direction: 'in',
        content: 'safe display',
        attachments: [expect.objectContaining({
          attachment_id: upload.body.id,
          href: expect.stringMatching(/^\/api\/inbound-media\/wc-/),
        })],
      }),
    ]));
    expect(JSON.stringify(messages)).not.toContain('javascript:');
    expect(JSON.stringify(messages)).not.toContain('/private/etc/passwd');
    expect(JSON.stringify(messages)).not.toContain('attacker.invalid');
  });

  test('POST /api/upload stores a UUID-named file and returns metadata', async () => {
    ctx = await startServer();
    const { res, body } = await uploadFile(ctx, { name: '../bad name.txt', content: 'abc' });

    expect(res.status).toBe(200);
    expect(body).toMatchObject({
      name: 'bad name.txt',
      size: 3,
      mime: 'text/plain',
      kind: 'file'
    });
    expect(body.id).toMatch(/^[0-9a-f-]{36}$/);

    const files = fs.readdirSync(path.join(ctx.root, 'web-console', 'media'));
    expect(files).toHaveLength(1);
    expect(files[0]).toMatch(/^wc-.*-[0-9a-f]{8}\.txt$/);
  });

  test('POST /api/upload returns 413 for oversized files', async () => {
    ctx = await startServer({ maxUploadMb: 1 });
    const form = new FormData();
    form.append('file', new Blob(['x'.repeat(1024 * 1024 + 1)], { type: 'text/plain' }), 'large.txt');

    const res = await fetch(`${ctx.baseUrl}/api/upload`, { method: 'POST', body: form });
    const body = await res.json();

    expect(res.status).toBe(413);
    expect(body.error).toBe('upload_too_large');
  });

  test('HTTP attachment-only send queues verbatim annotation content', async () => {
    ctx = await startServer();
    const upload = await uploadFile(ctx, { name: 'report.txt', content: 'abc' });
    const sent = await sendHttp(ctx, { message: '', attachments: [upload.body.id] });

    expect(sent.res.status).toBe(200);
    const latest = rows(ctx.dbPath).at(-1);
    expect(latest.content).toContain('[attachment:file ');
    expect(latest.content).toContain('name="report.txt" 3B]');
    expect(latest.content).toContain(' ---- reply via: node ');
    expect(latest.content).not.toContain('[C4] Full message');
  });

  test('WS attachment-only send queues verbatim annotation content', async () => {
    ctx = await startServer();
    const upload = await uploadFile(ctx, { name: 'image.png', type: 'image/png', content: 'pngbytes' });

    await new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/`);
      const timer = setTimeout(() => reject(new Error('timed out waiting for sent ack')), 3000);
      ws.on('open', () => {
        ws.send(JSON.stringify({ type: 'subscribe', since_id: 0 }));
        ws.send(JSON.stringify({ type: 'send', content: '', attachments: [upload.body.id], tempId: 't1' }));
      });
      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type !== 'sent') return;
        clearTimeout(timer);
        ws.close();
        msg.success ? resolve() : reject(new Error(msg.error || 'send failed'));
      });
      ws.on('error', reject);
    });

    const latest = rows(ctx.dbPath).at(-1);
    expect(latest.content).toContain('[attachment:image ');
    expect(latest.content).toContain('name="image.png" 8B]');
  });

  test('concurrent sends with the same upload id deliver exactly once', async () => {
    ctx = await startServer();
    const upload = await uploadFile(ctx, { name: 'report.txt', content: 'abc' });
    const payload = { message: '', attachments: [upload.body.id] };

    const results = await Promise.all([
      sendHttp(ctx, payload),
      sendHttp(ctx, payload)
    ]);

    const statuses = results.map((result) => result.res.status).sort();
    expect(statuses).toEqual([200, 400]);
    expect(results.map((result) => result.body.error).filter(Boolean)).toEqual(['invalid_attachment']);

    const queuedRows = rows(ctx.dbPath);
    expect(queuedRows).toHaveLength(1);
    expect(queuedRows[0].content).toContain('[attachment:file ');
    expect(queuedRows[0].content).toContain('name="report.txt" 3B]');
  });

  test('GET /api/media/:messageId fails closed for retired legacy media rows', async () => {
    ctx = await startServer();
    const imagePath = path.join(ctx.root, 'out.png');
    fs.writeFileSync(imagePath, Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
    const filePath = path.join(ctx.root, 'out.txt');
    fs.writeFileSync(filePath, 'download');
    const escapePath = path.join(ctx.root, 'escape');
    fs.symlinkSync('/etc/passwd', escapePath);

    const db = new Database(ctx.dbPath);
    const insert = db.prepare('INSERT INTO conversations (direction, channel, endpoint_id, content, timestamp) VALUES (?, ?, ?, ?, ?)');
    const imageId = insert.run('out', 'web-console', 'console', `[MEDIA:image]${imagePath}`, new Date().toISOString()).lastInsertRowid;
    const fileId = insert.run('out', 'web-console', 'console', `[MEDIA:file]${filePath}`, new Date().toISOString()).lastInsertRowid;
    const inRowId = insert.run('in', 'web-console', 'console', `[MEDIA:file]${filePath}`, new Date().toISOString()).lastInsertRowid;
    const wrongChannelId = insert.run('out', 'telegram', 'console', `[MEDIA:file]${filePath}`, new Date().toISOString()).lastInsertRowid;
    const wrongEndpointId = insert.run('out', 'web-console', 'other', `[MEDIA:file]${filePath}`, new Date().toISOString()).lastInsertRowid;
    const notMediaId = insert.run('out', 'web-console', 'console', `hello [MEDIA:file]${filePath}`, new Date().toISOString()).lastInsertRowid;
    const escapeId = insert.run('out', 'web-console', 'console', `[MEDIA:file]${escapePath}`, new Date().toISOString()).lastInsertRowid;
    db.close();

    for (const id of [
      999999, imageId, fileId, inRowId, wrongChannelId, wrongEndpointId, notMediaId, escapeId,
    ]) {
      const res = await fetch(`${ctx.baseUrl}/api/media/${id}`);
      expect(res.status).toBe(404);
    }
  });

  test('GET /api/inbound-media/:filename serves uploaded files from media dir', async () => {
    ctx = await startServer();
    const pngBytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00]);
    const mediaDir = path.join(ctx.root, 'web-console', 'media');

    const pngFile = path.join(mediaDir, 'wc-test-image.png');
    fs.writeFileSync(pngFile, pngBytes);
    const txtFile = path.join(mediaDir, 'wc-test-doc.txt');
    fs.writeFileSync(txtFile, 'hello');

    const imgRes = await fetch(`${ctx.baseUrl}/api/inbound-media/wc-test-image.png`);
    expect(imgRes.status).toBe(200);
    expect(imgRes.headers.get('content-type')).toBe('image/png');
    expect(imgRes.headers.get('content-disposition')).toContain('inline');

    const txtRes = await fetch(`${ctx.baseUrl}/api/inbound-media/wc-test-doc.txt`);
    expect(txtRes.status).toBe(200);
    expect(txtRes.headers.get('content-type')).toBe('application/octet-stream');
    expect(txtRes.headers.get('content-disposition')).toContain('attachment');
    expect(await txtRes.text()).toBe('hello');

    const missing = await fetch(`${ctx.baseUrl}/api/inbound-media/nonexistent.png`);
    expect(missing.status).toBe(404);

    const traversal = await fetch(`${ctx.baseUrl}/api/inbound-media/..%2F..%2F.env`);
    expect(traversal.status).toBe(404);
  });

  test('display queries ignore every retired legacy conversation row', async () => {
    ctx = await startServer();

    const db = new Database(ctx.dbPath);
    const insert = db.prepare('INSERT INTO conversations (direction, channel, endpoint_id, content, timestamp) VALUES (?, ?, ?, ?, ?)');
    insert.run('out', 'web-console', 'console', 'visible console message', new Date().toISOString());
    insert.run('out', 'web-console', null, 'null endpoint stays visible', new Date().toISOString());
    insert.run('out', 'void', 'session-handoff', 'internal handoff summary', new Date().toISOString());
    db.close();

    // History is projected only from delivered Core outbox commands.
    const recentRes = await fetch(`${ctx.baseUrl}/api/conversations/recent?limit=50`);
    expect(recentRes.status).toBe(200);
    const recentContents = (await recentRes.json()).map((row) => row.content);
    expect(recentContents).toEqual([]);

    // The parameterized channel query must not expose void even when asked directly.
    const voidRes = await fetch(`${ctx.baseUrl}/api/conversations?channel=void&limit=50`);
    expect(voidRes.status).toBe(200);
    expect(await voidRes.json()).toEqual([]);

    // A legacy table cannot become a fallback for the default query.
    const defaultRes = await fetch(`${ctx.baseUrl}/api/conversations?limit=50`);
    const defaultContents = (await defaultRes.json()).map((row) => row.content);
    expect(defaultContents).toEqual([]);
  });

  test('conversation history projects canonical Core inbound and delivered outbox facts', async () => {
    ctx = await startServer({ tenantId: 'web-history-tenant' });
    const db = new Database(ctx.dbPath);
    const accepted = acceptCompatibilityInbound(db, {
      inbound_event_id: 'web-history-event', trace_id: 'web-history-trace',
      occurred_at: '2020-01-01T00:00:00.000Z', received_at: '2020-01-01T00:00:00.000Z',
      region: 'global', tenant_id: 'web-history-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null, message_id: 'web-history-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: { kind: 'text', text: 'canonical inbound text', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'web-history-source',
    }, { now: () => '2020-01-01T00:00:00.000Z' });
    db.close();

    const poll = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(poll.status).toBe(200);
    const deliveredDb = new Database(ctx.dbPath);
    expect(deliveredDb.prepare('SELECT status FROM runtime_outbox WHERE turn_id = ?')
      .get(accepted.turn_id).status).toBe('delivered');
    deliveredDb.close();

    const response = await fetch(`${ctx.baseUrl}/api/conversations/recent?limit=10`);
    expect(response.status).toBe(200);
    const history = await response.json();
    expect(history.map(({ direction, content }) => ({ direction, content }))).toEqual([
      { direction: 'in', content: 'canonical inbound text' },
      { direction: 'out', content: expect.stringContaining('Message received.') },
    ]);
  });

  test('mailbox projection preserves Core queue FIFO when accepted timestamps run backward', async () => {
    ctx = await startServer({ tenantId: 'web-fifo-tenant' });
    const db = new Database(ctx.dbPath);
    const base = {
      region: 'global', tenant_id: 'web-fifo-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null,
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
    };
    acceptCompatibilityInbound(db, {
      ...base,
      inbound_event_id: 'web-fifo-first', trace_id: 'web-fifo-first-trace',
      occurred_at: '2026-07-21T00:00:02.000Z', received_at: '2026-07-21T00:00:02.000Z',
      message_id: 'web-fifo-first-message',
      content: { kind: 'text', text: 'accepted first', attachments: [] },
      source_ref: 'web-fifo-first-source',
    }, { now: () => '2026-07-21T00:00:02.000Z' });
    acceptCompatibilityInbound(db, {
      ...base,
      inbound_event_id: 'web-fifo-second', trace_id: 'web-fifo-second-trace',
      occurred_at: '2026-07-21T00:00:01.000Z', received_at: '2026-07-21T00:00:01.000Z',
      message_id: 'web-fifo-second-message',
      content: { kind: 'text', text: 'accepted second', attachments: [] },
      source_ref: 'web-fifo-second-source',
    }, { now: () => '2026-07-21T00:00:01.000Z' });
    db.close();

    const messages = await (await fetch(`${ctx.baseUrl}/api/poll?since_id=0`)).json();
    expect(messages.filter(({ direction }) => direction === 'in').map(({ content }) => content))
      .toEqual(['accepted first', 'accepted second']);
  });

  test('HTTP polling consumes and renders the authoritative Core outbox without WebSocket clients', async () => {
    ctx = await startServer({ tenantId: 'web-poll-tenant' });
    const db = new Database(ctx.dbPath);
    const accepted = acceptCompatibilityInbound(db, {
      inbound_event_id: 'web-poll-event', trace_id: 'web-poll-trace',
      occurred_at: '2020-01-01T00:00:00.000Z', received_at: '2020-01-01T00:00:00.000Z',
      region: 'global', tenant_id: 'web-poll-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null, message_id: 'web-poll-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: { kind: 'text', text: 'poll-only inbound', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'web-poll-source',
    }, { now: () => '2020-01-01T00:00:00.000Z' });
    expect(db.prepare('SELECT status FROM runtime_outbox WHERE turn_id = ?')
      .get(accepted.turn_id).status).toBe('pending');
    db.close();

    const invalidCursor = await fetch(`${ctx.baseUrl}/api/poll?since_id=-1`);
    expect(invalidCursor.status).toBe(400);
    const unchanged = new Database(ctx.dbPath);
    expect(unchanged.prepare('SELECT status FROM runtime_outbox WHERE turn_id = ?')
      .get(accepted.turn_id).status).toBe('pending');
    unchanged.close();

    const response = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(response.status).toBe(200);
    const messages = await response.json();

    const reopened = new Database(ctx.dbPath);
    const deliveryState = reopened.prepare(`
      SELECT outbox.status, outbox.attempt_count, outbox.command_json
      FROM runtime_outbox AS outbox WHERE outbox.turn_id = ?
    `).get(accepted.turn_id);
    expect(deliveryState).toMatchObject({ status: 'delivered', attempt_count: 1 });
    reopened.close();
    expect(messages.map(({ direction, content }) => ({ direction, content }))).toEqual([
      { direction: 'in', content: 'poll-only inbound' },
      { direction: 'out', content: expect.stringContaining('Message received.') },
    ]);
  });

  test('HTTP mailbox pagination exposes more than 100 mixed rows without a cursor gap', async () => {
    ctx = await startServer();
    const mailboxDb = new Database(path.join(ctx.root, 'web-console', 'web-console.db'));
    const scopeKey = crypto.createHash('sha256').update(JSON.stringify([
      'global', 'default', 'zylos', 'web-console', 'console',
    ])).digest('hex');
    const insert = mailboxDb.prepare(`
      INSERT INTO delivery_mailbox (
        source_key, delivery_id, direction, channel, endpoint_id,
        region, tenant_id, bot_id, content, timestamp
      ) VALUES (?, ?, ?, 'web-console', 'console', 'global', 'default', 'zylos', ?, ?)
    `);
    mailboxDb.transaction(() => {
      for (let index = 1; index <= 151; index += 1) {
        const outbound = index % 3 === 0;
        insert.run(
          `scope:${scopeKey}:route-backlog:${index}`,
          outbound ? `scope:${scopeKey}:route-delivery:${index}` : null,
          outbound ? 'out' : 'in',
          `route message ${index}`,
          new Date(Date.UTC(2026, 6, 21, 0, 0, index)).toISOString(),
        );
      }
    })();
    mailboxDb.close();

    const first = await (await fetch(`${ctx.baseUrl}/api/poll?since_id=0`)).json();
    const second = await (await fetch(
      `${ctx.baseUrl}/api/poll?since_id=${first.at(-1).id}`,
    )).json();
    const all = [...first, ...second];
    expect(first).toHaveLength(100);
    expect(second).toHaveLength(51);
    expect(all.map(({ id }) => id)).toEqual(Array.from({ length: 151 }, (_, index) => index + 1));
    expect(new Set(all.map(({ direction }) => direction))).toEqual(new Set(['in', 'out']));
  });

  test('HTTP polling renders a durable security notice with no turn event', async () => {
    ctx = await startServer({ tenantId: 'web-security-tenant' });
    const db = new Database(ctx.dbPath);
    const envelope = {
      contract: 'zylos.inbound-envelope', contract_version: '1.0',
      inbound_event_id: 'web-security-event', trace_id: 'web-security-trace',
      occurred_at: '2020-01-01T00:00:00.000Z', received_at: '2020-01-01T00:00:00.000Z',
      region: 'global', tenant_id: 'web-security-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null, message_id: 'web-security-message',
      actor: { type: 'user', actor_id: 'web-owner', authenticated: true, roles: ['bot_owner'] },
      content: { kind: 'text', text: '/permission safe', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source: { kind: 'platform_original', source_ref: 'web-security-source' },
    };
    envelope.idempotency_key = createIdempotencyKey('inbound', {
      region: envelope.region, tenant_id: envelope.tenant_id, channel: envelope.channel,
      bot_id: envelope.bot_id, inbound_event_id: envelope.inbound_event_id,
    });
    const accepted = acceptNormalInbound(db, envelope, {
      now: () => '2020-01-01T00:00:00.000Z',
    });
    const row = db.prepare(`
      SELECT outbox_id, command_json FROM runtime_outbox WHERE control_id = ?
    `).get(accepted.control_id);
    const command = JSON.parse(row.command_json);
    expect(command.mapping.turn_id).toBeNull();
    expect(command.event_sequence_through).toBeNull();
    db.close();

    const response = await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    expect(response.status).toBe(200);
    const delivered = await response.json();
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toMatchObject({ direction: 'out' });
    expect(delivered[0].content).toContain(command.render_model.text);

    const reopened = new Database(ctx.dbPath);
    expect(reopened.prepare('SELECT status FROM runtime_outbox WHERE outbox_id = ?')
      .get(row.outbox_id).status).toBe('delivered');
    reopened.close();
  });

  test('WebSocket delivery projects the durable inbound before its outbox reply', async () => {
    ctx = await startServer({ tenantId: 'web-ws-order-tenant' });
    const messages = [];
    const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/`);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out opening WebSocket')), 3000);
      ws.once('open', () => {
        clearTimeout(timer);
        resolve();
      });
      ws.once('error', reject);
    });
    ws.on('message', (raw) => {
      const event = JSON.parse(raw.toString());
      if (event.type === 'messages') messages.push(...event.data);
    });
    ws.send(JSON.stringify({ type: 'subscribe', since_id: 0 }));

    const db = new Database(ctx.dbPath);
    acceptCompatibilityInbound(db, {
      inbound_event_id: 'web-ws-order-event', trace_id: 'web-ws-order-trace',
      occurred_at: '2020-01-01T00:00:00.000Z', received_at: '2020-01-01T00:00:00.000Z',
      region: 'global', tenant_id: 'web-ws-order-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console',
      native_thread_or_topic_id: null, message_id: 'web-ws-order-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: { kind: 'text', text: 'ordered inbound', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'web-ws-order-source',
    }, { now: () => '2020-01-01T00:00:00.000Z' });
    db.close();

    const deadline = Date.now() + 5000;
    while ((!messages.some(({ direction }) => direction === 'in')
      || !messages.some(({ direction }) => direction === 'out'))
      && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    ws.close();
    expect(messages[0]).toMatchObject({ direction: 'in', content: 'ordered inbound' });
    expect(messages.slice(1).some(({ direction }) => direction === 'out')).toBe(true);
  });

  test('WebSocket clients advance independent durable projection cursors', async () => {
    ctx = await startServer({ tenantId: 'web-cursor-tenant' });
    const seedDb = new Database(ctx.dbPath);
    acceptCompatibilityInbound(seedDb, {
      inbound_event_id: 'web-cursor-seed', trace_id: 'web-cursor-seed-trace',
      occurred_at: '2020-01-01T00:00:00.000Z', received_at: '2020-01-01T00:00:00.000Z',
      region: 'global', tenant_id: 'web-cursor-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console', native_thread_or_topic_id: null,
      message_id: 'web-cursor-seed-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: { kind: 'text', text: 'cursor history only', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'web-cursor-seed-source',
    }, { now: () => '2020-01-01T00:00:00.000Z' });
    seedDb.close();
    await fetch(`${ctx.baseUrl}/api/poll?since_id=0`);
    const history = await (await fetch(`${ctx.baseUrl}/api/conversations/recent?limit=100`)).json();
    const historyCursor = Math.max(...history.map(({ id }) => id));

    async function subscribedClient(sinceId) {
      const received = [];
      const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/`);
      await new Promise((resolve, reject) => {
        ws.once('open', resolve);
        ws.once('error', reject);
      });
      ws.on('message', (raw) => {
        const event = JSON.parse(raw.toString());
        if (event.type === 'messages') received.push(...event.data);
      });
      ws.send(JSON.stringify({ type: 'subscribe', since_id: sinceId }));
      return { ws, received };
    }

    const fromBeginning = await subscribedClient(0);
    const fromCurrent = await subscribedClient(historyCursor);
    await new Promise((resolve) => setTimeout(resolve, 200));

    const nextDb = new Database(ctx.dbPath);
    acceptCompatibilityInbound(nextDb, {
      inbound_event_id: 'web-cursor-next', trace_id: 'web-cursor-next-trace',
      occurred_at: '2020-01-01T00:00:01.000Z', received_at: '2020-01-01T00:00:01.000Z',
      region: 'global', tenant_id: 'web-cursor-tenant', channel: 'web-console',
      bot_id: 'zylos', chat_type: 'dm', chat_id: 'console', native_thread_or_topic_id: null,
      message_id: 'web-cursor-next-message',
      actor: { type: 'user', actor_id: 'web-user', authenticated: true, roles: [] },
      content: { kind: 'text', text: 'cursor visible to both', attachments: [] },
      reply: { root_message_id: null, parent_message_id: null, reply_to_message_id: null },
      source_ref: 'web-cursor-next-source',
    }, { now: () => '2020-01-01T00:00:01.000Z' });
    nextDb.close();

    const deadline = Date.now() + 5000;
    while ((!fromBeginning.received.some(({ content }) => content === 'cursor visible to both')
      || !fromCurrent.received.some(({ content }) => content === 'cursor visible to both'))
      && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    fromBeginning.ws.close();
    fromCurrent.ws.close();

    expect(fromBeginning.received.some(({ content }) => content === 'cursor history only')).toBe(true);
    expect(fromCurrent.received.some(({ content }) => content === 'cursor history only')).toBe(false);
    expect(fromBeginning.received.some(({ content }) => content === 'cursor visible to both')).toBe(true);
    expect(fromCurrent.received.some(({ content }) => content === 'cursor visible to both')).toBe(true);
  });

  test('GET /api/conversations/recent does not project retired inbound rows', async () => {
    ctx = await startServer();
    const mediaDir = path.join(ctx.root, 'web-console', 'media');
    const imgFile = path.join(mediaDir, 'wc-uploaded.png');
    fs.writeFileSync(imgFile, Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));

    const db = new Database(ctx.dbPath);
    db.prepare('INSERT INTO conversations (direction, channel, endpoint_id, content, timestamp) VALUES (?, ?, ?, ?, ?)')
      .run('in', 'web-console', 'console',
        `hello\n[attachment:image ${imgFile} name="screenshot.png" 8B]`,
        new Date().toISOString());
    db.close();

    const res = await fetch(`${ctx.baseUrl}/api/conversations/recent?limit=10`);
    const conversations = await res.json();
    expect(conversations).toEqual([]);
  });
});
