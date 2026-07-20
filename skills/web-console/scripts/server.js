#!/usr/bin/env node
/**
 * Web Console Server with WebSocket
 * Provides HTTP API + WebSocket for real-time browser-based Claude communication
 *
 * Run with PM2: pm2 start server.js --name web-console
 * Default port: 3456
 */

import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import multer from 'multer';
import crypto from 'crypto';
import path from 'path';
import os from 'os';
import fs from 'fs';
import { spawn } from 'child_process';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import {
  MAX_ATTACHMENTS,
  buildAnnotatedContent,
  contentDisposition,
  generateStoredFileName,
  resolveAllowedPathSync,
  sanitizeDisplayName,
  sniffImage,
  uploadKind
} from './attachment-utils.js';
import {
  DeliveryMailbox,
  openDb,
  SessionStore,
  PersistentUploadRegistry,
} from './db.js';
import { readExecutorObservability } from '../../../runtime/observability/executor-snapshot-client.js';
import { projectRuntimeHealth } from '../../../runtime/observability/health-projection.js';
import { createDrainBarrier, createWebConsoleOutboxOwner } from './core-outbox-owner.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.WEB_CONSOLE_PORT || 3456;
const SERVICE_BIRTH_ID = crypto.randomUUID();

// Paths
const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
const SKILLS_DIR = process.env.WEB_CONSOLE_SKILLS_DIR || path.join(os.homedir(), 'zylos', '.claude', 'skills');
const DB_DIR = path.join(ZYLOS_DIR, 'comm-bridge');
const DB_PATH = path.join(DB_DIR, 'c4.db');
const MEDIA_DIR = path.join(ZYLOS_DIR, 'web-console', 'media');
const MAX_UPLOAD_MB = Number.parseInt(process.env.WEB_CONSOLE_MAX_UPLOAD_MB || '20', 10);
const MAX_UPLOAD_BYTES = Math.max(1, MAX_UPLOAD_MB) * 1024 * 1024;
const C4_SCRIPT_DIR = path.join(SKILLS_DIR, 'comm-bridge', 'scripts');

// Paths - __dirname is scripts/, public/ is one level up
const SKILL_ROOT = path.join(__dirname, '..');

// --- Authentication ---
import { parse as parseDotenv } from 'dotenv';

function readEnv() {
  const envPath = path.join(ZYLOS_DIR, '.env');
  try {
    return parseDotenv(fs.readFileSync(envPath, 'utf8'));
  } catch {
    return {};
  }
}

const ENV = readEnv();

function readEnvPassword() {
  return ENV.ZYLOS_WEB_PASSWORD || ENV.WEB_CONSOLE_PASSWORD || '';
}

const AUTH_PASSWORD = readEnvPassword();
const AUTH_ENABLED = AUTH_PASSWORD.length > 0;

const wcDb = openDb();
const sessionStore = new SessionStore(wcDb);
const uploadRegistry = new PersistentUploadRegistry(wcDb);
const deliveryMailbox = new DeliveryMailbox(wcDb);

fs.mkdirSync(MEDIA_DIR, { recursive: true });

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  for (const pair of cookieHeader.split(';')) {
    const [name, ...rest] = pair.trim().split('=');
    if (name) cookies[name] = rest.join('=');
  }
  return cookies;
}

function isAuthenticated(req) {
  if (!AUTH_ENABLED) return true;
  const cookies = parseCookies(req.headers.cookie);
  if (!cookies.wc_session || !sessionStore.has(cookies.wc_session)) return false;
  sessionStore.touch(cookies.wc_session);
  return true;
}

function getSessionId(req) {
  if (!AUTH_ENABLED) return 'local';
  const cookies = parseCookies(req.headers.cookie);
  return cookies.wc_session || null;
}

function authMiddleware(req, res, next) {
  // Auth endpoints are always accessible
  if (req.path === '/auth') return next();
  if (!isAuthenticated(req)) {
    return res.status(401).json({ error: 'Authentication required' });
  }
  next();
}

// Middleware
app.use(express.json());
app.use(express.static(path.join(SKILL_ROOT, 'public')));
app.use('/api', authMiddleware);

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, MEDIA_DIR),
    filename: (_req, file, cb) => cb(null, generateStoredFileName(file.originalname))
  }),
  limits: {
    fileSize: MAX_UPLOAD_BYTES,
    files: 1
  }
});

// Initialize database connection
let db;
try {
  fs.mkdirSync(DB_DIR, { recursive: true });
  db = new Database(DB_PATH, { readonly: false });
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
} catch (err) {
  console.error(`Failed to open database: ${err.message}`);
  process.exit(1);
}

// Track connected WebSocket clients
const clients = new Set();
const clientCursors = new WeakMap();

// Last known state for change detection
let lastStatus = null;

/**
 * Read provider-neutral Core runtime health.
 */
let cachedStatus = null;
let cachedStatusUntil = 0;
let statusReadInFlight = null;

async function readStatus() {
  if (cachedStatus !== null && Date.now() < cachedStatusUntil) return cachedStatus;
  if (statusReadInFlight !== null) return statusReadInFlight;
  statusReadInFlight = (async () => {
    try {
      cachedStatus = projectRuntimeHealth(
        await readExecutorObservability({ zylosDir: ZYLOS_DIR }),
      );
    } catch (err) {
      cachedStatus = {
        contract: 'zylos.observability-health-projection',
        contract_version: '1.0',
        state: 'unavailable',
        service: null,
        executors: null,
        turns: null,
        outbox: null,
        error: { code: 'executor_observability_unavailable', user_message: err.message },
      };
    }
    cachedStatusUntil = Date.now() + 2000;
    return cachedStatus;
  })();
  try {
    return await statusReadInFlight;
  } finally {
    statusReadInFlight = null;
  }
}

/**
 * Idempotently project canonical Core inbound facts into the channel-owned
 * durable mailbox. The mailbox assigns visibility cursors only when a message
 * becomes observable to this channel.
 */
function syncCoreInbound() {
  const rows = db.prepare(`
    SELECT inbound.inbound_event_id,
      conversation.chat_id AS endpoint_id,
      json_extract(inbound.envelope_json, '$.content.text') AS content,
      inbound.committed_at AS timestamp
    FROM runtime_inbound_events AS inbound
    JOIN runtime_turns AS turn ON turn.inbound_event_id = inbound.inbound_event_id
    JOIN runtime_conversations AS conversation
      ON conversation.conversation_id = turn.conversation_id
    WHERE json_extract(inbound.envelope_json, '$.channel') = 'web-console'
      AND conversation.chat_id = 'console'
    ORDER BY inbound.committed_at ASC, inbound.inbound_event_id ASC
  `).all();
  for (const row of rows) {
    deliveryMailbox.projectInbound({
      inboundEventId: row.inbound_event_id,
      endpointId: row.endpoint_id,
      content: row.content ?? '',
      timestamp: row.timestamp,
    });
  }
}

function getMailboxMessages({
  channel = 'web-console', sinceId = 0, limit = 100, latest = false,
} = {}) {
  if (channel !== 'web-console') return [];
  return deliveryMailbox.list({ sinceId, limit, latest });
}

function getNewMessages(sinceId) {
  return getMailboxMessages({ sinceId, limit: 100 });
}

function parseProjectionCursor(value) {
  if (value === undefined) return 0;
  const cursor = Number(value);
  if (!Number.isSafeInteger(cursor) || cursor < 0) {
    const error = new TypeError('since_id must be a non-negative safe integer');
    error.status = 400;
    throw error;
  }
  return cursor;
}

/**
 * Broadcast message to all connected clients
 */
function broadcast(type, data) {
  const message = JSON.stringify({ type, data });
  let delivered = 0;
  for (const client of clients) {
    if (client.readyState === 1) { // WebSocket.OPEN
      try {
        client.send(message);
        if (type === 'messages' && Array.isArray(data) && data.length > 0) {
          clientCursors.set(client, Math.max(
            clientCursors.get(client) ?? 0,
            ...data.map(({ id }) => id),
          ));
        }
        delivered += 1;
      } catch {
        clients.delete(client);
      }
    }
  }
  return delivered;
}

const deliveryOwner = createWebConsoleOutboxOwner({
  database: db,
  serviceInstanceId: `web-console-${SERVICE_BIRTH_ID}`,
  deliverMessage(message, delivery) {
    return deliveryMailbox.deliver({
      deliveryId: delivery.delivery_id,
      endpointId: message.endpoint_id,
      content: message.content,
      timestamp: message.timestamp,
    });
  },
});
const deliveryBarrier = createDrainBarrier({ drain: (options) => deliveryOwner.drain(options) });

async function drainWebOutbox() {
  return deliveryBarrier.run();
}

function normalizeAttachmentIds(value) {
  if (!Array.isArray(value)) return [];
  return value.filter((id) => typeof id === 'string' && id.length > 0);
}

function validateSendPayload(content, attachmentIds) {
  if (attachmentIds.length > MAX_ATTACHMENTS) {
    const err = new Error(`Maximum ${MAX_ATTACHMENTS} attachments per message`);
    err.status = 400;
    err.code = 'too_many_attachments';
    throw err;
  }

  if (!String(content || '').trim() && attachmentIds.length === 0) {
    const err = new Error('Message is required');
    err.status = 400;
    err.code = 'message_required';
    throw err;
  }
}

function buildSendContent(content, attachmentEntries) {
  const message = buildAnnotatedContent(content, attachmentEntries);
  if (!message.trim()) {
    const err = new Error('Message is required');
    err.status = 400;
    err.code = 'message_required';
    throw err;
  }
  return message;
}

function webMessageId(value) {
  if (typeof value !== 'string' || value.length === 0) {
    return `web-console-${crypto.randomUUID()}`;
  }
  return `web-console-${crypto.createHash('sha256').update(value).digest('hex')}`;
}

function sendToC4(content, messageId) {
  const c4Receive = path.join(C4_SCRIPT_DIR, 'c4-receive.js');

  return new Promise((resolve, reject) => {
    const child = spawn('node', [
      c4Receive,
      '--channel', 'web-console',
      '--endpoint', 'console',
      '--message-id', messageId,
      '--actor-id', 'web-console-user',
      '--content', content
    ], { stdio: 'pipe' });

    let stderr = '';
    child.stderr.on('data', (data) => { stderr += data; });
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        const err = new Error(stderr || 'Failed to send message');
        err.status = 500;
        err.code = 'send_failed';
        reject(err);
      }
    });
    child.on('error', (err) => {
      err.status = 500;
      err.code = 'send_failed';
      reject(err);
    });
  });
}

async function sendConsoleMessage({ content, attachmentIds, sessionId, messageId }) {
  const ids = normalizeAttachmentIds(attachmentIds);
  validateSendPayload(content, ids);

  let attachmentEntries = [];
  if (ids.length > 0) {
    attachmentEntries = uploadRegistry.getMany(ids, sessionId);
    if (attachmentEntries.length !== ids.length) {
      const err = new Error('Attachment upload id is invalid or expired');
      err.status = 400;
      err.code = 'invalid_attachment';
      throw err;
    }
  }

  const combined = buildSendContent(content, attachmentEntries);
  if (ids.length > 0) {
    attachmentEntries = uploadRegistry.consumeMany(ids, sessionId);
    if (!attachmentEntries) {
      const err = new Error('Attachment upload id is invalid or expired');
      err.status = 400;
      err.code = 'invalid_attachment';
      throw err;
    }
  }
  try {
    await sendToC4(combined, webMessageId(messageId));
  } catch (err) {
    uploadRegistry.restoreMany(attachmentEntries);
    throw err;
  }
  return {
    content: combined,
    attachments: attachmentEntries.map((entry) => ({
      kind: entry.kind,
      name: entry.name,
      size_label: entry.sizeLabel || null
    }))
  };
}

function jsonError(res, err) {
  return res.status(err.status || 500).json({
    success: false,
    error: err.code || err.message || 'request_failed',
    message: err.message
  });
}

/**
 * Check for status changes and new messages
 */
async function checkUpdates() {
  if (clients.size === 0) return;
  // Check status changes
  const currentStatus = await readStatus();
  if (!lastStatus || currentStatus.snapshot_id !== lastStatus.snapshot_id
    || currentStatus.state !== lastStatus.state) {
    lastStatus = currentStatus;
    broadcast('status', currentStatus);
  }

  function flushClient(client) {
    const cursor = clientCursors.get(client) ?? 0;
    const newMessages = getNewMessages(cursor);
    if (newMessages.length === 0 || client.readyState !== 1) return;
    try {
      client.send(JSON.stringify({ type: 'messages', data: newMessages }));
      clientCursors.set(client, Math.max(cursor, ...newMessages.map(({ id }) => id)));
    } catch {
      clients.delete(client);
    }
  }

  syncCoreInbound();
  for (const client of clients) flushClient(client);
  await drainWebOutbox();
  for (const client of clients) flushClient(client);
}

// Poll only while there are subscribed consumers. This bounds snapshot work
// and avoids advancing durable observability snapshots for an unused console.
let updateInFlight = null;
const updateTimer = setInterval(() => {
  if (clients.size === 0 || updateInFlight !== null) return;
  updateInFlight = checkUpdates().catch(() => {}).finally(() => { updateInFlight = null; });
}, 2000);

/**
 * WebSocket connection handler
 */
wss.on('connection', (ws, req) => {
  // Check auth for WebSocket connections
  if (AUTH_ENABLED) {
    const cookies = parseCookies(req.headers.cookie);
    if (!cookies.wc_session || !sessionStore.has(cookies.wc_session)) {
      ws.close(4001, 'Authentication required');
      return;
    }
    sessionStore.touch(cookies.wc_session);
  }

  console.log('WebSocket client connected');

  // Send current status immediately
  readStatus().then((status) => {
    if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'status', data: status }));
  }).catch(() => {});
  checkUpdates().catch(() => {});

  // Handle client messages
  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);

      if (msg.type === 'subscribe') {
        const sinceId = Number(msg.since_id);
        if (!Number.isSafeInteger(sinceId) || sinceId < 0) {
          ws.close(1008, 'Invalid durable projection cursor');
          return;
        }
        clientCursors.set(ws, sinceId);
        clients.add(ws);
        checkUpdates().catch(() => {});
      } else if (msg.type === 'send') {
        const tempId = msg.tempId; // Track client's temp ID
        try {
          await sendConsoleMessage({
            content: msg.content || '',
            attachmentIds: msg.attachments,
            sessionId: getSessionId(req),
            messageId: tempId,
          });
          ws.send(JSON.stringify({ type: 'sent', success: true, tempId }));
        } catch (err) {
          ws.send(JSON.stringify({
            type: 'sent',
            success: false,
            error: err.code || err.message,
            message: err.message,
            tempId
          }));
        }
      }
    } catch (err) {
      // Ignore invalid messages
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`WebSocket client disconnected (${clients.size} remaining)`);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    clients.delete(ws);
  });
});

/**
 * Get Claude status (HTTP fallback)
 */
app.get('/api/status', (req, res) => {
  readStatus().then((status) => res.json(status)).catch((err) => {
    res.status(503).json({ state: 'unavailable', error: err.message });
  });
});

/**
 * Get conversation history
 */
app.get('/api/conversations', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const channel = req.query.channel || 'web-console';
    syncCoreInbound();
    res.json(getMailboxMessages({ channel, limit, latest: true }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Get all recent conversations (for display)
 */
app.get('/api/conversations/recent', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    syncCoreInbound();
    res.json(getMailboxMessages({ limit, latest: true }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Upload one attachment for a later send call
 */
app.post('/api/upload', (req, res) => {
  upload.single('file')(req, res, (err) => {
    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          success: false,
          error: 'upload_too_large',
          message: `File exceeds ${MAX_UPLOAD_MB}MB limit`
        });
      }
      return res.status(400).json({ success: false, error: 'upload_failed', message: err.message });
    }

    if (!req.file) {
      return res.status(400).json({ success: false, error: 'file_required' });
    }

    const name = sanitizeDisplayName(req.file.originalname, 'attachment');
    const entry = uploadRegistry.add({
      sessionId: getSessionId(req),
      path: req.file.path,
      name,
      size: req.file.size,
      sizeLabel: req.file.size < 1024 ? `${req.file.size}B`
        : req.file.size < 1024 * 1024 ? `${(req.file.size / 1024).toFixed(1)}KB`
          : `${(req.file.size / (1024 * 1024)).toFixed(1)}MB`,
      mime: req.file.mimetype || 'application/octet-stream',
      kind: uploadKind(req.file)
    });

    return res.json({
      id: entry.id,
      name: entry.name,
      size: entry.size,
      mime: entry.mime,
      kind: entry.kind
    });
  });
});

/**
 * Send message to Claude (HTTP fallback)
 */
app.post('/api/send', (req, res) => {
  sendConsoleMessage({
    content: req.body.message || '',
    attachmentIds: req.body.attachments,
    sessionId: getSessionId(req),
    messageId: req.body.message_id,
  }).then(() => {
    res.json({ success: true, message: 'Message sent to Claude' });
  }).catch((err) => jsonError(res, err));
});

/**
 * Serve an outbound media row by message id
 */
app.get('/api/media/:messageId', (req, res) => {
  res.sendStatus(404);
});

/**
 * Serve a user-uploaded inbound media file by filename
 */
app.get('/api/inbound-media/:filename', (req, res) => {
  try {
    const filename = path.basename(req.params.filename);
    if (!filename || filename === '.' || filename === '..') return res.sendStatus(404);
    const target = path.join(MEDIA_DIR, filename);

    const allowedPath = resolveAllowedPathSync(target, [MEDIA_DIR]);
    if (!allowedPath) return res.sendStatus(404);

    let stat;
    try {
      stat = fs.statSync(allowedPath);
    } catch {
      return res.sendStatus(404);
    }
    if (!stat.isFile()) return res.sendStatus(404);

    const fd = fs.openSync(allowedPath, 'r');
    const head = Buffer.alloc(Math.min(16, stat.size));
    fs.readSync(fd, head, 0, head.length, 0);
    fs.closeSync(fd);

    const image = sniffImage(head);
    const disposition = image ? 'inline' : 'attachment';
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Content-Type', image?.mime || 'application/octet-stream');
    res.setHeader('Content-Disposition', contentDisposition(disposition, filename));
    res.sendFile(allowedPath);
  } catch {
    res.sendStatus(404);
  }
});

/**
 * Poll for new messages since given ID (HTTP fallback)
 */
app.get('/api/poll', async (req, res) => {
  try {
    const sinceId = parseProjectionCursor(req.query.since_id);
    syncCoreInbound();
    await drainWebOutbox();
    res.json(getNewMessages(sinceId));
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

/**
 * Check auth status
 */
app.get('/api/auth', (req, res) => {
  res.json({
    required: AUTH_ENABLED,
    authenticated: isAuthenticated(req),
    timezone: ENV.TZ || null
  });
});

/**
 * Login
 */
app.post('/api/auth', (req, res) => {
  if (!AUTH_ENABLED) {
    return res.json({ success: true, timezone: ENV.TZ || null });
  }

  const { password, remember } = req.body;
  if (password !== AUTH_PASSWORD) {
    return res.status(401).json({ success: false, error: 'Wrong password' });
  }

  const token = sessionStore.create();
  const maxAge = remember !== false ? `; Max-Age=${sessionStore.maxAgeSec}` : '';
  res.setHeader('Set-Cookie', `wc_session=${token}; Path=/; HttpOnly; SameSite=Strict${maxAge}`);
  res.json({ success: true, timezone: ENV.TZ || null });
});

/**
 * Logout
 */
app.post('/api/logout', (req, res) => {
  const cookies = parseCookies(req.headers.cookie);
  if (cookies.wc_session) sessionStore.delete(cookies.wc_session);
  res.setHeader('Set-Cookie', 'wc_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0');
  res.json({ success: true });
});

/**
 * Health check
 */
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    websocket_clients: clients.size
  });
});

// Serve index.html for root
app.get('/', (req, res) => {
  res.sendFile(path.join(SKILL_ROOT, 'public', 'index.html'));
});

// Start server (bind to localhost only for security)
const BIND_HOST = process.env.WEB_CONSOLE_BIND || '127.0.0.1';
server.listen(PORT, BIND_HOST, () => {
  console.log(`Web Console server running on http://${BIND_HOST}:${PORT}`);
  console.log(`WebSocket available at ws://${BIND_HOST}:${PORT}`);
  console.log(`Authentication: ${AUTH_ENABLED ? 'enabled' : 'disabled (no password set)'}`);
  console.log(`Database: ${DB_PATH}`);
});

// Graceful shutdown: stop new claims, await every fenced dispatch result, then
// close channel/Core databases. This prevents a normal stop from stranding a
// command after its durable mailbox side effect but before Core recordResult.
let shutdownPromise = null;
function shutdown() {
  if (shutdownPromise !== null) return shutdownPromise;
  console.log('Shutting down...');
  clearInterval(updateTimer);
  const serverClosed = server.listening
    ? new Promise((resolve) => server.close(() => resolve()))
    : Promise.resolve();
  for (const client of wss.clients) client.close(1001, 'Server shutting down');
  shutdownPromise = (async () => {
    await deliveryBarrier.stop();
    if (updateInFlight !== null) await updateInFlight;
    await serverClosed;
    wss.close();
    if (db) db.close();
    if (wcDb) wcDb.close();
  })();
  return shutdownPromise;
}

process.once('SIGINT', () => { void shutdown(); });
process.once('SIGTERM', () => { void shutdown(); });
