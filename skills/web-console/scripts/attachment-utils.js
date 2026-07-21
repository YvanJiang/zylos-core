import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const MAX_ATTACHMENTS = 20;
export const UPLOAD_TTL_MS = 30 * 60 * 1000;
const IMAGE_EXTENSIONS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp']);
const MEDIA_RE = /^\[MEDIA:(image|file)\]([^\r\n]+)$/;
const ATTACHMENT_RE = /^\[attachment:(image|file) (.+) name="([^"]*)" ([^\]]+)\]$/;
const CORE_CONTENT_REF_PREFIX = 'web-console-upload:';
const STORED_FILE_RE = /^wc-[A-Za-z0-9._-]+-[0-9a-f]{8}(?:\.[a-z0-9_-]{1,15})?$/;
const SAFE_MEDIA_TYPE_RE = /^[A-Za-z0-9!#$&^_.+-]+\/[A-Za-z0-9!#$&^_.+-]+$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return '?B';
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

export function sanitizeDisplayName(name, fallback = 'attachment') {
  const base = path.basename(String(name || fallback)).replace(/[\r\n\t"]/g, '_').trim();
  const safe = base.replace(/[/\\<>|*?\x00-\x1f"]/g, '_').replace(/\s+/g, ' ');
  return (safe || fallback).slice(0, 160);
}

export function sanitizedExtension(name) {
  const ext = path.extname(String(name || '')).toLowerCase();
  if (!ext || ext.length > 16) return '';
  return /^[.][a-z0-9_-]+$/.test(ext) ? ext : '';
}

export function generateStoredFileName(originalName, now = new Date()) {
  const stamp = now.toISOString().replace(/[:.]/g, '-');
  const uuid = crypto.randomUUID().replace(/-/g, '').slice(0, 8);
  return `wc-${stamp}-${uuid}${sanitizedExtension(originalName)}`;
}

export function uploadKind(file) {
  if (file?.mimetype?.startsWith('image/')) return 'image';
  if (IMAGE_EXTENSIONS.has(sanitizedExtension(file?.originalname || file?.name))) return 'image';
  return 'file';
}

export function buildAttachmentAnnotation(attachment) {
  const kind = attachment.kind === 'image' ? 'image' : 'file';
  const filePath = String(attachment.path || '');
  const name = sanitizeDisplayName(attachment.name);
  return `[attachment:${kind} ${filePath} name="${name}" ${formatBytes(attachment.size)}]`;
}

export function buildAnnotatedContent(message, attachments = []) {
  const text = String(message || '').trim();
  const annotations = attachments.map(buildAttachmentAnnotation);
  if (annotations.length === 0) return text;
  if (!text) return annotations.join('\n');
  return `${text}\n${annotations.join('\n')}`;
}

function validatedUpload(entry, mediaDir, maxBytes, { requireConsumed = false } = {}) {
  if (!entry || !UUID_RE.test(entry.id) || (requireConsumed && entry.consumed !== true)) {
    throw new TypeError('attachment is not a registered Web Console upload');
  }
  const allowedPath = resolveAllowedPathSync(entry.path, [mediaDir]);
  if (!allowedPath) throw new TypeError('attachment is outside the Web Console media directory');
  const filename = path.basename(allowedPath);
  if (!STORED_FILE_RE.test(filename)) throw new TypeError('attachment storage name is invalid');
  const stat = fs.statSync(allowedPath);
  if (!stat.isFile() || !Number.isSafeInteger(entry.size) || entry.size !== stat.size
    || entry.size < 0 || entry.size > maxBytes) {
    throw new TypeError('attachment size does not match the registered upload');
  }
  if (sanitizeDisplayName(entry.name) !== entry.name
    || typeof entry.mime !== 'string' || entry.mime.length > 127
    || !SAFE_MEDIA_TYPE_RE.test(entry.mime)) {
    throw new TypeError('attachment display metadata is invalid');
  }
  return { ...entry, path: allowedPath, annotationPath: entry.path, filename };
}

export function createCoreWebConsoleAttachment(entry, { mediaDir, maxBytes }) {
  const upload = validatedUpload(entry, mediaDir, maxBytes);
  return {
    attachment_id: upload.id,
    media_type: upload.mime,
    name: upload.name,
    content_ref: `${CORE_CONTENT_REF_PREFIX}${upload.filename}`,
    size_bytes: upload.size,
  };
}

export function projectCoreWebConsoleContent(content, {
  lookupUpload, mediaDir, maxBytes,
}) {
  if (!content || typeof content !== 'object' || !Array.isArray(content.attachments)) {
    throw new TypeError('Core content attachments are invalid');
  }
  const expectedLines = new Set();
  const allowedFields = new Set([
    'attachment_id', 'media_type', 'name', 'content_ref', 'size_bytes',
  ]);
  const attachments = content.attachments.map((attachment) => {
    const fields = Object.keys(attachment || {});
    if (fields.some((field) => !allowedFields.has(field))
      || [...allowedFields].some((field) => !Object.hasOwn(attachment, field))) {
      throw new TypeError('Core attachment has unsupported fields');
    }
    const upload = validatedUpload(
      lookupUpload(attachment.attachment_id), mediaDir, maxBytes, { requireConsumed: true },
    );
    if (attachment.content_ref !== `${CORE_CONTENT_REF_PREFIX}${upload.filename}`
      || attachment.media_type !== upload.mime || attachment.name !== upload.name
      || attachment.size_bytes !== upload.size) {
      throw new TypeError('Core attachment does not match the registered upload');
    }
    const annotation = buildAttachmentAnnotation({ ...upload, path: upload.annotationPath });
    if (expectedLines.has(annotation)) throw new TypeError('duplicate Core attachment annotation');
    expectedLines.add(annotation);
    return {
      attachment_id: upload.id,
      kind: upload.kind === 'image' ? 'image' : 'file',
      name: upload.name,
      media_type: upload.mime,
      size_bytes: upload.size,
      size_label: formatBytes(upload.size),
      href: `/api/inbound-media/${upload.filename}`,
    };
  });
  const bodyLines = [];
  for (const line of String(content.text ?? '').split('\n')) {
    if (expectedLines.delete(line)) continue;
    if (line.startsWith('[attachment:')) {
      throw new TypeError('unbound attachment annotation is not displayable');
    }
    bodyLines.push(line);
  }
  if (expectedLines.size !== 0) throw new TypeError('Core attachment annotation is missing');
  return { content: bodyLines.join('\n').trim(), attachments };
}

export function parseAttachmentAnnotation(line) {
  const match = String(line || '').match(ATTACHMENT_RE);
  if (!match) return null;
  const filePath = match[2].trim();
  if (!path.isAbsolute(filePath)) return null;
  return {
    kind: match[1],
    path: filePath,
    name: sanitizeDisplayName(match[3], 'attachment'),
    sizeLabel: match[4]
  };
}

export function splitContentAndAttachments(content) {
  const lines = String(content || '').split('\n');
  const bodyLines = [];
  const attachments = [];

  for (const line of lines) {
    const attachment = parseAttachmentAnnotation(line);
    if (attachment) {
      attachments.push(attachment);
    } else {
      bodyLines.push(line);
    }
  }

  return {
    content: bodyLines.join('\n').trim(),
    attachments
  };
}

export function parseMediaContent(content) {
  const match = String(content || '').match(MEDIA_RE);
  if (!match) return null;
  const localPath = match[2].trim();
  if (!path.isAbsolute(localPath)) return null;
  return {
    media_type: match[1],
    path: localPath,
    name: sanitizeDisplayName(path.basename(localPath), 'download')
  };
}

export function classifyConversationMessage(row, statFn = fs.statSync) {
  const media = parseMediaContent(row?.content);
  if (!media || row?.direction !== 'out' || row?.channel !== 'web-console' || row?.endpoint_id !== 'console') {
    return row;
  }
  let size = null;
  try {
    size = statFn(media.path).size;
  } catch {
    // Missing files are still represented as media rows; download returns 404.
  }
  return {
    id: row.id,
    direction: row.direction,
    channel: row.channel,
    endpoint_id: row.endpoint_id,
    timestamp: row.timestamp,
    kind: 'media',
    media_type: media.media_type,
    message_id: row.id,
    name: media.name,
    size
  };
}

export function resolveAllowedPathSync(targetPath, allowlistRoots) {
  let realTarget;
  try {
    realTarget = fs.realpathSync(targetPath);
  } catch {
    return null;
  }

  for (const root of allowlistRoots) {
    let realRoot;
    try {
      realRoot = fs.realpathSync(root);
    } catch {
      continue;
    }
    if (realTarget === realRoot || realTarget.startsWith(realRoot + path.sep)) {
      return realTarget;
    }
  }
  return null;
}

export function sniffImage(buffer) {
  if (!buffer || buffer.length < 4) return null;
  if (buffer.length >= 8
      && buffer[0] === 0x89
      && buffer[1] === 0x50
      && buffer[2] === 0x4e
      && buffer[3] === 0x47
      && buffer[4] === 0x0d
      && buffer[5] === 0x0a
      && buffer[6] === 0x1a
      && buffer[7] === 0x0a) {
    return { mime: 'image/png', extension: '.png' };
  }
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
    return { mime: 'image/jpeg', extension: '.jpg' };
  }
  const header = buffer.subarray(0, 6).toString('ascii');
  if (header === 'GIF87a' || header === 'GIF89a') {
    return { mime: 'image/gif', extension: '.gif' };
  }
  if (buffer.length >= 12
      && buffer.subarray(0, 4).toString('ascii') === 'RIFF'
      && buffer.subarray(8, 12).toString('ascii') === 'WEBP') {
    return { mime: 'image/webp', extension: '.webp' };
  }
  return null;
}

export function contentDisposition(disposition, filename) {
  const safe = sanitizeDisplayName(filename, 'download').replace(/[\\"]/g, '_');
  return `${disposition}; filename="${safe}"`;
}

export class UploadRegistry {
  constructor({ ttlMs = UPLOAD_TTL_MS, now = () => Date.now() } = {}) {
    this.ttlMs = ttlMs;
    this.now = now;
    this.items = new Map();
  }

  add(entry) {
    const id = crypto.randomUUID();
    const expiresAt = this.now() + this.ttlMs;
    this.items.set(id, { ...entry, id, expiresAt });
    return this.items.get(id);
  }

  cleanup() {
    const now = this.now();
    for (const [id, entry] of this.items) {
      if (entry.expiresAt <= now) this.items.delete(id);
    }
  }

  getMany(ids, sessionId) {
    this.cleanup();
    if (!Array.isArray(ids)) return [];
    if (new Set(ids).size !== ids.length) return [];
    return ids.map((id) => this.items.get(id)).filter((entry) => entry && entry.sessionId === sessionId);
  }

  consumeMany(ids, sessionId) {
    const entries = this.getMany(ids, sessionId);
    if (entries.length !== ids.length) return null;
    for (const id of ids) this.items.delete(id);
    return entries;
  }

  restoreMany(entries) {
    for (const entry of entries || []) {
      if (entry?.id) this.items.set(entry.id, entry);
    }
  }
}
