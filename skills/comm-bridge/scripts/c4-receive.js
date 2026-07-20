#!/usr/bin/env node

import { acceptCompatibilityInbound } from '../../../runtime/compatibility/c4-channel-fallback.js';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const ZYLOS_DIR = process.env.ZYLOS_DIR ?? path.join(os.homedir(), 'zylos');
const DATABASE_PATH = path.join(ZYLOS_DIR, 'comm-bridge', 'c4.db');

function openCoreDatabase() {
  fs.mkdirSync(path.dirname(DATABASE_PATH), { recursive: true });
  const database = new Database(DATABASE_PATH);
  database.pragma('journal_mode = WAL');
  database.pragma('busy_timeout = 5000');
  database.pragma('foreign_keys = ON');
  return database;
}

function printUsage() {
  console.error(`Usage: node c4-receive.js \\
  --channel <channel> --endpoint <chat_id> --message-id <native_message_id> \\
  --actor-id <authenticated_actor_id> [--chat-type dm|group|thread] \\
  [--thread-id <native_thread_id>] [--root-message-id <native_root_message_id>] \\
  [--occurred-at <RFC3339>] [--json] --content <message>`);
}

function parseArgs(args) {
  const parsed = {
    actorId: null,
    channel: null,
    chatType: 'dm',
    content: null,
    endpoint: null,
    json: false,
    messageId: null,
    occurredAt: null,
    rootMessageId: null,
    threadId: null,
  };
  const valueOptions = new Map([
    ['--actor-id', 'actorId'],
    ['--channel', 'channel'],
    ['--chat-type', 'chatType'],
    ['--content', 'content'],
    ['--endpoint', 'endpoint'],
    ['--message-id', 'messageId'],
    ['--occurred-at', 'occurredAt'],
    ['--root-message-id', 'rootMessageId'],
    ['--thread-id', 'threadId'],
  ]);
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === '--json') {
      parsed.json = true;
      continue;
    }
    const field = valueOptions.get(argument);
    if (field === undefined) return { error: `Unknown option: ${argument}`, json: parsed.json };
    const value = args[index + 1];
    if (value === undefined || value.startsWith('--')) {
      return { error: `${argument} requires a value`, json: parsed.json };
    }
    parsed[field] = value;
    index += 1;
  }
  return parsed;
}

function fail(json, code, message) {
  if (json) {
    process.stdout.write(`${JSON.stringify({ ok: false, error: { code, message } })}\n`);
  } else {
    console.error(`Error: ${message}`);
  }
  process.exitCode = 1;
}

function requireArguments(parsed) {
  for (const [field, option] of [
    ['channel', '--channel'],
    ['endpoint', '--endpoint'],
    ['messageId', '--message-id'],
    ['actorId', '--actor-id'],
    ['content', '--content'],
  ]) {
    if (typeof parsed[field] !== 'string' || parsed[field].length === 0) {
      throw new TypeError(`${option} is required`);
    }
  }
  if (!['dm', 'group', 'thread'].includes(parsed.chatType)) {
    throw new TypeError('--chat-type must be dm, group, or thread');
  }
  if (parsed.chatType === 'thread') {
    if (!parsed.threadId) throw new TypeError('--thread-id is required for a thread');
    if (!parsed.rootMessageId) {
      throw new TypeError('--root-message-id is required for a thread');
    }
  } else if (parsed.threadId !== null || parsed.rootMessageId !== null) {
    throw new TypeError('thread anchors are only valid with --chat-type thread');
  }
  if (parsed.occurredAt !== null && Number.isNaN(Date.parse(parsed.occurredAt))) {
    throw new TypeError('--occurred-at must be an RFC3339 timestamp');
  }
}

function compatibilityMessage(parsed, receivedAt) {
  return {
    inbound_event_id: parsed.messageId,
    trace_id: `c4:${parsed.channel}:${parsed.messageId}`,
    occurred_at: parsed.occurredAt ?? receivedAt,
    received_at: receivedAt,
    region: process.env.ZYLOS_REGION ?? 'global',
    tenant_id: process.env.ZYLOS_TENANT_ID ?? 'default',
    channel: parsed.channel,
    bot_id: process.env.ZYLOS_BOT_ID ?? 'zylos',
    chat_type: parsed.chatType,
    chat_id: parsed.endpoint,
    native_thread_or_topic_id: parsed.threadId,
    message_id: parsed.messageId,
    actor: {
      type: 'user',
      actor_id: parsed.actorId,
      authenticated: true,
      roles: [],
    },
    content: { kind: 'text', text: parsed.content, attachments: [] },
    reply: {
      root_message_id: parsed.rootMessageId,
      parent_message_id: null,
      reply_to_message_id: null,
    },
    source_ref: `c4:${parsed.channel}:${parsed.messageId}`,
  };
}

function emitAccepted(json, result) {
  const output = {
    ok: result.status === 'accepted',
    action: result.status === 'accepted' ? 'queued' : 'rejected',
    conversation_id: result.conversation_id,
    turn_id: result.turn_id,
    lineage_id: result.lineage_id,
    turn_version: result.turn_version,
    deduplicated: result.deduplicated,
    error: result.error,
  };
  if (json) process.stdout.write(`${JSON.stringify(output)}\n`);
  else if (output.ok) console.log(`[C4] Message durably queued in Core (turn=${output.turn_id})`);
  else console.error(`[C4] Core rejected the message: ${output.error?.user_message ?? 'unknown'}`);
  process.exitCode = output.ok ? 0 : 1;
}

function main() {
  let database = null;
  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.error) {
    printUsage();
    fail(parsed.json, 'INVALID_ARGS', parsed.error);
    return;
  }
  try {
    requireArguments(parsed);
  } catch (error) {
    printUsage();
    fail(parsed.json, 'INVALID_ARGS', error?.message ?? 'invalid compatibility ingress arguments');
    return;
  }
  try {
    const receivedAt = new Date().toISOString();
    database = openCoreDatabase();
    const result = acceptCompatibilityInbound(
      database,
      compatibilityMessage(parsed, receivedAt),
      { now: () => receivedAt },
    );
    emitAccepted(parsed.json, result);
  } catch (error) {
    fail(parsed.json, 'INTERNAL_ERROR', error?.message ?? 'compatibility ingress failed');
  } finally {
    database?.close();
  }
}

main();
