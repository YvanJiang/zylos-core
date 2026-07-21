import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { afterEach, describe, test } from '@jest/globals';

import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createWebConsoleOutboxOwner } from '../skills/web-console/scripts/core-outbox-owner.js';

const receiveCli = path.resolve('skills/comm-bridge/scripts/c4-receive.js');
const sendCli = path.resolve('skills/comm-bridge/scripts/c4-send.js');
const temporaryDirectories = [];

function fixture() {
  const zylosDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-c4-normal-caller-'));
  temporaryDirectories.push(zylosDir);
  return {
    zylosDir,
    env: {
      ...process.env,
      ZYLOS_DIR: zylosDir,
      ZYLOS_REGION: 'global',
      ZYLOS_TENANT_ID: 'tenant-c4',
      ZYLOS_BOT_ID: 'bot-c4',
    },
  };
}

function run(file, args, env, input = undefined) {
  return spawnSync(process.execPath, [file, ...args], {
    env,
    input,
    encoding: 'utf8',
  });
}

function claimTimeFor(database, channel, chatId) {
  const row = database.prepare(`
    SELECT command_json, next_attempt_at FROM runtime_outbox WHERE status = 'pending'
  `).all().find(({ command_json: commandJson }) => {
    const target = JSON.parse(commandJson).target;
    return target.channel === channel && target.chat_id === chatId;
  });
  assert.ok(row, `expected a pending ${channel}/${chatId} outbox command`);
  return new Date(Date.parse(row.next_attempt_at) + 1).toISOString();
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('normal C4 callers use durable Core contracts', () => {
  test('compatibility ingress atomically persists one Core turn and send_text outbox command', () => {
    const { zylosDir, env } = fixture();
    const args = [
      '--channel', 'web-console',
      '--endpoint', 'console',
      '--chat-type', 'dm',
      '--message-id', 'web-message-1',
      '--actor-id', 'local-console-user',
      '--occurred-at', '2026-07-21T00:00:00.000Z',
      '--content', 'hello from the compatibility channel',
      '--json',
    ];
    const first = run(receiveCli, args, env);
    assert.equal(first.status, 0, first.stderr);
    const accepted = JSON.parse(first.stdout.trim().split('\n').at(-1));
    assert.equal(accepted.ok, true);
    assert.equal(accepted.action, 'queued');
    assert.equal(accepted.deduplicated, false);
    assert.equal(typeof accepted.turn_id, 'string');

    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const legacyTables = database.prepare(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name IN ('checkpoints', 'conversations', 'control_queue', 'status_notice_cooldowns')
      ORDER BY name
    `).all();
    assert.equal(legacyTables.length, 0);
    assert.equal(database.prepare('SELECT state FROM runtime_turns WHERE turn_id = ?')
      .get(accepted.turn_id).state, 'queued');
    const command = JSON.parse(database.prepare(
      'SELECT command_json FROM runtime_outbox WHERE turn_id = ?',
    ).get(accepted.turn_id).command_json);
    assert.equal(command.contract, 'zylos.delivery-command');
    assert.equal(command.contract_version, '1.1');
    assert.equal(command.operation, 'send_text');
    assert.equal(command.target.channel, 'web-console');
    assert.equal(command.target.chat_id, 'console');
    assert.equal(command.target.native_thread_root_message_id, null);
    assert.equal(command.target.native_thread_reply_target_message_id, null);

    const replay = run(receiveCli, args, env);
    assert.equal(replay.status, 0, replay.stderr);
    const replayed = JSON.parse(replay.stdout.trim().split('\n').at(-1));
    assert.equal(replayed.ok, true);
    assert.equal(replayed.turn_id, accepted.turn_id);
    assert.equal(replayed.deduplicated, true);
    assert.equal(database.prepare('SELECT COUNT(*) AS count FROM runtime_turns').get().count, 1);
    database.close();
  });

  test('compatibility ingress does not consult activity files or add terminal reply instructions', () => {
    const source = fs.readFileSync(receiveCli, 'utf8');
    assert.match(source, /acceptCompatibilityInbound/);
    assert.doesNotMatch(source, /agent-status|activity-monitor|am\.sock|reply via|c4-dispatcher|send-keys|paste-buffer/i);
  });

  test('c4-send refuses external normal replies before invoking a channel script', () => {
    const { zylosDir, env } = fixture();
    const marker = path.join(zylosDir, 'channel-send-invoked');
    const scriptDirectory = path.join(zylosDir, '.claude', 'skills', 'web-console', 'scripts');
    fs.mkdirSync(scriptDirectory, { recursive: true });
    fs.writeFileSync(path.join(scriptDirectory, 'send.js'),
      `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)}, 'invoked');\n`);

    const result = run(sendCli, ['web-console', 'console'], env, 'must use durable outbox');
    assert.equal(result.status, 2);
    assert.match(result.stderr, /durable Core outbox/i);
    assert.equal(fs.existsSync(marker), false);
  });

  test('c4-send also rejects hidden record-only channels without creating a legacy database', () => {
    const { zylosDir, env } = fixture();
    const result = run(sendCli, ['void', 'session-handoff'], env, 'hidden global note');
    assert.equal(result.status, 2);
    assert.match(result.stderr, /durable Core outbox/i);
    assert.equal(fs.existsSync(path.join(zylosDir, 'comm-bridge', 'c4.db')), false);
  });

  test('a channel delivery owner cannot claim another channel or endpoint outbox lane', () => {
    const { zylosDir, env } = fixture();
    for (const [channel, endpoint, messageId] of [
      ['web-console', 'console', 'web-owned-message'],
      ['web-console', 'other-console', 'other-web-owned-message'],
      ['shell', '/tmp/disposable-shell.sock', 'shell-owned-message'],
      ['shell', '/tmp/other-disposable-shell.sock', 'other-shell-owned-message'],
    ]) {
      const accepted = run(receiveCli, [
        '--channel', channel,
        '--endpoint', endpoint,
        '--message-id', messageId,
        '--actor-id', 'fixture-user',
        '--occurred-at', '2026-07-21T00:10:00.000Z',
        '--content', `message for ${channel}`,
        '--json',
      ], env);
      assert.equal(accepted.status, 0, accepted.stderr);
    }

    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const webClaimTime = claimTimeFor(database, 'web-console', 'console');
    const owner = createOutboxService({
      database,
      channel: 'web-console',
      targetChatId: 'console',
      serviceInstanceId: 'web-console-owner-fixture',
      now: () => webClaimTime,
    });
    const webCommand = owner.claimNext();
    assert.equal(webCommand.target.channel, 'web-console');
    assert.equal(owner.claimNext(), null);
    assert.equal(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox WHERE status = 'pending'
    `).get().count, 3);

    const shellClaimTime = claimTimeFor(database, 'shell', '/tmp/disposable-shell.sock');
    const shellOwner = createOutboxService({
      database,
      channel: 'shell',
      targetChatId: '/tmp/disposable-shell.sock',
      serviceInstanceId: 'shell-owner-fixture',
      now: () => shellClaimTime,
    });
    assert.equal(shellOwner.claimNext().target.chat_id, '/tmp/disposable-shell.sock');
    assert.equal(shellOwner.claimNext(), null);
    assert.equal(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox WHERE status = 'pending'
    `).get().count, 2);
    database.close();
  });

  test('the Web Console owner renders, delivers, and fences its Core outbox result', async () => {
    const { zylosDir, env } = fixture();
    const acceptedProcess = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'web-owner-round-trip', '--actor-id', 'fixture-user',
      '--occurred-at', '2026-07-21T00:20:00.000Z', '--content', 'owner round trip', '--json',
    ], env);
    assert.equal(acceptedProcess.status, 0, acceptedProcess.stderr);
    const accepted = JSON.parse(acceptedProcess.stdout.trim());
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    assert.equal(database.prepare(`
      SELECT status FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id).status, 'pending');
    const browserDeliveries = [];
    const claimTime = claimTimeFor(database, 'web-console', 'console');
    const owner = createWebConsoleOutboxOwner({
      database,
      serviceInstanceId: 'web-console-owner-round-trip',
      now: () => claimTime,
      deliverMessage(message, delivery) {
        browserDeliveries.push(message);
        return { platform_message_id: `mailbox:${delivery.delivery_id}` };
      },
    });
    assert.deepEqual(await owner.drain(), { status: 'delivered', delivered: 1 });
    assert.match(browserDeliveries[0].content, /Message received/);
    assert.equal(database.prepare(`
      SELECT status FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id).status, 'delivered');
    assert.equal(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_message_mappings WHERE turn_id = ?
    `).get(accepted.turn_id).count, 1);
    database.close();
  });

  test('shell and web-console describe Core outbox owners, not direct model sends', () => {
    const shellSkill = fs.readFileSync(path.resolve('skills/shell/SKILL.md'), 'utf8');
    const shellCli = fs.readFileSync(path.resolve('cli/commands/shell.js'), 'utf8');
    const webServer = fs.readFileSync(path.resolve('skills/web-console/scripts/server.js'), 'utf8');
    assert.doesNotMatch(shellSkill, /Claude responds via `c4-send`|send\.js connects/i);
    assert.match(shellCli, /createOutboxService|createChannelNeutralTextRenderer/);
    assert.match(webServer, /createWebConsoleOutboxOwner/);
  });
});
