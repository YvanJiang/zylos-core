import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { afterEach, describe, test } from '@jest/globals';

import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createWebConsoleOutboxOwner } from '../skills/web-console/scripts/core-outbox-owner.js';
import { DeliveryMailbox, openDb } from '../skills/web-console/scripts/db.js';

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

  test('compatibility ingress preserves option-like user text as the content value', () => {
    const { zylosDir, env } = fixture();
    const result = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'option-like-content', '--actor-id', 'local-console-user',
      '--content', '--literal-text', '--json',
    ], env);
    assert.equal(result.status, 0, result.stderr);

    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const envelope = JSON.parse(database.prepare(`
      SELECT envelope_json FROM runtime_inbound_events WHERE inbound_event_id = ?
    `).get('option-like-content').envelope_json);
    database.close();
    assert.equal(envelope.content.text, '--literal-text');
  });

  test('compatibility ingress still rejects option-like control and identity values', () => {
    const { zylosDir, env } = fixture();
    const result = run(receiveCli, [
      '--channel', '--json', '--endpoint', 'console',
      '--message-id', 'malformed-route', '--actor-id', 'actor',
      '--content', 'hello',
    ], env);
    assert.equal(result.status, 1);
    assert.match(result.stderr, /--channel requires a value/);
    assert.equal(fs.existsSync(path.join(zylosDir, 'comm-bridge', 'c4.db')), false);
  });

  test('compatibility ingress preserves validated public attachment facts', () => {
    const { zylosDir, env } = fixture();
    const attachments = [{
      attachment_id: 'channel-upload-1',
      media_type: 'text/plain',
      name: 'report.txt',
      content_ref: 'channel-owned-ref-1',
      size_bytes: 12,
    }];
    const accepted = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'web-attachment-1', '--actor-id', 'local-console-user',
      '--occurred-at', '2026-07-21T00:00:00.000Z',
      '--attachments-json', JSON.stringify(attachments),
      '--content', 'structured attachment', '--json',
    ], env);
    assert.equal(accepted.status, 0, accepted.stderr);

    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const envelope = JSON.parse(database.prepare(`
      SELECT envelope_json FROM runtime_inbound_events WHERE inbound_event_id = ?
    `).get('web-attachment-1').envelope_json);
    database.close();
    assert.equal(envelope.content.kind, 'mixed');
    assert.deepEqual(envelope.content.attachments, attachments);

    const invalid = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'web-attachment-invalid', '--actor-id', 'local-console-user',
      '--attachments-json', '{not-json}', '--content', 'invalid', '--json',
    ], env);
    assert.equal(invalid.status, 1);
    assert.equal(JSON.parse(invalid.stdout).error.code, 'INVALID_ARGS');
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

  test('the Web Console owner cannot claim the same endpoint in another durable Core scope', async () => {
    const { zylosDir, env } = fixture();
    for (const [region, tenantId, botId, messageId] of [
      ['global', 'tenant-c4', 'bot-c4', 'owned-web-scope'],
      ['region-foreign', 'tenant-c4', 'bot-c4', 'foreign-web-region'],
      ['global', 'tenant-foreign', 'bot-c4', 'foreign-web-tenant'],
      ['global', 'tenant-c4', 'bot-foreign', 'foreign-web-bot'],
    ]) {
      const accepted = run(receiveCli, [
        '--channel', 'web-console', '--endpoint', 'console',
        '--message-id', messageId, '--actor-id', 'fixture-user',
        '--occurred-at', '2026-07-21T00:10:00.000Z', '--content', messageId, '--json',
      ], {
        ...env, ZYLOS_REGION: region, ZYLOS_TENANT_ID: tenantId, ZYLOS_BOT_ID: botId,
      });
      assert.equal(accepted.status, 0, accepted.stderr);
    }

    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const lastAttempt = database.prepare(`
      SELECT MAX(next_attempt_at) AS value FROM runtime_outbox
    `).get().value;
    const deliveries = [];
    const owner = createWebConsoleOutboxOwner({
      database,
      region: 'global',
      tenantId: 'tenant-c4',
      botId: 'bot-c4',
      serviceInstanceId: 'web-console-scoped-owner',
      now: () => new Date(Date.parse(lastAttempt) + 1).toISOString(),
      projectInbound() {},
      deliverMessage(message) {
        deliveries.push(message);
        return { platform_message_id: `scoped-mailbox:${message.delivery_id}` };
      },
    });
    assert.deepEqual(await owner.drain({ limit: 10 }), { status: 'delivered', delivered: 1 });
    assert.equal(deliveries.length, 1);
    const states = database.prepare(`
      SELECT status, command_json FROM runtime_outbox ORDER BY created_at, outbox_id
    `).all().map(({ status, command_json: commandJson }) => ({
      status, target: JSON.parse(commandJson).target,
    }));
    assert.equal(states.some(({ status, target }) => status === 'delivered'
      && target.region === 'global' && target.tenant_id === 'tenant-c4'
      && target.bot_id === 'bot-c4'), true);
    assert.equal(states.filter(({ target }) => target.region !== 'global'
      || target.tenant_id !== 'tenant-c4' || target.bot_id !== 'bot-c4')
      .every(({ status }) => status === 'pending'), true);
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
    const projectionOrder = [];
    const claimTime = claimTimeFor(database, 'web-console', 'console');
    const owner = createWebConsoleOutboxOwner({
      database,
      region: 'global',
      tenantId: 'tenant-c4',
      botId: 'bot-c4',
      serviceInstanceId: 'web-console-owner-round-trip',
      now: () => claimTime,
      projectInbound(command) {
        assert.equal(command.outbox_id.length > 0, true);
        assert.equal(command.mapping.turn_id, accepted.turn_id);
        projectionOrder.push('inbound');
      },
      deliverMessage(message, delivery) {
        projectionOrder.push('outbox');
        browserDeliveries.push(message);
        return { platform_message_id: `mailbox:${delivery.delivery_id}` };
      },
    });
    assert.deepEqual(await owner.drain(), { status: 'delivered', delivered: 1 });
    assert.deepEqual(projectionOrder, ['inbound', 'outbox']);
    assert.match(browserDeliveries[0].content, /Message received/);
    assert.equal(database.prepare(`
      SELECT status FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id).status, 'delivered');
    assert.equal(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_message_mappings WHERE turn_id = ?
    `).get(accepted.turn_id).count, 1);
    database.close();
  });

  test('the Web Console owner safely recovers an unknown mailbox acknowledgement after restart', async () => {
    const { zylosDir, env } = fixture();
    const acceptedProcess = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'web-owner-unknown-ack', '--actor-id', 'fixture-user',
      '--occurred-at', '2026-07-21T00:20:30.000Z', '--content', 'unknown mailbox ack', '--json',
    ], env);
    assert.equal(acceptedProcess.status, 0, acceptedProcess.stderr);
    const accepted = JSON.parse(acceptedProcess.stdout.trim());
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const mailboxDatabase = openDb(path.join(zylosDir, 'web-console', 'mailbox.db'));
    const mailbox = new DeliveryMailbox(mailboxDatabase, {
      region: 'global', tenantId: 'tenant-c4', botId: 'bot-c4',
    });
    const claimTime = claimTimeFor(database, 'web-console', 'console');
    let deliveryCalls = 0;
    const deliverToMailbox = (message, delivery) => {
      deliveryCalls += 1;
      const effect = mailbox.deliver({
        deliveryId: delivery.delivery_id,
        endpointId: message.endpoint_id,
        content: message.content,
        timestamp: message.timestamp,
      });
      if (deliveryCalls === 1) throw new Error('mailbox acknowledgement was lost');
      return effect;
    };
    const firstOwner = createWebConsoleOutboxOwner({
      database,
      region: 'global', tenantId: 'tenant-c4', botId: 'bot-c4',
      serviceInstanceId: 'web-console-owner-before-restart',
      now: () => claimTime,
      projectInbound() {},
      deliverMessage: deliverToMailbox,
    });

    await assert.rejects(firstOwner.drain(), /acknowledgement was lost/);
    assert.equal(mailbox.list().length, 1);
    assert.deepEqual({ ...database.prepare(`
      SELECT status, result_json, pre_action_fenced_at
      FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id) }, {
      status: 'delivering', result_json: null, pre_action_fenced_at: claimTime,
    });

    const recoveryTime = new Date(Date.parse(claimTime) + 11_000).toISOString();
    const restartedOwner = createWebConsoleOutboxOwner({
      database,
      region: 'global', tenantId: 'tenant-c4', botId: 'bot-c4',
      serviceInstanceId: 'web-console-owner-after-restart',
      now: () => recoveryTime,
      projectInbound() {},
      deliverMessage: deliverToMailbox,
    });
    assert.deepEqual(await restartedOwner.drain(), { status: 'delivered', delivered: 1 });
    assert.equal(deliveryCalls, 2);
    assert.equal(mailbox.list().length, 1);
    assert.equal(database.prepare(`
      SELECT status FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id).status, 'delivered');
    mailboxDatabase.close();
    database.close();
  });

  test('the Web Console owner stops before mailbox delivery when a command changes', async () => {
    const { zylosDir, env } = fixture();
    const acceptedProcess = run(receiveCli, [
      '--channel', 'web-console', '--endpoint', 'console',
      '--message-id', 'web-owner-command-change', '--actor-id', 'fixture-user',
      '--occurred-at', '2026-07-21T00:21:00.000Z', '--content', 'command change', '--json',
    ], env);
    assert.equal(acceptedProcess.status, 0, acceptedProcess.stderr);
    const accepted = JSON.parse(acceptedProcess.stdout.trim());
    const database = new Database(path.join(zylosDir, 'comm-bridge', 'c4.db'));
    const claimTime = claimTimeFor(database, 'web-console', 'console');
    let mailboxDeliveries = 0;
    const owner = createWebConsoleOutboxOwner({
      database,
      region: 'global',
      tenantId: 'tenant-c4',
      botId: 'bot-c4',
      serviceInstanceId: 'web-console-owner-command-change',
      now: () => claimTime,
      projectInbound(command) {
        const persisted = database.prepare(`
          SELECT command_json FROM runtime_outbox WHERE outbox_id = ?
        `).get(command.outbox_id);
        const changedCommand = JSON.parse(persisted.command_json);
        changedCommand.render_model.text = 'changed after projection';
        database.prepare(`
          UPDATE runtime_outbox SET command_json = ? WHERE outbox_id = ?
        `).run(JSON.stringify(changedCommand), command.outbox_id);
      },
      deliverMessage() {
        mailboxDeliveries += 1;
        return { platform_message_id: 'must-not-be-written' };
      },
    });

    await assert.rejects(owner.drain(), /delivery claim is stale/i);
    assert.equal(mailboxDeliveries, 0);
    assert.deepEqual({ ...database.prepare(`
      SELECT status, result_json, lease_owner FROM runtime_outbox WHERE turn_id = ?
    `).get(accepted.turn_id) }, {
      status: 'delivering',
      result_json: null,
      lease_owner: 'web-console-owner-command-change',
    });
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
