import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { afterEach, describe, test } from '@jest/globals';

import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

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
});
