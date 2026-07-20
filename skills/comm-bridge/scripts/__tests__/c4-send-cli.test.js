import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { describe, it } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const CLI_PATH = fileURLToPath(new URL('../c4-send.js', import.meta.url));

function run(args, env, input = undefined) {
  return spawnSync(process.execPath, [CLI_PATH, ...args], {
    env: { ...process.env, ...env },
    encoding: 'utf8',
    input,
    timeout: 5000,
  });
}

describe('retired c4-send boundary', () => {
  it('fails closed for external channels before a channel script can run', () => {
    const zylosDir = fs.mkdtempSync(path.join(os.tmpdir(), 'c4-send-retired-'));
    try {
      const marker = path.join(zylosDir, 'invoked');
      const scripts = path.join(zylosDir, '.claude', 'skills', 'telegram', 'scripts');
      fs.mkdirSync(scripts, { recursive: true });
      fs.writeFileSync(path.join(scripts, 'send.js'),
        `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)}, 'yes');\n`);
      const result = run(['telegram', 'chat-1', 'message'], { ZYLOS_DIR: zylosDir });
      assert.equal(result.status, 2);
      assert.match(result.stderr, /durable Core outbox/i);
      assert.equal(fs.existsSync(marker), false);
    } finally {
      fs.rmSync(zylosDir, { recursive: true, force: true });
    }
  });

  it('fails closed for the former void record path without creating a database', () => {
    const zylosDir = fs.mkdtempSync(path.join(os.tmpdir(), 'c4-send-retired-'));
    try {
      const result = run(
        ['void', 'session-handoff'],
        { ZYLOS_DIR: zylosDir },
        'hidden cross-conversation state',
      );
      assert.equal(result.status, 2);
      assert.match(result.stderr, /durable Core outbox/i);
      assert.equal(fs.existsSync(path.join(zylosDir, 'comm-bridge', 'c4.db')), false);
    } finally {
      fs.rmSync(zylosDir, { recursive: true, force: true });
    }
  });
});
