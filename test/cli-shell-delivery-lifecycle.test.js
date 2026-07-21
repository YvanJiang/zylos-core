import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';

import { describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createDeliveryDrain,
  createShellRuntimeIdentity,
} from '../cli/commands/shell.js';

describe('shell Core outbox owner lifecycle', () => {
  test('derives socket and owner identity from process birth UUID, never PID state', () => {
    const ids = ['birth-a', 'birth-b'];
    const first = createShellRuntimeIdentity({
      randomUUID: () => ids.shift(), temporaryDirectory: () => '/tmp/disposable-shell',
    });
    const second = createShellRuntimeIdentity({
      randomUUID: () => ids.shift(), temporaryDirectory: () => '/tmp/disposable-shell',
    });
    expect(first).toEqual({
      birthId: 'birth-a',
      socketPath: path.join('/tmp/disposable-shell', 'zylos-shell-birth-a.sock'),
      serviceInstanceId: 'shell-birth-a',
    });
    expect(second.socketPath).not.toBe(first.socketPath);
    expect(second.serviceInstanceId).not.toBe(first.serviceInstanceId);

    const source = fs.readFileSync(path.resolve('cli/commands/shell.js'), 'utf8');
    expect(source).not.toMatch(/process\.pid|process\.kill|zylos-shell-\(\\d\+\)/);
  });

  test('installs signal fencing before creating any delivery-capable runtime resource', () => {
    const source = fs.readFileSync(path.resolve('cli/commands/shell.js'), 'utf8');
    const handler = source.indexOf("process.once('SIGTERM'");
    expect(handler).toBeGreaterThan(-1);
    for (const resource of ['net.createServer(', 'new Database(', 'setInterval(']) {
      expect(handler).toBeLessThan(source.indexOf(resource));
    }
  });

  test('routes fatal socket errors through the same awaited shutdown fence', () => {
    const source = fs.readFileSync(path.resolve('cli/commands/shell.js'), 'utf8');
    const listenerStart = source.indexOf("server.on('error'");
    const listenerEnd = source.indexOf('\n  });', listenerStart);
    const listener = source.slice(listenerStart, listenerEnd);
    expect(listener).toMatch(/shutdown\(\)/);
    expect(listener).toMatch(/process\.exitCode\s*=\s*1/);
    expect(listener).not.toMatch(/process\.exit\(/);
  });

  test('closes its early socket and exits when database startup fails after listen', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'shell-startup-failure-'));
    const socketDir = path.join(root, 'sockets');
    const receivePath = path.join(
      root, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-receive.js',
    );
    fs.mkdirSync(path.dirname(receivePath), { recursive: true });
    fs.mkdirSync(socketDir);
    fs.writeFileSync(receivePath, '');
    fs.writeFileSync(path.join(root, 'comm-bridge'), 'blocks database directory creation');

    const child = spawn(process.execPath, ['cli/zylos.js', 'shell'], {
      cwd: path.resolve('.'),
      env: { ...process.env, ZYLOS_DIR: root, TMPDIR: socketDir },
      stdio: ['ignore', 'ignore', 'ignore'],
    });
    const close = new Promise((resolve) => child.once('close', (code, signal) => {
      resolve({ code, signal });
    }));
    const result = await Promise.race([
      close,
      new Promise((resolve) => setTimeout(() => resolve(null), 1000)),
    ]);
    if (result === null) {
      child.kill('SIGKILL');
      await close;
    }

    try {
      expect(result).toEqual({ code: 1, signal: null });
      expect(fs.readdirSync(socketDir).filter((name) => name.endsWith('.sock'))).toEqual([]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('routes post-listen initialization exceptions through awaited shutdown', () => {
    const source = fs.readFileSync(path.resolve('cli/commands/shell.js'), 'utf8');
    expect(source).toMatch(
      /try \{\s*fs\.mkdirSync\(path\.dirname\(CORE_DATABASE_PATH\)[\s\S]*database = new Database[\s\S]*catch \(error\) \{[\s\S]*await shutdown\(\);/,
    );
  });

  test('normal shell ingress preserves option-like user text', async () => {
    const root = fs.mkdtempSync('/tmp/zylos-shell-option-');
    const socketDir = root;
    const receivePath = path.join(
      root, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-receive.js',
    );
    fs.mkdirSync(path.dirname(receivePath), { recursive: true });
    fs.symlinkSync(path.resolve('skills/comm-bridge/scripts/c4-receive.js'), receivePath);

    const child = spawn(process.execPath, ['cli/zylos.js', 'shell'], {
      cwd: path.resolve('.'),
      env: {
        ...process.env, ZYLOS_DIR: root, TMPDIR: socketDir,
        ZYLOS_TENANT_ID: 'shell-option-tenant',
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.on('data', (chunk) => { output += chunk.toString(); });
    child.stderr.on('data', (chunk) => { output += chunk.toString(); });
    const close = new Promise((resolve) => child.once('close', resolve));

    try {
      const startDeadline = Date.now() + 3000;
      while (!output.includes('you> ') && Date.now() < startDeadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(output).toContain('you> ');
      child.stdin.write('--literal-shell-text\n');

      const dbPath = path.join(root, 'comm-bridge', 'c4.db');
      let content = null;
      const ingressDeadline = Date.now() + 3000;
      while (content === null && Date.now() < ingressDeadline) {
        if (fs.existsSync(dbPath)) {
          let shellDb;
          try {
            shellDb = new Database(dbPath);
            const rows = shellDb.prepare('SELECT envelope_json FROM runtime_inbound_events').all();
            const matching = rows.map(({ envelope_json: json }) => JSON.parse(json))
              .find((envelope) => envelope.content?.text === '--literal-shell-text');
            if (matching) content = matching.content.text;
          } catch {
            // The shell may have created the file but not finished its schema transaction yet.
          } finally {
            shellDb?.close();
          }
        }
        if (content === null) await new Promise((resolve) => setTimeout(resolve, 20));
      }
      if (content === null) throw new Error(`shell did not persist option-like text: ${output}`);
      expect(content).toBe('--literal-shell-text');
    } finally {
      if (child.exitCode === null) child.kill('SIGTERM');
      await close;
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('stop waits for the current fenced dispatch result and prevents another claim', async () => {
    let releaseDispatch;
    let dispatchCalls = 0;
    const dispatchBlocked = new Promise((resolve) => { releaseDispatch = resolve; });
    const drain = createDeliveryDrain({
      async dispatchNext() {
        dispatchCalls += 1;
        await dispatchBlocked;
        return { status: 'delivered' };
      },
    });

    const active = drain.drain();
    let stopped = false;
    const stopping = drain.stop().then(() => { stopped = true; });
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(dispatchCalls).toBe(1);

    releaseDispatch();
    await Promise.all([active, stopping]);
    expect(stopped).toBe(true);
    await drain.drain();
    expect(dispatchCalls).toBe(1);
  });

  test('SIGTERM cancels a pending response and cannot run prompt logic after stop', async () => {
    const root = fs.mkdtempSync('/tmp/zylos-shell-stop-');
    const socketDir = root;
    const receivePath = path.join(
      root, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-receive.js',
    );
    fs.mkdirSync(path.dirname(receivePath), { recursive: true });
    fs.mkdirSync(path.join(root, 'comm-bridge'));
    fs.writeFileSync(receivePath, '#!/usr/bin/env node\n');

    const child = spawn(process.execPath, ['cli/zylos.js', 'shell'], {
      cwd: path.resolve('.'),
      env: { ...process.env, ZYLOS_DIR: root, TMPDIR: socketDir },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.stderr.on('data', (chunk) => { output += chunk; });
    const close = new Promise((resolve) => child.once('close', (code, signal) => {
      resolve({ code, signal });
    }));

    try {
      await new Promise((resolve, reject) => {
        const deadline = setTimeout(() => reject(new Error(`shell did not start: ${output}`)), 1000);
        const poll = setInterval(() => {
          if (!output.includes('you> ')) return;
          clearInterval(poll);
          clearTimeout(deadline);
          resolve();
        }, 10);
      });
      child.stdin.write('queued message\n');
      await new Promise((resolve, reject) => {
        const deadline = setTimeout(() => reject(new Error(`shell did not wait: ${output}`)), 1000);
        const poll = setInterval(() => {
          if (!output.includes('thinking...')) return;
          clearInterval(poll);
          clearTimeout(deadline);
          resolve();
        }, 10);
      });

      const stopOffset = output.length;
      child.kill('SIGTERM');
      const result = await Promise.race([
        close,
        new Promise((resolve) => setTimeout(() => resolve(null), 750)),
      ]);
      if (result === null) child.kill('SIGKILL');

      expect(result).toEqual({ code: 0, signal: null });
      expect(output.slice(stopOffset)).not.toMatch(/no response within timeout|you> /);
      expect(fs.readdirSync(socketDir).filter((name) => name.endsWith('.sock'))).toEqual([]);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await close;
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
