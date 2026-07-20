import fs from 'node:fs';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

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
});
