import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { describe, expect, test } from '@jest/globals';

const CURSOR_PATH = path.resolve('skills/web-console/public/mailbox-cursor.js');

function loadCursorApi() {
  const browser = { globalThis: {} };
  vm.runInNewContext(fs.readFileSync(CURSOR_PATH, 'utf8'), browser);
  return browser.globalThis.ZylosMailboxCursor;
}

describe('Web Console mailbox cursor state', () => {
  test('resets the cursor and requests DOM replacement when the mailbox namespace changes', () => {
    const api = loadCursorApi();
    const scopeA = `web-console-mailbox-v1:${'a'.repeat(64)}`;
    const scopeB = `web-console-mailbox-v1:${'b'.repeat(64)}`;
    let resets = 0;
    expect(api.acceptScope(
      { cursorScope: scopeA, lastMessageId: 88 }, scopeB, () => { resets += 1; },
    ))
      .toEqual({ cursorScope: scopeB, lastMessageId: 0, reset: true });
    expect(resets).toBe(1);
  });

  test('preserves the durable cursor within the same validated namespace', () => {
    const api = loadCursorApi();
    const scopeA = `web-console-mailbox-v1:${'a'.repeat(64)}`;
    expect(api.acceptScope({ cursorScope: scopeA, lastMessageId: 88 }, scopeA))
      .toEqual({ cursorScope: scopeA, lastMessageId: 88, reset: false });
    expect(() => api.acceptScope({ cursorScope: null, lastMessageId: 0 }, '../unsafe'))
      .toThrow(/cursor scope/i);
  });
});
