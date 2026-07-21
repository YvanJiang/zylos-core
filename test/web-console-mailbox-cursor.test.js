import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { describe, expect, jest, test } from '@jest/globals';

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

  test('scope reset clears and revokes composer attachment state', () => {
    const api = loadCursorApi();
    const revoked = [];
    const uploadRequest = { abort: jest.fn() };
    const client = {
      messagesContainer: { replaceChildren: jest.fn() },
      pendingMessages: new Map([['temp', 'draft']]),
      pendingUploads: new Map([['upload-a', uploadRequest]]),
      pendingAttachments: [
        { id: 'upload-a', previewUrl: 'blob:a' },
        { id: 'upload-b', previewUrl: null },
      ],
      messageInput: { value: 'old tenant draft' },
      updateAttachmentTray: jest.fn(),
      showEmptyState: jest.fn(),
    };
    api.resetScopedClientState(client, (url) => revoked.push(url));
    expect(client.messagesContainer.replaceChildren).toHaveBeenCalledTimes(1);
    expect(client.pendingMessages.size).toBe(0);
    expect(uploadRequest.abort).toHaveBeenCalledTimes(1);
    expect(client.pendingUploads.size).toBe(0);
    expect(client.pendingAttachments).toEqual([]);
    expect(client.messageInput.value).toBe('');
    expect(client.updateAttachmentTray).toHaveBeenCalledTimes(1);
    expect(client.showEmptyState).toHaveBeenCalledTimes(1);
    expect(revoked).toEqual(['blob:a']);
  });

  test('ignores a delayed old-scope HTTP response after a newer scope was accepted', () => {
    const api = loadCursorApi();
    const scopeA = `web-console-mailbox-v1:${'a'.repeat(64)}`;
    const scopeB = `web-console-mailbox-v1:${'b'.repeat(64)}`;
    const stateA = { cursorScope: scopeA, lastMessageId: 88, scopeGeneration: 4 };
    const stateB = api.acceptScopedResponse(stateA, scopeB, 4);
    expect(stateB).toEqual({
      accepted: true, reset: true, cursorScope: scopeB, lastMessageId: 0,
      scopeGeneration: 5,
    });

    expect(api.acceptScopedResponse(stateB, scopeA, 4)).toEqual({
      accepted: false, reset: false, cursorScope: scopeB, lastMessageId: 0,
      scopeGeneration: 5,
    });
  });

  test('identifies events from a superseded WebSocket connection generation', () => {
    const api = loadCursorApi();
    expect(api.isCurrentGeneration(7, 7)).toBe(true);
    expect(api.isCurrentGeneration(7, 8)).toBe(false);
  });
});
