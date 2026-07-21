(function exposeMailboxCursor(root) {
  const CURSOR_SCOPE_PATTERN = /^web-console-mailbox-v1:[a-f0-9]{64}$/;

  function acceptScope(state, nextScope, onReset = null) {
    if (!CURSOR_SCOPE_PATTERN.test(nextScope)) {
      throw new TypeError('Invalid durable mailbox cursor scope.');
    }
    const previousScope = state?.cursorScope ?? null;
    const lastMessageId = state?.lastMessageId ?? 0;
    if (!Number.isSafeInteger(lastMessageId) || lastMessageId < 0) {
      throw new TypeError('Invalid durable mailbox cursor.');
    }
    const reset = previousScope !== null && previousScope !== nextScope;
    if (reset && onReset !== null) {
      if (typeof onReset !== 'function') throw new TypeError('onReset must be a function.');
      onReset();
    }
    return Object.freeze({
      cursorScope: nextScope,
      lastMessageId: reset ? 0 : lastMessageId,
      reset,
    });
  }

  root.ZylosMailboxCursor = Object.freeze({ acceptScope });
}(globalThis));
