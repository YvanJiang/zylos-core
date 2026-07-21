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

  function resetScopedClientState(client, revokeObjectUrl = (url) => URL.revokeObjectURL(url)) {
    client.messagesContainer.replaceChildren();
    client.pendingMessages.clear();
    for (const request of client.pendingUploads.values()) request.abort();
    client.pendingUploads.clear();
    for (const attachment of client.pendingAttachments) {
      if (typeof attachment?.previewUrl === 'string') revokeObjectUrl(attachment.previewUrl);
    }
    client.pendingAttachments = [];
    client.messageInput.value = '';
    client.updateAttachmentTray();
    client.showEmptyState();
  }

  function isCurrentGeneration(expectedGeneration, currentGeneration) {
    return Number.isSafeInteger(expectedGeneration)
      && expectedGeneration >= 0
      && expectedGeneration === currentGeneration;
  }

  function acceptScopedResponse(state, nextScope, requestGeneration, onReset = null) {
    const scopeGeneration = state?.scopeGeneration;
    if (!Number.isSafeInteger(scopeGeneration) || scopeGeneration < 0) {
      throw new TypeError('Invalid mailbox scope generation.');
    }
    if (!isCurrentGeneration(requestGeneration, scopeGeneration)) {
      return Object.freeze({
        accepted: false,
        reset: false,
        cursorScope: state.cursorScope,
        lastMessageId: state.lastMessageId,
        scopeGeneration,
      });
    }
    const accepted = acceptScope(state, nextScope, onReset);
    const changed = state.cursorScope !== accepted.cursorScope;
    return Object.freeze({
      accepted: true,
      ...accepted,
      scopeGeneration: scopeGeneration + (changed ? 1 : 0),
    });
  }

  function acceptMutationResponse(state, status, payload, requestGeneration, onReset = null) {
    if (status !== 409) {
      return Object.freeze({
        handled: false,
        accepted: false,
        reset: false,
        cursorScope: state.cursorScope,
        lastMessageId: state.lastMessageId,
        scopeGeneration: state.scopeGeneration,
      });
    }
    return Object.freeze({
      handled: true,
      ...acceptScopedResponse(
        state, payload?.cursor_scope, requestGeneration, onReset,
      ),
    });
  }

  root.ZylosMailboxCursor = Object.freeze({
    acceptScope,
    acceptMutationResponse,
    acceptScopedResponse,
    isCurrentGeneration,
    resetScopedClientState,
  });
}(globalThis));
