(function exposeMessageReconciliation(root) {
  function attachmentKey(attachments) {
    if (!Array.isArray(attachments) || attachments.length === 0) return null;
    const ids = attachments.map((attachment) => attachment?.attachment_id);
    if (ids.some((id) => typeof id !== 'string' || id.length === 0)
      || new Set(ids).size !== ids.length) return null;
    return JSON.stringify([...ids].sort());
  }

  function matchesOptimisticMessage(optimistic, canonical) {
    const canonicalKey = attachmentKey(canonical?.attachments);
    if (canonicalKey !== null) {
      const optimisticKey = typeof optimistic?.attachment_key === 'string'
        ? optimistic.attachment_key : attachmentKey(optimistic?.attachments);
      return optimisticKey === canonicalKey;
    }
    return typeof optimistic?.content === 'string'
      && optimistic.content === canonical?.content;
  }

  function reconcileOptimisticMessages(messages, canonical) {
    if (!Array.isArray(messages)) return false;
    const index = messages.findIndex((message) => matchesOptimisticMessage(message, canonical));
    if (index < 0) return false;
    messages.splice(index, 1);
    return true;
  }

  function reconcileOptimisticElements(elements, canonical) {
    const candidates = Array.from(elements || []);
    const element = candidates.find((candidate) => matchesOptimisticMessage({
      content: candidate?.dataset?.rawContent,
      attachment_key: candidate?.dataset?.attachmentKey || null,
    }, canonical));
    if (!element || typeof element.remove !== 'function') return false;
    element.remove();
    return true;
  }

  function safeAttachmentHref(href) {
    if (typeof href !== 'string'
      || !/^\/api\/inbound-media\/wc-[A-Za-z0-9._-]+$/.test(href)) return null;
    return href;
  }

  root.ZylosMessageReconciliation = Object.freeze({
    attachmentKey,
    matchesOptimisticMessage,
    reconcileOptimisticMessages,
    reconcileOptimisticElements,
    safeAttachmentHref,
  });
}(globalThis));
