/**
 * Shared formatter for session-start context injection.
 *
 * Historical record and memory utilities use this stable framing. It contains
 * no runtime routing, lifecycle, or health authority.
 */

/**
 * Wrap a labeled section with a matching header and footer.
 *
 * @param {string} label   - Section label, rendered verbatim. Convention: UPPERCASE.
 * @param {string} content - Section body. Trimmed; nullish/empty renders as `(empty)`.
 * @returns {string} `=== LABEL ===\n<content>\n=== END LABEL ===`
 */
export function formatSection(label, content) {
  const body = (content == null ? '' : String(content)).trim();
  return `=== ${label} ===\n${body.length > 0 ? body : '(empty)'}\n=== END ${label} ===`;
}
