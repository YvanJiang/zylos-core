/**
 * Tombstone for the retired per-session runtime registry.
 *
 * Normal provider selection is owned by the long-lived executor daemon. This
 * module deliberately imports no legacy adapter, so importing the historical
 * registry can never load or select a retired per-session implementation.
 */

export const SUPPORTED_RUNTIMES = Object.freeze([]);

function disabled() {
  const error = new Error('Legacy runtime adapters are disabled; use the Core executor service.');
  error.code = 'legacy_runtime_adapter_disabled';
  throw error;
}

export function getAdapterClass() {
  return disabled();
}

export function getAdapter() {
  return disabled();
}

export function getActiveAdapter() {
  return disabled();
}
