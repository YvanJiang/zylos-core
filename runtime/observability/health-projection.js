import { validateObservabilitySnapshot } from '../../contracts/public/index.js';

export function projectRuntimeHealth(snapshot) {
  const observed = validateObservabilitySnapshot(snapshot).forwarded;
  return Object.freeze({
    contract: observed.contract,
    contract_version: observed.contract_version,
    snapshot_id: observed.snapshot_id,
    generated_at: observed.generated_at,
    state: observed.service.health,
    service: Object.freeze({
      health: observed.service.health,
      maintenance: observed.service.maintenance,
      draining: observed.service.draining,
      reconciling: observed.service.reconciling,
      service_instance_id: observed.service.service_instance_id,
    }),
    executors: structuredClone(observed.executors),
    turns: structuredClone(observed.turns),
    outbox: structuredClone(observed.outbox),
    error: structuredClone(observed.error),
  });
}
