import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const START_FENCE_CONTRACT = 'zylos.executor-start-fence@1';

export function executorStartFencePath(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  return path.join(zylosDir, 'runtime', 'executor-start-fence.json');
}

function requireIssuanceProof(proof) {
  if (!proof || typeof proof !== 'object' || Array.isArray(proof)) {
    throw new TypeError('Executor start fence requires an explicit issuance proof.');
  }
  if (proof.kind === 'fresh_clean') {
    if (proof.installation_root_absent !== true) {
      throw new Error('Executor start fence fresh-clean proof is not authoritative.');
    }
    return Object.freeze({
      issuance_kind: 'fresh_clean',
      installation_root_absent: true,
    });
  }
  if (proof.kind === 'committed_reconciliation') {
    if (typeof proof.upgrade_id !== 'string' || proof.upgrade_id.length === 0
      || proof.legacy_services_quiesced !== true
      || proof.legacy_registrations_absent !== true
      || proof.legacy_artifacts_reconciled !== true) {
      throw new Error('Executor start fence committed reconciliation proof is incomplete.');
    }
    return Object.freeze({
      issuance_kind: 'committed_reconciliation',
      upgrade_id: proof.upgrade_id,
      legacy_services_quiesced: true,
      legacy_registrations_absent: true,
      legacy_artifacts_reconciled: true,
    });
  }
  throw new Error('Executor start fence issuance proof kind is invalid.');
}

function writeAtomicJson(file, document) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.partial`;
  const descriptor = fs.openSync(temporary, 'wx', 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(document)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  try {
    fs.renameSync(temporary, file);
    const directory = fs.openSync(path.dirname(file), 'r');
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  } finally {
    fs.rmSync(temporary, { force: true });
  }
}

export function issueExecutorStartFence({
  zylosDir,
  proof,
  now = () => new Date().toISOString(),
} = {}) {
  const issuance = requireIssuanceProof(proof);
  const reconciledAt = now();
  if (typeof reconciledAt !== 'string' || reconciledAt.length === 0) {
    throw new TypeError('Executor start fence reconciliation time is required.');
  }
  const fence = Object.freeze({
    contract: START_FENCE_CONTRACT,
    runtime_generation: 'executor_only',
    reconciled_at: reconciledAt,
    ...issuance,
  });
  const fencePath = executorStartFencePath(zylosDir);
  writeAtomicJson(fencePath, fence);
  return Object.freeze({ path: fencePath, fence });
}

export function assertExecutorStartFence({ zylosDir, readFileSync = fs.readFileSync } = {}) {
  let fence;
  try {
    fence = JSON.parse(readFileSync(executorStartFencePath(zylosDir), 'utf8'));
  } catch {
    throw new Error('Executor startup requires a completed one-time runtime reconciliation.');
  }
  if (fence?.contract !== START_FENCE_CONTRACT
    || fence.runtime_generation !== 'executor_only'
    || typeof fence.reconciled_at !== 'string'
    || !['fresh_clean', 'committed_reconciliation'].includes(fence.issuance_kind)) {
    throw new Error('Executor startup reconciliation fence is invalid.');
  }
  requireIssuanceProof(fence.issuance_kind === 'fresh_clean'
    ? { kind: 'fresh_clean', installation_root_absent: fence.installation_root_absent }
    : {
      kind: 'committed_reconciliation',
      upgrade_id: fence.upgrade_id,
      legacy_services_quiesced: fence.legacy_services_quiesced,
      legacy_registrations_absent: fence.legacy_registrations_absent,
      legacy_artifacts_reconciled: fence.legacy_artifacts_reconciled,
    });
  return Object.freeze(fence);
}
