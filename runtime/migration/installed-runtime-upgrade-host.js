import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';

import { canonicalizeJson } from '../../contracts/public/index.js';
import { createRuntimeUpgradeCoordinator } from './runtime-upgrade-coordinator.js';
import { createRuntimeUpgradeService } from './runtime-upgrade-service.js';
import {
  runtimeUpgradeOwnershipLockPath,
  runtimeUpgradeOwnershipMarkerPath,
} from './upgrade-state.js';

export function createInstalledRuntimeUpgradeHost({
  database,
  snapshotAdapter,
  releaseAdapter,
  legacySourceAdapter = null,
  executorAdapter = null,
  noticeAdapter = null,
  zylosDir,
  now = () => new Date().toISOString(),
  generateId,
  lockOwnerStaleMs = 300_000,
  processId = process.pid,
  hostName = os.hostname(),
  isProcessAlive = (pid) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return error?.code === 'EPERM';
    }
  },
}) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  const ownershipMarkerPath = runtimeUpgradeOwnershipMarkerPath(zylosDir);
  const ownershipLockPath = runtimeUpgradeOwnershipLockPath(zylosDir);
  if (!Number.isSafeInteger(lockOwnerStaleMs) || lockOwnerStaleMs <= 0) {
    throw new TypeError('lockOwnerStaleMs must be a positive safe integer');
  }
  const upgradeService = createRuntimeUpgradeService({
    database,
    now,
    ...(generateId === undefined ? {} : { generateId }),
  });
  const coordinator = createRuntimeUpgradeCoordinator({
    database,
    upgradeService,
    snapshotAdapter,
    releaseAdapter,
    legacySourceAdapter,
    executorAdapter,
    noticeAdapter,
    now,
  });

  async function writeOwnershipMarker() {
    const marker = {
      schema_version: 1,
      runtime_owner: 'durable_executor',
      upgrade_protocol: 'runtime_upgrade_runs',
      legacy_self_upgrade: 'disabled',
      database_path: typeof database.name === 'string' ? database.name : null,
    };
    await fs.mkdir(path.dirname(ownershipMarkerPath), { recursive: true });
    const temporaryPath = `${ownershipMarkerPath}.partial`;
    await fs.writeFile(temporaryPath, `${canonicalizeJson(marker)}\n`, { mode: 0o600 });
    await fs.rename(temporaryPath, ownershipMarkerPath);
    return Object.freeze(marker);
  }

  async function acquireOwnershipLock() {
    await fs.mkdir(path.dirname(ownershipLockPath), { recursive: true });
    const owner = Object.freeze({
      schema_version: 1,
      host: hostName,
      process_id: processId,
      owner_id: crypto.randomUUID(),
      acquired_at: now(),
    });
    for (let attempt = 0; attempt < 3; attempt += 1) {
      let created = false;
      try {
        await fs.mkdir(ownershipLockPath);
        created = true;
      } catch (error) {
        if (error?.code !== 'EEXIST') throw error;
      }
      if (created) {
        try {
          await fs.writeFile(
            path.join(ownershipLockPath, 'owner.json'),
            `${canonicalizeJson(owner)}\n`,
            { mode: 0o600 },
          );
          return owner;
        } catch (error) {
          await fs.rm(ownershipLockPath, { recursive: true, force: true });
          throw error;
        }
      }
      let existing = null;
        try {
          existing = JSON.parse(await fs.readFile(
            path.join(ownershipLockPath, 'owner.json'), 'utf8',
          ));
        } catch {
          // A creator may still be writing owner.json. Only age can make this safely adoptable.
        }
        const localOwner = existing?.schema_version === 1
          && existing.host === hostName
          && Number.isSafeInteger(existing.process_id);
        const locallyAlive = localOwner && isProcessAlive(existing.process_id);
        const locallyDead = localOwner && !locallyAlive;
        const stat = await fs.stat(ownershipLockPath).catch(() => null);
        const expired = stat !== null && Date.now() - stat.mtimeMs >= lockOwnerStaleMs;
        if (locallyAlive || (!locallyDead && !expired)) {
          throw new Error('Another runtime upgrade owns the installation lock.');
        }
        const stalePath = `${ownershipLockPath}.stale-${crypto.randomUUID()}`;
        try {
          await fs.rename(ownershipLockPath, stalePath);
        } catch (renameError) {
          if (renameError?.code === 'ENOENT') continue;
          throw renameError;
        }
      await fs.rm(stalePath, { recursive: true, force: true });
    }
    throw new Error('Runtime upgrade ownership lock could not be acquired after stale adoption.');
  }

  async function releaseOwnershipLock(owner) {
    let current = null;
    try {
      current = JSON.parse(await fs.readFile(path.join(ownershipLockPath, 'owner.json'), 'utf8'));
    } catch {
      return;
    }
    if (current.owner_id !== owner.owner_id) {
      throw new Error('Runtime upgrade ownership lock changed owners before release.');
    }
    await fs.rm(ownershipLockPath, { recursive: true, force: true });
  }

  async function attach(preflight) {
    if (!preflight || typeof preflight !== 'object' || Array.isArray(preflight)) {
      throw new TypeError('preflight must be an object');
    }
    const lockOwner = await acquireOwnershipLock();
    try {
      await writeOwnershipMarker();
      upgradeService.preflight(preflight);
      const run = upgradeService.get(preflight.upgrade_id);
      return Object.freeze({
        upgrade_id: run.upgrade_id,
        state: run.state,
        state_version: run.state_version,
        ownership_marker_path: ownershipMarkerPath,
      });
    } finally {
      await releaseOwnershipLock(lockOwner);
    }
  }

  return Object.freeze({
    advance: (upgradeId, input) => coordinator.advance(upgradeId, input),
    attach,
    get: (upgradeId) => upgradeService.get(upgradeId),
    ownership_marker_path: ownershipMarkerPath,
    requestRollback: (upgradeId, failure) => upgradeService.fail(upgradeId, failure),
  });
}
