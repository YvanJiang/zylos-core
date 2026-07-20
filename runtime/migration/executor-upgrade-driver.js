const MAX_TRANSITIONS = 64;

function waitingStatus(result) {
  return typeof result?.status === 'string' && result.status.startsWith('waiting_for_');
}

function upgradeResult(preflight, result, extra = {}) {
  return Object.freeze({
    action: 'self_upgrade',
    from: preflight.from_release,
    to: preflight.to_release,
    state: result.state,
    ...extra,
  });
}

async function advanceUntil({ host, preflight, legacyBatch, terminal, onAdvance = () => {} }) {
  for (let transition = 0; transition < MAX_TRANSITIONS; transition += 1) {
    const result = await host.advance(preflight.upgrade_id, { legacyBatch });
    onAdvance(result);
    if (waitingStatus(result)) {
      throw new Error(`Installed runtime upgrade cannot proceed: ${result.status}`);
    }
    if (terminal(result)) return result;
  }
  throw new Error(`Installed runtime upgrade exceeded ${MAX_TRANSITIONS} durable transitions.`);
}

export async function driveInstalledRuntimeUpgrade({ host, preflight, legacyBatch }) {
  if (!host || typeof host.attach !== 'function' || typeof host.advance !== 'function'
    || typeof host.requestRollback !== 'function') {
    throw new TypeError('host must expose the installed runtime upgrade interface');
  }
  if (!preflight || typeof preflight !== 'object' || Array.isArray(preflight)) {
    throw new TypeError('preflight must be an object');
  }
  if (!legacyBatch || typeof legacyBatch.batch_id !== 'string'
    || !Array.isArray(legacyBatch.records)) {
    throw new TypeError('legacyBatch must be a canonical migration batch');
  }
  let latest = await host.attach(preflight);
  if (latest?.state === 'rolled_back') {
    return upgradeResult(preflight, latest, {
      success: false,
      error: latest.failure?.message ?? 'Durable runtime upgrade was rolled back.',
      rollback: { performed: true, state: latest.state, steps: [] },
    });
  }
  if (latest?.state === 'rollback_required') {
    try {
      const rolledBack = await advanceUntil({
        host,
        preflight,
        legacyBatch,
        onAdvance: (result) => { latest = result; },
        terminal: (result) => result.state === 'rolled_back',
      });
      return upgradeResult(preflight, rolledBack, {
        success: false,
        error: latest.failure?.message ?? 'Durable runtime upgrade rollback resumed.',
        rollback: { performed: true, state: rolledBack.state, steps: [] },
      });
    } catch (rollbackError) {
      return upgradeResult(preflight, { state: 'rollback_failed' }, {
        success: false,
        error: latest.failure?.message ?? 'Durable runtime upgrade rollback remains incomplete.',
        rollback: { performed: false, error: rollbackError.message },
      });
    }
  }
  try {
    const committed = await advanceUntil({
      host,
      preflight,
      legacyBatch,
      onAdvance: (result) => { latest = result; },
      terminal: (result) => result.state === 'committed'
        && result.completed_step === 'postcommit-cleanup',
    });
    return upgradeResult(preflight, committed, {
      success: true,
      completedStep: committed.completed_step,
    });
  } catch (error) {
    if (latest?.state === 'committed') {
      return upgradeResult(preflight, latest, {
        success: false,
        committed: true,
        failedStep: 0,
        error: error.message,
        rollback: { performed: false, reason: 'already_committed' },
      });
    }
    try {
      host.requestRollback(preflight.upgrade_id, {
        boundary: 'installed_executor_upgrade',
        code: 'upgrade_effect_failed',
        message: error.message,
      });
      const rolledBack = await advanceUntil({
        host,
        preflight,
        legacyBatch,
        onAdvance: (result) => { latest = result; },
        terminal: (result) => result.state === 'rolled_back',
      });
      return upgradeResult(preflight, rolledBack, {
        success: false,
        failedStep: 0,
        error: error.message,
        rollback: { performed: true, state: rolledBack.state, steps: [] },
      });
    } catch (rollbackError) {
      return upgradeResult(preflight, { state: 'rollback_failed' }, {
        success: false,
        failedStep: 0,
        error: error.message,
        rollback: { performed: false, error: rollbackError.message },
      });
    }
  }
}
