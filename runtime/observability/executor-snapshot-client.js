import path from 'node:path';

import { validateObservabilitySnapshot } from '../../contracts/public/index.js';
import { requestExecutorService } from '../executor/service-host.js';

function requireZylosDir(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an absolute path');
  }
  if (path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must not be a filesystem root');
  }
  return zylosDir;
}

export function executorObservabilitySocketPath(zylosDir) {
  return path.join(requireZylosDir(zylosDir), 'runtime', 'executor-service.sock');
}

export async function readExecutorObservability({
  zylosDir,
  requestFn = requestExecutorService,
} = {}) {
  if (typeof requestFn !== 'function') {
    throw new TypeError('requestFn must be a function');
  }
  const response = await requestFn(
    executorObservabilitySocketPath(zylosDir),
    { action: 'health' },
  );
  if (response?.ok !== true || response.result?.snapshot === undefined) {
    throw new Error(response?.error ?? 'executor_observability_unavailable');
  }
  return validateObservabilitySnapshot(response.result.snapshot).forwarded;
}
