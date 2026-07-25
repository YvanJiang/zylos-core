import { describe, expect, test } from '@jest/globals';

import { resolveCodexExecutionPolicy } from '../runtime/executor/daemon.js';

describe('executor daemon Codex policy', () => {
  test('keeps the fenced migration defaults', () => {
    expect(resolveCodexExecutionPolicy({})).toEqual({
      approvalPolicy: 'on-request',
      sandbox: 'workspace-write',
    });
  });

  test('honors an explicitly authorized full-access deployment pair', () => {
    expect(resolveCodexExecutionPolicy({
      CODEX_APPROVAL_POLICY: 'never',
      CODEX_SANDBOX_MODE: 'danger-full-access',
    })).toEqual({
      approvalPolicy: 'never',
      sandbox: 'danger-full-access',
    });
  });
});
