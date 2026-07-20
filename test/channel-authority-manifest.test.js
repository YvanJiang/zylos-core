import fs from 'node:fs';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import {
  isCertifiedChannelAuthority,
  readChannelAuthorityManifest,
  validateChannelAuthorityManifest,
} from '../runtime/migration/channel-authority-manifest.js';

function scope(overrides = {}) {
  return {
    channel: 'feishu', region: 'cn', tenant_id: 'tenant-one', bot_id: 'app-one',
    verified_at: '2026-07-21T00:00:00.000Z', verification_source: 'authenticated_event',
    provider_instance_id: 'provider-one', ...overrides,
  };
}

function document(scopes) {
  return { schema_version: 1, contract: 'zylos.channel-authority', scopes };
}

describe('legacy channel authority manifest', () => {
  test('normalizes an authenticated scope and produces a stable content hash', () => {
    const first = validateChannelAuthorityManifest(document([scope()]));
    const second = validateChannelAuthorityManifest(document([scope()]));
    expect(first.document.scopes).toEqual([scope()]);
    expect(first.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(second.sha256).toBe(first.sha256);
  });

  test('rejects ambiguous scopes, synthetic proofs, and channel-region mismatch', () => {
    expect(() => validateChannelAuthorityManifest(document([scope(), scope()])))
      .toThrow('ambiguous authority scopes');
    expect(() => validateChannelAuthorityManifest(document([
      scope({ verification_source: 'configuration' }),
    ]))).toThrow('authenticated event proof');
    expect(() => validateChannelAuthorityManifest(document([
      scope({ region: 'global' }),
    ]))).toThrow('requires authoritative region cn');
    expect(() => validateChannelAuthorityManifest(document([
      scope(),
      scope({ channel: 'lark', region: 'global', tenant_id: 'tenant-two', bot_id: 'app-two' }),
    ]))).toThrow('provider instance binding is ambiguous');
  });

  test('reads only an explicit owner-controlled manifest file', () => {
    const root = fs.mkdtempSync('/tmp/zylos-channel-authority-');
    const installationRoot = path.join(root, 'zylos');
    const file = path.join(installationRoot, 'runtime', 'channel-authority.json');
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, JSON.stringify(document([scope()])), { mode: 0o600 });
      const certified = readChannelAuthorityManifest(file, { expectedPath: file });
      expect(certified).toMatchObject({
        path: fs.realpathSync(file), document: document([scope()]),
        provider_binding: 'owner_only_exact_path_authenticated_event',
      });
      expect(isCertifiedChannelAuthority(certified, {
        installationRoot: fs.realpathSync(installationRoot),
      })).toBe(true);
      expect(isCertifiedChannelAuthority({
        ...certified, provider_binding: 'owner_only_exact_path_authenticated_event',
      }, { installationRoot: fs.realpathSync(installationRoot) })).toBe(false);
      expect(() => readChannelAuthorityManifest(file, {
        expectedPath: path.join(root, 'different-authority.json'),
      })).toThrow('provider prerequisite path');
      fs.chmodSync(file, 0o666);
      expect(() => readChannelAuthorityManifest(file)).toThrow('ownership or mode is unsafe');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
