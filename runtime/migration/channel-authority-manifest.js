import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import { canonicalizeJson } from '../../contracts/public/index.js';

const CHANNEL_REGIONS = Object.freeze({ feishu: 'cn', lark: 'global' });
const SCOPE_KEYS = Object.freeze([
  'channel', 'region', 'tenant_id', 'bot_id', 'verified_at',
  'verification_source', 'provider_instance_id',
]);

function requireText(name, value) {
  if (typeof value !== 'string' || value.length === 0 || value.trim() !== value) {
    throw new TypeError(`${name} must be a non-empty trimmed string`);
  }
  return value;
}

export function validateChannelAuthorityManifest(document) {
  if (!document || typeof document !== 'object' || Array.isArray(document)
    || document.schema_version !== 1 || document.contract !== 'zylos.channel-authority'
    || !Array.isArray(document.scopes)) {
    throw new TypeError('Channel authority manifest is invalid.');
  }
  const documentKeys = Object.keys(document).sort();
  if (JSON.stringify(documentKeys) !== JSON.stringify(['contract', 'schema_version', 'scopes'])) {
    throw new TypeError('Channel authority manifest contains unsupported fields.');
  }
  const scopes = new Map();
  for (const [index, candidate] of document.scopes.entries()) {
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)
      || JSON.stringify(Object.keys(candidate).sort()) !== JSON.stringify([...SCOPE_KEYS].sort())) {
      throw new TypeError(`Channel authority scope ${index} has an invalid shape.`);
    }
    const scope = {};
    for (const key of SCOPE_KEYS) scope[key] = requireText(`scopes[${index}].${key}`, candidate[key]);
    const requiredRegion = CHANNEL_REGIONS[scope.channel];
    if (requiredRegion !== undefined && scope.region !== requiredRegion) {
      throw new Error(`Channel ${scope.channel} requires authoritative region ${requiredRegion}.`);
    }
    if (scope.verification_source !== 'authenticated_event') {
      throw new Error('Channel authority tenant scope requires an authenticated event proof.');
    }
    if (!Number.isFinite(Date.parse(scope.verified_at))) {
      throw new TypeError(`scopes[${index}].verified_at must be an ISO timestamp.`);
    }
    if (scopes.has(scope.channel)) {
      throw new Error(`Legacy channel ${scope.channel} has ambiguous authority scopes.`);
    }
    scopes.set(scope.channel, Object.freeze(scope));
  }
  const normalized = Object.freeze({
    schema_version: 1,
    contract: 'zylos.channel-authority',
    scopes: Object.freeze([...scopes.values()]),
  });
  return Object.freeze({
    document: normalized,
    sha256: crypto.createHash('sha256').update(canonicalizeJson(normalized)).digest('hex'),
  });
}

export function readChannelAuthorityManifest(file) {
  if (typeof file !== 'string' || !path.isAbsolute(file) || path.parse(file).root === file) {
    throw new TypeError('Channel authority manifest must be an explicit absolute non-root file.');
  }
  const resolved = fs.realpathSync(file);
  const stat = fs.statSync(resolved);
  if (!stat.isFile() || (stat.mode & 0o022) !== 0
    || (typeof process.getuid === 'function' && stat.uid !== process.getuid())) {
    throw new Error('Channel authority manifest ownership or mode is unsafe.');
  }
  return Object.freeze({ ...validateChannelAuthorityManifest(JSON.parse(fs.readFileSync(resolved, 'utf8'))), path: resolved });
}

export function authorityScopeFor(channelAuthority, channel) {
  if (channelAuthority === null || channelAuthority === undefined) return null;
  const document = channelAuthority.document?.contract === 'zylos.channel-authority'
    ? channelAuthority.document : channelAuthority;
  return validateChannelAuthorityManifest(document).document.scopes
    .find((scope) => scope.channel === channel) ?? null;
}
