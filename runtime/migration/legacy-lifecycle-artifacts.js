import fs from 'node:fs';
import path from 'node:path';

function requireZylosDir(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an absolute path');
  }
  if (path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must not be a filesystem root');
  }
  return path.resolve(zylosDir);
}

const RETIRED_RUNTIME_SKILLS = new Set(['activity-monitor']);

export function isRetiredRuntimeSkill(skillName) {
  return RETIRED_RUNTIME_SKILLS.has(skillName);
}

// These exact identifiers exist only for one-time post-commit cleanup. Nothing in
// the normal runtime imports, dispatches, executes, or selects these artifacts.
export function legacyLifecycleArtifactPaths(zylosDir) {
  const root = requireZylosDir(zylosDir);
  return Object.freeze([
    path.join(root, 'pm2', 'tmux-runtime.config.cjs'),
    path.join(root, '.zylos', 'tmux-runtime.json'),
    path.join(root, 'bin', 'start-tmux-runtime.sh'),
    path.join(root, '.claude', 'skills', 'activity-monitor'),
    path.join(root, '.codex', 'skills', 'activity-monitor'),
  ]);
}

export function cleanupObsoleteLifecycleArtifacts({ zylosDir, upgradeState }) {
  if (upgradeState !== 'committed') {
    throw new Error('Obsolete lifecycle artifacts may be removed only after the upgrade is durably committed.');
  }
  const removed = [];
  for (const artifact of legacyLifecycleArtifactPaths(zylosDir)) {
    let stat;
    try {
      stat = fs.lstatSync(artifact);
    } catch (error) {
      if (error?.code === 'ENOENT') continue;
      throw error;
    }
    if (!stat.isFile() && !stat.isSymbolicLink() && !stat.isDirectory()) {
      throw new Error(`Refusing to remove unexpected lifecycle artifact type: ${artifact}`);
    }
    if (stat.isDirectory()) fs.rmSync(artifact, { recursive: true });
    else fs.unlinkSync(artifact);
    removed.push(artifact);
  }
  return Object.freeze({ removed: Object.freeze(removed) });
}
