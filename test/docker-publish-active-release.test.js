import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import { publishDockerActiveRelease } from '../docker/publish-active-release.js';

function fixture() {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'zylos-docker-release-'));
  const zylosDir = path.join(directory, 'zylos');
  const packageRoot = path.join(directory, 'package');
  fs.mkdirSync(path.join(packageRoot, 'runtime', 'executor'), { recursive: true });
  fs.mkdirSync(path.join(packageRoot, 'skills', 'comm-bridge', 'scripts'), { recursive: true });
  fs.writeFileSync(path.join(packageRoot, 'package.json'), JSON.stringify({
    name: 'zylos',
    version: '0.6.0',
  }));
  fs.writeFileSync(path.join(packageRoot, 'runtime', 'executor', 'daemon.js'), '');
  fs.writeFileSync(path.join(packageRoot, 'skills', 'comm-bridge', 'scripts', 'c4-receive.js'), '');
  fs.mkdirSync(zylosDir, { recursive: true });
  return { directory, zylosDir, packageRoot };
}

describe('Docker active release publisher', () => {
  test('copies the installed Core package into the shared release store', () => {
    const { directory, zylosDir, packageRoot } = fixture();
    try {
      const result = publishDockerActiveRelease({
        zylosDir,
        packageRoot,
        hostname: 'container-001',
        upgradeId: 'docker-test',
      });

      expect(result.releasePath).toBe(path.join(
        zylosDir,
        'runtime',
        'releases',
        'docker-0.6.0-container-001',
      ));
      expect(fs.existsSync(path.join(
        result.releasePath,
        'skills',
        'comm-bridge',
        'scripts',
        'c4-receive.js',
      ))).toBe(true);
      expect(JSON.parse(fs.readFileSync(result.activeReleasePath, 'utf8'))).toEqual({
        release_ref: 'docker-0.6.0-container-001',
        release_path: result.releasePath,
        upgrade_id: 'docker-test',
      });
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });
});
