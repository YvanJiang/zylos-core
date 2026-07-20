import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import { removeManagedDataDirectory } from '../cli/commands/self-uninstall.js';

describe('executor uninstall destructive boundary', () => {
  test('deletes only an explicit non-root Zylos fixture', () => {
    const fixture = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-uninstall-'));
    fs.mkdirSync(path.join(fixture, 'runtime'), { recursive: true });
    fs.writeFileSync(path.join(fixture, 'runtime', 'executor-service.sock'), 'fixture');

    expect(removeManagedDataDirectory(fixture)).toEqual({ removed: fixture });
    expect(fs.existsSync(fixture)).toBe(false);
  });

  test('refuses roots and the user home directory', () => {
    expect(() => removeManagedDataDirectory(path.parse(process.cwd()).root)).toThrow('unsafe');
    expect(() => removeManagedDataDirectory(os.homedir())).toThrow('unsafe');
  });
});
