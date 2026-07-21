import fs from 'node:fs';
import path from 'node:path';

import { describe, expect, jest, test } from '@jest/globals';

import { reportOperatorSetupRequired } from '../cli/lib/components.js';

describe('AI component install runtime boundary', () => {
  test('reports explicit operator setup without model/session injection or ownerless C4', () => {
    const output = [];
    const log = jest.spyOn(console, 'log').mockImplementation((line = '') => output.push(String(line)));
    try {
      expect(reportOperatorSetupRequired('install', {
        component: 'fixture-component',
        skillDir: '/tmp/disposable-component/fixture-component',
      })).toEqual({
        status: 'operator_setup_required',
        component: 'fixture-component',
        readme: path.join('/tmp/disposable-component/fixture-component', 'README.md'),
      });
    } finally {
      log.mockRestore();
    }
    expect(output.join('\n')).toMatch(/operator setup/i);
    expect(output.join('\n')).not.toMatch(/ZYLOS_TASK|COMPONENT_TASK|Claude|telegram|C4|zylos-cli/i);

    const addSource = fs.readFileSync(path.resolve('cli/commands/add.js'), 'utf8');
    expect(addSource).toMatch(/reportOperatorSetupRequired/);
    expect(addSource).not.toMatch(/outputTask/);
  });
});
