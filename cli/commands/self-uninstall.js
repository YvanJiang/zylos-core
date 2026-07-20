/** Remove the installed executor service and Zylos package. */

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

import { ZYLOS_DIR } from '../lib/config.js';
import {
  removeExecutorServiceRegistration,
  stopExecutorService,
} from '../lib/executor-service-lifecycle.js';
import { bold, dim, green, red, cyan, success, error, heading } from '../lib/colors.js';
import { promptYesNo } from '../lib/prompts.js';

function requireSafeManagedDirectory(directory) {
  if (typeof directory !== 'string' || !path.isAbsolute(directory)) {
    throw new TypeError('unsafe managed directory: expected an absolute path');
  }
  const resolved = path.resolve(directory);
  if (resolved === path.parse(resolved).root || resolved === path.resolve(os.homedir())) {
    throw new Error(`unsafe managed directory: ${resolved}`);
  }
  return resolved;
}

export function removeManagedDataDirectory(directory) {
  const resolved = requireSafeManagedDirectory(directory);
  fs.rmSync(resolved, { recursive: true, force: true });
  return Object.freeze({ removed: resolved });
}

function cleanShellProfiles() {
  const modified = [];
  for (const name of ['.bashrc', '.zshrc', '.profile', '.bash_profile']) {
    const file = path.join(os.homedir(), name);
    let original;
    try { original = fs.readFileSync(file, 'utf8'); } catch { continue; }
    const lines = original.split('\n').filter((line) => (
      !/^#\s*added by zylos/i.test(line)
      && !/export\s+PATH=.*zylos/i.test(line)
    ));
    const updated = lines.join('\n').replace(/\n{3,}/g, '\n\n');
    if (updated === original) continue;
    fs.writeFileSync(file, updated);
    modified.push(file);
  }
  return modified;
}

export async function selfUninstall(args) {
  const force = args.includes('--force') || args.includes('-f');
  console.log(`\n${heading('Zylos Uninstall')}\n`);
  console.log(bold('This will:'));
  console.log('  1. Ask Core to stop the executor service and verify its identity');
  console.log(`  2. Uninstall the ${cyan('zylos')} npm package`);
  console.log('  3. Clean Zylos shell PATH entries');
  console.log(`  4. ${force ? 'Remove' : 'Optionally remove'} ${cyan(ZYLOS_DIR)}`);
  console.log(`\n${dim('Provider CLIs, Node.js, PM2, and their user configuration are preserved.')}\n`);

  if (!force && !await promptYesNo(red(bold('Continue? [y/N] ')))) {
    console.log('\nCancelled.');
    return { ok: false, cancelled: true };
  }

  console.log(`\n${heading('Stopping executor service')}`);
  const stopped = await stopExecutorService({ zylosDir: ZYLOS_DIR });
  if (!stopped.ok) {
    console.error(error(`Executor shutdown was not confirmed: ${stopped.error}`));
    console.error(dim('No package or data was removed. Restore service health, then retry.'));
    process.exitCode = 1;
    return { ok: false, phase: 'executor_shutdown', error: stopped.error };
  }
  console.log(success(`Executor stopped: ${stopped.serviceInstanceId}`));
  const registration = removeExecutorServiceRegistration();
  if (!registration.ok) {
    console.error(error(`Executor supervisor cleanup failed: ${registration.error}`));
    console.error(dim('The package and managed data were preserved.'));
    process.exitCode = 1;
    return { ok: false, phase: 'supervisor_cleanup', error: registration.error };
  }

  console.log(`\n${heading('Uninstalling package')}`);
  try {
    execFileSync('npm', ['uninstall', '-g', 'zylos'], { stdio: 'pipe' });
  } catch (cause) {
    console.error(error(`Package uninstall failed: ${cause.message}`));
    process.exitCode = 1;
    return { ok: false, phase: 'package_uninstall', error: cause.message };
  }
  console.log(success('zylos package uninstalled'));

  const profiles = cleanShellProfiles();
  if (profiles.length > 0) console.log(success('Zylos shell PATH entries removed'));

  const removeData = force || await promptYesNo(
    red(bold(`Permanently delete ${ZYLOS_DIR}? [y/N] `)),
  );
  if (removeData) {
    removeManagedDataDirectory(ZYLOS_DIR);
    console.log(success(`Removed ${ZYLOS_DIR}`));
  } else {
    console.log(dim(`Data preserved at ${ZYLOS_DIR}`));
  }
  console.log(`\n${green(bold('Zylos has been uninstalled.'))}`);
  return { ok: true, serviceInstanceId: stopped.serviceInstanceId, dataRemoved: removeData };
}
