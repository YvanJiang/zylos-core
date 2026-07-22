#!/usr/bin/env node

import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  CHANNEL_FAULT_PROBE_PREFIX,
  runChannelDeliveryFaultProbe,
} from '../lib/channel-delivery-fault-probe.js';

const [, , repository, repositoryDirectory, scenario] = process.argv;
const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const defaultCoreDirectory = path.resolve(scriptsDirectory, '..', '..');

try {
  const evidence = await runChannelDeliveryFaultProbe({
    repository,
    repositoryDirectory,
    scenario,
    coreDirectory: process.env.ZYLOS_CORE_FAULT_REPO || defaultCoreDirectory,
  });
  process.stdout.write(`${CHANNEL_FAULT_PROBE_PREFIX}${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Global47 channel fault probe failed: ${error.message}\n`);
  process.exitCode = 1;
}
