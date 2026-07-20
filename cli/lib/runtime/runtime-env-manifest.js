import fs from 'node:fs';
import path from 'node:path';

/** Deploy the provider-neutral runtime environment manifest once. */
export function deployManifestTemplate(templatePath, zylosDir) {
  const destination = path.join(zylosDir, '.zylos', 'runtime-env.manifest');
  if (fs.existsSync(destination)) return 'exists';
  if (!fs.existsSync(templatePath)) return 'template_missing';
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.copyFileSync(templatePath, destination);
  return 'created';
}
