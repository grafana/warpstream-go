const fs = require('node:fs');
const path = require('node:path');

function discoverWorkloads(root = path.resolve(__dirname, '../..')) {
  const directory = 'pkg/internal/simulation';
  const workloads = [{ name: 'direct', directory, goVersionFile: 'go.mod', timeout: '3m', batches: 1 }];
  for (const entry of fs.readdirSync(path.join(root, directory), { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isDirectory() || !fs.existsSync(path.join(root, directory, entry.name, 'go.mod'))) continue;
    if (!/^[a-z][a-z0-9-]*$/.test(entry.name) || entry.name === 'direct') throw new Error('Invalid integration workload name');
    const module = `${directory}/${entry.name}`;
    workloads.push({ name: entry.name, directory: module, goVersionFile: `${module}/go.mod`, timeout: '20m', batches: 5 });
  }
  return workloads;
}

module.exports = { discoverWorkloads };
if (require.main === module) {
  const workloads = discoverWorkloads();
  const selected = process.argv.includes('--batches')
    ? workloads.flatMap(workload => Array.from({ length: workload.batches }, (_, i) => ({ ...workload, batch: i + 1 })))
    : workloads;
  process.stdout.write(JSON.stringify(selected));
}
