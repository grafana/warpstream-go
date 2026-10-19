const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { discoverWorkloads } = require('./simulation-workloads.cjs');

test('discovers the direct suite without requiring integration modules', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-workloads-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  fs.mkdirSync(path.join(root, 'pkg/internal/simulation'), { recursive: true });
  assert.deepEqual(discoverWorkloads(root).map(w => w.name), ['direct']);
});

test('discovers nested Go modules and skips ordinary source directories', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-workloads-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const simulation = path.join(root, 'pkg/internal/simulation');
  fs.mkdirSync(path.join(simulation, 'runner'), { recursive: true });
  fs.mkdirSync(path.join(simulation, 'integration'));
  fs.writeFileSync(path.join(simulation, 'integration/go.mod'), 'module example.com/integration\n');
  assert.deepEqual(discoverWorkloads(root).map(w => w.name), ['direct', 'integration']);
  assert.equal(discoverWorkloads(root)[1].goVersionFile, 'pkg/internal/simulation/integration/go.mod');
  assert.equal(discoverWorkloads(root)[1].timeout, '20m');
});

test('rejects names that cannot safely identify CI artifacts', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-workloads-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const module = path.join(root, 'pkg/internal/simulation/bad name');
  fs.mkdirSync(module, { recursive: true });
  fs.writeFileSync(path.join(module, 'go.mod'), 'module example.com/integration\n');
  assert.throws(() => discoverWorkloads(root), /Invalid integration/);
});
