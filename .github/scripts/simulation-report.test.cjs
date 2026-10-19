const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport, renderWorkload, validateReport } = require('./simulation-report.cjs');

const row = (passes, failures = []) => ({ scenario: 'rolling restart', wgo: { attempts: 120, passes }, kgo: { attempts: 120, passes: 29 }, failures });

test('shows the independent minimum for each client without requiring equal pass counts', () => {
  const first = row(120); first.kgo.passes = 25;
  const output = renderWorkload('Mimir', [[first], [row(118, ['success below floor'])], [row(119, ['success below floor'])]]);
  assert.ok(output.includes('| rolling restart | **118/120** | 25/120 |'));
  assert.match(output, /wgo worst of 3 \| kgo worst of 3/);
  assert.doesNotMatch(output, /run 1|gates met/);
});

test('reports missing runs without fabricating successes', () => {
  const output = renderWorkload('Mimir', [[row(120)], null, [row(120)]]);
  assert.match(output, /Run 2 did not produce a report/);
  assert.match(output, /\| rolling restart \| — \| — \|/);
  assert.doesNotMatch(output, /120\/120/);
});

test('includes a zero-pass worst run', () => {
  assert.ok(renderWorkload('Mimir', [[row(120)], [row(0)], [row(119)]]).includes('| rolling restart | 0/120 | **29/120** |'));
});

test('rejects incomplete scenario coverage and inconsistent attempt counts', () => {
  assert.throws(() => renderWorkload('Mimir', [[row(120)], [{ ...row(120), scenario: 'other' }], [row(120)]]), /scenario names differ/);
  const different = row(100); different.wgo.attempts = 119;
  assert.throws(() => renderWorkload('Mimir', [[row(120)], [different], [row(120)]]), /attempt counts differ/);
});

test('rejects invalid counts and duplicate scenarios', () => {
  assert.throws(() => validateReport([row(121)]), /Invalid wgo counts/);
  assert.throws(() => validateReport([row(120), row(120)]), /duplicate scenario/);
});

test('escapes table delimiters and neutralizes mentions', () => {
  const input = [{ ...row(120), scenario: '@all | <scenario>\nnext' }];
  const output = renderWorkload('Mimir', [input, input, input]);
  assert.match(output, /@\u200ball \\\| &lt;scenario&gt; next/);
});

test('scenario expectation failures remain informational', () => {
  const reports = [1, 2, 3].map(() => [{scenario: 'unstable', wgo: {attempts: 120, passes: 0}, kgo: {attempts: 120, passes: 120}, failures: ['success rate below floor']}]);
  const report = renderWorkload('Direct client', reports);
  assert.match(report, /0\/120/);
  assert.match(report, /\*\*120\/120\*\*/);
});

function batchReports(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-batch-reports-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  for (const run of [1, 2, 3]) for (const batch of [1, 2, 3, 4, 5]) {
    const target = path.join(directory, `simulation-mimir-${run}-batch-${batch}`);
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, 'report.json'), JSON.stringify([{ ...row(120 - run), scenario: `scenario ${batch}` }]));
  }
  return directory;
}

test('combines all scenario batches before computing worst of three', t => {
  const output = buildReport(batchReports(t));
  for (const batch of [1, 2, 3, 4, 5]) assert.ok(output.includes(`| scenario ${batch} | **117/120** | 29/120 |`));
});

test('missing batches invalidate only their scenario results', t => {
  const directory = batchReports(t);
  fs.rmSync(path.join(directory, 'simulation-mimir-2-batch-3'), { recursive: true });
  const output = buildReport(directory);
  assert.match(output, /Mimir: run 2, batch 3\/5 did not produce a report/);
  assert.ok(output.includes('| scenario 3 | — | — |'));
  assert.ok(output.includes('| scenario 1 | **117/120** | 29/120 |'));
});

test('rejects duplicated scenarios across batches', t => {
  const directory = batchReports(t);
  fs.writeFileSync(path.join(directory, 'simulation-mimir-1-batch-2/report.json'), JSON.stringify([{ ...row(120), scenario: 'scenario 1' }]));
  assert.throws(() => buildReport(directory), /duplicate scenario/);
});

test('rejects changed scenario coverage even when a batch is missing', t => {
  const directory = batchReports(t);
  fs.rmSync(path.join(directory, 'simulation-mimir-2-batch-3'), { recursive: true });
  fs.writeFileSync(path.join(directory, 'simulation-mimir-2-batch-1/report.json'), JSON.stringify([row(120)]));
  assert.throws(() => buildReport(directory), /batch 1 scenario names differ/);
});
