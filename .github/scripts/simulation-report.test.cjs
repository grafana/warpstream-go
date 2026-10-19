const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport, renderWorkload, validateReport, mergeClientReports } = require('./simulation-report.cjs');

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

const lane = (client, passes, scenario = 'rolling restart') => ({ scenario, [client]: { attempts: 120, passes } });

test('merges separate client reports by scenario name', () => {
  const merged = mergeClientReports([lane('wgo', 120), lane('wgo', 119, 'healthy')], [lane('kgo', 120, 'healthy'), lane('kgo', 29)]);
  assert.equal(merged[0].kgo.passes, 29);
  assert.equal(merged[1].kgo.passes, 120);
  assert.throws(() => mergeClientReports([lane('wgo', 120)], [lane('kgo', 120, 'other')]), /scenario names differ/);
  assert.throws(() => mergeClientReports([row(120)], [lane('kgo', 29)]), /Unexpected kgo/);
  assert.throws(() => mergeClientReports([lane('kgo', 29)], null), /Invalid wgo/);
});

test('keeps a complete lane visible when the other lane has a missing run', () => {
  const reports = [mergeClientReports([lane('wgo', 120)], [lane('kgo', 29)]),
    mergeClientReports([lane('wgo', 118)], null), mergeClientReports([lane('wgo', 119)], [lane('kgo', 30)])];
  const output = renderWorkload('Mimir', reports);
  assert.match(output, /Run 2 did not produce a kgo report/);
  assert.ok(output.includes('| rolling restart | 118/120 | — |'));
  assert.doesNotMatch(output, /\*\*118\/120\*\*/);
});

test('reads the workload-client-run artifact layout', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-client-reports-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  for (const run of [1, 2, 3]) for (const client of ['wgo', 'kgo']) {
    const target = path.join(directory, `simulation-direct-${client}-${run}`);
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, 'report.json'), JSON.stringify([lane(client, client === 'wgo' ? 120 - run : run)]));
  }
  assert.ok(buildReport(directory).includes('| rolling restart | **117/120** | 1/120 |'));
});
