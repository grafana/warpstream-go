const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport } = require('./simulation-report.cjs');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { renderWorkload, validateReport } = require('./simulation-report.cjs');

const row = (passes, failures = []) => ({ scenario: 'rolling restart', wgo: { attempts: 120, passes }, kgo: { attempts: 120, passes: 29 }, failures });

test('shows the independent observed range for each client without requiring equal pass counts', () => {
  const first = row(120); first.kgo.passes = 25;
  const output = renderWorkload('Mimir', [[first], [row(118, ['success below floor'])], [row(119, ['success below floor'])]]);
  assert.ok(output.includes('| rolling restart | ✅ **99.2 ± 0.8 %** | — | 22.5 ± 1.7 % |'));
  assert.match(output, /wgo \(PR\) success \| wgo \(main\) success \| kgo success/);
  assert.doesNotMatch(output, /gates met/);
});

test('reports missing runs without fabricating successes', () => {
  const output = renderWorkload('Mimir', [[row(120)], null, [row(120)]]);
  assert.match(output, /PR run 2 did not produce a report/);
  assert.match(output, /\| rolling restart \| — \| — \| — \|/);
  assert.doesNotMatch(output, /120\/120/);
});

test('includes a zero-pass worst run', () => {
  assert.ok(renderWorkload('Mimir', [[row(120)], [row(0)], [row(119)]]).includes('| rolling restart | 50.0 ± 50.0 % | — | 24.2 ± 0.0 % |'));
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
  assert.match(report, /0.0 ± 0.0 %/);
  assert.match(report, /\*\*100%\*\*/);
});

test('normalizes each real remote-write run before computing its range', () => {
  const remote = (attempts, passes) => ({ scenario: 'prometheus', observation_kind: 'remote_write_http_attempt', wgo: { attempts, passes }, kgo: { attempts: 100, passes: 75 } });
  const output = renderWorkload('Mimir', [[remote(10, 9)], [remote(100, 80)], [remote(200, 170)]]);
  assert.ok(output.includes('| prometheus (HTTP attempts) | ✅ **85.0 ± 5.0 %** | — | 75.0 ± 0.0 % |'));
});

test('rejects inconsistent observation kinds and unknown event kinds', () => {
  const remote = { ...row(100), observation_kind: 'remote_write_http_attempt' };
  assert.throws(() => renderWorkload('Mimir', [[remote], [row(100)], [remote]]), /observation kinds differ/);
  assert.throws(() => validateReport([{ ...row(100), observation_kind: 'unknown' }]), /Invalid observation kind/);
});


test('compares PR and main through the same scenarios', () => {
  const pr = [[row(110)], [row(105)], [row(108)]];
  const main = [[row(100)], [row(101)], [row(102)]];
  const output = renderWorkload('Mimir', pr, main);
  assert.ok(output.includes('| rolling restart | ✅ **89.6 ± 2.1 %** | 84.2 ± 0.8 % | 24.2 ± 0.0 % |'));
  assert.throws(() => renderWorkload('Mimir', pr, [[{ ...row(100), scenario: 'other' }], ...main.slice(1)]), /scenario names differ/);
});

test('sorts the weakest PR result first, independently of the main result', () => {
  const rows = [ { ...row(120), scenario: 'a healthy' }, { ...row(5), scenario: 'z weak' } ];
  const main = [ { ...row(0), scenario: 'a healthy' }, { ...row(120), scenario: 'z weak' } ];
  const output = renderWorkload('Mimir', [rows, rows, rows], [main, main, main]);
  assert.ok(output.indexOf('| z weak |') < output.indexOf('| a healthy |'));
});

test('ties highlight PR wgo only across three columns', () => {
  const rows = [{ ...row(120), kgo: { attempts: 120, passes: 120 } }];
  const output = renderWorkload('Mimir', [rows, rows, rows], [rows, rows, rows]);
  assert.ok(output.includes('| rolling restart | ✅ **100%** | 100% | 100% |'));
});

test('validates frozen main and common harness provenance', t => {
  const fs = require('node:fs');
  const os = require('node:os');
  const path = require('node:path');
  const { buildReport } = require('./simulation-report.cjs');
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-provenance-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const mainSHA = 'a'.repeat(40);
  const harnessSHA = 'b'.repeat(40);
  for (const variant of ['pr', 'main']) {
    for (const run of [1, 2, 3]) {
      const artifact = path.join(directory, `simulation-direct-${variant}-${run}-batch-1`);
      fs.mkdirSync(artifact);
      fs.writeFileSync(path.join(artifact, 'report.json'), JSON.stringify([row(120)]));
      fs.writeFileSync(path.join(artifact, 'main-sha.txt'), mainSHA);
      fs.writeFileSync(path.join(artifact, 'harness-sha.txt'), harnessSHA);
      fs.writeFileSync(path.join(artifact, 'client-sha.txt'), variant === 'main' ? mainSHA : harnessSHA);
    }
  }
  assert.match(buildReport(directory), new RegExp(mainSHA));
  const prArtifact = path.join(directory, 'simulation-direct-pr-3-batch-1');
  fs.writeFileSync(path.join(prArtifact, 'client-sha.txt'), mainSHA);
  assert.throws(() => buildReport(directory), /PR harness revision/);
  fs.writeFileSync(path.join(prArtifact, 'client-sha.txt'), harnessSHA);
  const artifact = path.join(directory, 'simulation-direct-main-3-batch-1');
  fs.writeFileSync(path.join(artifact, 'client-sha.txt'), 'c'.repeat(40));
  assert.throws(() => buildReport(directory), /frozen main revision/);
  fs.writeFileSync(path.join(artifact, 'client-sha.txt'), mainSHA);
  fs.writeFileSync(path.join(artifact, 'harness-sha.txt'), 'c'.repeat(40));
  assert.throws(() => buildReport(directory), /one PR harness/);
});

function batchReports(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'simulation-batch-reports-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  for (const variant of ['pr', 'main']) for (const run of [1, 2, 3]) for (const batch of [1, 2, 3, 4, 5]) {
    const target = path.join(directory, `simulation-mimir-${variant}-${run}-batch-${batch}`);
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, 'main-sha.txt'), 'a'.repeat(40));
    fs.writeFileSync(path.join(target, 'harness-sha.txt'), 'b'.repeat(40));
    fs.writeFileSync(path.join(target, 'client-sha.txt'), (variant === 'main' ? 'a' : 'b').repeat(40));
    fs.writeFileSync(path.join(target, 'report.json'), JSON.stringify([{ ...row(120 - run), scenario: `scenario ${batch}` }]));
  }
  return directory;
}

test('combines all scenario batches before computing ranges', t => {
  const output = buildReport(batchReports(t));
  for (const batch of [1, 2, 3, 4, 5]) assert.ok(output.includes(`| scenario ${batch} | 98.3 ± 0.8 % | 98.3 ± 0.8 % | 24.2 ± 0.0 % |`));
});

test('missing batches invalidate only their scenario results', t => {
  const directory = batchReports(t);
  fs.rmSync(path.join(directory, 'simulation-mimir-pr-2-batch-3'), { recursive: true });
  const output = buildReport(directory);
  assert.match(output, /Mimir: pr run 2, batch 3\/5 did not produce a report/);
  assert.ok(output.includes('| scenario 3 | — | ✅ **98.3 ± 0.8 %** | — |'));
  assert.ok(output.includes('| scenario 1 | 98.3 ± 0.8 % | 98.3 ± 0.8 % | 24.2 ± 0.0 % |'));
});

test('rejects duplicated scenarios across batches', t => {
  const directory = batchReports(t);
  fs.writeFileSync(path.join(directory, 'simulation-mimir-pr-1-batch-2/report.json'), JSON.stringify([{ ...row(120), scenario: 'scenario 1' }]));
  assert.throws(() => buildReport(directory), /duplicate scenario/);
});

test('retains scenarios absent from the first available run', t => {
  const directory = batchReports(t);
  fs.rmSync(path.join(directory, 'simulation-mimir-pr-1-batch-3'), { recursive: true });
  const output = buildReport(directory);
  assert.ok(output.includes('| scenario 3 | — | ✅ **98.3 ± 0.8 %** | — |'));
});

test('rejects changed scenario coverage even when a batch is missing', t => {
  const directory = batchReports(t);
  fs.rmSync(path.join(directory, 'simulation-mimir-pr-2-batch-3'), { recursive: true });
  fs.writeFileSync(path.join(directory, 'simulation-mimir-pr-2-batch-1/report.json'), JSON.stringify([row(120)]));
  assert.throws(() => buildReport(directory), /batch 1 scenario names differ/);
});


test('reports acknowledged sample ranges independently of HTTP attempts', () => {
  const remote = samples => ({ ...row(120), observation_kind: 'remote_write_http_attempt',
    wgo_sender: { samples_accepted_observed_total: samples },
    kgo_sender: { samples_accepted_observed_total: 500 } });
  const output = renderWorkload('Mimir', [[remote(1000)], [remote(3000)], [remote(2000)]]);
  assert.ok(output.includes('| rolling restart | ✅ **2,000 ± 1,000** | — | 500 ± 0 |'));
  assert.match(output, /Samples acknowledged by Mimir/);
  assert.match(output, /\*\*100%\*\*/);
});

test('does not invent sample measurements when evidence is missing', () => {
  const remote = { ...row(120), observation_kind: 'remote_write_http_attempt' };
  const output = renderWorkload('Mimir', [[remote], [remote], [remote]]);
  assert.ok(output.includes('| rolling restart | — | — | — |'));
  assert.throws(() => validateReport([{ ...remote, wgo_sender: { samples_accepted_observed_total: -1 } }]), /accepted sample count/);
});

test('rounding never presents partial success as perfect', () => {
  const remote = { ...row(120), observation_kind: 'remote_write_http_attempt', wgo: { attempts: 1000000, passes: 999999 } };
  const output = renderWorkload('Mimir', [[remote], [remote], [remote]]);
  assert.match(output, /99.9999 ± 0.0 %/);
});


test('overlapping success ranges have no highlighted winner', () => {
  const rows = (low, high) => [low, high, low].map(passes => [{ ...row(passes), kgo: { attempts: 120, passes: 0 } }]);
  const output = renderWorkload('Mimir', rows(12, 46), rows(24, 37));
  assert.ok(output.includes('| rolling restart | 24.2 ± 14.2 % | 25.4 ± 5.4 % | 0.0 ± 0.0 % |'));
  assert.doesNotMatch(output, /\*\*/);
});

test('touching success ranges have no highlighted winner', () => {
  const rows = passes => passes.map(p => [{ ...row(p), kgo: { attempts: 120, passes: 0 } }]);
  assert.doesNotMatch(renderWorkload('Mimir', rows([30, 60, 45]), rows([60, 90, 75])), /\*\*/);
});

test('overlapping sample ranges have no highlighted winner', () => {
  const rows = samples => samples.map(n => [{ ...row(120), observation_kind: 'remote_write_http_attempt',
    wgo_sender: { samples_accepted_observed_total: n },
    kgo_sender: { samples_accepted_observed_total: 0 } }]);
  const output = renderWorkload('Mimir', rows([100, 400, 250]), rows([200, 300, 250]));
  assert.ok(output.includes('| rolling restart | 250 ± 150 | 250 ± 50 | 0 ± 0 |'));
});
