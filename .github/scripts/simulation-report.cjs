const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { discoverWorkloads } = require('./simulation-workloads.cjs');

const marker = '<!-- wgo-simulation-report -->';

function validateReport(rows) {
  if (!Array.isArray(rows) || rows.length === 0) throw new Error('Empty simulation report');
  const names = new Set();
  for (const row of rows) {
    if (typeof row.scenario !== 'string' || !row.scenario || row.scenario.length > 200 || names.has(row.scenario)) {
      throw new Error('Invalid or duplicate scenario name');
    }
    names.add(row.scenario);
    if (row.observation_kind !== undefined && row.observation_kind !== 'remote_write_http_attempt') {
      throw new Error('Invalid observation kind');
    }
    for (const client of ['wgo', 'kgo']) {
      const counts = row[client];
      if (!counts || !Number.isSafeInteger(counts.attempts) || counts.attempts <= 0 ||
          !Number.isSafeInteger(counts.passes) || counts.passes < 0 || counts.passes > counts.attempts) {
        throw new Error(`Invalid ${client} counts`);
      }
      const samples = row[`${client}_sender`]?.samples_accepted_observed_total;
      if (samples !== undefined && (!Number.isSafeInteger(samples) || samples < 0)) {
        throw new Error(`Invalid ${client} accepted sample count`);
      }
    }
    if (row.benchmark !== undefined && typeof row.benchmark !== 'boolean') throw new Error('Invalid benchmark flag');
    if (row.failures !== undefined && (!Array.isArray(row.failures) || row.failures.some(f => typeof f !== 'string'))) {
      throw new Error('Invalid expectation failures');
    }
  }
  return rows;
}

function escapeCell(value) {
  return value.replaceAll('|', '\\|').replaceAll('\n', ' ').replaceAll('\r', ' ').replaceAll('@', '@\u200b').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function percentage(value) {
  let precision = 1;
  while (precision < 15 && value > 0 && value < 100 && [0, 100].includes(Number(value.toFixed(precision)))) precision++;
  return value.toFixed(precision);
}

function successRange(counts) {
  const rates = counts.map(c => c.passes / c.attempts);
  const low = Math.min(...rates), high = Math.max(...rates);
  const mean = rates.reduce((sum, rate) => sum + rate, 0) / rates.length;
  return { cell: low === 1 ? '100%' : `${percentage(50 * (low + high))} ± ${percentage(50 * (high - low))} %`, rate: low, high, mean };
}

// Icons shown in front of the PR wgo result. The gate only applies to benchmark scenarios; the
// trend compares the PR with main for every scenario.
const icons = { pass: '✅', fail: '❌', up: '🟩', down: '🟥', same: '🟦', none: '', warn: '⚠️' };

// Two results within this many percentage points, or whose three-run ranges overlap, count as
// the same: three runs of one build already differ by that much from timing alone.
const sameBandPoints = 2;

// trendIcon compares the PR's wgo range with main's. Perfection is held to a stricter rule:
// a drop from 100% is a regression however small, and reaching 100% from below is an
// improvement however small.
function trendIcon(pr, main) {
  if (pr.cell === '—' || main.cell === '—') return icons.none;
  if (main.rate === 1) return pr.rate === 1 ? icons.same : icons.down;
  if (pr.rate === 1) return icons.up;
  const overlap = pr.rate <= main.high && main.rate <= pr.high;
  const delta = 100 * (pr.mean - main.mean);
  if (overlap || Math.abs(delta) <= sameBandPoints) return icons.same;
  return delta > 0 ? icons.up : icons.down;
}

// gateIcon is only shown for benchmark scenarios, whose expectations are meant to gate a PR.
function gateIcon(benchmark, prRuns) {
  if (!benchmark) return icons.none;
  const rows = prRuns.filter(Boolean);
  if (rows.length !== prRuns.length || rows.length === 0) return icons.none;
  return rows.every(row => !(row.failures || []).some(aboutWgo)) ? icons.pass : icons.fail;
}

// Failures about the kgo reference run (for example a fault that never reached kgo's traffic)
// check that the scenario was set up the same for both clients and say nothing about wgo.
const aboutKgo = failure => failure.startsWith('kgo');
const aboutWgo = failure => !aboutKgo(failure);

// kgoWarning marks a kgo result that cannot be compared with wgo's, so the gate ignores it
// but a reader does not take the kgo number at face value.
function kgoWarning(prRuns) {
  return prRuns.some(row => (row?.failures || []).some(aboutKgo)) ? icons.warn : icons.none;
}

function sampleRange(runs, client) {
  const counts = runs.map(row => row?.[`${client}_sender`]?.samples_accepted_observed_total);
  if (counts.length !== 3 || counts.some(c => c === undefined)) return { cell: '—', rate: -1 };
  const low = Math.min(...counts), high = Math.max(...counts);
  return { cell: `${((low + high) / 2).toLocaleString('en-US')} ± ${((high - low) / 2).toLocaleString('en-US')}`, rate: low, high };
}

// above reports whether one range is clearly better than another: entirely above it and, for
// success percentages, by more than the band that the trend icon treats as "the same". Without
// this a bold value could sit next to a "=" icon.
function clearlyAbove(candidate, other) {
  if (!(candidate.rate > other.high)) return false;
  if (candidate.mean === undefined || other.mean === undefined) return true;
  return 100 * (candidate.mean - other.mean) > sameBandPoints;
}

function comparisonCells(columns, prefix = '', kgoMark = '') {
  const perfect = columns.findIndex(c => c.cell === '100%');
  const best = perfect !== -1 ? perfect : columns.findIndex((candidate, i) =>
    candidate.cell !== '—' && columns.every((other, j) =>
      i === j || other.cell === '—' || clearlyAbove(candidate, other)));
  return columns.map((column, i) => {
    const cell = column.cell !== '—' && i === best ? `**${column.cell}**` : column.cell;
    if (i === 0 && prefix) return `${prefix} ${cell}`;
    return i === columns.length - 1 && kgoMark ? `${kgoMark} ${cell}` : cell;
  }).join(' | ');
}

// The rows keep one fixed order, so a scenario sits in the same place in every comment and the
// two sides of a threshold pair are adjacent. A scenario missing from the list goes last.
const scenarioOrder = require('./simulation-order.json');
const orderOf = name => {
  const index = scenarioOrder.indexOf(name);
  return index === -1 ? scenarioOrder.length : index;
};

function renderWorkload(title, reports, mainReports = [null, null, null], incomplete = false) {
  const lines = [`### ${title}`, ''];
  for (const [label, runs] of [['PR', reports], ['main', mainReports]]) {
    for (let i = 0; i < runs.length; i++) {
      if (!runs[i]) lines.push(`${label} run ${i + 1} did not produce a report; see the failed CI job.`, '');
    }
  }
  const available = [...reports, ...mainReports].filter(Boolean);
  if (!available.length) return lines.join('\n');
  for (const rows of available) validateReport(rows);
  const expected = [...new Set(available.flatMap(rows => rows.map(r => r.scenario)))].sort();
  for (const rows of available) {
    if (!incomplete && JSON.stringify(rows.map(r => r.scenario).sort()) !== JSON.stringify(expected)) {
      throw new Error(`${title}: scenario names differ between runs`);
    }
  }
  const prRuns = reports.map(rows => rows && new Map(rows.map(row => [row.scenario, row])));
  const baseRuns = mainReports.map(rows => rows && new Map(rows.map(row => [row.scenario, row])));
  const entries = expected.map(name => {
    const pr = prRuns.map(run => run?.get(name));
    const main = baseRuns.map(run => run?.get(name));
    const rows = [...pr, ...main].filter(Boolean);
    const kinds = rows.map(row => row.observation_kind || 'application_write');
    if (kinds.some(kind => kind !== kinds[0])) throw new Error(`${title}: observation kinds differ between runs`);
    const variableAttempts = kinds[0] === 'remote_write_http_attempt';
    const allCounts = rows.flatMap(row => [row.wgo.attempts, row.kgo.attempts]);
    if (!variableAttempts && allCounts.some(value => value !== allCounts[0])) {
      throw new Error(`${title}: attempt counts differ between runs`);
    }
    const columns = [[pr, 'wgo'], [main, 'wgo'], [pr, 'kgo']].map(([runs, client]) => {
      const counts = runs.filter(Boolean).map(row => row[client]);
      if (counts.length !== 3) return { cell: '—', rate: -1 };
      return successRange(counts);
    });
    const benchmark = rows.some(row => row.benchmark === true);
    const samples = [[pr, 'wgo'], [main, 'wgo'], [pr, 'kgo']].map(([runs, client]) => sampleRange(runs, client));
    return { name, variableAttempts, columns, samples, benchmark, gate: gateIcon(benchmark, pr), kgoMark: kgoWarning(pr), trend: trendIcon(columns[0], columns[1]) };
  });
  entries.sort((a, b) => orderOf(a.name) - orderOf(b.name) || a.name.localeCompare(b.name));
  const render = (title, group) => {
    if (!group.length) return;
    if (lines[lines.length - 1] !== '') lines.push('');
    lines.push(`#### ${title}`, '', '| Scenario | wgo (PR) success | wgo (main) success | kgo success |', '| --- | ---: | ---: | ---: |');
    for (const entry of group) {
      const prefix = [entry.gate, entry.trend].filter(Boolean).join(' ');
      lines.push(`| ${escapeCell(entry.name)}${entry.variableAttempts ? ' (HTTP attempts)' : ''} | ${comparisonCells(entry.columns, prefix, entry.kgoMark)} |`);
    }
  };
  render('Gated checks', entries.filter(entry => entry.benchmark));
  render('Other checks', entries.filter(entry => !entry.benchmark));
  const remoteEntries = entries.filter(entry => entry.variableAttempts);
  if (remoteEntries.length) {
    lines.push('', '#### Samples acknowledged by Mimir', '',
      '| Scenario | wgo (PR) samples | wgo (main) samples | kgo samples |',
      '| --- | ---: | ---: | ---: |');
    for (const entry of remoteEntries) lines.push(`| ${escapeCell(entry.name)} | ${comparisonCells(entry.samples)} |`);
  }
  return lines.join('\n');
}

function buildReport(directory, heading = 'Simulation results', source = '') {
  const sections = [marker, `## ${heading}`, '',
    'Three independent runs per client variant and scenario, using the same PR harness. Success is normalized to percentages. Partial results show the midpoint ± half the observed min/max range, with one percent sign for both numbers (for example, 25.0 ± 5.0 % spans 20–30%); 100% means all three runs were perfect. This is an observed range, not a confidence interval. Rows keep a fixed order, so each scenario sits in the same place in every comment and the two sides of a threshold pair are adjacent. HTTP attempts include real remote-write retries; failed attempts do not imply lost samples. Scenario gates are informational; build and harness errors still fail CI.', '',
    '> **Run-to-run variance:** Results below 100% success can vary because of randomness and wall-clock timing during testing.', '',
    'kgo comes from the PR runs. Main-run kgo measurements are retained in the artifacts. All runs, gates, latency, sender, fault and error details are available in the artifacts.', ''];
  sections.push('Sample counts show midpoint ± half-range across three runs; more is better. They count samples in remote-write requests issued during the observation window that receive HTTP 2xx, including their bounded drain. Warmup and failed attempts are excluded. Acknowledgements do not establish unique or durably stored samples.', '');
  const mainSHAs = new Set();
  const harnessSHAs = new Set();
  for (const workload of discoverWorkloads()) {
    const title = workload.name === 'direct' ? 'Direct client' : workload.name[0].toUpperCase() + workload.name.slice(1);
    let incomplete = false;
    const batchNames = new Map();
    const load = variant => [1, 2, 3].map(run => {
      const rows = [];
      for (let batch = 1; batch <= workload.batches; batch++) {
        const artifact = path.join(directory, `simulation-${workload.name}-${variant}-${run}-batch-${batch}`);
        const file = path.join(artifact, 'report.json');
        if (!fs.existsSync(file)) {
          incomplete = true;
          sections.push(`${title}: ${variant} run ${run}, batch ${batch}/${workload.batches} did not produce a report; see the failed CI job.`, '');
          continue;
        }
        if (fs.statSync(file).size > 1024 * 1024) throw new Error('Simulation report too large');
        const sha = name => {
          const value = fs.readFileSync(path.join(artifact, name), 'utf8').trim();
          if (!/^[a-f0-9]{40}$/.test(value)) throw new Error(`Invalid ${name}`);
          return value;
        };
        const main = sha('main-sha.txt');
        mainSHAs.add(main);
        const harness = sha('harness-sha.txt');
        harnessSHAs.add(harness);
        const client = sha('client-sha.txt');
        if (variant === 'main' && client !== main) throw new Error('Main client does not match the frozen main revision');
        if (variant === 'pr' && client !== harness) throw new Error('PR client does not match the PR harness revision');
        const batchRows = validateReport(JSON.parse(fs.readFileSync(file, 'utf8')));
        const names = JSON.stringify(batchRows.map(row => row.scenario).sort());
        if (batchNames.has(batch) && batchNames.get(batch) !== names) throw new Error(`${title}: batch ${batch} scenario names differ between runs`);
        batchNames.set(batch, names);
        rows.push(...batchRows);
      }
      return rows.length ? validateReport(rows) : null;
    });
    const prReports = load('pr');
    const mainReports = load('main');
    sections.push(renderWorkload(title, prReports, mainReports, incomplete), '');
  }
  if (mainSHAs.size > 1 || harnessSHAs.size > 1) throw new Error('Client variants did not use one main revision and one PR harness');
  if (mainSHAs.size) sections.push(`Main client: \`${[...mainSHAs][0]}\` · Harness: \`${[...harnessSHAs][0]}\``, '');
  if (source) sections.push(source, '');
  sections.push(`The icon in front of each PR wgo result compares it with main: ${icons.up} improved, ${icons.down} regressed, ${icons.same} the same, meaning within ${sameBandPoints} points or with overlapping ranges. A scenario that was 100% on main and is lower in any PR run is always ${icons.down}, and reaching 100% from below is always ${icons.up}. Gated checks also show ${icons.pass} when the scenario met its expectations in all three PR runs and ${icons.fail} when it missed them in at least one. Failures about the kgo reference run alone, such as a fault that never reached kgo's traffic, are ignored, because they say nothing about wgo; they are marked ${icons.warn} in front of the kgo result, which means the kgo comparison is inconclusive: rerun before relying on it. Only these checks are meant to block a PR later, and CI does not enforce them yet.`, '');
  sections.push('**Bold** marks a range entirely above every other available range, or perfect 100% success. Overlapping or touching partial ranges have no highlighted winner. Perfect ties prefer PR wgo. These three-run ranges do not establish statistical significance.', '');
  return sections.join('\n');
}

function api(endpoint, options = []) {
  return JSON.parse(execFileSync('gh', ['api', endpoint, ...options], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }));
}

function publish(directory) {
  const repository = process.env.GH_REPO;
  const number = process.env.PR_NUMBER;
  const sha = process.env.PR_HEAD_SHA;
  if (!/^\d+$/.test(number || '') || !/^[a-f0-9]{40}$/.test(sha || '')) throw new Error('Invalid PR identity');
  const pr = api(`repos/${repository}/pulls/${number}`);
  if (pr.head.sha !== sha) return console.log('Skipping report for an outdated PR head');
  const runURL = `${process.env.GITHUB_SERVER_URL}/${repository}/actions/runs/${process.env.GITHUB_RUN_ID}`;
  const body = buildReport(directory, 'Simulation results', `Commit: \`${sha}\` · [CI run and artifacts](${runURL})`);
  if (body.length > 60000) throw new Error('Simulation comment too large');
  const pages = api(`repos/${repository}/issues/${number}/comments`, ['--paginate', '--slurp']);
  const previous = pages.flat().find(comment => comment.user.login === 'github-actions[bot]' && comment.body.includes(marker));
  const endpoint = previous ? `repos/${repository}/issues/comments/${previous.id}` : `repos/${repository}/issues/${number}/comments`;
  const result = execFileSync('gh', ['api', endpoint, '--method', previous ? 'PATCH' : 'POST', '--input', '-'], {
    input: JSON.stringify({ body }), encoding: 'utf8', maxBuffer: 1024 * 1024,
  });
  console.log(JSON.parse(result).html_url);
}

module.exports = { buildReport, renderWorkload, validateReport, trendIcon, icons };
if (require.main === module) {
  const directory = process.argv[2];
  if (!directory) throw new Error('Usage: simulation-report.cjs DIRECTORY [--publish]');
  if (process.argv.includes('--publish')) publish(directory);
  else process.stdout.write(buildReport(directory));
}
