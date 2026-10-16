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
    for (const client of ['wgo', 'kgo']) {
      const counts = row[client];
      if (!counts || !Number.isSafeInteger(counts.attempts) || counts.attempts <= 0 ||
          !Number.isSafeInteger(counts.passes) || counts.passes < 0 || counts.passes > counts.attempts) {
        throw new Error(`Invalid ${client} counts`);
      }
    }
    if (row.failures !== undefined && (!Array.isArray(row.failures) || row.failures.some(f => typeof f !== 'string'))) {
      throw new Error('Invalid expectation failures');
    }
  }
  return rows;
}

function escapeCell(value) {
  return value.replaceAll('|', '\\|').replaceAll('\n', ' ').replaceAll('\r', ' ').replaceAll('@', '@\u200b').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function renderWorkload(title, reports) {
  const available = reports.filter(Boolean);
  const lines = [`### ${title}`, ''];
  for (let i = 0; i < reports.length; i++) {
    if (!reports[i]) lines.push(`Run ${i + 1} did not produce a report; see the failed CI job.`, '');
  }
  if (!available.length) return lines.join('\n');
  for (const rows of available) validateReport(rows);
  const expected = available[0].map(r => r.scenario).sort();
  for (const rows of available) {
    if (JSON.stringify(rows.map(r => r.scenario).sort()) !== JSON.stringify(expected)) {
      throw new Error(`${title}: scenario names differ between runs`);
    }
  }
  const byRun = reports.map(rows => rows && new Map(rows.map(row => [row.scenario, row])));
  lines.push('| Scenario | wgo worst of 3 | kgo worst of 3 |');
  lines.push('| --- | ---: | ---: |');
  for (const name of expected) {
    const rows = byRun.map(run => run?.get(name));
    const cells = [];
    const successRates = [];
    for (const client of ['wgo', 'kgo']) {
      const attempts = rows.filter(Boolean).map(row => row[client].attempts);
      if (attempts.some(value => value !== attempts[0])) throw new Error(`${title}: attempt counts differ between runs`);
      const passes = rows.filter(Boolean).map(row => row[client].passes);
      cells.push(available.length === 3 ? `${Math.min(...passes)}/${attempts[0]}` : '—');
      successRates.push(Math.min(...passes) / attempts[0]);
    }
    const best = successRates[0] >= successRates[1] ? 0 : 1;
    const formatted = cells.map((cell, i) => cell !== '—' && i === best ? `**${cell}**` : cell);
    lines.push(`| ${escapeCell(name)} | ${formatted.join(' | ')} |`);
  }
  return lines.join('\n');
}

function buildReport(directory, heading = 'Simulation results', source = '') {
  const sections = [marker, `## ${heading}`, '',
    'Three independent runs per scenario. Each client shows its lowest successful application writes / attempts across the three runs. Scenario gates are informational; build and harness errors still fail CI.', '',
    'All individual runs, scenario gates, latency, fault and error details are available in the run artifacts.', ''];
  for (const workload of discoverWorkloads()) {
    const key = workload.name;
    const title = key === 'direct' ? 'Direct client' : key[0].toUpperCase() + key.slice(1);
    const reports = [1, 2, 3].map(run => {
      const file = path.join(directory, `simulation-${key}-${run}`, 'report.json');
      if (!fs.existsSync(file)) return null;
      if (fs.statSync(file).size > 1024 * 1024) throw new Error('Simulation report too large');
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    });
    sections.push(renderWorkload(title, reports), '');
  }
  if (source) sections.push(source, '');
  sections.push('**Bold** marks the better success rate in each row; ties highlight wgo only.', '');
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

module.exports = { buildReport, renderWorkload, validateReport };
if (require.main === module) {
  const directory = process.argv[2];
  if (!directory) throw new Error('Usage: simulation-report.cjs DIRECTORY [--publish]');
  if (process.argv.includes('--publish')) publish(directory);
  else process.stdout.write(buildReport(directory));
}
