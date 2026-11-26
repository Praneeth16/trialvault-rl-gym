import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, '..');
const repoRoot = path.resolve(webRoot, '..');
const outFile = path.join(webRoot, 'src', 'data', 'generated.json');

const runsDir = path.join(repoRoot, 'benchmarking-runs');
const mutantsDir = path.join(
  repoRoot,
  'tasks',
  'trialvault-consent-withdrawal',
  'tests',
  'mutants'
);

// When deployed with Vercel Root Directory set to `web/`, parent repo folders may be absent.
// In that case, keep the committed src/data/generated.json intact.
if (!fs.existsSync(runsDir) || !fs.existsSync(mutantsDir)) {
  if (fs.existsSync(outFile)) {
    console.log('Parent repo artifacts not present; keeping existing src/data/generated.json');
    process.exit(0);
  }
  console.error('Missing both parent artifacts and src/data/generated.json');
  process.exit(1);
}

const summary = JSON.parse(fs.readFileSync(path.join(runsDir, 'summary.json'), 'utf8'));
const runFiles = [
  { id: 'codex-480', file: 'codex-gpt-6.2-sol_run1.json', display: 'GPT-6.2 Sol (480s)', stage: 'Pre-audit v2' },
  { id: 'haiku-600', file: 'claude-haiku-4.5_run1.json', display: 'Claude Haiku 4.5', stage: 'Pre-audit v2' },
  { id: 'codex-720', file: 'codex-gpt-6.2-sol-full_run1.json', display: 'GPT-6.2 Sol (720s)', stage: 'Pre-audit v2' },
  { id: 'sonnet-669', file: 'claude-sonnet-5_run1.json', display: 'Claude Sonnet 5', stage: 'Post-audit v2' },
];

const runs = runFiles.map((meta) => {
  const raw = JSON.parse(fs.readFileSync(path.join(runsDir, meta.file), 'utf8'));
  const checks = {};
  for (const [criterion, detail] of Object.entries(raw.details || {})) {
    for (const item of detail.criteria || []) {
      checks[item.name] = {
        criterion,
        weight: item.weight,
        score: item.value,
        reasoning: item.reasoning,
      };
    }
  }
  return {
    id: meta.id,
    display: meta.display,
    stage: meta.stage,
    harness: raw.harness,
    model: raw.model,
    elapsedSec: raw.elapsed_sec,
    exitCode: raw.agent_exit_code,
    changedFilesCount: (raw.changed_files || []).length,
    reward: raw.scores.reward,
    criteriaScores: Object.fromEntries(
      Object.entries(raw.scores).filter(([k]) => k !== 'reward')
    ),
    checks,
  };
});

// Build canonical ordered list of criteria and checks from the latest run
const canonicalCriteria = [
  { id: 'api_contract', label: 'API contract and idempotency', weight: 10.0 },
  { id: 'active_store_deid', label: 'Active-store HIPAA Safe Harbor', weight: 26.0 },
  { id: 'clinical_retention', label: 'FDA 21 CFR Part 11 and retention', weight: 16.0 },
  { id: 'async_convergence', label: 'Async convergence and queues', weight: 12.0 },
  { id: 'replay_and_late_webhooks', label: 'Late webhooks and replay suppression', weight: 14.0 },
  { id: 'failure_and_recovery', label: 'Transient fault recovery', weight: 8.0 },
  { id: 'isolation_and_survivors', label: 'Site isolation, hold, and survivors', weight: 10.0 },
  { id: 'baseline_health', label: 'Post-withdrawal baseline health', weight: 4.0 },
];

const checkMetaList = [];
const refRun = runs[runs.length - 1];
for (const crit of canonicalCriteria) {
  for (const [checkId, info] of Object.entries(refRun.checks)) {
    if (info.criterion === crit.id) {
      checkMetaList.push({
        id: checkId,
        criterion: crit.id,
        weight: info.weight,
      });
    }
  }
}

const manifest = JSON.parse(fs.readFileSync(path.join(mutantsDir, 'manifest.json'), 'utf8'));
const mutants = manifest.map((m) => {
  const diff = fs.readFileSync(path.join(mutantsDir, m.patch), 'utf8');
  return {
    id: m.id,
    patch: m.patch,
    description: m.description,
    mustFailChecks: m.must_fail_checks,
    diff,
  };
});

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(
  outFile,
  JSON.stringify(
    {
      summary,
      criteria: canonicalCriteria,
      checks: checkMetaList,
      runs,
      mutants,
    },
    null,
    2
  ) + '\n',
  'utf8'
);
console.log(`Synced ${runs.length} calibration runs, ${checkMetaList.length} checks, and ${mutants.length} mutants to src/data/generated.json`);
