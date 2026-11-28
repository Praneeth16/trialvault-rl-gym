import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, '..');
const repoRoot = path.resolve(webRoot, '..');
const outFile = path.join(webRoot, 'src', 'data', 'generated.json');

const runsDir = path.join(repoRoot, 'benchmarking-runs');
const tasksDir = path.join(repoRoot, 'tasks');

if (!fs.existsSync(runsDir) || !fs.existsSync(tasksDir)) {
  if (fs.existsSync(outFile)) {
    console.log('Parent repo artifacts not present; keeping existing src/data/generated.json');
    process.exit(0);
  }
  console.error('Missing both parent artifacts and src/data/generated.json');
  process.exit(1);
}

const summary = JSON.parse(fs.readFileSync(path.join(runsDir, 'summary.json'), 'utf8'));

const labelDisplay = {
  'claude-haiku-4.5': 'Claude Haiku 4.5',
  'claude-sonnet-5': 'Claude Sonnet 5',
  'codex-gpt-6.2-sol': 'GPT-6.2 Sol',
};

const TASK_CONFIGS = [
  {
    slug: 'trialvault-consent-withdrawal',
    title: 'Consent Withdrawal (HIPAA + Part 11)',
    stageLabel: 'v4 multi-DB',
    runsSubdir: 'consent-withdrawal-v4',
    criteria: [
      { id: 'api_contract', label: 'API contract and idempotency', weight: 9.0 },
      { id: 'active_store_deid', label: 'Active-store & SDTM HIPAA Safe Harbor', weight: 24.0 },
      { id: 'clinical_retention', label: 'FDA 21 CFR Part 11 and retention', weight: 13.0 },
      { id: 'async_convergence', label: 'Async convergence and concurrency', weight: 12.0 },
      { id: 'replay_and_late_webhooks', label: 'Late webhooks and replay suppression', weight: 11.0 },
      { id: 'failure_and_recovery', label: 'Cross-DB & OS fault recovery', weight: 9.0 },
      { id: 'isolation_and_survivors', label: 'Isolation, manifests, SDV logs & survivors', weight: 10.0 },
      { id: 'withdrawal_scope', label: 'Follow-up-only scope and escalation', weight: 10.0 },
      { id: 'baseline_health', label: 'Post-withdrawal baseline health', weight: 2.0 },
    ],
  },
  {
    slug: 'trialvault-sae-expedited-reporting',
    title: 'SAE Expedited Reporting (21 CFR 312.32)',
    stageLabel: 'v1 4-DB PV',
    runsSubdir: 'sae-expedited-reporting',
    criteria: [
      { id: 'e2b_api_and_idempotency', label: 'E2B intake, auth & out-of-order merge', weight: 14.0 },
      { id: 'susar_triage_and_rsi', label: '3-leg SUSAR triage, OR-causality & RSI', weight: 16.0 },
      { id: 'selective_unblinding_and_iwrs', label: 'ICH E9 selective IWRS unblinding', weight: 12.0 },
      { id: 'blinding_wall_and_projections', label: 'Study blinding wall, search & listings', weight: 16.0 },
      { id: 'day0_clock_and_tightening', label: 'Day-0 anchoring & 15d->7d tightening', weight: 16.0 },
      { id: 'fda_gateway_and_fault_recovery', label: 'FDA ESG gateway, NACK & DB fault retry', weight: 11.0 },
      { id: 'part11_audit_and_concurrency', label: 'Part 11 hash chain & 4-worker race', weight: 10.0 },
      { id: 'baseline_pv_preservation', label: 'Baseline PV & post-SAE workflow health', weight: 5.0 },
    ],
  },
];

function parseChecksPy(taskSlug, canonicalCriteria) {
  const testsRoot = path.join(tasksDir, taskSlug, 'tests');
  const checkMetaList = [];
  for (const crit of canonicalCriteria) {
    const file = path.join(testsRoot, crit.id, 'checks.py');
    const src = fs.readFileSync(file, 'utf8');
    const regex = /name\s*=\s*["']([^"']+)["']\s*,\s*weight\s*=\s*([\d.]+)/g;
    const items = [];
    let match;
    while ((match = regex.exec(src)) !== null) {
      items.push({ id: match[1], rawWeight: Number(match[2]) });
    }
    const totalRaw = items.reduce((s, x) => s + x.rawWeight, 0) || 1;
    for (const it of items) {
      checkMetaList.push({
        id: it.id,
        criterion: crit.id,
        weight: (crit.weight * it.rawWeight) / totalRaw,
      });
    }
  }
  return checkMetaList;
}

const tasks = TASK_CONFIGS.map((cfg) => {
  const subRunsDir = path.join(runsDir, cfg.runsSubdir);
  const runFiles = fs.existsSync(subRunsDir)
    ? fs
        .readdirSync(subRunsDir)
        .filter((f) => /_run\d+\.json$/.test(f))
        .sort()
    : [];

  const runs = runFiles.map((file) => {
    const [label, idx] = file.replace(/\.json$/, '').split('_run');
    const raw = JSON.parse(fs.readFileSync(path.join(subRunsDir, file), 'utf8'));
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
      id: `${cfg.slug}-${label}-${idx}`,
      display: `${labelDisplay[label] ?? label} #${idx}`,
      stage: cfg.stageLabel,
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

  const checks = parseChecksPy(cfg.slug, cfg.criteria);
  const mutDir = path.join(tasksDir, cfg.slug, 'tests', 'mutants');
  const manifest = JSON.parse(fs.readFileSync(path.join(mutDir, 'manifest.json'), 'utf8'));
  const mutants = manifest.map((m) => ({
    id: m.id,
    patch: m.patch,
    description: m.description,
    mustFailChecks: m.must_fail_checks,
    diff: fs.readFileSync(path.join(mutDir, m.patch), 'utf8'),
  }));

  const fullPasses = runs.filter((r) => r.reward >= 1.0).length;
  const meanReward =
    runs.length > 0 ? runs.reduce((s, r) => s + r.reward, 0) / runs.length : 0;

  return {
    slug: cfg.slug,
    title: cfg.title,
    stageLabel: cfg.stageLabel,
    criteria: cfg.criteria,
    checks,
    runs,
    mutants,
    stats: {
      checksCount: checks.length,
      criteriaCount: cfg.criteria.length,
      mutantsCount: mutants.length,
      runsCount: runs.length,
      fullPassRuns: `${fullPasses}/${runs.length}`,
      fullPassRatePct: runs.length > 0 ? Number(((100 * fullPasses) / runs.length).toFixed(1)) : 0,
      meanReward: Number(meanReward.toFixed(4)),
    },
  };
});

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(
  outFile,
  JSON.stringify(
    {
      summary,
      tasks,
    },
    null,
    2
  ) + '\n',
  'utf8'
);

const totRuns = tasks.reduce((s, t) => s + t.runs.length, 0);
const totChecks = tasks.reduce((s, t) => s + t.checks.length, 0);
const totMutants = tasks.reduce((s, t) => s + t.mutants.length, 0);
console.log(
  `Synced ${tasks.length} tasks (${totRuns} calibration runs, ${totChecks} checks, ${totMutants} mutants) to src/data/generated.json`
);
