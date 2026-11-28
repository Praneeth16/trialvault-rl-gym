import { useState } from 'react';
import rawData from './data/generated.json';
import type { GeneratedRepoData } from './types';
import { SimulatorView } from './components/SimulatorView';
import { CalibrationView } from './components/CalibrationView';
import { MutantsView } from './components/MutantsView';

const repoData = rawData as unknown as GeneratedRepoData;

type WorkbenchTab = 'simulator' | 'calibration' | 'mutants';

export function App() {
  const [tab, setTab] = useState<WorkbenchTab>('simulator');
  const [taskSlug, setTaskSlug] = useState<string>(repoData.tasks[0]?.slug ?? '');

  const totalChecks = repoData.tasks.reduce((s, t) => s + t.stats.checksCount, 0);
  const totalMutants = repoData.tasks.reduce((s, t) => s + t.stats.mutantsCount, 0);

  return (
    <>
      <header className="topbar">
        <div className="topbar-inner">
          <a
            className="brand"
            href="https://github.com/Praneeth16/trialvault-rl-gym"
            target="_blank"
            rel="noreferrer"
          >
            <span className="brand-mark">TrialVault RL Gym</span>
            <span className="brand-sub">Harbor v1.4 and RewardKit</span>
          </a>

          <div className="segmented" role="tablist" aria-label="Workbench views">
            <button
              type="button"
              role="tab"
              aria-selected={tab === 'simulator'}
              className="segmented-item"
              onClick={() => setTab('simulator')}
            >
              Simulator
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={tab === 'calibration'}
              className="segmented-item"
              onClick={() => setTab('calibration')}
            >
              Calibration
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={tab === 'mutants'}
              className="segmented-item"
              onClick={() => setTab('mutants')}
            >
              Mutants
            </button>
          </div>

          <nav className="topbar-links" aria-label="Repository links">
            <a
              className="nav-link"
              href="https://github.com/Praneeth16/trialvault-rl-gym"
              target="_blank"
              rel="noreferrer"
            >
              Repository
            </a>
            <a
              className="nav-link"
              href="https://github.com/Praneeth16/trialvault-rl-gym/actions/workflows/ci.yml"
              target="_blank"
              rel="noreferrer"
            >
              Docker CI
            </a>
          </nav>
        </div>
      </header>

      <main className="shell">
        <div className="page-header">
          <div>
            <h1 className="page-title">
              {tab === 'simulator' && 'Clinical trial compliance environments under HIPAA, 21 CFR Part 11 & 312.32'}
              {tab === 'calibration' && `Multi-task RewardKit verifier (${totalChecks} checks) & live model calibration`}
              {tab === 'mutants' && `Planted bug mutants (${totalMutants}/${totalMutants} killed across 2 tasks)`}
            </h1>
            <p className="page-desc">
              {tab === 'simulator' &&
                'Interactive replica of participant withdrawal, asynchronous worker convergence, delayed ePRO suppression, FDA pre-withdrawal lab ingestion, and OS-level fault recovery.'}
              {tab === 'calibration' &&
                'All runs executed inside isolated starter sandboxes without access to tests or reference solutions, graded across three fresh randomized-seed trials per task.'}
              {tab === 'mutants' &&
                'Each single-bug unified diff mutates Reference Solution 1 and is verified in CI to drop targeted RewardKit checks to 0.0.'}
            </p>
          </div>

          <div className="header-metrics" aria-label="Verifier contract metrics">
            <div className="metric-pair">
              <span className="metric-value">
                {repoData.tasks.length} tasks · {totalChecks} checks
              </span>
              <span className="metric-label">
                {repoData.summary.verifier_spec.total_criteria_count} weighted criteria
              </span>
            </div>
            <div className="metric-pair">
              <span className="metric-value state-pass">
                {totalMutants}/{totalMutants}
              </span>
              <span className="metric-label">Mutants killed</span>
            </div>
            <div className="metric-pair">
              <span className="metric-value state-pass">0.0 / 1.0</span>
              <span className="metric-label">Starter vs dual oracles</span>
            </div>
          </div>
        </div>

        {tab === 'simulator' && <SimulatorView />}
        {tab === 'calibration' && (
          <CalibrationView
            data={repoData}
            selectedSlug={taskSlug}
            onSelectTask={setTaskSlug}
          />
        )}
        {tab === 'mutants' && (
          <MutantsView
            data={repoData}
            selectedSlug={taskSlug}
            onSelectTask={setTaskSlug}
          />
        )}
      </main>

      <footer className="site-footer">
        <span>
          Gym packages: <span className="mono">{repoData.tasks.map((t) => t.slug).join(' · ')}</span>
        </span>
        <span>
          Separate no-network verifiers · Harbor {repoData.summary.verifier_spec.harbor_version} ·{' '}
          {repoData.summary.verifier_spec.framework}
        </span>
      </footer>
    </>
  );
}
