import { useState } from 'react';
import rawData from './data/generated.json';
import type { GeneratedRepoData } from './types';
import { SimulatorView } from './components/SimulatorView';
import { CalibrationView } from './components/CalibrationView';
import { MutantsView } from './components/MutantsView';

const repoData = rawData as GeneratedRepoData;

type WorkbenchTab = 'simulator' | 'calibration' | 'mutants';

export function App() {
  const [tab, setTab] = useState<WorkbenchTab>('simulator');

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
            <span className="brand-mark">TrialVault Workbench</span>
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
              {tab === 'simulator' && 'Clinical consent withdrawal under HIPAA and 21 CFR Part 11'}
              {tab === 'calibration' && 'RewardKit 28-check verifier and live model calibration'}
              {tab === 'mutants' && 'Planted bug mutants proving black-box verifier coverage'}
            </h1>
            <p className="page-desc">
              {tab === 'simulator' &&
                'Step through participant withdrawal, asynchronous worker convergence, delayed ePRO suppression, FDA pre-withdrawal lab ingestion, and OS-level fault recovery.'}
              {tab === 'calibration' &&
                'All runs executed inside isolated starter sandboxes without access to tests or reference solutions, scored across three fresh randomized-seed trials.'}
              {tab === 'mutants' &&
                'Each single-bug patch mutates Reference Solution 1 and is verified in CI to drop the targeted RewardKit check to 0.0.'}
            </p>
          </div>

          <div className="header-metrics" aria-label="Verifier contract metrics">
            <div className="metric-pair">
              <span className="metric-value">
                {repoData.summary.verifier_spec.checks_count} checks
              </span>
              <span className="metric-label">
                {repoData.summary.verifier_spec.criteria_count} weighted criteria
              </span>
            </div>
            <div className="metric-pair">
              <span className="metric-value state-pass">
                {repoData.summary.boundary_and_mutant_proofs.mutants_killed}
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
        {tab === 'calibration' && <CalibrationView data={repoData} />}
        {tab === 'mutants' && <MutantsView data={repoData} />}
      </main>

      <footer className="site-footer">
        <span>
          Task package: <span className="mono">{repoData.summary.task}</span>
        </span>
        <span>
          Separate no-network verifier · Harbor {repoData.summary.verifier_spec.harbor_version} ·{' '}
          {repoData.summary.verifier_spec.framework}
        </span>
      </footer>
    </>
  );
}
