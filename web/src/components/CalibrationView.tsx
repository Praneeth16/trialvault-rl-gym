import { Fragment, useState } from 'react';
import type { GeneratedRepoData } from '../types';

interface Props {
  data: GeneratedRepoData;
}

export function CalibrationView({ data }: Props) {
  const total = data.summary.verifier_spec.checks_count;
  const [expandedCheck, setExpandedCheck] = useState<string | null>('blobs_and_metadata_clean');

  const toggleCheck = (id: string) => {
    setExpandedCheck((prev) => (prev === id ? null : id));
  };

  return (
    <div>
      <section aria-label="Model run overview" style={{ marginBottom: '1.75rem' }}>
        <h2 className="section-heading">Multi-model calibration summary</h2>
        <p className="section-copy">
          Each agent ran inside an isolated copy of the starter repository with only instruction.md
          and domain policy docs, then was graded across three fresh randomized-seed runs by
          RewardKit. Full passes: {data.summary.calibration_aggregate_v3.full_pass_runs} runs
          {` (${data.summary.calibration_aggregate_v3.full_pass_rate_pct}%)`}, mean partial credit{' '}
          {(data.summary.calibration_aggregate_v3.mean_reward * 100).toFixed(1)}%.
        </p>
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Configuration</th>
                <th>Harness</th>
                <th>Stage</th>
                <th className="num">Elapsed</th>
                <th className="num">Reward</th>
                <th className="num">Checks passed</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Untouched starter workspace</td>
                <td className="mono">harbor nop</td>
                <td>Docker CI baseline</td>
                <td className="num">6s</td>
                <td className="num">
                  <span className="score-bar-track" aria-hidden="true">
                    <span className="score-bar-fill" style={{ width: '0%' }} />
                  </span>
                  <span className="state-fail">0.0%</span>
                </td>
                <td className="num">0 / {total}</td>
              </tr>
              {data.runs.map((run) => {
                const passed = Object.values(run.checks).filter((c) => c.score >= 1.0).length;
                const pct = (run.reward * 100).toFixed(1);
                return (
                  <tr key={run.id}>
                    <td>
                      <strong>{run.display}</strong>
                    </td>
                    <td className="mono">{run.harness}</td>
                    <td>{run.stage}</td>
                    <td className="num">{Math.round(run.elapsedSec)}s</td>
                    <td className="num">
                      <span className="score-bar-track" aria-hidden="true">
                        <span
                          className="score-bar-fill"
                          style={{
                            width: `${pct}%`,
                            backgroundColor: run.reward >= 1 ? 'var(--status-pass)' : 'var(--accent)',
                          }}
                        />
                      </span>
                      <span className={run.reward >= 1 ? 'state-pass' : 'state-warn'}>{pct}%</span>
                    </td>
                    <td className="num">{passed} / {total}</td>
                  </tr>
                );
              })}
              <tr>
                <td>Oracle Solution 1 (Outbox ledger)</td>
                <td className="mono">harbor oracle</td>
                <td>Docker CI verified</td>
                <td className="num">18s</td>
                <td className="num">
                  <span className="score-bar-track" aria-hidden="true">
                    <span
                      className="score-bar-fill"
                      style={{ width: '100%', backgroundColor: 'var(--status-pass)' }}
                    />
                  </span>
                  <span className="state-pass">100.0%</span>
                </td>
                <td className="num">{total} / {total}</td>
              </tr>
              <tr>
                <td>Oracle Solution 2 (Checkpointed saga)</td>
                <td className="mono">harbor oracle</td>
                <td>Docker CI verified</td>
                <td className="num">19s</td>
                <td className="num">
                  <span className="score-bar-track" aria-hidden="true">
                    <span
                      className="score-bar-fill"
                      style={{ width: '100%', backgroundColor: 'var(--status-pass)' }}
                    />
                  </span>
                  <span className="state-pass">100.0%</span>
                </td>
                <td className="num">{total} / {total}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section aria-label="Per-check RewardKit matrix">
        <h2 className="section-heading">Per-check RewardKit breakdown ({total} paired checks)</h2>
        <p className="section-copy">
          Select any check row to inspect exact black-box evidence strings recorded by RewardKit
          across all {data.runs.length} live model trajectories.
        </p>

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Criterion and check</th>
                <th className="num">Weight</th>
                {data.runs.map((r) => (
                  <th key={r.id} className="num">
                    {r.display}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.criteria.map((crit) => {
                const critChecks = data.checks.filter((c) => c.criterion === crit.id);
                return (
                  <Fragment key={crit.id}>
                    <tr className="criterion-group-row">
                      <td>{crit.label}</td>
                      <td className="num">{crit.weight.toFixed(1)}%</td>
                      {data.runs.map((r) => (
                        <td key={r.id} className="num">
                          {((r.criteriaScores[crit.id] ?? 0) * 100).toFixed(0)}%
                        </td>
                      ))}
                    </tr>
                    {critChecks.map((chk) => {
                      const isOpen = expandedCheck === chk.id;
                      return (
                        <Fragment key={chk.id}>
                          <tr
                            className="clickable"
                            onClick={() => toggleCheck(chk.id)}
                            aria-expanded={isOpen}
                          >
                            <td className="mono">{chk.id}</td>
                            <td className="num">{chk.weight.toFixed(1)}%</td>
                            {data.runs.map((r) => {
                              const val = r.checks[chk.id]?.score ?? 0;
                              return (
                                <td
                                  key={r.id}
                                  className={`num ${val >= 1 ? 'state-pass' : 'state-fail'}`}
                                >
                                  {val >= 1 ? 'Pass' : 'Fail'}
                                </td>
                              );
                            })}
                          </tr>
                          {isOpen && (
                            <tr className="expanded-row">
                              <td colSpan={2 + data.runs.length}>
                                <div className="kv-grid">
                                  {data.runs.map((r) => (
                                    <div key={r.id} className="kv-block">
                                      <div className="list-item-row">
                                        <strong>{r.display}</strong>
                                        <span
                                          className={
                                            (r.checks[chk.id]?.score ?? 0) >= 1
                                              ? 'state-pass'
                                              : 'state-fail'
                                          }
                                        >
                                          score {(r.checks[chk.id]?.score ?? 0).toFixed(1)}
                                        </span>
                                      </div>
                                      <p className="mono" style={{ margin: '0.375rem 0 0', whiteSpace: 'pre-wrap' }}>
                                        {r.checks[chk.id]?.reasoning}
                                      </p>
                                    </div>
                                  ))}
                                </div>
                              </td>
                            </tr>
                          )}
                        </Fragment>
                      );
                    })}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="audit-grid" aria-label="Verifier audit and scaling findings">
        <div className="audit-item">
          <h3>{data.summary.verifier_audit_fixes.length} verifier bugs caught by the failing-check audit</h3>
          {data.summary.verifier_audit_fixes.map((fix) => (
            <p key={fix.check}>
              <strong className="mono">{fix.check}</strong>: {fix.issue_found_in_calibration}{' '}
              Fixed: {fix.fix}
            </p>
          ))}
        </div>
        <div className="audit-item">
          <h3>Embedded stack ceiling and multi-service roadmap</h3>
          <p>{data.summary.difficulty_takeaway}</p>
        </div>
      </section>
    </div>
  );
}
