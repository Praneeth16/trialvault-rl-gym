import { useState } from 'react';
import {
  SUBJECT_SCENARIOS,
  applySimAction,
  createInitialSimState,
  type SimAction,
  type SubjectScenarioId,
} from '../simulatorEngine';

type StoreTab = 'edc' | 'search_cache' | 'blobs_dsmb' | 'spool_audit';

const NEXT_ACTION_OPTIONS: Array<{ value: SimAction; label: string }> = [
  { value: 'post_withdrawal', label: 'POST /v1/subjects/{id}/withdrawal-requests' },
  { value: 'run_worker', label: 'Run worker (python -m trialvault.worker --once)' },
  { value: 'advance_clock', label: 'Advance worker clock (--now 2026-05-01)' },
  { value: 'webhook_post_withdrawal', label: 'Late HL7 webhook (collected 2026-03-15, post-withdrawal)' },
  { value: 'webhook_pre_withdrawal', label: 'Late HL7 webhook (collected 2026-03-08, pre-withdrawal FDA rule)' },
  { value: 'inject_blob_fault', label: 'Inject transient OS fault (IsADirectoryError on lab blob)' },
  { value: 'recover_blob_fault', label: 'Clear transient blob fault before retry' },
];

export function SimulatorView() {
  const [scenarioId, setScenarioId] = useState<SubjectScenarioId>('elena');
  const [sim, setSim] = useState(() => createInitialSimState('elena'));
  const [storeTab, setStoreTab] = useState<StoreTab>('edc');
  const [selectedAction, setSelectedAction] = useState<SimAction>('post_withdrawal');

  const handleSelectScenario = (id: SubjectScenarioId) => {
    setScenarioId(id);
    setSim(createInitialSimState(id));
    setSelectedAction('post_withdrawal');
  };

  const handleExecuteAction = () => {
    const next = applySimAction(sim, selectedAction);
    setSim(next);
    // Automatically advance suggested next step for smooth keyboard/click stepping
    if (selectedAction === 'post_withdrawal' && next.requestStatus === 'pending') {
      setSelectedAction('run_worker');
    } else if (selectedAction === 'run_worker' && next.requestStatus === 'completed') {
      setSelectedAction('webhook_pre_withdrawal');
    } else if (selectedAction === 'inject_blob_fault') {
      setSelectedAction('run_worker');
    } else if (selectedAction === 'recover_blob_fault') {
      setSelectedAction('run_worker');
    }
  };

  const handleReset = () => {
    setSim(createInitialSimState(scenarioId));
    setSelectedAction('post_withdrawal');
  };

  return (
    <div className="workbench-grid">
      <aside className="rail" aria-label="Trial subjects and audit log">
        <div>
          <h2 className="section-heading">Clinical trial subjects</h2>
          <div className="list-panel" role="listbox" aria-label="Select clinical subject">
            {SUBJECT_SCENARIOS.map((item) => {
              const active = item.id === scenarioId;
              return (
                <button
                  key={item.id}
                  type="button"
                  role="option"
                  aria-selected={active}
                  className="list-item"
                  onClick={() => handleSelectScenario(item.id)}
                >
                  <div className="list-item-row">
                    <span className="list-item-title">{item.name}</span>
                    <span className="list-item-meta mono">{item.subjectId}</span>
                  </div>
                  <span className="list-item-meta">{item.subtitle}</span>
                </button>
              );
            })}
          </div>
        </div>

        <div className="list-panel">
          <div className="event-log">
            <div className="list-item-row">
              <span className="list-item-title">Transition log</span>
              <span className="list-item-meta mono">{sim.clockIso.slice(0, 10)}</span>
            </div>
            <ul className="event-log-list">
              {sim.log.slice(0, 6).map((entry, idx) => (
                <li key={idx} className="event-log-entry">
                  <span className="mono">{entry.step}</span>
                  <span>{entry.detail}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </aside>

      <section className="stage-panel" aria-label="Five-store interactive inspector">
        <p className="simulator-note">
          Illustrative client-side replica of a full-scope withdrawal. It does not run the Python
          app or the verifier; real scores come from the Calibration and Mutants tabs.
        </p>
        <div className="stage-toolbar">
          <div className="stage-controls">
            <label htmlFor="sim-action-select" className="list-item-meta">
              Action
            </label>
            <select
              id="sim-action-select"
              className="scenario-select"
              value={selectedAction}
              onChange={(e) => setSelectedAction(e.target.value as SimAction)}
            >
              {NEXT_ACTION_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
            <button type="button" className="primary-btn" onClick={handleExecuteAction}>
              Execute step
            </button>
          </div>
          <button type="button" className="text-action" onClick={handleReset}>
            Reset subject state
          </button>
        </div>

        {sim.lastResponse ? (
          <div className={`response-strip tone-${sim.lastResponse.tone}`}>
            <div>
              <strong className="mono">
                {sim.lastResponse.method} {sim.lastResponse.httpStatus}
              </strong>{' '}
              <span className="mono">{sim.lastResponse.path}</span>
            </div>
            <span>{sim.lastResponse.summary}</span>
          </div>
        ) : (
          <div className="response-strip">
            <span>{sim.subject.roleNote}</span>
            <span className="mono">Request status: {sim.requestStatus}</span>
          </div>
        )}

        <div className="subtabs" role="tablist" aria-label="Data store views">
          <button
            type="button"
            role="tab"
            aria-selected={storeTab === 'edc'}
            className="subtab"
            onClick={() => setStoreTab('edc')}
          >
            Relational EDC
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={storeTab === 'search_cache'}
            className="subtab"
            onClick={() => setStoreTab('search_cache')}
          >
            FTS5 search and cache
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={storeTab === 'blobs_dsmb'}
            className="subtab"
            onClick={() => setStoreTab('blobs_dsmb')}
          >
            Blobs and DSMB export
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={storeTab === 'spool_audit'}
            className="subtab"
            onClick={() => setStoreTab('spool_audit')}
          >
            Spool and Part 11 audit
          </button>
        </div>

        <div className="stage-body">
          {storeTab === 'edc' && (
            <div className="kv-grid">
              <div className="kv-block">
                <h3 className="kv-block-title">subjects row (HIPAA Safe Harbor)</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      subject_id: sim.subject.subject_id,
                      status: sim.subject.status,
                      mrn: sim.subject.mrn,
                      full_name: sim.subject.full_name,
                      dob: sim.subject.dob,
                      age_at_enrollment: sim.subject.age_at_enrollment,
                      age_bucket: sim.subject.age_bucket,
                      street_address: sim.subject.street_address,
                      state: sim.subject.state,
                      postal_code: sim.subject.postal_code,
                      treatment_arm: sim.subject.treatment_arm,
                      randomization_date: sim.subject.randomization_date,
                      regulatory_hold: sim.subject.regulatory_hold,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
              <div className="kv-block">
                <h3 className="kv-block-title">visits and lab_results (retained clinical data)</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      visit_clinical_notes: sim.clinicalNotes,
                      lab_results: sim.labRows,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
            </div>
          )}

          {storeTab === 'search_cache' && (
            <div className="kv-grid">
              <div className="kv-block">
                <h3 className="kv-block-title">search.db (SQLite FTS5 index)</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      subject_indexed: sim.ftsIndexed,
                      search_by_mrn_hits: sim.ftsIndexed ? 1 : 0,
                      co_enrolled_survivor_hits: 1,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
              <div className="kv-block">
                <h3 className="kv-block-title">cache.json (projection cache keys)</h3>
                <pre className="code-block">{JSON.stringify(sim.cacheKeys, null, 2)}</pre>
              </div>
            </div>
          )}

          {storeTab === 'blobs_dsmb' && (
            <div className="kv-grid">
              <div className="kv-block">
                <h3 className="kv-block-title">blobs/ (eConsent and central-lab sidecar)</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      econsent_blob_exists: sim.econsentPresent,
                      transient_fault_active: sim.blobFaultActive,
                      lab_blob_meta_json: sim.labBlobMeta,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
              <div className="kv-block">
                <h3 className="kv-block-title">exports/ DSMB snapshot and manifest</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      manifest_sha256: sim.dsmbSha256,
                      checksum_valid: sim.dsmbChecksumValid,
                      subject_entry: sim.dsmbSubjectEntry,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
            </div>
          )}

          {storeTab === 'spool_audit' && (
            <div className="kv-grid">
              <div className="kv-block">
                <h3 className="kv-block-title">spool/ and scheduled_jobs</h3>
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      day30_followup_job_status: sim.scheduledDay30Status,
                      outbound_queue_count: sim.outboundSpoolEntries.length,
                      dead_letters_count: sim.dlqEntries.length,
                      audit_pii_details_rows: sim.auditPiiCount,
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
              <div className="kv-block">
                <h3 className="kv-block-title">audit_log (21 CFR Part 11 SHA-256 chain)</h3>
                <pre className="code-block">{JSON.stringify(sim.auditChainEntries, null, 2)}</pre>
              </div>
            </div>
          )}
        </div>

        <div className="invariant-bar" aria-label="Protected trial safety summary invariants">
          <div className="invariant-cell">
            <span className="invariant-label">Trial safety subjects</span>
            <span className="invariant-val">
              {sim.safetySummary.total_subjects} ({sim.safetySummary.drug_a_subjects} Drug A,{' '}
              {sim.safetySummary.placebo_subjects} Placebo)
            </span>
          </div>
          <div className="invariant-cell">
            <span className="invariant-label">ALT biomarker summary</span>
            <span className="invariant-val">
              n = {sim.safetySummary.alt_count}, mean {sim.safetySummary.alt_mean} U/L
            </span>
          </div>
          <div className="invariant-cell">
            <span className="invariant-label">21 CFR Part 11 chain</span>
            <span className="invariant-val state-pass">
              Valid ({sim.auditChainEntries.length} entries)
            </span>
          </div>
          <div className="invariant-cell">
            <span className="invariant-label">Request convergence</span>
            <span
              className={`invariant-val ${
                sim.requestStatus === 'completed'
                  ? 'state-pass'
                  : sim.requestStatus === 'blocked_regulatory_hold'
                  ? 'state-warn'
                  : 'state-muted'
              }`}
            >
              {sim.requestStatus}
            </span>
          </div>
        </div>
      </section>
    </div>
  );
}
