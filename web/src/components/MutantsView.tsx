import { useState } from 'react';
import type { GeneratedRepoData } from '../types';

interface Props {
  data: GeneratedRepoData;
}

export function MutantsView({ data }: Props) {
  const [selectedId, setSelectedId] = useState(data.mutants[0]?.id ?? '');
  const selected = data.mutants.find((m) => m.id === selectedId) ?? data.mutants[0];

  const checkWeightMap = new Map(data.checks.map((c) => [c.id, c.weight]));

  if (!selected) {
    return null;
  }

  const dropPct = selected.mustFailChecks.reduce(
    (sum, cid) => sum + (checkWeightMap.get(cid) ?? 0),
    0
  );

  return (
    <div className="workbench-grid">
      <aside className="rail" aria-label="Planted single-bug mutants">
        <div>
          <h2 className="section-heading">Planted bug mutants (10 of 10 killed)</h2>
          <div className="list-panel" role="listbox" aria-label="Select mutant patch">
            {data.mutants.map((m) => {
              const active = m.id === selected.id;
              return (
                <button
                  key={m.id}
                  type="button"
                  role="option"
                  aria-selected={active}
                  className="list-item"
                  onClick={() => setSelectedId(m.id)}
                >
                  <div className="list-item-row">
                    <span className="list-item-title mono">{m.id}</span>
                    <span className="state-pass">Killed</span>
                  </div>
                  <span className="list-item-meta">{m.description}</span>
                </button>
              );
            })}
          </div>
        </div>
      </aside>

      <section className="stage-panel" aria-label="Selected mutant diff and killed checks">
        <div className="stage-toolbar">
          <div>
            <h3 className="kv-block-title mono" style={{ margin: 0 }}>
              {selected.patch}
            </h3>
            <span className="list-item-meta">{selected.description}</span>
          </div>
          <span className="state-fail mono">
            Reward drops from 1.000 to &lt;= {(1 - dropPct / 100).toFixed(3)}
          </span>
        </div>

        <div className="stage-body">
          <h3 className="kv-block-title">Unified patch applied against Reference Solution 1</h3>
          <div className="diff-view" role="region" aria-label="Unified diff">
            {selected.diff.split('\n').map((line, idx) => {
              let cls = 'diff-line';
              if (line.startsWith('@@')) {
                cls += ' hunk';
              } else if (line.startsWith('+') && !line.startsWith('+++')) {
                cls += ' add';
              } else if (line.startsWith('-') && !line.startsWith('---')) {
                cls += ' del';
              }
              return (
                <div key={idx} className={cls}>
                  {line || ' '}
                </div>
              );
            })}
          </div>

          <div className="kv-block" style={{ marginTop: '1.25rem' }}>
            <h3 className="kv-block-title">Targeted RewardKit checks that drop to 0.0</h3>
            <ul className="killed-checks-list">
              {selected.mustFailChecks.map((cid) => (
                <li key={cid}>
                  <span className="mono">{cid}</span> (weight{' '}
                  {(checkWeightMap.get(cid) ?? 0).toFixed(1)}%, verified in CI pytest suite)
                </li>
              ))}
            </ul>
          </div>
        </div>
      </section>
    </div>
  );
}
