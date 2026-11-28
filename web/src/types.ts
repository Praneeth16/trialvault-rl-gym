export interface CriterionMeta {
  id: string;
  label: string;
  weight: number;
}

export interface CheckMeta {
  id: string;
  criterion: string;
  weight: number;
}

export interface CheckResult {
  criterion: string;
  weight: number;
  score: number;
  reasoning: string;
}

export interface CalibrationRun {
  id: string;
  display: string;
  stage: string;
  harness: string;
  model: string;
  elapsedSec: number;
  exitCode: number;
  changedFilesCount: number;
  reward: number;
  criteriaScores: Record<string, number>;
  checks: Record<string, CheckResult>;
}

export interface MutantItem {
  id: string;
  patch: string;
  description: string;
  mustFailChecks: string[];
  diff: string;
}

export interface AuditFix {
  check: string;
  issue_found_in_calibration: string;
  fix: string;
}

export interface GeneratedRepoData {
  summary: {
    task: string;
    verifier_spec: {
      framework: string;
      harbor_version: string;
      criteria_count: number;
      checks_count: number;
      max_single_check_weight_pct: number;
      fresh_seeded_runs_per_eval: number;
    };
    boundary_and_mutant_proofs: {
      starter_untouched: number;
      oracle_solution_1_outbox: number;
      oracle_solution_2_saga: number;
      mutants_killed: string;
      harbor_lint_root_agent: string;
    };
    verifier_audit_fixes: AuditFix[];
    calibration_aggregate_v3: {
      full_pass_runs: string;
      full_pass_rate_pct: number;
      mean_reward: number;
    };
    difficulty_takeaway: string;
  };
  criteria: CriterionMeta[];
  checks: CheckMeta[];
  runs: CalibrationRun[];
  mutants: MutantItem[];
}
