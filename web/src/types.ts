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

export interface TaskDataset {
  slug: string;
  title: string;
  stageLabel: string;
  criteria: CriterionMeta[];
  checks: CheckMeta[];
  runs: CalibrationRun[];
  mutants: MutantItem[];
  stats: {
    checksCount: number;
    criteriaCount: number;
    mutantsCount: number;
    runsCount: number;
    fullPassRuns: string;
    fullPassRatePct: number;
    meanReward: number;
  };
}

export interface GeneratedRepoData {
  summary: {
    gym: string;
    verifier_spec: {
      framework: string;
      harbor_version: string;
      tasks_count: number;
      total_criteria_count: number;
      total_checks_count: number;
      max_single_check_weight_pct: number;
      fresh_seeded_runs_per_eval: number;
    };
    boundary_and_mutant_proofs: {
      starter_untouched: number;
      oracle_solution_1: number;
      oracle_solution_2: number;
      mutants_killed: string;
      harbor_lint_root_agent: string;
    };
    verifier_audit_fixes: AuditFix[];
    difficulty_takeaway: string;
  };
  tasks: TaskDataset[];
}
