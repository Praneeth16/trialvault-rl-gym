"""SOLUTION=2: Pure functional, side-effect-free safety case reducer (FDA 21 CFR 312.32)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SERIOUSNESS_FLAGS: tuple[str, ...] = (
    "death",
    "life_threatening",
    "hospitalization",
    "disability",
    "congenital_anomaly",
    "medically_important",
)

SUSPECTED_TERMS: frozenset[str] = frozenset({"related", "possibly_related"})


def parse_iso_utc(raw: str) -> datetime:
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class ReportEvent:
    report_id: str
    message_id: str
    followup_num: int
    aware_at: str
    meddra_pt: str
    ctcae_grade: int
    seriousness: dict[str, bool]
    investigator_causality: str
    sponsor_causality: str | None
    narrative: str
    processed: bool


@dataclass(frozen=True)
class RsiTermRule:
    meddra_pt: str
    max_expected_grade: int
    fatal_expected: bool


@dataclass(frozen=True)
class SubmissionSnapshot:
    submission_id: str
    sequence_num: int
    submission_type: str
    parent_submission_id: str | None
    report_clock: str
    day0_at: str
    due_at: str
    status: str
    covered_followup_num: int
    covered_grade: int
    covered_seriousness: dict[str, bool]


@dataclass(frozen=True)
class SubmissionDirective:
    action: str  # "none" | "create_initial" | "update_pending" | "create_followup"
    target_submission_id: str | None
    parent_submission_id: str | None
    sequence_num: int
    report_clock: str
    day0_at: str
    due_at: str
    covered_followup_num: int
    covered_grade: int
    covered_seriousness: dict[str, bool]


@dataclass(frozen=True)
class ReducedCaseState:
    latest_followup_num: int
    initial_aware_at: str
    latest_aware_at: str
    meddra_pt: str
    ctcae_grade: int
    seriousness: dict[str, bool]
    is_serious: bool
    investigator_causality: str
    sponsor_causality: str | None
    narrative: str
    has_unprocessed: bool
    is_suspected: bool
    is_unexpected: bool
    qualifies_three_legs: bool
    directive: SubmissionDirective | None


def compute_initial_aware_at(reports: list[ReportEvent]) -> str:
    r0 = next((r for r in reports if r.followup_num == 0), None)
    if r0 is not None:
        return fmt_iso_utc(parse_iso_utc(r0.aware_at))
    earliest = min(reports, key=lambda r: (parse_iso_utc(r.aware_at), r.followup_num))
    return fmt_iso_utc(parse_iso_utc(earliest.aware_at))


def _fold_subset(subset: list[ReportEvent]) -> tuple[str, int, dict[str, bool], str, str | None, str, int]:
    ordered_fu = sorted(subset, key=lambda r: r.followup_num)
    top = ordered_fu[-1]
    spon_list = [r.sponsor_causality for r in ordered_fu if r.sponsor_causality is not None]
    spon = spon_list[-1] if spon_list else None
    ser = {flag: any(r.seriousness.get(flag, False) for r in ordered_fu) for flag in SERIOUSNESS_FLAGS}
    max_grade = max(r.ctcae_grade for r in ordered_fu)
    return (
        top.meddra_pt,
        max_grade,
        ser,
        top.investigator_causality,
        spon,
        top.narrative,
        top.followup_num,
    )


def _eval_unexpected(
    meddra_pt: str,
    grade: int,
    ser: dict[str, bool],
    rsi_map: dict[str, RsiTermRule] | None,
) -> bool:
    if rsi_map is None:
        return True
    rule = rsi_map.get(meddra_pt)
    if rule is None:
        return True
    if grade > rule.max_expected_grade:
        return True
    if (ser["death"] or ser["life_threatening"]) and not rule.fatal_expected:
        return True
    return False


def reduce_case_history(
    reports: list[ReportEvent],
    rsi_map: dict[str, RsiTermRule] | None,
    existing_submissions: list[SubmissionSnapshot],
) -> ReducedCaseState:
    chrono = sorted(reports, key=lambda r: (parse_iso_utc(r.aware_at), r.followup_num))
    initial_aware = compute_initial_aware_at(reports)
    latest_aware = fmt_iso_utc(parse_iso_utc(chrono[-1].aware_at))
    meddra_pt, grade, ser, inv_c, spon_c, narrative, latest_fu = _fold_subset(chrono)

    is_serious = any(ser.values())
    is_suspected = (inv_c in SUSPECTED_TERMS) or (spon_c is not None and spon_c in SUSPECTED_TERMS)
    is_unexpected = _eval_unexpected(meddra_pt, grade, ser, rsi_map)
    qualifies = is_serious and is_suspected and is_unexpected
    has_unprocessed = any(not r.processed for r in reports)

    directive: SubmissionDirective | None = None
    if qualifies:
        if not existing_submissions:
            thresh_idx = len(chrono) - 1
            thresh_ser = ser
            for idx in range(len(chrono)):
                p_pt, p_grade, p_ser, p_inv, p_spon, _, _ = _fold_subset(chrono[: idx + 1])
                p_is_ser = any(p_ser.values())
                p_is_susp = (p_inv in SUSPECTED_TERMS) or (
                    p_spon is not None and p_spon in SUSPECTED_TERMS
                )
                p_is_unexp = _eval_unexpected(p_pt, p_grade, p_ser, rsi_map)
                if p_is_ser and p_is_susp and p_is_unexp:
                    thresh_idx = idx
                    thresh_ser = p_ser
                    break

            d0_dt = parse_iso_utc(chrono[thresh_idx].aware_at)
            if thresh_ser["death"] or thresh_ser["life_threatening"]:
                clock = "7_day"
                due_dt = d0_dt + timedelta(days=7)
            else:
                clock = "15_day"
                due_dt = d0_dt + timedelta(days=15)
                for r_later in chrono[thresh_idx + 1 :]:
                    if r_later.seriousness.get("death") or r_later.seriousness.get("life_threatening"):
                        t_up = parse_iso_utc(r_later.aware_at)
                        clock = "7_day"
                        due_dt = min(due_dt, t_up + timedelta(days=7))
                        break

            directive = SubmissionDirective(
                action="create_initial",
                target_submission_id=None,
                parent_submission_id=None,
                sequence_num=1,
                report_clock=clock,
                day0_at=fmt_iso_utc(d0_dt),
                due_at=fmt_iso_utc(due_dt),
                covered_followup_num=latest_fu,
                covered_grade=grade,
                covered_seriousness=ser,
            )
        else:
            last_sub = sorted(existing_submissions, key=lambda s: s.sequence_num)[-1]
            if last_sub.status in {"pending", "retry_pending"}:
                clock = last_sub.report_clock
                due_dt = parse_iso_utc(last_sub.due_at)
                if clock == "15_day" and (ser["death"] or ser["life_threatening"]):
                    for r in chrono:
                        if r.seriousness.get("death") or r.seriousness.get("life_threatening"):
                            t_up = parse_iso_utc(r.aware_at)
                            clock = "7_day"
                            due_dt = min(due_dt, t_up + timedelta(days=7))
                            break
                directive = SubmissionDirective(
                    action="update_pending",
                    target_submission_id=last_sub.submission_id,
                    parent_submission_id=last_sub.parent_submission_id,
                    sequence_num=last_sub.sequence_num,
                    report_clock=clock,
                    day0_at=last_sub.day0_at,
                    due_at=fmt_iso_utc(due_dt),
                    covered_followup_num=latest_fu,
                    covered_grade=grade,
                    covered_seriousness=ser,
                )
            elif last_sub.status == "submitted":
                candidates = [r for r in chrono if r.followup_num > last_sub.covered_followup_num]
                trigger: ReportEvent | None = None
                for r in candidates:
                    inc_grade = r.ctcae_grade > last_sub.covered_grade
                    inc_flag = any(
                        r.seriousness.get(f, False) and not last_sub.covered_seriousness.get(f, False)
                        for f in SERIOUSNESS_FLAGS
                    )
                    if inc_grade or inc_flag:
                        trigger = r
                        break

                if trigger is not None:
                    fu_d0 = parse_iso_utc(trigger.aware_at)
                    if (
                        trigger.seriousness.get("death")
                        or trigger.seriousness.get("life_threatening")
                        or last_sub.covered_seriousness.get("death")
                        or last_sub.covered_seriousness.get("life_threatening")
                    ):
                        fu_clock = "7_day"
                        fu_due = fu_d0 + timedelta(days=7)
                    else:
                        fu_clock = "15_day"
                        fu_due = fu_d0 + timedelta(days=15)
                        for r_after in candidates:
                            if parse_iso_utc(r_after.aware_at) >= fu_d0 and (
                                r_after.seriousness.get("death")
                                or r_after.seriousness.get("life_threatening")
                            ):
                                fu_clock = "7_day"
                                fu_due = min(fu_due, parse_iso_utc(r_after.aware_at) + timedelta(days=7))
                                break

                    directive = SubmissionDirective(
                        action="create_followup",
                        target_submission_id=None,
                        parent_submission_id=last_sub.submission_id,
                        sequence_num=last_sub.sequence_num + 1,
                        report_clock=fu_clock,
                        day0_at=fmt_iso_utc(fu_d0),
                        due_at=fmt_iso_utc(fu_due),
                        covered_followup_num=latest_fu,
                        covered_grade=grade,
                        covered_seriousness=ser,
                    )

    return ReducedCaseState(
        latest_followup_num=latest_fu,
        initial_aware_at=initial_aware,
        latest_aware_at=latest_aware,
        meddra_pt=meddra_pt,
        ctcae_grade=grade,
        seriousness=ser,
        is_serious=is_serious,
        investigator_causality=inv_c,
        sponsor_causality=spon_c,
        narrative=narrative,
        has_unprocessed=has_unprocessed,
        is_suspected=is_suspected,
        is_unexpected=is_unexpected,
        qualifies_three_legs=qualifies,
        directive=directive,
    )
