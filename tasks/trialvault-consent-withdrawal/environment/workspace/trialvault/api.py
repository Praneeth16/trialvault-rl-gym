"""TrialVault FastAPI HTTP API."""

from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from trialvault.cache import ProjectionCache
from trialvault.config import DataPaths, get_data_paths
from trialvault.db import connect_edc, init_edc_db, lookup_site_by_token
from trialvault.exports import generate_dsmb_snapshot, read_dsmb_snapshot
from trialvault.queries import list_subject_queries, open_data_query
from trialvault.search import SearchIndex
from trialvault.service import (
    cache_subject_projection,
    compute_safety_summary,
    enroll_subject,
    ingest_central_lab_webhook,
    record_adverse_event,
    record_visit,
    schedule_epro_reminder,
)


def _paths() -> DataPaths:
    paths = get_data_paths()
    init_edc_db(paths)
    return paths


def get_current_site(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing_bearer_token")
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="empty_bearer_token")
    paths = _paths()
    with connect_edc(paths.edc_db) as conn:
        site = lookup_site_by_token(conn, token)
    if site is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_bearer_token")
    return site


def require_scope(site: dict[str, Any], scope: str) -> None:
    if scope not in site["scopes"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"missing_scope:{scope}")


class SubjectEnrollRequest(BaseModel):
    subject_id: str | None = None
    mrn: str
    full_name: str
    email: str
    phone: str
    ssn: str
    dob: str
    age_at_enrollment: int
    street_address: str
    city: str
    state: str
    postal_code: str
    device_serial: str
    ip_address: str
    portal_url: str
    emergency_contact_name: str
    emergency_contact_phone: str
    treatment_arm: str
    randomization_date: str
    regulatory_hold: bool = False
    hold_reason: str | None = None
    household_group_id: str | None = None
    investigator_name: str = "Dr. Hannah Lin"


class VisitCreateRequest(BaseModel):
    visit_id: str | None = None
    visit_number: int
    visit_date: str
    days_from_randomization: int
    systolic_bp: int
    diastolic_bp: int
    clinical_notes: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class AdverseEventCreateRequest(BaseModel):
    ae_id: str | None = None
    meddra_term: str
    ctcae_grade: int
    serious: bool = False
    onset_date: str
    onset_study_day: int
    relatedness: str
    reporter_notes: str
    payload: dict[str, Any] = Field(default_factory=dict)


class DataQueryCreateRequest(BaseModel):
    query_id: str | None = None
    field_name: str
    query_text: str
    response_text: str | None = None
    status: str = "open"


class EproReminderRequest(BaseModel):
    notif_id: str | None = None
    channel: str = "email"
    recipient: str | None = None
    template: str = "daily_diary_reminder"
    scheduled_for: str | None = None
    status: str = "pending"
    sent_at: str | None = None
    payload: dict[str, Any] | None = None


class CentralLabWebhookRequest(BaseModel):
    site_id: str
    trial_id: str | None = None
    subject_id: str | None = None
    mrn: str | None = None
    accession_no: str
    analyte: str
    value_num: float
    unit: str
    reference_flag: str = "NORMAL"
    collected_date: str
    study_day: int
    patient_demographics: dict[str, Any] = Field(default_factory=dict)


app = FastAPI(title="TrialVault Clinical Trial EDC", version="0.5.0")


@app.get("/health")
def health() -> dict[str, str]:
    _paths()
    return {"status": "ok", "service": "trialvault"}


@app.post("/v1/subjects", status_code=status.HTTP_201_CREATED)
def api_enroll_subject(
    body: SubjectEnrollRequest,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    return enroll_subject(
        _paths(),
        site_id=site["site_id"],
        trial_id=site["trial_id"],
        actor=site.get("actor", f"site:{site['site_id']}"),
        data=body.model_dump(),
    )


@app.get("/v1/subjects/search")
def api_search_subjects(
    q: str = Query(..., min_length=1),
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:read")
    hits = SearchIndex(_paths().search_db).search(site["site_id"], q)
    return {"site_id": site["site_id"], "query": q, "hits": hits}


@app.get("/v1/subjects/{subject_id}")
def api_get_subject(
    subject_id: str,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:read")
    paths = _paths()
    cache = ProjectionCache(paths.cache_file)
    cached = cache.get(f"site:{site['site_id']}:subject:{subject_id}:profile")
    if cached is not None:
        return cached
    with connect_edc(paths.edc_db) as conn:
        row = conn.execute("SELECT site_id FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
        if row["site_id"] != site["site_id"]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")
        proj = cache_subject_projection(conn, cache, subject_id)
    assert proj is not None
    return proj


@app.post("/v1/subjects/{subject_id}/visits", status_code=status.HTTP_201_CREATED)
def api_record_visit(
    subject_id: str,
    body: VisitCreateRequest,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    try:
        return record_visit(
            _paths(),
            site_id=site["site_id"],
            subject_id=subject_id,
            actor=site.get("actor", f"site:{site['site_id']}"),
            data=body.model_dump(),
        )
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")


@app.post("/v1/subjects/{subject_id}/adverse-events", status_code=status.HTTP_201_CREATED)
def api_record_ae(
    subject_id: str,
    body: AdverseEventCreateRequest,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    try:
        return record_adverse_event(
            _paths(),
            site_id=site["site_id"],
            subject_id=subject_id,
            actor=site.get("actor", f"site:{site['site_id']}"),
            data=body.model_dump(),
        )
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")


@app.post("/v1/subjects/{subject_id}/queries", status_code=status.HTTP_201_CREATED)
def api_open_query(
    subject_id: str,
    body: DataQueryCreateRequest,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    try:
        return open_data_query(
            _paths(),
            site_id=site["site_id"],
            subject_id=subject_id,
            actor=site.get("actor", f"site:{site['site_id']}"),
            data=body.model_dump(),
        )
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")


@app.get("/v1/subjects/{subject_id}/queries")
def api_list_queries(
    subject_id: str,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:read")
    try:
        queries = list_subject_queries(_paths(), site_id=site["site_id"], subject_id=subject_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")
    return {"subject_id": subject_id, "queries": queries}


@app.post("/v1/subjects/{subject_id}/epro-reminders", status_code=status.HTTP_201_CREATED)
def api_schedule_epro(
    subject_id: str,
    body: EproReminderRequest,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    try:
        return schedule_epro_reminder(
            _paths(),
            site_id=site["site_id"],
            subject_id=subject_id,
            data={k: v for k, v in body.model_dump().items() if v is not None},
        )
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")


@app.post("/v1/webhooks/central-lab")
def api_central_lab_webhook(body: CentralLabWebhookRequest) -> JSONResponse:
    try:
        code, result = ingest_central_lab_webhook(_paths(), body.model_dump())
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="subject_not_found")
    return JSONResponse(status_code=code, content=result)


@app.post("/v1/trials/{trial_id}/dsmb-snapshots", status_code=status.HTTP_201_CREATED)
def api_create_dsmb_snapshot(
    trial_id: str,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:write")
    try:
        return generate_dsmb_snapshot(_paths(), trial_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="trial_not_found")


@app.get("/v1/trials/{trial_id}/dsmb-snapshots/{snapshot_id}")
def api_get_dsmb_snapshot(
    trial_id: str,
    snapshot_id: str,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:read")
    try:
        return read_dsmb_snapshot(_paths(), trial_id, snapshot_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="snapshot_not_found")


@app.get("/v1/trials/{trial_id}/safety-summary")
def api_safety_summary(
    trial_id: str,
    site: dict[str, Any] = Depends(get_current_site),
) -> dict[str, Any]:
    require_scope(site, "edc:read")
    try:
        return compute_safety_summary(_paths(), trial_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="trial_not_found")
