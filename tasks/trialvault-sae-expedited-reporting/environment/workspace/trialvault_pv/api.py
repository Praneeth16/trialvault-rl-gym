"""TrialVault PV FastAPI HTTP service (starter baseline endpoints)."""

from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

from trialvault_pv.config import DataPaths, get_data_paths
from trialvault_pv.db import connect_edc, init_all_dbs, lookup_auth_principal
from trialvault_pv.listings import read_dsmb_susar_listing, read_site_blinded_listing
from trialvault_pv.search import SearchIndex
from trialvault_pv.service import compute_trial_blinded_summary, enroll_subject


def _paths() -> DataPaths:
    paths = get_data_paths()
    init_all_dbs(paths)
    return paths


def get_principal(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing_bearer_token",
        )
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="empty_bearer_token",
        )
    paths = _paths()
    with connect_edc(paths.edc_db) as conn:
        principal = lookup_auth_principal(conn, token)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid_bearer_token",
        )
    return principal


def require_any_scope(principal: dict[str, Any], *allowed_scopes: str) -> None:
    if not (principal["scopes"] & set(allowed_scopes)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="insufficient_scope",
        )


class SubjectEnrollRequest(BaseModel):
    subject_id: str | None = None
    trial_id: str
    site_id: str
    age: int = Field(..., ge=18, le=120)
    sex: str
    enrollment_date: str
    treatment_arm: str
    is_investigational_drug: bool = True


app = FastAPI(title="TrialVault PV — Safety & Expedited Reporting", version="0.4.0")


@app.get("/health")
def health() -> dict[str, str]:
    _paths()
    return {"status": "ok", "service": "trialvault_pv"}


@app.post("/v1/subjects", status_code=status.HTTP_201_CREATED)
def api_enroll_subject(
    body: SubjectEnrollRequest,
    principal: dict[str, Any] = Depends(get_principal),
) -> dict[str, Any]:
    require_any_scope(principal, "site:safety", "pv:unblinded")
    if "pv:unblinded" not in principal["scopes"] and principal["site_id"] != body.site_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")
    try:
        return enroll_subject(
            _paths(),
            actor=principal["actor"],
            data=body.model_dump(),
        )
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="site_or_trial_not_found")


@app.get("/v1/safety-cases/search")
def api_search_safety_cases(
    q: str = Query(..., min_length=1),
    principal: dict[str, Any] = Depends(get_principal),
) -> dict[str, Any]:
    require_any_scope(principal, "site:safety", "pv:unblinded")
    scoped_site = None if "pv:unblinded" in principal["scopes"] else principal["site_id"]
    hits = SearchIndex(_paths().search_db).search_cases(q, site_id=scoped_site)
    return {"query": q, "site_id": scoped_site, "hits": hits}


@app.get("/v1/sites/{site_id}/blinded-line-listing")
def api_get_site_blinded_listing(
    site_id: str,
    principal: dict[str, Any] = Depends(get_principal),
) -> dict[str, Any]:
    require_any_scope(principal, "site:safety", "pv:unblinded")
    if "pv:unblinded" not in principal["scopes"] and principal["site_id"] != site_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_site_access")
    try:
        return read_site_blinded_listing(_paths(), site_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="site_not_found")


@app.get("/v1/pv/trials/{trial_id}/dsmb-line-listing")
def api_get_dsmb_line_listing(
    trial_id: str,
    principal: dict[str, Any] = Depends(get_principal),
) -> dict[str, Any]:
    require_any_scope(principal, "pv:unblinded")
    try:
        return read_dsmb_susar_listing(_paths(), trial_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="trial_not_found")


@app.get("/v1/trials/{trial_id}/blinded-summary")
def api_get_trial_blinded_summary(
    trial_id: str,
    principal: dict[str, Any] = Depends(get_principal),
) -> dict[str, Any]:
    require_any_scope(principal, "site:safety", "pv:unblinded")
    if "pv:unblinded" not in principal["scopes"] and principal["trial_id"] != trial_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross_trial_access")
    try:
        return compute_trial_blinded_summary(_paths(), trial_id)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="trial_not_found")
