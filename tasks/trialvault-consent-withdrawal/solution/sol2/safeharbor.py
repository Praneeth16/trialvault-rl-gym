"""SOLUTION=2: Standalone HIPAA Safe Harbor transformation rules."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

LOW_POP_ZIP3_PREFIXES = frozenset(
    {
        "036", "059", "063", "102", "203", "556", "692", "790",
        "821", "823", "830", "831", "878", "879", "884", "890", "893",
    }
)

STRIP_KEYS = frozenset(
    {
        "subject_id", "mrn", "full_name", "patient_name", "subject_name",
        "email", "recipient", "phone", "ssn", "dob", "street_address", "city",
        "postal_code", "device_serial", "ip_address", "signature_ip", "portal_url",
        "emergency_contact_name", "emergency_contact_phone",
        "collected_date", "visit_date", "onset_date", "randomization_date",
    }
)


def digest_identifier(raw: str) -> str:
    return hashlib.sha256(raw.strip().encode("utf-8")).hexdigest()


def safe_harbor_zip3(raw_zip: str | None) -> str | None:
    if not raw_zip:
        return None
    nums = "".join(c for c in raw_zip if c.isdigit())
    if len(nums) < 3:
        return "000"
    prefix = nums[:3]
    return "000" if prefix in LOW_POP_ZIP3_PREFIXES else prefix


def safe_harbor_demographics(age: int | None, dob: str | None) -> dict[str, Any]:
    if age is not None and age > 89:
        return {"age_at_enrollment": None, "age_bucket": "90+", "dob": None}
    return {
        "age_at_enrollment": age,
        "age_bucket": str(age) if age is not None else None,
        "dob": dob[:4] if dob and len(dob) >= 4 else None,
    }


def extract_sensitive_strings(subject: dict[str, Any], dates: list[str]) -> list[str]:
    found: set[str] = set()
    for key in (
        "subject_id", "mrn", "full_name", "email", "phone", "ssn", "dob",
        "street_address", "city", "device_serial", "ip_address", "portal_url",
        "emergency_contact_name", "emergency_contact_phone", "randomization_date",
    ):
        v = subject.get(key)
        if isinstance(v, str) and v.strip():
            found.add(v.strip())
    for name in (subject.get("full_name") or "", *json.loads(subject.get("aliases_json") or "[]")):
        toks = name.split()
        if toks:
            found.add(name.strip())
        found.update(t for t in toks if len(t) >= 3)
        if len(toks) > 1:
            found.add(f"{toks[0][0]}.{toks[-1][0]}.")
    for dt in dates:
        if dt and dt.strip():
            found.add(dt.strip())
    return sorted(found, key=len, reverse=True)


def scrub_string(value: str, sensitive: list[str]) -> str:
    res = value
    for item in sensitive:
        res = re.sub(re.escape(item), "[REDACTED]", res, flags=re.IGNORECASE)
    return res


def scrub_payload(value: Any, sensitive: list[str], anon_ref: str) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if k in STRIP_KEYS:
                if k == "subject_id":
                    out["anon_subject_ref"] = anon_ref
                continue
            out[k] = scrub_payload(v, sensitive, anon_ref)
        return out
    if isinstance(value, list):
        return [scrub_payload(x, sensitive, anon_ref) for x in value]
    if isinstance(value, str):
        return scrub_string(value, sensitive)
    return value
