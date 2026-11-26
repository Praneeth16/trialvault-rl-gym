"""File-backed blob store for signed eConsent forms and central-lab reports."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class BlobStore:
    def __init__(self, blobs_dir: Path) -> None:
        self.blobs_dir = blobs_dir
        self.blobs_dir.mkdir(parents=True, exist_ok=True)

    def _object_path(self, key: str) -> Path:
        clean = key.lstrip("/")
        return self.blobs_dir / clean

    def _meta_path(self, key: str) -> Path:
        obj = self._object_path(key)
        return obj.parent / f"{obj.name}.meta.json"

    def _atomic_write_json(self, target: Path, payload: dict[str, Any]) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
            os.replace(tmp_name, target)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def put_json(self, key: str, payload: dict[str, Any], metadata: dict[str, Any]) -> None:
        self._atomic_write_json(self._object_path(key), payload)
        self._atomic_write_json(self._meta_path(key), metadata)

    def get_json(self, key: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        obj_path = self._object_path(key)
        meta_path = self._meta_path(key)
        if not obj_path.exists():
            return None
        payload = json.loads(obj_path.read_text(encoding="utf-8"))
        metadata = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        return payload, metadata

    def delete(self, key: str) -> bool:
        removed = False
        for path in (self._object_path(key), self._meta_path(key)):
            if path.exists():
                path.unlink()
                removed = True
        return removed

    def list_keys(self, prefix: str = "") -> list[str]:
        results: list[str] = []
        if not self.blobs_dir.exists():
            return results
        for path in sorted(self.blobs_dir.rglob("*.json")):
            if path.name.endswith(".meta.json"):
                continue
            rel = path.relative_to(self.blobs_dir).as_posix()
            if rel.startswith(prefix):
                results.append(rel)
        return results
