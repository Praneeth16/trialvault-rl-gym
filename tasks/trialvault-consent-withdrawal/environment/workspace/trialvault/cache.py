"""Persistent read-through JSON projection cache."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class ProjectionCache:
    def __init__(self, cache_file: Path) -> None:
        self.cache_file = cache_file
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.cache_file.exists():
            self._write_all({})

    def _read_all(self) -> dict[str, Any]:
        if not self.cache_file.exists():
            return {}
        raw = self.cache_file.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        return json.loads(raw)

    def _write_all(self, data: dict[str, Any]) -> None:
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(self.cache_file.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, sort_keys=True)
            os.replace(tmp_name, self.cache_file)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def get(self, key: str) -> Any | None:
        return self._read_all().get(key)

    def set(self, key: str, value: Any) -> None:
        data = self._read_all()
        data[key] = value
        self._write_all(data)

    def delete(self, key: str) -> bool:
        data = self._read_all()
        if key in data:
            del data[key]
            self._write_all(data)
            return True
        return False

    def delete_by_prefix(self, prefix: str) -> int:
        data = self._read_all()
        to_remove = [k for k in data if k.startswith(prefix)]
        for k in to_remove:
            del data[k]
        if to_remove:
            self._write_all(data)
        return len(to_remove)

    def all_items(self) -> dict[str, Any]:
        return self._read_all()
