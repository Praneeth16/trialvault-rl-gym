"""JSONL outbound ePRO notification spool and dead-letter queue."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class SpoolStore:
    def __init__(self, outbound_queue: Path, dead_letters: Path) -> None:
        self.outbound_queue = outbound_queue
        self.dead_letters = dead_letters
        self.outbound_queue.parent.mkdir(parents=True, exist_ok=True)
        self.outbound_queue.touch(exist_ok=True)
        self.dead_letters.touch(exist_ok=True)

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
        return records

    @staticmethod
    def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(json.dumps(rec, sort_keys=True) + "\n")
            os.replace(tmp_name, path)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def append_outbound(self, record: dict[str, Any]) -> None:
        with self.outbound_queue.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")

    def append_dead_letter(self, record: dict[str, Any]) -> None:
        with self.dead_letters.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")

    def read_outbound(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self.outbound_queue)

    def read_dead_letters(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self.dead_letters)

    def replace_outbound(self, records: list[dict[str, Any]]) -> None:
        self._write_jsonl(self.outbound_queue, records)

    def replace_dead_letters(self, records: list[dict[str, Any]]) -> None:
        self._write_jsonl(self.dead_letters, records)
