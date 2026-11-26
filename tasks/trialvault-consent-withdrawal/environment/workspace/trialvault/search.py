"""SQLite FTS5 full-text search index for clinical site coordinators."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class SearchIndex:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS subject_search_fts USING fts5(
                    subject_id UNINDEXED,
                    site_id UNINDEXED,
                    doc_type UNINDEXED,
                    content
                )
                """
            )
            conn.commit()

    def upsert_document(self, subject_id: str, site_id: str, doc_type: str, content: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM subject_search_fts WHERE subject_id = ? AND site_id = ? AND doc_type = ?",
                (subject_id, site_id, doc_type),
            )
            conn.execute(
                "INSERT INTO subject_search_fts (subject_id, site_id, doc_type, content) VALUES (?, ?, ?, ?)",
                (subject_id, site_id, doc_type, content),
            )
            conn.commit()

    def delete_subject(self, subject_id: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM subject_search_fts WHERE subject_id = ?",
                (subject_id,),
            )
            conn.commit()
            return cur.rowcount

    def search(self, site_id: str, query: str, limit: int = 25) -> list[dict[str, Any]]:
        cleaned = query.strip()
        if not cleaned:
            return []
        escaped = '"' + cleaned.replace('"', '""') + '"'
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT subject_id, site_id, doc_type, content
                FROM subject_search_fts
                WHERE site_id = ? AND subject_search_fts MATCH ?
                LIMIT ?
                """,
                (site_id, escaped, limit),
            ).fetchall()
        return [dict(r) for r in rows]
