"""SQLite FTS5 blinded safety case search index."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class SearchIndex:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS case_search_fts USING fts5(
                    case_uid UNINDEXED,
                    subject_id UNINDEXED,
                    site_id UNINDEXED,
                    trial_id UNINDEXED,
                    content
                )
                """
            )
            conn.commit()

    def upsert_case(
        self,
        *,
        case_uid: str,
        subject_id: str,
        site_id: str,
        trial_id: str,
        content: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM case_search_fts WHERE case_uid = ?", (case_uid,))
            conn.execute(
                """
                INSERT INTO case_search_fts (case_uid, subject_id, site_id, trial_id, content)
                VALUES (?, ?, ?, ?, ?)
                """,
                (case_uid, subject_id, site_id, trial_id, content),
            )
            conn.commit()

    def search_cases(
        self,
        query: str,
        *,
        site_id: str | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        cleaned = query.strip()
        if not cleaned:
            return []
        escaped = '"' + cleaned.replace('"', '""') + '"'
        with self._connect() as conn:
            if site_id is not None:
                rows = conn.execute(
                    """
                    SELECT case_uid, subject_id, site_id, trial_id, content
                    FROM case_search_fts
                    WHERE site_id = ? AND case_search_fts MATCH ?
                    LIMIT ?
                    """,
                    (site_id, escaped, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT case_uid, subject_id, site_id, trial_id, content
                    FROM case_search_fts
                    WHERE case_search_fts MATCH ?
                    LIMIT ?
                    """,
                    (escaped, limit),
                ).fetchall()
        return [dict(r) for r in rows]
