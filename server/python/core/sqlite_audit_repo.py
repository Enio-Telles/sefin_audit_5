import sqlite3
import json
from pathlib import Path
from .execution_trace import ExecutionTrace


class SqliteAuditRepo:
    def __init__(self, db_path: str | Path = "sefin_audit.db"):
        self.db_path = Path(db_path)

    def _get_connection(self):
        # Allow connecting from any thread if needed, and use WAL for better concurrency
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def save_trace(self, trace: ExecutionTrace) -> None:
        """Persists the execution trace, its events, and artifacts to the SQLite database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Upsert execution
            cursor.execute(
                """
                INSERT INTO audit_executions
                (execution_id, scope, cnpj, user_name, status, code_version, parameters_json, host_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(execution_id) DO UPDATE SET
                    status=excluded.status,
                    parameters_json=excluded.parameters_json
                """,
                (
                    trace.execution_id,
                    trace.scope,
                    trace.cnpj,
                    trace.user,
                    trace.events[-1].status if trace.events else "created",
                    trace.code_version,
                    json.dumps(trace.parameters, ensure_ascii=False),
                    trace.host,
                ),
            )

            # Insert events
            # To avoid duplicates on re-saves, we can delete existing events for this execution_id and re-insert,
            # or rely on the fact that events are appended. For simplicity and robustness, delete and insert.
            cursor.execute(
                "DELETE FROM audit_execution_events WHERE execution_id = ?",
                (trace.execution_id,),
            )
            for event in trace.events:
                cursor.execute(
                    """
                    INSERT INTO audit_execution_events
                    (execution_id, stage, status, message, context_json, started_at, finished_at, duration_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.execution_id,
                        event.stage,
                        event.status,
                        event.message,
                        json.dumps(event.context, ensure_ascii=False)
                        if event.context
                        else None,
                        event.started_at,
                        event.finished_at,
                        event.duration_ms,
                    ),
                )

            # Insert artifacts
            cursor.execute(
                "DELETE FROM audit_artifacts WHERE execution_id = ?",
                (trace.execution_id,),
            )
            for artifact in trace.artifacts:
                # Copy artifact dict to not mutate original
                meta = dict(artifact)
                # Remove fields that go into columns
                meta.pop("execution_id", None)
                art_type = meta.pop("artifact_type", "unknown")
                path = meta.pop("path", "")
                meta.pop("created_at", None)

                cursor.execute(
                    """
                    INSERT INTO audit_artifacts
                    (execution_id, artifact_type, artifact_path, metadata_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        trace.execution_id,
                        art_type,
                        path,
                        json.dumps(meta, ensure_ascii=False) if meta else None,
                    ),
                )
            conn.commit()
