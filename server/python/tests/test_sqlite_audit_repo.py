import pytest
import sqlite3
from core.execution_trace import build_execution_trace
from core.sqlite_audit_repo import SqliteAuditRepo


@pytest.fixture
def test_db_path(tmp_path):
    db_path = tmp_path / "test_audit.db"
    conn = sqlite3.connect(db_path)
    # Mocking the tables since the schema is created by init_db.ts
    conn.execute("""
        CREATE TABLE audit_executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT UNIQUE,
            scope TEXT, cnpj TEXT, user_name TEXT, status TEXT, code_version TEXT,
            parameters_json TEXT, host_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE audit_execution_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT,
            stage TEXT, status TEXT, message TEXT, context_json TEXT,
            started_at TEXT, finished_at TEXT, duration_ms INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE audit_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT,
            artifact_type TEXT, artifact_path TEXT, metadata_json TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def test_sqlite_audit_repo_save(test_db_path):
    repo = SqliteAuditRepo(test_db_path)
    trace = build_execution_trace("test_scope", "123")
    trace.add_event("stage_1", "success")
    trace.add_artifact("parquet", "/path/to/file.parquet")

    repo.save_trace(trace)

    conn = sqlite3.connect(test_db_path)
    conn.row_factory = sqlite3.Row

    exec_row = conn.execute(
        "SELECT * FROM audit_executions WHERE execution_id = ?", (trace.execution_id,)
    ).fetchone()
    assert exec_row is not None
    assert exec_row["scope"] == "test_scope"
    assert exec_row["cnpj"] == "123"

    events = conn.execute(
        "SELECT * FROM audit_execution_events WHERE execution_id = ?",
        (trace.execution_id,),
    ).fetchall()
    assert len(events) == 1
    assert events[0]["stage"] == "stage_1"

    artifacts = conn.execute(
        "SELECT * FROM audit_artifacts WHERE execution_id = ?", (trace.execution_id,)
    ).fetchall()
    assert len(artifacts) == 1
    assert artifacts[0]["artifact_type"] == "parquet"

    conn.close()
