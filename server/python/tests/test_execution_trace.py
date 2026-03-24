from core.execution_trace import (
    build_execution_trace,
    ExecutionTimer,
    ExecutionTraceStore,
)
import json


def test_execution_trace_creation():
    trace = build_execution_trace("test_scope", "12345678901234")
    assert trace.scope == "test_scope"
    assert trace.cnpj == "12345678901234"
    assert trace.execution_id is not None
    assert trace.user == "system"
    assert len(trace.events) == 0


def test_execution_timer():
    trace = build_execution_trace("test_scope")
    with ExecutionTimer(trace, stage="test_stage", message="test"):
        pass

    assert len(trace.events) == 1
    event = trace.events[0]
    assert event.stage == "test_stage"
    assert event.status == "success"
    assert event.duration_ms is not None


def test_execution_trace_store(tmp_path):
    trace = build_execution_trace("test_scope")
    trace.add_event("stage1", "running")
    trace.events[0].status = "success"

    store = ExecutionTraceStore(tmp_path)
    store.save(trace)

    assert store.summary_path.exists()
    assert store.events_path.exists()

    summary = json.loads(store.summary_path.read_text())
    assert summary["execution_id"] == trace.execution_id
    assert summary["scope"] == "test_scope"
