from __future__ import annotations

import json
import logging
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("sefin_audit_python")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ExecutionEvent:
    execution_id: str
    stage: str
    status: str
    message: str = ""
    started_at: str = field(default_factory=_utc_now_iso)
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionTrace:
    execution_id: str
    scope: str
    cnpj: str = ""
    user: str = "system"
    created_at: str = field(default_factory=_utc_now_iso)
    host: str = field(default_factory=socket.gethostname)
    code_version: str = "local"
    parameters: dict[str, Any] = field(default_factory=dict)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    events: list[ExecutionEvent] = field(default_factory=list)

    def add_event(self, stage: str, status: str, message: str = "", **context: Any) -> ExecutionEvent:
        event = ExecutionEvent(
            execution_id=self.execution_id,
            stage=stage,
            status=status,
            message=message,
            context=context,
        )
        self.events.append(event)
        return event

    def finish_event(self, event: ExecutionEvent, status: Optional[str] = None, message: Optional[str] = None, **context: Any) -> None:
        event.finished_at = _utc_now_iso()
        if event.started_at and event.finished_at:
            started = datetime.fromisoformat(event.started_at)
            finished = datetime.fromisoformat(event.finished_at)
            event.duration_ms = int((finished - started).total_seconds() * 1000)
        if status is not None:
            event.status = status
        if message is not None:
            event.message = message
        if context:
            event.context.update(context)

    def add_artifact(self, artifact_type: str, path: str, **metadata: Any) -> None:
        self.artifacts.append(
            {
                "execution_id": self.execution_id,
                "artifact_type": artifact_type,
                "path": path,
                "created_at": _utc_now_iso(),
                **metadata,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "scope": self.scope,
            "cnpj": self.cnpj,
            "user": self.user,
            "created_at": self.created_at,
            "host": self.host,
            "code_version": self.code_version,
            "parameters": self.parameters,
            "artifacts": self.artifacts,
            "events": [asdict(event) for event in self.events],
        }


class ExecutionTraceStore:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @property
    def summary_path(self) -> Path:
        return self.base_dir / "execution_summary.json"

    @property
    def events_path(self) -> Path:
        return self.base_dir / "execution_events.jsonl"

    def save(self, trace: ExecutionTrace) -> None:
        self.summary_path.write_text(
            json.dumps(trace.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with self.events_path.open("w", encoding="utf-8") as fh:
            for event in trace.events:
                fh.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")


class ExecutionTimer:
    def __init__(self, trace: ExecutionTrace, stage: str, message: str = "", **context: Any):
        self.trace = trace
        self.stage = stage
        self.message = message
        self.context = context
        self.event: Optional[ExecutionEvent] = None
        self._start = 0.0

    def __enter__(self) -> ExecutionEvent:
        self._start = time.perf_counter()
        self.event = self.trace.add_event(self.stage, "running", self.message, **self.context)
        return self.event

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.event is None:
            return
        status = "success" if exc is None else "error"
        duration_ms = int((time.perf_counter() - self._start) * 1000)
        self.trace.finish_event(
            self.event,
            status=status,
            message=str(exc) if exc else self.message,
            duration_ms=duration_ms,
        )
        if exc is not None:
            logger.exception("Erro na etapa '%s' da execução '%s'", self.stage, self.trace.execution_id)


def build_execution_trace(scope: str, cnpj: str = "", user: str = "system", **parameters: Any) -> ExecutionTrace:
    execution_id = str(uuid.uuid4())
    return ExecutionTrace(
        execution_id=execution_id,
        scope=scope,
        cnpj=cnpj,
        user=user,
        parameters=parameters,
    )
