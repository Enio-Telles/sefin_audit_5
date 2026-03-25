from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime, timezone

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class JobState(BaseModel):
    job_id: str
    job_type: str = "oracle_extraction"
    status: str = "queued"  # queued, running, success, error, cancelled
    progress: float = 0.0
    stage: str = "init"
    message: str = ""
    total_queries: int = 0
    completed_queries: int = 0
    results: list[dict[str, Any]] = Field(default_factory=list)
    error_message: Optional[str] = None
    execution_id: Optional[str] = None
    output_dir: Optional[str] = None
    created_at: str = Field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
