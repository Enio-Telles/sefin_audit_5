import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Optional
import logging
import sqlite3
import json
from pathlib import Path
from .job_models import JobState, _utc_now_iso

logger = logging.getLogger("sefin_audit_python.job_manager")

class JobManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(JobManager, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_workers: int = 4, db_path: str = "sefin_audit.db"):
        with self._lock:
            if self._initialized:
                return
            self._jobs: dict[str, JobState] = {}
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
            self.db_path = Path(db_path)
            self._init_db()
            self._load_jobs_from_db()
            self._initialized = True

    def _get_connection(self):
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT,
                    status TEXT,
                    progress REAL,
                    stage TEXT,
                    message TEXT,
                    total_queries INTEGER,
                    completed_queries INTEGER,
                    results_json TEXT,
                    error_message TEXT,
                    execution_id TEXT,
                    output_dir TEXT,
                    created_at TEXT,
                    started_at TEXT,
                    finished_at TEXT
                )
            """)
            conn.commit()

    def _load_jobs_from_db(self):
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM audit_jobs")
            rows = cursor.fetchall()
            for row in rows:
                data = dict(row)
                data["results"] = json.loads(data["results_json"]) if data["results_json"] else []
                del data["results_json"]
                # Se o job estava rodando ao reiniciar, marca como erro (órfão)
                if data["status"] in ["queued", "running"]:
                    data["status"] = "error"
                    data["error_message"] = "Job interrompido por reinicialização do sistema"
                    data["stage"] = "failed"
                    data["finished_at"] = _utc_now_iso()
                    self._persist_job(data["job_id"], data)
                self._jobs[data["job_id"]] = JobState(**data)

    def _persist_job(self, job_id: str, job_dict: dict = None):
         if job_dict is None:
             job_dict = self._jobs[job_id].model_dump()
         with self._get_connection() as conn:
             conn.execute("""
                 INSERT INTO audit_jobs
                 (job_id, job_type, status, progress, stage, message, total_queries, completed_queries, results_json, error_message, execution_id, output_dir, created_at, started_at, finished_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(job_id) DO UPDATE SET
                     status=excluded.status,
                     progress=excluded.progress,
                     stage=excluded.stage,
                     message=excluded.message,
                     total_queries=excluded.total_queries,
                     completed_queries=excluded.completed_queries,
                     results_json=excluded.results_json,
                     error_message=excluded.error_message,
                     execution_id=excluded.execution_id,
                     output_dir=excluded.output_dir,
                     started_at=excluded.started_at,
                     finished_at=excluded.finished_at
             """, (
                 job_dict["job_id"],
                 job_dict["job_type"],
                 job_dict["status"],
                 job_dict["progress"],
                 job_dict["stage"],
                 job_dict["message"],
                 job_dict["total_queries"],
                 job_dict["completed_queries"],
                 json.dumps(job_dict["results"], ensure_ascii=False) if job_dict.get("results") else None,
                 job_dict["error_message"],
                 job_dict["execution_id"],
                 job_dict["output_dir"],
                 job_dict["created_at"],
                 job_dict["started_at"],
                 job_dict["finished_at"]
             ))
             conn.commit()

    def submit_job(self, job_id: str, job_type: str, func: Callable, *args, **kwargs) -> JobState:
        with self._lock:
            job_state = JobState(job_id=job_id, job_type=job_type)
            self._jobs[job_id] = job_state
            self._persist_job(job_id)

        self._executor.submit(self._run_job_wrapper, job_id, func, *args, **kwargs)
        return job_state

    def _run_job_wrapper(self, job_id: str, func: Callable, *args, **kwargs):
        try:
            with self._lock:
                if self._jobs[job_id].status == "cancelled":
                    return # Job cancelled before starting
                self._jobs[job_id].status = "running"
                self._jobs[job_id].started_at = _utc_now_iso()
                self._persist_job(job_id)

            # Pass job_id so the function can poll status
            func(job_id, *args, **kwargs)

            with self._lock:
                if self._jobs[job_id].status != "cancelled":
                     self._jobs[job_id].status = "success"
                     self._jobs[job_id].progress = 100.0
                     self._jobs[job_id].stage = "completed"
                     self._persist_job(job_id)
        except Exception as e:
            logger.exception(f"Error in job {job_id}")
            with self._lock:
                if self._jobs[job_id].status != "cancelled":
                    self._jobs[job_id].status = "error"
                    self._jobs[job_id].error_message = str(e)
                    self._jobs[job_id].stage = "failed"
                    self._persist_job(job_id)
        finally:
             with self._lock:
                 self._jobs[job_id].finished_at = _utc_now_iso()
                 self._persist_job(job_id)

    def get_job(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            # Return a copy to avoid accidental mutation
            job = self._jobs.get(job_id)
            return job.model_copy() if job else None

    def list_jobs(self) -> list[JobState]:
        with self._lock:
            # Return a copy
            return [j.model_copy() for j in self._jobs.values()]

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                if job.status in ["queued", "running"]:
                    job.status = "cancelled"
                    job.stage = "cancelled"
                    self._persist_job(job_id)
                    return True
            return False

    def update_job(self, job_id: str, **kwargs) -> None:
        with self._lock:
            if job_id in self._jobs:
                 job = self._jobs[job_id]
                 for key, value in kwargs.items():
                      if hasattr(job, key):
                           setattr(job, key, value)
                 self._persist_job(job_id)

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            return job is not None and job.status == "cancelled"

job_manager = JobManager()
