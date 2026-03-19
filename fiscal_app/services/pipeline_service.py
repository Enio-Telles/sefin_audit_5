from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from fiscal_app.config import CONSULTAS_ROOT, PIPELINE_SCRIPT, SQL_DIR


@dataclass
class PipelineResult:
    ok: bool
    stdout: str
    stderr: str
    cnpj: str
    returncode: int


class PipelineService:
    def __init__(self, pipeline_script: Path = PIPELINE_SCRIPT, sql_dir: Path = SQL_DIR, output_root: Path = CONSULTAS_ROOT) -> None:
        self.pipeline_script = pipeline_script
        self.sql_dir = sql_dir
        self.output_root = output_root

    @staticmethod
    def sanitize_cnpj(cnpj: str) -> str:
        digits = re.sub(r"\D", "", cnpj or "")
        if len(digits) != 14:
            raise ValueError("Informe um CNPJ com 14 dígitos.")
        return digits

    def run_for_cnpj(self, cnpj: str, data_limite: str | None = None) -> PipelineResult:
        cnpj = self.sanitize_cnpj(cnpj)
        if not self.pipeline_script.exists():
            raise FileNotFoundError(f"Pipeline não encontrado: {self.pipeline_script}")

        cmd = [
            sys.executable,
            str(self.pipeline_script),
            "--cnpj",
            cnpj,
            "--sql-dir",
            str(self.sql_dir),
            "--saida",
            str(self.output_root),
        ]
        if data_limite:
            cmd.extend(["--data-limite", data_limite])

        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        return PipelineResult(
            ok=proc.returncode == 0,
            stdout=proc.stdout,
            stderr=proc.stderr,
            cnpj=cnpj,
            returncode=proc.returncode,
        )
