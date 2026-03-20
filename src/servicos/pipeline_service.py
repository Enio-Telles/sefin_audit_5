from __future__ import annotations

import re
import subprocess
import sys
import shutil
from dataclasses import dataclass
from pathlib import Path

# Importar constantes do config
from src.config import PIPELINE_SCRIPT, SQL_DIR, CNPJ_ROOT

@dataclass
class PipelineResult:
    ok: bool
    stdout: str
    stderr: str
    cnpj: str
    returncode: int

class PipelineService:
    def __init__(self, pipeline_script: Path = PIPELINE_SCRIPT, sql_dir: Path = SQL_DIR, output_root: Path = CNPJ_ROOT) -> None:
        """
        Serviço para disparar o orquestrador via subprocesso.
        output_root padrão é CNPJ_ROOT (dados/CNPJ).
        """
        self.pipeline_script = pipeline_script
        self.sql_dir = sql_dir
        self.output_root = output_root

    @staticmethod
    def sanitize_cnpj(cnpj: str) -> str:
        digits = re.sub(r"\D", "", cnpj or "")
        if len(digits) != 14:
            raise ValueError("Informe um CNPJ com 14 dígitos.")
        return digits

    def _run_cmd(self, cnpj: str, extra_args: list[str] = None, data_limite: str | None = None) -> PipelineResult:
        cnpj = self.sanitize_cnpj(cnpj)
        if not self.pipeline_script.exists():
            raise FileNotFoundError(f"Pipeline não encontrado: {self.pipeline_script}")

        cmd = [
            sys.executable,
            str(self.pipeline_script),
            "--cnpj", cnpj,
            "--sql-dir", str(self.sql_dir),
            "--saida", str(self.output_root),
        ]
        if data_limite:
            cmd.extend(["--data-limite", data_limite])
        if extra_args:
            cmd.extend(extra_args)

        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        return PipelineResult(
            ok=proc.returncode == 0,
            stdout=proc.stdout,
            stderr=proc.stderr,
            cnpj=cnpj,
            returncode=proc.returncode,
        )

    def run_full_pipeline(self, cnpj: str, data_limite: str | None = None) -> PipelineResult:
        """Executa extração + processamento."""
        return self._run_cmd(cnpj, data_limite=data_limite)

    def run_extraction(self, cnpj: str, data_limite: str | None = None) -> PipelineResult:
        """Executa apenas a extração do Oracle."""
        return self._run_cmd(cnpj, extra_args=["--apenas-extrair"], data_limite=data_limite)

    def run_processing(self, cnpj: str) -> PipelineResult:
        """Executa apenas o processamento dos arquivos locais."""
        return self._run_cmd(cnpj, extra_args=["--apenas-processar"])

    def delete_cnpj_all(self, cnpj: str) -> bool:
        """Exclui toda a pasta do CNPJ em dados/CNPJ."""
        cnpj = self.sanitize_cnpj(cnpj)
        pasta = self.output_root / cnpj
        if pasta.exists() and pasta.is_dir():
            shutil.rmtree(pasta)
            return True
        return False

    def delete_processed_data(self, cnpj: str) -> bool:
        """Exclui apenas a pasta 'analises' do CNPJ."""
        cnpj = self.sanitize_cnpj(cnpj)
        pasta_analises = self.output_root / cnpj / "analises"
        if pasta_analises.exists() and pasta_analises.is_dir():
            shutil.rmtree(pasta_analises)
            return True
        return False
