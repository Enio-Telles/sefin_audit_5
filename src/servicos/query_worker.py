"""
Worker QThread para execução assíncrona de consultas Oracle refatorado.
"""
from __future__ import annotations

import polars as pl
from PySide6.QtCore import QThread, Signal
from typing import Any

from src.extracao.conexao import conectar as conectar_oracle

class QueryWorker(QThread):
    progress = Signal(str)
    finished_ok = Signal(object)
    failed = Signal(str)

    def __init__(self, sql: str, binds: dict[str, Any], fetch_size: int = 50_000, parent=None) -> None:
        super().__init__(parent)
        self.sql = sql
        self.binds = binds
        self.fetch_size = fetch_size

    def run(self) -> None:
        conn = None
        try:
            self.progress.emit("Conectando ao Oracle...")
            conn = conectar_oracle()
            if conn is None:
                raise RuntimeError("Falha ao conectar ao Oracle.")

            self.progress.emit("Executando consulta...")
            with conn.cursor() as cursor:
                cursor.arraysize = self.fetch_size
                cursor.execute(self.sql, self.binds)
                columns = [desc[0].lower() for desc in cursor.description]
                all_rows = cursor.fetchall()

            df = pl.DataFrame(all_rows, schema=columns, orient="row")
            self.progress.emit(f"Concluído: {df.height:,} linhas.")
            self.finished_ok.emit(df)

        except Exception as exc:
            self.failed.emit(str(exc))
        finally:
            if conn:
                conn.close()
