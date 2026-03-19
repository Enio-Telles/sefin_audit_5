"""
Worker QThread para execução assíncrona de consultas Oracle.

Evita congelamento da interface durante consultas demoradas.
Emite sinais de progresso, sucesso e falha.
"""
from __future__ import annotations

import os
import re
import socket
from pathlib import Path
from typing import Any

import polars as pl
from PySide6.QtCore import QThread, Signal

# ---------------------------------------------------------------------------
# Reutiliza funções do pipeline existente quando possível
# ---------------------------------------------------------------------------
try:
    from pipeline_oracle_parquet import conectar_oracle
except ImportError:
    conectar_oracle = None  # type: ignore[assignment]


def _conectar_oracle_fallback():
    """Conexão Oracle standalone caso o import falhe."""
    try:
        import oracledb
    except ImportError as exc:
        raise RuntimeError("O pacote 'oracledb' não está instalado.") from exc

    from dotenv import load_dotenv

    root = Path(__file__).resolve().parents[2]
    for candidate in [Path.cwd() / ".env", root / ".env"]:
        if candidate.exists():
            load_dotenv(candidate, override=False, encoding="latin-1")
            break

    host = os.getenv("ORACLE_HOST", "exa01-scan.sefin.ro.gov.br").strip()
    porta = int(os.getenv("ORACLE_PORT", "1521").strip())
    servico = os.getenv("ORACLE_SERVICE", "sefindw").strip()
    usuario = os.getenv("DB_USER", "").strip()
    senha = os.getenv("DB_PASSWORD", "").strip()

    if not usuario or not senha:
        raise RuntimeError("Credenciais Oracle não encontradas. Preencha DB_USER e DB_PASSWORD no .env")

    dsn = oracledb.makedsn(host, porta, service_name=servico)
    conn = oracledb.connect(user=usuario, password=senha, dsn=dsn)
    with conn.cursor() as cursor:
        cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
    return conn


class QueryWorker(QThread):
    """
    Executa uma consulta SQL no Oracle em thread separada.

    Signals:
        progress(str): mensagens de status intermediárias
        finished_ok(pl.DataFrame): DataFrame Polars com os resultados
        failed(str): mensagem de erro
    """

    progress = Signal(str)
    finished_ok = Signal(object)  # pl.DataFrame
    failed = Signal(str)

    def __init__(
        self,
        sql: str,
        binds: dict[str, Any],
        fetch_size: int = 50_000,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.sql = sql
        self.binds = binds
        self.fetch_size = fetch_size

    def run(self) -> None:
        conn = None
        try:
            self.progress.emit("Conectando ao Oracle...")
            if conectar_oracle is not None:
                conn = conectar_oracle()
            else:
                conn = _conectar_oracle_fallback()

            self.progress.emit("Executando consulta...")
            with conn.cursor() as cursor:
                cursor.arraysize = self.fetch_size
                cursor.prefetchrows = self.fetch_size
                cursor.execute(self.sql, self.binds)

                columns = [desc[0] for desc in cursor.description]
                all_rows: list[tuple] = []

                batch_num = 0
                while True:
                    rows = cursor.fetchmany(self.fetch_size)
                    if not rows:
                        break
                    all_rows.extend(rows)
                    batch_num += 1
                    self.progress.emit(f"Lidas {len(all_rows):,} linhas...")

            if not all_rows:
                self.progress.emit("Consulta retornou 0 linhas.")
                df = pl.DataFrame({col: [] for col in columns})
            else:
                # Converter para Polars via dicts (mais seguro com tipos mistos)
                records = [dict(zip(columns, row)) for row in all_rows]
                df = pl.DataFrame(records, infer_schema_length=min(len(records), 1000))

            self.progress.emit(f"Concluído: {df.height:,} linhas, {df.width} colunas.")
            self.finished_ok.emit(df)

        except Exception as exc:
            self.failed.emit(str(exc))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
