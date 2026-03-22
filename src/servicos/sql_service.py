"""
Serviço para leitura, parse e extração de parâmetros de arquivos SQL refatorado.
"""

import re
from pathlib import Path
from typing import Any
from dataclasses import dataclass

from src.config import SQL_DIR
from src.extracao.extrair_parametros import extrair_parametros_sql

WIDGET_DATE = "date"
WIDGET_TEXT = "text"

@dataclass
class ParamInfo:
    name: str
    widget_type: str = WIDGET_TEXT
    placeholder: str = ""

@dataclass
class SqlFileInfo:
    path: Path
    display_name: str
    source_dir: str

class SqlService:
    def __init__(self, primary_dir: Path = SQL_DIR) -> None:
        self.primary_dir = primary_dir

    def list_sql_files(self) -> list[SqlFileInfo]:
        if not self.primary_dir.exists():
            return []
        files = []
        for p in sorted(self.primary_dir.rglob("*.sql"), key=lambda x: x.name.lower()):
            files.append(SqlFileInfo(path=p, display_name=p.stem, source_dir="sql"))
        return files

    @staticmethod
    def read_sql(path: Path) -> str:
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                return path.read_text(encoding=enc).strip().rstrip(";")
            except UnicodeDecodeError:
                continue
        raise RuntimeError(f"Erro ao ler SQL: {path}")

    @staticmethod
    def extract_params(sql: str) -> list[ParamInfo]:
        names = extrair_parametros_sql(sql)
        params = []
        for name in sorted(list(names)):
            low = name.lower()
            wtype = WIDGET_DATE if ("data" in low or "dt_" in low) else WIDGET_TEXT
            params.append(ParamInfo(name=name, widget_type=wtype))
        return params

    @staticmethod
    def build_binds(sql: str, values: dict[str, Any]) -> dict[str, Any]:
        params = extrair_parametros_sql(sql)
        provided = {k.lower(): v for k, v in values.items()}
        return {p: provided.get(p.lower()) for p in params}
