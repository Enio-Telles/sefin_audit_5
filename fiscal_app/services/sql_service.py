"""
Serviço para leitura, parse e extração de parâmetros de arquivos SQL.

Responsabilidades:
- Listar arquivos .sql de múltiplos diretórios
- Ler conteúdo SQL com fallback de encoding
- Extrair bind variables Oracle (:param) e inferir tipo de widget
- Construir dicionário de binds para execução
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fiscal_app.config import SQL_DIR

# ---------------------------------------------------------------------------
# Importação da função de extração de parâmetros de c:\funcoes
# ---------------------------------------------------------------------------
_FUNCOES_AUX = Path(r"c:\funcoes\funcoes_auxiliares")
if str(_FUNCOES_AUX) not in sys.path:
    sys.path.insert(0, str(_FUNCOES_AUX))

try:
    from extrair_parametros import extrair_parametros_sql as _extrair_raw
except ImportError:
    # Fallback caso o import falhe (ex.: ambiente sem c:\funcoes)
    def _extrair_raw(sql: str) -> set[str]:  # type: ignore[misc]
        return set(re.findall(r":(\w+)", sql))


# ---------------------------------------------------------------------------
# Tipos auxiliares
# ---------------------------------------------------------------------------
WIDGET_DATE = "date"
WIDGET_TEXT = "text"


@dataclass
class ParamInfo:
    """Informações sobre um parâmetro SQL detectado."""
    name: str
    widget_type: str = WIDGET_TEXT  # "date" | "text"
    placeholder: str = ""


@dataclass
class SqlFileInfo:
    """Metadados de um arquivo SQL."""
    path: Path
    display_name: str
    source_dir: str  # identificação da origem


# ---------------------------------------------------------------------------
# Diretórios extras de SQL (c:\funcoes\consultas_fonte)
# ---------------------------------------------------------------------------
_EXTRA_SQL_DIRS: list[Path] = [Path(r"c:\funcoes\consultas_fonte")]


# ---------------------------------------------------------------------------
# Serviço
# ---------------------------------------------------------------------------
class SqlService:

    def __init__(
        self,
        primary_dir: Path = SQL_DIR,
        extra_dirs: list[Path] | None = None,
    ) -> None:
        self.primary_dir = primary_dir
        self.extra_dirs = extra_dirs if extra_dirs is not None else _EXTRA_SQL_DIRS

    # ------------------------------------------------------------------
    # Listagem
    # ------------------------------------------------------------------
    def list_sql_files(self) -> list[SqlFileInfo]:
        """Retorna todos os arquivos .sql encontrados nos diretórios configurados."""
        result: list[SqlFileInfo] = []
        seen: set[str] = set()

        for sql_dir, label in self._iter_dirs():
            if not sql_dir.exists():
                continue
            for p in sorted(sql_dir.rglob("*.sql"), key=lambda x: x.name.lower()):
                key = p.name.lower()
                if key in seen:
                    continue
                seen.add(key)
                result.append(SqlFileInfo(
                    path=p,
                    display_name=p.stem,
                    source_dir=label,
                ))
        return result

    def _iter_dirs(self):
        yield self.primary_dir, "projeto"
        for d in self.extra_dirs:
            yield d, d.name

    # ------------------------------------------------------------------
    # Leitura
    # ------------------------------------------------------------------
    @staticmethod
    def read_sql(path: Path) -> str:
        """Lê arquivo SQL com fallback de encoding."""
        for enc in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
            try:
                return path.read_text(encoding=enc).strip().rstrip(";")
            except UnicodeDecodeError:
                continue
        raise RuntimeError(f"Não foi possível ler o SQL: {path}")

    # ------------------------------------------------------------------
    # Extração de parâmetros
    # ------------------------------------------------------------------
    @staticmethod
    def extract_params(sql: str) -> list[ParamInfo]:
        """
        Extrai parâmetros Oracle :bind do texto SQL.

        Usa ``extrair_parametros_sql`` de c:\\funcoes quando disponível,
        com fallback para regex simples.  Retorna lista sem duplicatas,
        na ordem de aparição no SQL.
        """
        # Conjunto de nomes (sem dois-pontos)
        raw_names = _extrair_raw(sql)

        # Manter a ordem de aparição no SQL (o set perde a ordem)
        all_matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        ordered: list[str] = []
        for name in all_matches:
            low = name.lower()
            if low not in seen:
                seen.add(low)
                ordered.append(name)

        params: list[ParamInfo] = []
        for name in ordered:
            wtype = SqlService._infer_widget_type(name)
            placeholder = SqlService._infer_placeholder(name)
            params.append(ParamInfo(name=name, widget_type=wtype, placeholder=placeholder))
        return params

    @staticmethod
    def _infer_widget_type(name: str) -> str:
        low = name.lower()
        if low.startswith(("data_", "dt_", "date_")) or low in ("data_limite_processamento",):
            return WIDGET_DATE
        return WIDGET_TEXT

    @staticmethod
    def _infer_placeholder(name: str) -> str:
        low = name.lower()
        if "cnpj" in low:
            return "Somente dígitos"
        if low.startswith(("data_", "dt_")):
            return "DD/MM/AAAA"
        return ""

    # ------------------------------------------------------------------
    # Construção de binds
    # ------------------------------------------------------------------
    @staticmethod
    def build_binds(sql: str, values: dict[str, Any]) -> dict[str, Any]:
        """Constrói dict de binds para execução, mapeando nomes encontrados no SQL."""
        provided = {k.lower(): v for k, v in values.items()}
        binds: dict[str, Any] = {}
        matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        for name in matches:
            low = name.lower()
            if low not in seen:
                seen.add(low)
                binds[name] = provided.get(low)
        return binds
