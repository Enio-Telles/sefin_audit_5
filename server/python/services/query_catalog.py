from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

from core.utils import extrair_parametros_sql, ler_sql


@dataclass(slots=True)
class QueryCatalogItem:
    id: str
    nome: str
    caminho: str
    descricao: str
    parametros: list[str]
    categoria: str
    origem: str = "sql_file"

    def to_dict(self) -> dict:
        return asdict(self)


class QueryCatalogService:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)

    def _infer_categoria(self, path: Path) -> str:
        rel_parts = path.relative_to(self.base_dir).parts if path.is_relative_to(self.base_dir) else path.parts
        if len(rel_parts) >= 2:
            return rel_parts[0]
        return "geral"

    def _build_item(self, path: Path) -> QueryCatalogItem:
        sql = ler_sql(path)
        params = sorted(
            p for p in extrair_parametros_sql(sql)
            if p.lower() not in ("cnpj", "cnpj_raiz")
        )
        return QueryCatalogItem(
            id=str(path.resolve()),
            nome=path.stem,
            caminho=str(path.resolve()),
            descricao=f"Consulta SQL: {path.name}",
            parametros=params,
            categoria=self._infer_categoria(path),
        )

    def list_queries(self) -> list[dict]:
        if not self.base_dir.exists():
            return []

        items: list[QueryCatalogItem] = []
        for sql_file in sorted(self.base_dir.rglob("*.sql")):
            if not sql_file.is_file():
                continue
            try:
                items.append(self._build_item(sql_file))
            except Exception:
                items.append(
                    QueryCatalogItem(
                        id=str(sql_file.resolve()),
                        nome=sql_file.stem,
                        caminho=str(sql_file.resolve()),
                        descricao=f"Consulta SQL: {sql_file.name}",
                        parametros=[],
                        categoria=self._infer_categoria(sql_file),
                    )
                )
        return [item.to_dict() for item in items]

    def list_auxiliary_queries(self, auxiliary_dir: str | Path) -> list[dict]:
        aux_dir = Path(auxiliary_dir)
        if not aux_dir.exists():
            return []
        items: list[dict] = []
        for sql_file in sorted(aux_dir.glob("*.sql")):
            try:
                items.append(self._build_item(sql_file).to_dict())
            except Exception:
                items.append(
                    QueryCatalogItem(
                        id=str(sql_file.resolve()),
                        nome=sql_file.stem,
                        caminho=str(sql_file.resolve()),
                        descricao=f"Tabela auxiliar: {sql_file.name}",
                        parametros=[],
                        categoria="auxiliar",
                    ).to_dict()
                )
        return items

    def summarize(self) -> dict:
        queries = self.list_queries()
        categorias: dict[str, int] = {}
        for item in queries:
            categorias[item["categoria"]] = categorias.get(item["categoria"], 0) + 1
        return {
            "base_dir": str(self.base_dir.resolve()),
            "total_consultas": len(queries),
            "categorias": categorias,
            "consultas": queries,
        }
