from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from fiscal_app.config import CONSULTAS_ROOT, DEFAULT_PAGE_SIZE


@dataclass
class FilterCondition:
    column: str
    operator: str
    value: str = ""


@dataclass
class PageResult:
    total_rows: int
    df_all_columns: pl.DataFrame
    df_visible: pl.DataFrame
    columns: list[str]
    visible_columns: list[str]


class ParquetService:
    def __init__(self, root: Path = CONSULTAS_ROOT) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_cnpjs(self) -> list[str]:
        if not self.root.exists():
            return []
        rows = [p.name for p in self.root.iterdir() if p.is_dir() and (p.name.isdigit() and len(p.name) >= 11)]
        return sorted(rows)

    def cnpj_dir(self, cnpj: str) -> Path:
        return self.root / cnpj

    def list_parquet_files(self, cnpj: str) -> list[Path]:
        base = self.cnpj_dir(cnpj)
        if not base.exists():
            return []
        
        # New structure
        brutos = base / "arquivos_parquet"
        analises = base / "analises" / "produtos"
        # Old structure fallback
        old_prod = base / "produtos"
        
        files = []
        if brutos.exists():
            files.extend(brutos.glob("*.parquet"))
        if analises.exists():
            files.extend(analises.glob("*.parquet"))
        if old_prod.exists():
            files.extend(old_prod.glob("*.parquet"))
        
        # Also check root of CNPJ folder for any loose parquets
        files.extend(base.glob("*.parquet"))
        
        return sorted(list(set(files)), key=lambda p: (str(p.parent), p.name))

    def get_schema(self, parquet_path: Path) -> list[str]:
        return list(pl.scan_parquet(parquet_path).collect_schema().names())

    def _build_expr(self, cond: FilterCondition) -> pl.Expr:
        col = pl.col(cond.column)
        value = cond.value or ""
        op = cond.operator

        if op == "contém":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(value.lower(), literal=True)
        if op == "igual":
            return col.cast(pl.Utf8, strict=False).fill_null("") == value
        if op == "começa com":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.starts_with(value.lower())
        if op == "termina com":
            return col.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.ends_with(value.lower())
        if op == "é nulo":
            return col.is_null() | (col.cast(pl.Utf8, strict=False).fill_null("") == "")
        if op == "não é nulo":
            return ~(col.is_null() | (col.cast(pl.Utf8, strict=False).fill_null("") == ""))

        numeric_col = col.cast(pl.Float64, strict=False)
        try:
            numeric_value = float(value.replace(",", "."))
        except Exception:
            numeric_value = None

        if op in {">", ">=", "<", "<="} and numeric_value is not None:
            mapping = {
                ">": numeric_col > numeric_value,
                ">=": numeric_col >= numeric_value,
                "<": numeric_col < numeric_value,
                "<=": numeric_col <= numeric_value,
            }
            return mapping[op]

        return col.cast(pl.Utf8, strict=False).fill_null("") == value

    def apply_filters(self, lf: pl.LazyFrame, conditions: Iterable[FilterCondition]) -> pl.LazyFrame:
        filtered = lf
        for cond in conditions:
            if not cond.column:
                continue
            if cond.operator not in {"é nulo", "não é nulo"} and cond.value == "":
                continue
            filtered = filtered.filter(self._build_expr(cond))
        return filtered

    def build_lazyframe(self, parquet_path: Path, conditions: Iterable[FilterCondition] | None = None) -> pl.LazyFrame:
        lf = pl.scan_parquet(parquet_path)
        if conditions:
            lf = self.apply_filters(lf, conditions)
        return lf

    def get_page(
        self,
        parquet_path: Path,
        conditions: list[FilterCondition],
        visible_columns: list[str] | None,
        page: int,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> PageResult:
        page = max(page, 1)
        lf_all = self.build_lazyframe(parquet_path, conditions)
        total_rows = int(lf_all.select(pl.len().alias("n")).collect().item())
        all_columns = self.get_schema(parquet_path)
        if not visible_columns:
            visible_columns = all_columns[:]
        offset = (page - 1) * page_size
        df_all = lf_all.slice(offset, page_size).collect()
        df_visible = df_all.select([c for c in visible_columns if c in df_all.columns])
        return PageResult(
            total_rows=total_rows,
            df_all_columns=df_all,
            df_visible=df_visible,
            columns=all_columns,
            visible_columns=visible_columns,
        )

    def load_dataset(self, parquet_path: Path, conditions: list[FilterCondition] | None = None, columns: list[str] | None = None) -> pl.DataFrame:
        lf = self.build_lazyframe(parquet_path, conditions or [])
        if columns:
            lf = lf.select(columns)
        return lf.collect()

    def save_dataset(self, parquet_path: Path, df: pl.DataFrame) -> None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path, compression="snappy")
