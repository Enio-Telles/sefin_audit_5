import difflib
from functools import lru_cache
import re
import os
import sys
import traceback
import logging
import hashlib
from datetime import UTC, datetime
import polars as pl
from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query
from core.models import *
from core.produto_batch_lote import *
from core.produto_runtime import *
from core.utils import validar_cnpj
from ._utils import *

logger = logging.getLogger("sefin_audit_python")
router = APIRouter()

@router.get("/produtos/codigos-multidescricao")
async def get_produtos_codigos_multidescricao(
    cnpj: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(50),
    sort_column: str | None = Query(None),
    sort_direction: str = Query("desc"),
    show_verified: bool = Query(False),
):
    """Retorna os codigos que aparecem com multiplas descricoes para o CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        page_norm = _normalize_page(page)
        page_size_norm = _normalize_page_size(page_size, default=50, max_size=200)
        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"

        if not path_codigos.exists():
            return {
                "success": True,
                "file_path": str(path_codigos),
                "data": [],
                "page": page_norm,
                "page_size": page_size_norm,
                "total": 0,
                "total_pages": 1,
                "summary": {"total_codigos": 0, "total_descricoes": 0, "total_grupos": 0},
            }

        df = pl.read_parquet(str(path_codigos))
        df = df.with_columns(pl.lit("").alias("status_analise"))

        if not show_verified:
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            if status_path.exists():
                df_status = pl.read_parquet(str(status_path))
                df_codigo_status = (
                    df_status.filter(pl.col("tipo_ref") == "POR_CODIGO")
                    .select(
                        [
                            pl.col("ref_id").cast(pl.Utf8).alias("codigo"),
                            pl.col("status_analise").cast(pl.Utf8),
                        ]
                    )
                    .unique(subset=["codigo"], keep="last")
                )
                df = df.join(df_codigo_status, on="codigo", how="left", suffix="_joined").with_columns(
                    pl.coalesce([pl.col("status_analise_joined"), pl.col("status_analise")]).alias("status_analise")
                ).drop("status_analise_joined", strict=False)
                verified_codes = set(
                    df_status.filter(
                        (pl.col("tipo_ref") == "POR_CODIGO") & (pl.col("status_analise") == "VERIFICADO_SEM_ACAO")
                    ).get_column("ref_id").cast(pl.Utf8).to_list()
                )
                if verified_codes:
                    df = df.filter(~pl.col("codigo").is_in(sorted(verified_codes)))
        else:
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            if status_path.exists():
                df_status = pl.read_parquet(str(status_path))
                df_codigo_status = (
                    df_status.filter(pl.col("tipo_ref") == "POR_CODIGO")
                    .select(
                        [
                            pl.col("ref_id").cast(pl.Utf8).alias("codigo"),
                            pl.col("status_analise").cast(pl.Utf8),
                        ]
                    )
                    .unique(subset=["codigo"], keep="last")
                )
                df = df.join(df_codigo_status, on="codigo", how="left", suffix="_joined").with_columns(
                    pl.coalesce([pl.col("status_analise_joined"), pl.col("status_analise")]).alias("status_analise")
                ).drop("status_analise_joined", strict=False)

        summary = {
            "total_codigos": int(df.height),
            "total_descricoes": int(df.select(pl.sum("qtd_descricoes")).item() or 0) if "qtd_descricoes" in df.columns else 0,
            "total_grupos": int(df.select(pl.sum("qtd_grupos_descricao_afetados")).item() or 0)
            if "qtd_grupos_descricao_afetados" in df.columns
            else 0,
        }

        sort_col = str(sort_column or "").strip()
        sort_desc = str(sort_direction or "desc").lower() != "asc"
        sortable = set(df.columns)
        if sort_col in sortable:
            df = df.sort(sort_col, descending=sort_desc, nulls_last=True)
        elif "qtd_descricoes" in sortable:
            df = df.sort("qtd_descricoes", descending=True, nulls_last=True)

        paged_df, total, total_pages = _paginate_frame(df, page_norm, page_size_norm)

        return {
            "success": True,
            "file_path": str(path_codigos),
            "data": paged_df.to_dicts(),
            "page": min(page_norm, total_pages),
            "page_size": page_size_norm,
            "total": total,
            "total_pages": total_pages,
            "summary": summary,
        }
    except Exception as e:
        logger.error("[get_produtos_codigos_multidescricao] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


def _carregar_codigo_multidescricao_resumo(dir_analises: Path, cnpj_limpo: str, codigo_limpo: str) -> dict[str, Any]:
    path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"
    path_indexados = dir_analises / f"produtos_indexados_{cnpj_limpo}.parquet"

    resumo: dict[str, Any] = {}
    grupos_descricao: list[dict[str, Any]] = []
    opcoes_consenso: dict[str, list[dict[str, Any]]] = {"descricao": [], "ncm": [], "cest": [], "gtin": []}

    if path_codigos.exists():
        df_codigos = pl.read_parquet(str(path_codigos)).with_columns(pl.col("codigo").cast(pl.Utf8))
        row = df_codigos.filter(pl.col("codigo") == codigo_limpo)
        if not row.is_empty():
            resumo = row.to_dicts()[0]

    if path_indexados.exists():
        df_indexados = (
            pl.read_parquet(str(path_indexados))
            .with_columns(
                [
                    pl.col("codigo").cast(pl.Utf8),
                    pl.col("descricao").cast(pl.Utf8),
                    pl.col("descr_compl").cast(pl.Utf8),
                    pl.col("tipo_item").cast(pl.Utf8),
                    pl.col("ncm").cast(pl.Utf8),
                    pl.col("cest").cast(pl.Utf8),
                    pl.col("gtin").cast(pl.Utf8),
                    pl.col("lista_unidades").cast(pl.Utf8),
                    pl.col("lista_fontes").cast(pl.Utf8),
                ]
            )
            .filter(pl.col("codigo") == codigo_limpo)
        )

        if not df_indexados.is_empty():
            grupos_descricao = (
                df_indexados.group_by("descricao")
                .agg(
                    [
                        pl.sum("qtd_linhas").cast(pl.Int64).alias("qtd_linhas"),
                        pl.len().cast(pl.Int64).alias("qtd_combinacoes"),
                        pl.col("chave_produto").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_chave"),
                        pl.col("descr_compl").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_descr_compl"),
                        pl.col("tipo_item").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_tipo_item"),
                        pl.col("ncm").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_ncm"),
                        pl.col("cest").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_cest"),
                        pl.col("gtin").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_gtin"),
                        pl.col("lista_unidades").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_unidades"),
                        pl.col("lista_fontes").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_fontes"),
                    ]
                )
                .with_columns(
                    [
                        pl.col("__lista_chave").list.join(", ").alias("lista_chave_produto"),
                        pl.col("__lista_descr_compl").list.join(" | ").alias("lista_descr_compl"),
                        pl.col("__lista_tipo_item").list.join(", ").alias("lista_tipo_item"),
                        pl.col("__lista_ncm").list.join(", ").alias("lista_ncm"),
                        pl.col("__lista_cest").list.join(", ").alias("lista_cest"),
                        pl.col("__lista_gtin").list.join(", ").alias("lista_gtin"),
                        pl.col("__lista_unidades").list.join(" | ").alias("lista_unidades"),
                        pl.col("__lista_fontes").list.join(" | ").alias("lista_fontes"),
                    ]
                )
                .drop(
                    [
                        "__lista_chave",
                        "__lista_descr_compl",
                        "__lista_tipo_item",
                        "__lista_ncm",
                        "__lista_cest",
                        "__lista_gtin",
                        "__lista_unidades",
                        "__lista_fontes",
                    ]
                )
                .sort(["qtd_linhas", "descricao"], descending=[True, False])
                .to_dicts()
            )

            for field_name in ["descricao", "ncm", "cest", "gtin"]:
                df_opcoes = (
                    df_indexados.group_by(field_name)
                    .agg(pl.sum("qtd_linhas").cast(pl.Int64).alias("qtd_linhas"))
                    .rename({field_name: "valor"})
                    .with_columns(pl.col("valor").cast(pl.Utf8).fill_null(""))
                    .sort(["qtd_linhas", "valor"], descending=[True, False])
                )
                opcoes_consenso[field_name] = df_opcoes.to_dicts()

    return {
        "codigo": codigo_limpo,
        "resumo": resumo,
        "grupos_descricao": grupos_descricao,
        "opcoes_consenso": opcoes_consenso,
    }



@router.get("/produtos/codigo-multidescricao-resumo")
async def get_codigo_multidescricao_resumo(cnpj: str = Query(...), codigo: str = Query(...)):
    """Retorna um resumo indexado do codigo multidescricao para uso direto nos popups."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    codigo_limpo = str(codigo or "").strip()
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if not codigo_limpo:
        raise HTTPException(status_code=400, detail="Codigo invalido")

    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        payload = _carregar_codigo_multidescricao_resumo(dir_analises, cnpj_limpo, codigo_limpo)
        return {"success": True, **payload}
    except Exception as e:
        logger.error("[get_codigo_multidescricao_resumo] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
