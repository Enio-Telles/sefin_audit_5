import re
import traceback
import logging
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

def _carregar_detalhes_codigo(dir_analises: Path, cnpj_limpo: str, codigo: str) -> list[dict[str, Any]]:
    detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
    agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

    if not detalhes_path.exists():
        return []

    lf = pl.scan_parquet(str(detalhes_path))

    if str(codigo).startswith("ID_"):
        if agregados_path.exists():
            df_agregado = pl.read_parquet(str(agregados_path))
            row = df_agregado.filter(pl.col("chave_produto") == codigo)
            if not row.is_empty():
                descr = row["descricao"][0]
                return lf.filter(pl.col("descricao") == descr).collect().to_dicts()

    if str(codigo).upper().endswith("_AGR"):
        codigo_real = codigo.rsplit("_", 1)[0]
        codigo_norm = codigo_real.lstrip("0") or "0"
        return lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect().to_dicts()

    if "_" in codigo:
        parts = codigo.rsplit("_", 1)
        codigo_real = parts[0]
        tipo_item_val = parts[1]
        codigo_norm = codigo_real.lstrip("0") or "0"
        return (
            lf.filter((pl.col("codigo").str.replace("^0+", "") == codigo_norm) & (pl.col("tipo_item") == tipo_item_val))
            .collect()
            .to_dicts()
        )

    codigo_norm = codigo.lstrip("0") or "0"
    return lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect().to_dicts()



@router.get("/produtos/detalhes-codigo")
async def get_detalhes_produto(cnpj: str = Query(...), codigo: str = Query(...)):
    """Retorna as linhas originais (fontes) associadas a um codigo master ou chave_produto."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        return {"success": True, "codigo": codigo, "itens": _carregar_detalhes_codigo(dir_analises, cnpj_limpo, str(codigo))}
    except Exception as e:
        logger.error("[get_detalhes_produto] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/detalhes-multi-codigo")
async def get_detalhes_multi_produtos(req: ResolverManualMultiDetalhesRequest):
    """Retorna as linhas originais (fontes) associadas a multiplos codigos master."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not detalhes_path.exists():
            return {"success": True, "data": []}

        lf = pl.scan_parquet(str(detalhes_path))
        df_agregado = pl.read_parquet(str(agregados_path)) if agregados_path.exists() else None
        filters = []
        for c in req.codigos:
            if str(c).startswith("ID_") and df_agregado is not None:
                row = df_agregado.filter(pl.col("chave_produto") == c)
                if not row.is_empty():
                    descr = row["descricao"][0]
                    filters.append(pl.col("descricao") == descr)
                    continue

            if str(c).upper().endswith("_AGR"):
                cod_real_raw = c.rsplit("_", 1)[0]
                cod_real = cod_real_raw.lstrip("0") if cod_real_raw.lstrip("0") else "0"
                filters.append(pl.col("codigo").str.replace("^0+", "") == cod_real)
            elif "_" in c:
                parts = c.rsplit("_", 1)
                cod_real = parts[0].lstrip("0") if parts[0].lstrip("0") else "0"
                tipo_val = parts[1]
                filters.append(
                    (pl.col("codigo").str.replace("^0+", "") == cod_real)
                    & (pl.col("tipo_item") == tipo_val)
                )
            else:
                cod_norm = c.lstrip("0") if c.lstrip("0") else "0"
                filters.append(pl.col("codigo").str.replace("^0+", "") == cod_norm)

        if filters:
            final_filter = filters[0]
            for f in filters[1:]:
                final_filter = final_filter | f
            df = lf.filter(final_filter).collect()
        else:
            df = pl.DataFrame(schema=lf.collect_schema())

        return {"success": True, "itens": df.to_dicts()}
    except Exception as e:
        logger.error("[get_detalhes_multi_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
