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

@router.get("/produtos/runtime-status")
async def get_runtime_produtos_status(cnpj: str = Query(...)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "runtime": obter_runtime_produtos_status(dir_analises, cnpj_limpo),
        }
    except Exception as e:
        logger.error("[get_runtime_produtos_status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/rebuild-runtime")
async def rebuild_runtime_produtos(req: ProdutoUnidRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        df = _reprocessar_produtos(dir_analises, cnpj_limpo)
        runtime = obter_runtime_produtos_status(dir_analises, cnpj_limpo)
        return {
            "success": True,
            "message": "Pipeline de produtos reconstruido com sucesso.",
            "rows": int(df.height),
            "runtime": runtime,
        }
    except Exception as e:
        logger.error("[rebuild_runtime_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/produtos/status-analise")
async def get_status_analise_produtos(cnpj: str = Query(...), include_data: bool = Query(True)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        df_status = pl.read_parquet(str(status_path)) if status_path.exists() else pl.DataFrame(schema={c: pl.Utf8 for c in _STATUS_ANALISE_COLUMNS})
        return {
            "success": True,
            "file_path": str(status_path),
            "data": df_status.to_dicts() if include_data else [],
            "resumo": _resumir_status_analise(dir_analises, cnpj_limpo, df_status),
        }
    except Exception as e:
        logger.error("[get_status_analise_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
