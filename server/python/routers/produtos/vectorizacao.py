import re
import traceback
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from core.models import *
from core.produto_batch_lote import *
from core.produto_runtime import *
from core.utils import validar_cnpj
from ._utils import *

logger = logging.getLogger("sefin_audit_python")
router = APIRouter()

@router.get("/produtos/vectorizacao-status")
async def get_vectorizacao_status(cnpj: str = Query(...)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"
        current_base_hash = compute_file_sha1(agregados_path) if agregados_path.exists() else None
        faiss_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.json")
        light_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.json")
        return {
            "success": True,
            "status": obter_status_vectorizacao(),
            "current_base_hash": current_base_hash,
            "caches": {
                "faiss": {
                    **faiss_cache,
                    "stale": bool(current_base_hash and faiss_cache and faiss_cache.get("input_base_hash") != current_base_hash),
                },
                "light": {
                    **light_cache,
                    "stale": bool(current_base_hash and light_cache and light_cache.get("input_base_hash") != current_base_hash),
                },
            },
        }
    except Exception as e:
        logger.error("[get_vectorizacao_status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/produtos/vectorizacao-clear-cache")
async def clear_vectorizacao_cache(cnpj: str = Query(...), metodo: str = Query("all")):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    metodo_norm = str(metodo or "all").strip().lower()
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if metodo_norm not in {"faiss", "light", "all"}:
        raise HTTPException(status_code=400, detail="Metodo invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        targets: list[Path] = []
        if metodo_norm in {"faiss", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.json",
                ]
            )
        if metodo_norm in {"light", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.json",
                ]
            )

        removed: list[str] = []
        for target in targets:
            if target.exists():
                target.unlink()
                removed.append(str(target))

        return {
            "success": True,
            "message": "Cache vetorizado removido.",
            "removed": removed,
        }
    except Exception as e:
        logger.error("[clear_vectorizacao_cache] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
