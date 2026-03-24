import re
import traceback
import logging
import polars as pl
from fastapi import APIRouter, HTTPException, Query
from core.models import *
from core.produto_batch_lote import *
from core.produto_runtime import *
from core.utils import validar_cnpj
from ._utils import *

logger = logging.getLogger("sefin_audit_python")
router = APIRouter()

@router.get("/produtos/revisao-manual")
async def get_produtos_revisao_manual(cnpj: str = Query(...)):
    """Retorna os produtos que requerem revisao manual para o CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not agregados_path.exists():
            return {"success": True, "data": []}

        df = pl.scan_parquet(str(agregados_path)).filter(pl.col("requer_revisao_manual") == True).collect()

        return {"success": True, "data": df.to_dicts()}
    except Exception as e:
        logger.error("[get_produtos_revisao_manual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/revisao-final")
async def get_produtos_revisao_final(cnpj: str = Query(...)):
    """Retorna metadados da tabela final de produtos ja desagregada para a tela unica de revisao."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not agregados_path.exists():
            return {
                "success": True,
                "available": False,
                "file_path": str(agregados_path),
                "summary": {
                    "total_grupos": 0,
                    "grupos_revisao_manual": 0,
                    "grupos_com_gtin": 0,
                    "grupos_com_cest": 0,
                },
            }

        df = pl.read_parquet(str(agregados_path))
        expected_columns = {
            "lista_codigos",
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "codigo_padrao",
            "lista_descr_compl",
        }
        if not expected_columns.issubset(set(df.columns)):
            logger.info(
                "[get_produtos_revisao_final] parquet com schema antigo detectado para %s; regenerando tabela final.",
                cnpj_limpo,
            )
            df = unificar_produtos_unidades(cnpj_limpo, projeto_dir=_PROJETO_DIR)

        return {
            "success": True,
            "available": True,
            "file_path": str(agregados_path),
            "summary": {
                "total_grupos": int(df.height),
                "grupos_revisao_manual": int(df.filter(pl.col("requer_revisao_manual") == True).height)
                if "requer_revisao_manual" in df.columns
                else 0,
                "grupos_com_gtin": int(df.filter(pl.col("gtin_consenso").cast(pl.Utf8) != "").height)
                if "gtin_consenso" in df.columns
                else 0,
                "grupos_com_cest": int(df.filter(pl.col("cest_consenso").cast(pl.Utf8) != "").height)
                if "cest_consenso" in df.columns
                else 0,
            },
        }
    except Exception as e:
        logger.error("[get_produtos_revisao_final] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/produtos/revisao-manual/submit")
async def submit_revisao_manual(req: RevisaoManualSubmitRequest):
    """Grava as decisoes de revisao manual e roda o script de unificacao de produtos."""
    from core.models import RevisaoManualSubmitRequest

    if not isinstance(req, RevisaoManualSubmitRequest):
        pass

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var('obter_diretorios_cnpj')
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        decisoes = [item.dict() for item in req.decisoes]
        df_novo = pl.DataFrame(decisoes)
        _merge_manual_map(mapa_path, df_novo, default_acao="AGREGAR")

        logger.info("Revisoes gravadas com sucesso no arquivo %s", mapa_path.name)
        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {"success": True, "message": "Revisoes aplicadas com sucesso."}
    except Exception as e:
        logger.error("[submit_revisao_manual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
