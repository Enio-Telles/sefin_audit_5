import re
import sys
import traceback
import logging
import polars as pl
from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

from core.models import AnaliseFaturamentoRequest, AuditPipelineRequest
from core.utils import validar_cnpj

# Novos services extraídos
from core.audit_metadata_service import (
    processar_fatores_excel,
    obter_diagnostico_fatores,
)
from core.audit_response_service import construir_resposta_status
from core.audit_pipeline_service import (
    executar_pipeline_auditoria,
    iniciar_status_agendado,
)


logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python", tags=["analysis"])

_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(_PROJETO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJETO_DIR))


@router.post("/analises/analise_faturamento_periodo")
async def analise_faturamento_periodo(req: AnaliseFaturamentoRequest):
    """Soma do valor_total por ano_mes com filtros opcionais."""
    try:
        base = Path(req.input_dir)
        if not base.exists():
            raise HTTPException(
                status_code=404, detail="Diretório de entrada não encontrado"
            )

        parquet_name = req.arquivo_base or "nfe_saida.parquet"
        src = base / parquet_name
        if not src.exists():
            raise HTTPException(
                status_code=404, detail=f"Arquivo base não encontrado: {src}"
            )

        df = pl.read_parquet(str(src))
        cols = {c.lower(): c for c in df.columns}
        col_data = cols.get("emissao_data", "emissao_data")
        col_valor = cols.get("valor_total", "valor_total")
        col_cnpj = cols.get("cnpj_emitente", "cnpj_emitente")

        if req.cnpj and col_cnpj in df.columns:
            cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
            if cnpj_limpo:
                df = df.filter(pl.col(col_cnpj) == cnpj_limpo)

        if col_data in df.columns and df[col_data].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col_data).str.slice(0, 10).alias(col_data))

        if req.data_ini and col_data in df.columns:
            df = df.filter(pl.col(col_data) >= pl.lit(req.data_ini))
        if req.data_fim and col_data in df.columns:
            df = df.filter(pl.col(col_data) <= pl.lit(req.data_fim))

        if col_data not in df.columns or col_valor not in df.columns:
            raise HTTPException(
                status_code=400,
                detail="Colunas esperadas não encontradas (emissao_data, valor_total).",
            )

        out = (
            df.with_columns(pl.col(col_data).str.slice(0, 7).alias("ano_mes"))
            .group_by("ano_mes")
            .agg(pl.col(col_valor).sum().alias("faturamento"))
            .sort("ano_mes")
        )

        Path(req.output_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(req.output_dir) / "analise_faturamento_periodo.parquet"
        out.write_parquet(str(out_path))

        return {
            "success": True,
            "rows": out.height,
            "columns": out.width,
            "file": str(out_path),
            "sample": out.head(10).to_dicts(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[analise_faturamento] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/auditoria/pipeline")
async def audit_pipeline(req: AuditPipelineRequest, background_tasks: BackgroundTasks):
    """Pipeline completo de auditoria."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    from core.config_loader import get_config_var

    try:
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        DIR_SQL = get_config_var("DIR_SQL")
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        iniciar_status_agendado(dir_analises)

        background_tasks.add_task(
            executar_pipeline_auditoria,
            req,
            cnpj_limpo,
            dir_parquet,
            dir_analises,
            dir_relatorios,
            DIR_SQL,
            _PROJETO_DIR,
        )

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "job_status": "agendada",
            "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente.",
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[pipeline] Erro ao agendar: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fatores/import-excel")
async def importar_fatores_excel(
    cnpj: str = Query(...),
    file: Any = None,
):
    """Importa um arquivo Excel de fatores de conversão."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        fatores_path = dir_analises / f"fatores_conversao_{cnpj_limpo}.parquet"

        if not fatores_path.exists():
            raise HTTPException(
                status_code=404, detail="Arquivo de fatores não encontrado."
            )

        content = await file.read()
        try:
            resultado = processar_fatores_excel(fatores_path, content)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))

        return {"success": True, "cnpj": cnpj_limpo, **resultado}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[importar_fatores_excel] Erro: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro ao importar fatores: {e}")


@router.get("/fatores/diagnostico")
async def diagnostico_fatores_excel(cnpj: str = Query(...)):
    """Gera um diagnostico de fragilidades dos fatores de conversao."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")

    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
        fatores_path = dir_analises / f"fatores_conversao_{cnpj_limpo}.parquet"

        resultado = obter_diagnostico_fatores(fatores_path, cnpj_limpo)
        return resultado
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[diagnostico_fatores_excel] Erro: %s", e)
        raise HTTPException(
            status_code=500, detail=f"Erro ao diagnosticar fatores: {e}"
        )


@router.get("/auditoria/status/{cnpj}")
async def get_audit_status(cnpj: str):
    """Retorna o status atual da auditoria baseada no status_pipeline.json e arquivos em disco."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    from core.config_loader import get_config_var

    try:
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        return construir_resposta_status(
            cnpj_limpo, dir_parquet, dir_analises, dir_relatorios
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
