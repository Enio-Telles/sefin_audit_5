import re
import sys
import json
import traceback
import logging
import polars as pl
from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

from core.models import AnaliseFaturamentoRequest, AuditPipelineRequest
from core.utils import validar_cnpj

# Novos services extraídos
from core.audit_status_service import atualizar_status_pipeline, obter_status_pipeline
from core.audit_oracle_service import obter_conexao_oracle
from core.audit_extraction_service import executar_extracao_sql
from core.audit_products_service import executar_unificacao_produtos
from core.audit_reports_service import gerar_relatorios_finais
from core.audit_metadata_service import processar_fatores_excel, obter_diagnostico_fatores

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


async def run_audit_pipeline_bg(
    req: AuditPipelineRequest,
    cnpj_limpo: str,
    dir_parquet,
    dir_analises,
    dir_relatorios,
    dir_sql,
):
    try:
        erros = []
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Auditoria em andamento.",
            etapas=[
                {"etapa": "Extração de Dados", "status": "executando"},
                {"etapa": "Cruzamentos e Análises", "status": "pendente"},
                {"etapa": "Análise de Produtos", "status": "pendente"},
                {"etapa": "Geração de Relatórios", "status": "pendente"},
            ]
        )

        # ETAPA 1: Extração
        try:
            conexao = obter_conexao_oracle(_PROJETO_DIR)
            arquivos_extraidos, erros_extracao = executar_extracao_sql(
                conexao,
                cnpj_limpo,
                dir_parquet,
                dir_sql,
                req.data_limite_processamento
            )
            conexao.close()
            if erros_extracao:
                erros.extend(erros_extracao)
                raise Exception("Falhas detectadas na extração Oracle.")

            atualizar_status_pipeline(
                dir_analises,
                "executando",
                "Extração concluída.",
                etapas=[{"etapa": "Extração de Dados", "status": "concluida"}],
            )
        except Exception as e:
            erros.append(f"Extração Oracle: {str(e)}")
            atualizar_status_pipeline(
                dir_analises,
                "executando",
                "Erros detectados na extração. Prosseguindo...",
                etapas=[{"etapa": "Extração de Dados", "status": "erro", "motivo": "Ver erros"}],
                erros=erros
            )

        # ETAPA 2: Análises
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando cruzamentos e análises...",
            etapas=[{"etapa": "Cruzamentos e Análises", "status": "executando"}]
        )

        arquivos_analises = []
        arquivos_produtos = []
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando unificação de produtos...",
            etapas=[
                {"etapa": "Cruzamentos e Análises", "status": "concluida"},
                {"etapa": "Análise de Produtos", "status": "executando"}
            ]
        )

        try:
            arquivos_produtos = executar_unificacao_produtos(
                cnpj_limpo,
                dir_parquet,
                dir_analises,
                _PROJETO_DIR
            )
            atualizar_status_pipeline(
                dir_analises,
                "executando",
                "Unificação Master e Produtos concluída.",
                etapas=[{"etapa": "Análise de Produtos", "status": "concluida"}],
            )
        except HTTPException:
            raise
        except Exception as e:
            erros.append(f"Processamento de Produtos: {str(e)}")
            logger.error(f"[audit_pipeline] Erro em Produtos: {e}")
            atualizar_status_pipeline(
                dir_analises,
                "executando",
                "Erro no processamento de produtos.",
                etapas=[{"etapa": "Análise de Produtos", "status": "erro", "motivo": str(e)}],
                erros=erros
            )

        # ETAPA 3: Relatórios
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando geração de relatórios...",
            etapas=[{"etapa": "Geração de Relatórios", "status": "executando"}]
        )

        arquivos_relatorios, erros_relatorios = gerar_relatorios_finais(
            cnpj_limpo,
            dir_analises,
            dir_relatorios,
            _PROJETO_DIR
        )
        if erros_relatorios:
            erros.extend(erros_relatorios)

        atualizar_status_pipeline(
            dir_analises,
            "concluida" if not erros else "erro",
            "Verifique a aba de arquivos gerados" if not erros else "Auditoria concluída com erros.",
            etapas=[{"etapa": "Geração de Relatórios", "status": "concluida" if not any("Relatórios" in e for e in erros) else "erro"}],
            erros=erros
        )
    except Exception as e:
        logger.error(f"[pipeline bg] Erro: {e}\n{traceback.format_exc()}")
        try:
            erros_final = erros if 'erros' in locals() else []
            erros_final.append(str(e))
            atualizar_status_pipeline(
                dir_analises,
                "erro",
                str(e),
                erros=erros_final
            )
        except Exception:
            pass


@router.post("/auditoria/pipeline")
async def audit_pipeline(req: AuditPipelineRequest, background_tasks: BackgroundTasks):
    """Pipeline completo de auditoria."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    import importlib.util
    try:
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_config_path)
        )
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        obter_diretorios_cnpj = _sefin_config.obter_diretorios_cnpj
        DIR_SQL = _sefin_config.DIR_SQL
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump(
                {
                    "status": "agendada",
                    "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente.",
                },
                f,
            )

        background_tasks.add_task(
            run_audit_pipeline_bg,
            req,
            cnpj_limpo,
            dir_parquet,
            dir_analises,
            dir_relatorios,
            DIR_SQL,
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
        import importlib.util

        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_config)
        _, dir_analises, _ = _config.obter_diretorios_cnpj(cnpj_limpo)
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

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            **resultado
        }
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
        import importlib.util

        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_config)
        _, dir_analises, _ = _config.obter_diretorios_cnpj(cnpj_limpo)
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

    import importlib.util
    from core.audit_artifacts_service import obter_arquivos_auditoria

    try:
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_config_path)
        )
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        obter_diretorios_cnpj = _sefin_config.obter_diretorios_cnpj
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        status_dados = obter_status_pipeline(dir_analises)
        arquivos = obter_arquivos_auditoria(cnpj_limpo, dir_parquet, dir_analises, dir_relatorios)

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            **status_dados,
            "arquivos_extraidos": arquivos.get("arquivos_extraidos", []),
            "arquivos_analises": arquivos.get("arquivos_analises", []),
            "arquivos_produtos": arquivos.get("arquivos_produtos", []),
            "arquivos_relatorios": arquivos.get("arquivos_relatorios", []),
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
