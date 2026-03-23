import traceback
import logging
from pathlib import Path
from fastapi import HTTPException

from core.models import AuditPipelineRequest
from core.audit_status_service import atualizar_status_pipeline
from core.audit_oracle_service import obter_conexao_oracle
from core.audit_extraction_service import executar_extracao_sql
from core.audit_products_service import executar_unificacao_produtos
from core.audit_reports_service import gerar_relatorios_finais

logger = logging.getLogger("sefin_audit_python")


async def run_audit_pipeline_bg(
    req: AuditPipelineRequest,
    cnpj_limpo: str,
    dir_parquet: Path,
    dir_analises: Path,
    dir_relatorios: Path,
    dir_sql: Path,
    projeto_dir: Path,
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
            ],
        )

        # ETAPA 1: Extração
        try:
            conexao = obter_conexao_oracle(projeto_dir)
            arquivos_extraidos, erros_extracao = executar_extracao_sql(
                conexao, cnpj_limpo, dir_parquet, dir_sql, req.data_limite_processamento
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
                etapas=[
                    {
                        "etapa": "Extração de Dados",
                        "status": "erro",
                        "motivo": "Ver erros",
                    }
                ],
                erros=erros,
            )

        # ETAPA 2: Análises
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando cruzamentos e análises...",
            etapas=[{"etapa": "Cruzamentos e Análises", "status": "executando"}],
        )

        arquivos_analises = []
        arquivos_produtos = []
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando unificação de produtos...",
            etapas=[
                {"etapa": "Cruzamentos e Análises", "status": "concluida"},
                {"etapa": "Análise de Produtos", "status": "executando"},
            ],
        )

        try:
            arquivos_produtos = executar_unificacao_produtos(
                cnpj_limpo, dir_parquet, dir_analises, projeto_dir
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
                etapas=[
                    {"etapa": "Análise de Produtos", "status": "erro", "motivo": str(e)}
                ],
                erros=erros,
            )

        # ETAPA 3: Relatórios
        atualizar_status_pipeline(
            dir_analises,
            "executando",
            "Iniciando geração de relatórios...",
            etapas=[{"etapa": "Geração de Relatórios", "status": "executando"}],
        )

        arquivos_relatorios, erros_relatorios = gerar_relatorios_finais(
            cnpj_limpo, dir_analises, dir_relatorios, projeto_dir
        )
        if erros_relatorios:
            erros.extend(erros_relatorios)

        atualizar_status_pipeline(
            dir_analises,
            "concluida" if not erros else "erro",
            "Verifique a aba de arquivos gerados"
            if not erros
            else "Auditoria concluída com erros.",
            etapas=[
                {
                    "etapa": "Geração de Relatórios",
                    "status": "concluida"
                    if not any("Relatórios" in e for e in erros)
                    else "erro",
                }
            ],
            erros=erros,
        )
    except Exception as e:
        logger.error(f"[pipeline bg] Erro: {e}\n{traceback.format_exc()}")
        try:
            erros_final = erros if "erros" in locals() else []
            erros_final.append(str(e))
            atualizar_status_pipeline(dir_analises, "erro", str(e), erros=erros_final)
        except Exception:
            pass
