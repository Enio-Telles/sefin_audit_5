import traceback
import logging
import json
from pathlib import Path
from fastapi import HTTPException
from typing import List

from core.models import AuditPipelineRequest
from core.audit_status_service import atualizar_status_pipeline
from core.audit_oracle_service import obter_conexao_oracle
from core.audit_extraction_service import executar_extracao_sql
from core.audit_products_service import executar_unificacao_produtos
from core.audit_reports_service import gerar_relatorios_finais

logger = logging.getLogger("sefin_audit_python")


def iniciar_status_agendado(dir_analises: Path) -> None:
    status_file = dir_analises / "status_pipeline.json"
    with open(status_file, "w") as f:
        json.dump(
            {
                "status": "agendada",
                "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente.",
            },
            f,
        )


def _iniciar_pipeline(dir_analises: Path) -> None:
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


def _atualizar_etapa(
    dir_analises: Path, mensagem: str, etapas: List[dict], erros: List[str] = None
) -> None:
    atualizar_status_pipeline(
        dir_analises,
        "executando",
        mensagem,
        etapas=etapas,
        erros=erros or [],
    )


def _encerrar_com_sucesso(dir_analises: Path, erros: List[str] = None) -> None:
    erros = erros or []
    status = "concluida" if not erros else "erro"
    mensagem = (
        "Verifique a aba de arquivos gerados"
        if not erros
        else "Auditoria concluída com erros."
    )
    status_etapa_relatorios = (
        "concluida" if not any("Relatórios" in e for e in erros) else "erro"
    )

    atualizar_status_pipeline(
        dir_analises,
        status,
        mensagem,
        etapas=[{"etapa": "Geração de Relatórios", "status": status_etapa_relatorios}],
        erros=erros,
    )


def _encerrar_com_erro(dir_analises: Path, e: Exception, erros: List[str] = None) -> None:
    logger.error(f"[pipeline bg] Erro: {e}\n{traceback.format_exc()}")
    try:
        erros_final = erros or []
        erros_final.append(str(e))
        atualizar_status_pipeline(dir_analises, "erro", str(e), erros=erros_final)
    except Exception:
        pass


async def executar_pipeline_auditoria(
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
        _iniciar_pipeline(dir_analises)

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

            _atualizar_etapa(
                dir_analises,
                "Extração concluída.",
                [{"etapa": "Extração de Dados", "status": "concluida"}],
            )
        except Exception as e:
            erros.append(f"Extração Oracle: {str(e)}")
            _atualizar_etapa(
                dir_analises,
                "Erros detectados na extração. Prosseguindo...",
                [
                    {
                        "etapa": "Extração de Dados",
                        "status": "erro",
                        "motivo": "Ver erros",
                    }
                ],
                erros,
            )

        # ETAPA 2: Análises e Produtos
        arquivos_analises = []
        arquivos_produtos = []
        _atualizar_etapa(
            dir_analises,
            "Iniciando cruzamentos e análises...",
            [{"etapa": "Cruzamentos e Análises", "status": "executando"}],
        )

        _atualizar_etapa(
            dir_analises,
            "Iniciando unificação de produtos...",
            [
                {"etapa": "Cruzamentos e Análises", "status": "concluida"},
                {"etapa": "Análise de Produtos", "status": "executando"},
            ],
        )

        try:
            arquivos_produtos = executar_unificacao_produtos(
                cnpj_limpo, dir_parquet, dir_analises, projeto_dir
            )
            _atualizar_etapa(
                dir_analises,
                "Unificação Master e Produtos concluída.",
                [{"etapa": "Análise de Produtos", "status": "concluida"}],
            )
        except HTTPException:
            raise
        except Exception as e:
            erros.append(f"Processamento de Produtos: {str(e)}")
            logger.error(f"[audit_pipeline] Erro em Produtos: {e}")
            _atualizar_etapa(
                dir_analises,
                "Erro no processamento de produtos.",
                [
                    {"etapa": "Análise de Produtos", "status": "erro", "motivo": str(e)}
                ],
                erros,
            )

        # ETAPA 3: Relatórios
        _atualizar_etapa(
            dir_analises,
            "Iniciando geração de relatórios...",
            [{"etapa": "Geração de Relatórios", "status": "executando"}],
        )

        arquivos_relatorios, erros_relatorios = gerar_relatorios_finais(
            cnpj_limpo, dir_analises, dir_relatorios, projeto_dir
        )
        if erros_relatorios:
            erros.extend(erros_relatorios)

        _encerrar_com_sucesso(dir_analises, erros)

    except Exception as e:
        _encerrar_com_erro(dir_analises, e, erros if "erros" in locals() else [])
