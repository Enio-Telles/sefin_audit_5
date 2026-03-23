import logging
from pathlib import Path
import polars as pl

from core.produto_runtime import (
    produto_pipeline_em_modo_compatibilidade,
    unificar_produtos_unidades,
)

logger = logging.getLogger("sefin_audit_python")

def executar_unificacao_produtos(
    cnpj_limpo: str,
    dir_parquet: Path,
    dir_analises: Path,
    projeto_dir: Path
) -> list[dict]:
    """
    Executa o pipeline master de produtos e aplica os guardrails para validar se
    a base_detalhes foi gerada adequadamente.
    Retorna lista de dicionários contendo os metadados dos arquivos de produtos gerados.
    Levanta ValueError em caso de falha nos guardrails.
    """
    unificar_produtos_unidades(cnpj_limpo, projeto_dir=projeto_dir)

    fontes_produto = [
        dir_parquet / f"NFe_{cnpj_limpo}.parquet",
        dir_parquet / f"NFCe_{cnpj_limpo}.parquet",
        dir_parquet / f"c170_simplificada_{cnpj_limpo}.parquet",
        dir_parquet / f"reg_0200_{cnpj_limpo}.parquet",
        dir_parquet / f"bloco_h_{cnpj_limpo}.parquet",
    ]
    fontes_com_dados = []
    for fp in fontes_produto:
        if fp.exists():
            try:
                n_rows = pl.scan_parquet(str(fp)).select(pl.len()).collect().item()
                if n_rows > 0:
                    fontes_com_dados.append((fp.name, int(n_rows)))
            except Exception:
                continue

    base_detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
    if fontes_com_dados:
        if (
            not base_detalhes_path.exists()
            and not produto_pipeline_em_modo_compatibilidade()
        ):
            fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
            raise ValueError(
                f"Unificação de produtos não gerou base_detalhes. Fontes com dados: {fontes_txt}"
            )

        base_rows = (
            pl.scan_parquet(str(base_detalhes_path)).select(pl.len()).collect().item()
            if base_detalhes_path.exists()
            else 0
        )
        if int(base_rows) == 0 and not produto_pipeline_em_modo_compatibilidade():
            fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
            raise ValueError(
                f"Unificação de produtos gerou 0 linhas com fontes preenchidas. Verifique mapeamento de colunas. Fontes: {fontes_txt}"
            )
        if produto_pipeline_em_modo_compatibilidade() and not base_detalhes_path.exists():
            logger.warning(
                "[audit_pipeline] modo de compatibilidade de produtos ativo; base_detalhes nao foi regenerada para %s",
                cnpj_limpo,
            )

    arquivos_produtos = []
    targets = [
        (f"produtos_agregados_{cnpj_limpo}.parquet", "Tabela Final"),
        (f"base_detalhes_produtos_{cnpj_limpo}.parquet", "Base Detalhes"),
        (f"status_analise_produtos_{cnpj_limpo}.parquet", "Status de Analise"),
        (f"mapa_auditoria_agregados_{cnpj_limpo}.parquet", "Mapa de Agregados"),
        (f"mapa_auditoria_desagregados_{cnpj_limpo}.parquet", "Mapa de Desagregados"),
    ]

    for file_name, label in targets:
        p = dir_analises / file_name
        if p.exists():
            try:
                info = pl.scan_parquet(str(p)).collect_schema()
                row_count = pl.scan_parquet(str(p)).select(pl.len()).collect().item()

                arquivos_produtos.append(
                    {
                        "name": file_name,
                        "path": str(p.resolve()),
                        "rows": row_count,
                        "columns": len(info.names()),
                        "analise": label,
                    }
                )
            except Exception:
                arquivos_produtos.append(
                    {
                        "name": file_name,
                        "path": str(p.resolve()),
                        "analise": label,
                    }
                )

    return arquivos_produtos
