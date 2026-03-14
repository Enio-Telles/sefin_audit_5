from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import Dict

import polars as pl

if __package__:
    from ._produto_unid_analytics import (
        _aplicar_auto_consenso_por_descricao,
        construir_tabela_codigos_multidescricao,
        construir_tabela_produtos_indexados,
        construir_tabelas_analiticas,
    )
    from ._produto_unid_manual import (
        aplicar_mapa_descricoes_manual,
        aplicar_mapa_revisao_manual,
        gerar_mapa_auditoria_descricoes_manual,
        gerar_mapas_auditoria_manual,
    )
    from ._produto_unid_shared import (
        FONTE_PIPELINE_CONFIGS,
        _lista_valor_auditavel_expr,
        aplicar_mapeamento_e_schema,
    )
else:
    from _produto_unid_analytics import (
        _aplicar_auto_consenso_por_descricao,
        construir_tabela_codigos_multidescricao,
        construir_tabela_produtos_indexados,
        construir_tabelas_analiticas,
    )
    from _produto_unid_manual import (
        aplicar_mapa_descricoes_manual,
        aplicar_mapa_revisao_manual,
        gerar_mapa_auditoria_descricoes_manual,
        gerar_mapas_auditoria_manual,
    )
    from _produto_unid_shared import (
        FONTE_PIPELINE_CONFIGS,
        _lista_valor_auditavel_expr,
        aplicar_mapeamento_e_schema,
    )


def _montar_caminhos_entrada_padrao(cnpj: str) -> tuple[Dict[str, str], str]:
    projeto_dir = Path(__file__).resolve().parent.parent.parent
    config_path = projeto_dir / "config.py"

    spec = importlib.util.spec_from_file_location("sefin_config", str(config_path))
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)

    dir_parquet, dir_analises, _ = config.obter_diretorios_cnpj(cnpj)
    caminhos_entrada = {
        chave: os.path.join(dir_parquet, cfg["arquivo"].format(cnpj=cnpj))
        for chave, cfg in FONTE_PIPELINE_CONFIGS.items()
    }
    return caminhos_entrada, str(dir_analises)



def _carregar_fontes_produto(caminhos_entrada: Dict[str, str]) -> list[pl.LazyFrame]:
    lazy_frames: list[pl.LazyFrame] = []

    for chave, cfg in FONTE_PIPELINE_CONFIGS.items():
        caminho = caminhos_entrada.get(chave)
        if not caminho or not os.path.exists(caminho):
            continue

        lf_fonte = pl.scan_parquet(caminho)
        lazy_frames.append(aplicar_mapeamento_e_schema(lf_fonte, cfg["mapping"], cfg["nome"]))

    return lazy_frames



def _gravar_base_detalhes(lf_base_detalhes: pl.LazyFrame, path_detalhes: str) -> None:
    logging.info("Consolidando base de detalhes...")
    (
        lf_base_detalhes.with_columns(
            _lista_valor_auditavel_expr("fonte").alias("lista_fontes")
        )
        .sink_parquet(path_detalhes)
    )



def _materializar_tabela_agrupada(
    lf_agrupado_descricao: pl.LazyFrame,
    lf_detalhes_reloaded: pl.LazyFrame,
) -> pl.DataFrame:
    fonte_counts_df = lf_detalhes_reloaded.group_by("fonte").agg(pl.len().alias("qtd")).collect(engine="streaming")
    fonte_counts = {str(r["fonte"]): int(r["qtd"]) for r in fonte_counts_df.to_dicts()}
    total_registros_fontes = int(sum(fonte_counts.values()))

    df_agrupado = lf_agrupado_descricao.collect(engine="streaming")
    sort_cols = [c for c in ["descricao", "codigo_consenso", "ncm_consenso", "cest_consenso"] if c in df_agrupado.columns]
    if sort_cols:
        df_agrupado = df_agrupado.sort(sort_cols)

    return df_agrupado.with_columns(
        [
            pl.format("ID_{}", pl.int_range(1, df_agrupado.height + 1).cast(pl.Utf8).str.zfill(4)).alias("chave_produto"),
            pl.lit(fonte_counts.get("NFe", 0)).cast(pl.Int64).alias("auditoria_total_fonte_nfe"),
            pl.lit(fonte_counts.get("NFCe", 0)).cast(pl.Int64).alias("auditoria_total_fonte_nfce"),
            pl.lit(fonte_counts.get("EFD_0200", 0)).cast(pl.Int64).alias("auditoria_total_fonte_efd_0200"),
            pl.lit(fonte_counts.get("EFD_C170", 0)).cast(pl.Int64).alias("auditoria_total_fonte_efd_c170"),
            pl.lit(fonte_counts.get("Bloco_H", 0)).cast(pl.Int64).alias("auditoria_total_fonte_bloco_h"),
            pl.lit(total_registros_fontes).cast(pl.Int64).alias("auditoria_total_registros_fontes"),
        ]
    ).select([pl.col("chave_produto"), pl.all().exclude("chave_produto")])



def unificar_produtos_unidades(cnpj: str) -> Dict[str, str]:
    caminhos_entrada, diretorio_saida = _montar_caminhos_entrada_padrao(cnpj)
    return processar_produtos_cnpj(cnpj, caminhos_entrada, diretorio_saida)



def processar_produtos_cnpj(cnpj: str, caminhos_entrada: Dict[str, str], diretorio_saida: str) -> Dict[str, str]:
    os.makedirs(diretorio_saida, exist_ok=True)

    path_mapa_manual = os.path.join(diretorio_saida, f"mapa_manual_unificacao_{cnpj}.parquet")
    path_mapa_descricoes = os.path.join(diretorio_saida, f"mapa_manual_descricoes_{cnpj}.parquet")

    with pl.StringCache():
        lazy_frames = _carregar_fontes_produto(caminhos_entrada)
        if not lazy_frames:
            logging.warning("Nenhum arquivo de produto encontrado para o CNPJ %s", cnpj)
            return {}

        lf_base_detalhes = pl.concat(lazy_frames, how="diagonal_relaxed")
        lf_base_detalhes = aplicar_mapa_descricoes_manual(lf_base_detalhes, path_mapa_descricoes)
        lf_base_detalhes = aplicar_mapa_revisao_manual(lf_base_detalhes, path_mapa_manual)
        lf_base_detalhes = _aplicar_auto_consenso_por_descricao(lf_base_detalhes)

        path_detalhes = os.path.join(diretorio_saida, f"base_detalhes_produtos_{cnpj}.parquet")
        _gravar_base_detalhes(lf_base_detalhes, path_detalhes)

        lf_detalhes_reloaded = pl.scan_parquet(path_detalhes)
        lf_variacoes, lf_agrupado_descricao = construir_tabelas_analiticas(lf_detalhes_reloaded)

        df_agrupado = _materializar_tabela_agrupada(lf_agrupado_descricao, lf_detalhes_reloaded)

        path_agrupado = os.path.join(diretorio_saida, f"produtos_agregados_{cnpj}.parquet")
        df_agrupado.write_parquet(path_agrupado)
        logging.info("Tabela de Agrupamento salva: %s", path_agrupado)

        path_variacoes = os.path.join(diretorio_saida, f"variacoes_produtos_{cnpj}.parquet")
        lf_variacoes.collect(engine="streaming").write_parquet(path_variacoes)

        lf_chaves_produto = df_agrupado.select(["chave_produto", "descricao"]).lazy()

        path_indexados = os.path.join(diretorio_saida, f"produtos_indexados_{cnpj}.parquet")
        construir_tabela_produtos_indexados(lf_detalhes_reloaded, lf_chaves_produto).collect(
            engine="streaming"
        ).write_parquet(path_indexados)

        path_codigos_multidescricao = os.path.join(diretorio_saida, f"codigos_multidescricao_{cnpj}.parquet")
        construir_tabela_codigos_multidescricao(lf_detalhes_reloaded, lf_chaves_produto).collect(
            engine="streaming"
        ).write_parquet(path_codigos_multidescricao)

        gerar_mapas_auditoria_manual(path_mapa_manual, diretorio_saida, cnpj)
        gerar_mapa_auditoria_descricoes_manual(path_mapa_descricoes, diretorio_saida, cnpj)

        return {
            "success": True,
            "cnpj": cnpj,
            "base_detalhes": path_detalhes,
            "mapa_manual_descricoes": path_mapa_descricoes,
            "codigos_multidescricao": path_codigos_multidescricao,
            "produtos_indexados": path_indexados,
            "produtos_agregados": path_agrupado,
            "variacoes_produtos": path_variacoes,
        }


__all__ = [
    "processar_produtos_cnpj",
    "unificar_produtos_unidades",
]
