from __future__ import annotations

import logging
from typing import Tuple

import polars as pl

if __package__:
    from ._produto_unid_shared import (
        AUTO_CONSENSO_FIELDS,
        SCHEMA_FISCAL_PRODUTOS,
        _lista_valor_auditavel_expr,
        _normalized_text_expr,
        _source_priority_expr,
        limpar_caracteres_especiais,
    )
else:
    from _produto_unid_shared import (
        AUTO_CONSENSO_FIELDS,
        SCHEMA_FISCAL_PRODUTOS,
        _lista_valor_auditavel_expr,
        _normalized_text_expr,
        _source_priority_expr,
        limpar_caracteres_especiais,
    )


def _consenso_por_descricao_field(lf_base_detalhes: pl.LazyFrame, field_name: str, alias: str) -> pl.LazyFrame:
    value_expr = _normalized_text_expr(field_name)

    return (
        lf_base_detalhes.select(
            [
                pl.col("descricao"),
                value_expr.alias("__valor_consenso"),
                _source_priority_expr("fonte").alias("__fonte_prioridade"),
            ]
        )
        .group_by(["descricao", "__valor_consenso"])
        .agg(
            [
                pl.len().alias("__freq_valor"),
                pl.col("__fonte_prioridade").min().alias("__melhor_prioridade_fonte"),
            ]
        )
        .with_columns(
            pl.when(pl.col("__valor_consenso") == "")
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("__valor_vazio")
        )
        .sort(
            by=["descricao", "__valor_vazio", "__freq_valor", "__melhor_prioridade_fonte", "__valor_consenso"],
            descending=[False, False, True, False, False],
        )
        .group_by("descricao", maintain_order=True)
        .agg(pl.col("__valor_consenso").first().alias(alias))
        .with_columns(
            pl.when(pl.col(alias) == "").then(pl.lit(None)).otherwise(pl.col(alias)).alias(alias)
        )
    )


def _construir_consensos_por_descricao(lf_base_detalhes: pl.LazyFrame, prefix: str) -> pl.LazyFrame:
    lf_consensos = lf_base_detalhes.select("descricao").unique()

    for field_name in AUTO_CONSENSO_FIELDS:
        lf_consensos = lf_consensos.join(
            _consenso_por_descricao_field(lf_base_detalhes, field_name, f"{prefix}{field_name}"),
            on="descricao",
            how="left",
        )

    return lf_consensos


def _aplicar_auto_consenso_por_descricao(lf_base_detalhes: pl.LazyFrame) -> pl.LazyFrame:
    manual_flags = [f"__manual_{field_name}_set" for field_name in AUTO_CONSENSO_FIELDS]
    cols_presentes = set(lf_base_detalhes.collect_schema().names())

    missing_flag_exprs = [pl.lit(False).alias(col_name) for col_name in manual_flags if col_name not in cols_presentes]
    if missing_flag_exprs:
        lf_base_detalhes = lf_base_detalhes.with_columns(missing_flag_exprs)

    consenso_prefix = "__auto_consenso_"
    consenso_cols = [f"{consenso_prefix}{field_name}" for field_name in AUTO_CONSENSO_FIELDS]
    lf_consensos = _construir_consensos_por_descricao(lf_base_detalhes, consenso_prefix)

    exprs = []
    for field_name in AUTO_CONSENSO_FIELDS:
        dtype = SCHEMA_FISCAL_PRODUTOS[field_name]
        manual_flag_col = f"__manual_{field_name}_set"
        consenso_col = f"{consenso_prefix}{field_name}"
        exprs.append(
            pl.when(pl.col(manual_flag_col).fill_null(False))
            .then(pl.col(field_name).cast(pl.Utf8))
            .otherwise(pl.coalesce([pl.col(consenso_col), pl.col(field_name).cast(pl.Utf8)]))
            .cast(dtype)
            .alias(field_name)
        )

    return (
        lf_base_detalhes.join(lf_consensos, on="descricao", how="left")
        .with_columns(exprs)
        .drop(consenso_cols + manual_flags)
    )


def construir_tabelas_analiticas(lf_base_detalhes: pl.LazyFrame) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    logging.info("Construindo Tabelas Analiticas de Variacao e Agrupamento...")

    lf_variacoes = (
        lf_base_detalhes.group_by(["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]).agg(
            [
                _lista_valor_auditavel_expr("unid").unique().alias("lista_unid"),
                _lista_valor_auditavel_expr("fonte").unique().alias("lista_fontes"),
                pl.len().alias("qtd_transacoes"),
            ]
        )
    )

    lf_codigo_stats = (
        lf_base_detalhes.select(["codigo", "descricao"])
        .unique()
        .group_by("codigo")
        .agg(
            [
                pl.len().alias("qtd_descricoes_codigo"),
                _lista_valor_auditavel_expr("descricao").unique().alias("lista_descricoes_codigo"),
            ]
        )
        .with_columns(
            [
                (pl.col("qtd_descricoes_codigo") > 1).alias("tem_codigo_multidescricao"),
                pl.format("[{};{}]", pl.col("codigo"), pl.col("qtd_descricoes_codigo")).alias("cod_var_str"),
            ]
        )
    )

    lf_base_detalhes_str = lf_base_detalhes.with_columns(
        [
            pl.col("ncm").cast(pl.Utf8),
            pl.col("cest").cast(pl.Utf8),
            pl.col("tipo_item").cast(pl.Utf8),
            pl.col("unid").cast(pl.Utf8),
            pl.col("fonte").cast(pl.Utf8),
            pl.coalesce(
                [
                    limpar_caracteres_especiais(pl.col("descricao_ori").cast(pl.String), manter_espacos=True),
                    pl.col("descricao").cast(pl.Utf8),
                ]
            ).alias("descricao_encontrada"),
        ]
    )

    lf_consensos_descricao = _construir_consensos_por_descricao(lf_base_detalhes_str, "__descricao_consenso_")
    lf_base_enriched = lf_base_detalhes_str.join(lf_codigo_stats, on="codigo", how="left")

    lf_agrupado_descricao = (
        lf_base_enriched.group_by("descricao")
        .agg(
            [
                _lista_valor_auditavel_expr("codigo").unique().alias("lista_codigo"),
                _lista_valor_auditavel_expr("descricao_encontrada").unique().alias("lista_descricao"),
                _lista_valor_auditavel_expr("descr_compl").unique().alias("lista_descr_compl"),
                _lista_valor_auditavel_expr("tipo_item").unique().alias("lista_tipo_item"),
                _lista_valor_auditavel_expr("ncm").unique().alias("lista_ncm"),
                _lista_valor_auditavel_expr("cest").unique().alias("lista_cest"),
                _lista_valor_auditavel_expr("gtin").unique().alias("lista_gtin"),
                _lista_valor_auditavel_expr("unid").unique().alias("lista_unid_raw"),
                _lista_valor_auditavel_expr("fonte").unique().alias("lista_fontes"),
                pl.col("codigo").drop_nulls().mode().first().cast(pl.Utf8).alias("codigo_consenso"),
                pl.col("tipo_item").drop_nulls().mode().first().cast(pl.Utf8).alias("tipo_item_consenso"),
                pl.col("ncm").drop_nulls().mode().first().cast(pl.Utf8).alias("ncm_consenso"),
                pl.col("cest").drop_nulls().mode().first().cast(pl.Utf8).alias("cest_consenso"),
                pl.col("gtin").drop_nulls().mode().first().cast(pl.Utf8).alias("gtin_consenso"),
                pl.col("unid").drop_nulls().mode().first().cast(pl.Utf8).alias("unid_consenso"),
                pl.col("tem_codigo_multidescricao").any().alias("requer_revisao_manual"),
                pl.col("cod_var_str").cast(pl.Utf8).unique().alias("lista_cod_var"),
                (pl.col("fonte") == "NFe").sum().cast(pl.Int64).alias("qtd_fonte_nfe"),
                (pl.col("fonte") == "NFCe").sum().cast(pl.Int64).alias("qtd_fonte_nfce"),
                (pl.col("fonte") == "EFD_0200").sum().cast(pl.Int64).alias("qtd_fonte_efd_0200"),
                (pl.col("fonte") == "EFD_C170").sum().cast(pl.Int64).alias("qtd_fonte_efd_c170"),
                (pl.col("fonte") == "Bloco_H").sum().cast(pl.Int64).alias("qtd_fonte_bloco_h"),
                pl.col("fonte").drop_nulls().n_unique().cast(pl.Int64).alias("qtd_fontes_distintas"),
                pl.len().alias("qtd_transacoes_total"),
            ]
        )
        .join(lf_consensos_descricao, on="descricao", how="left")
        .with_columns(
            [
                pl.coalesce(["__descricao_consenso_tipo_item", "tipo_item_consenso"]).alias("tipo_item_consenso"),
                pl.coalesce(["__descricao_consenso_ncm", "ncm_consenso"]).alias("ncm_consenso"),
                pl.coalesce(["__descricao_consenso_cest", "cest_consenso"]).alias("cest_consenso"),
                pl.coalesce(["__descricao_consenso_gtin", "gtin_consenso"]).alias("gtin_consenso"),
                pl.col("lista_descricao").list.join("<<#>>").alias("lista_descricao"),
                pl.col("lista_descricao").list.len().cast(pl.Int64).alias("qtd_descricoes"),
                pl.col("lista_codigo").list.len().cast(pl.Int64).alias("qtd_codigos"),
                pl.col("lista_cod_var").list.join(" | ").alias("descricoes_conflitantes"),
                pl.col("lista_unid_raw").list.join(", ").alias("lista_unid"),
                pl.format(
                    "NFe:{} | NFCe:{} | EFD_0200:{} | EFD_C170:{} | Bloco_H:{}",
                    pl.col("qtd_fonte_nfe"),
                    pl.col("qtd_fonte_nfce"),
                    pl.col("qtd_fonte_efd_0200"),
                    pl.col("qtd_fonte_efd_c170"),
                    pl.col("qtd_fonte_bloco_h"),
                ).alias("auditoria_contagem_fontes"),
            ]
        )
        .drop(
            [
                "__descricao_consenso_tipo_item",
                "__descricao_consenso_ncm",
                "__descricao_consenso_cest",
                "__descricao_consenso_gtin",
            ]
        )
    )

    return lf_variacoes, lf_agrupado_descricao


def _anexar_chave_produto(
    lf_base_detalhes: pl.LazyFrame,
    lf_chaves_produto: pl.LazyFrame,
) -> pl.LazyFrame:
    return lf_base_detalhes.join(lf_chaves_produto, on="descricao", how="left")


def construir_tabela_produtos_indexados(
    lf_base_detalhes: pl.LazyFrame,
    lf_chaves_produto: pl.LazyFrame,
) -> pl.LazyFrame:
    lf_indexada = _anexar_chave_produto(lf_base_detalhes, lf_chaves_produto)

    return (
        lf_indexada.group_by(
            ["chave_produto", "codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]
        ).agg(
            [
                _lista_valor_auditavel_expr("unid").unique().alias("lista_unidades"),
                _lista_valor_auditavel_expr("fonte").unique().alias("lista_fontes"),
                pl.len().cast(pl.Int64).alias("qtd_linhas"),
            ]
        )
        .with_columns(
            [
                pl.col("lista_unidades").list.join(", ").alias("lista_unidades"),
                pl.col("lista_fontes").list.join(", ").alias("lista_fontes"),
            ]
        )
    )


def construir_tabela_codigos_multidescricao(
    lf_base_detalhes: pl.LazyFrame,
    lf_chaves_produto: pl.LazyFrame,
) -> pl.LazyFrame:
    lf_indexada = _anexar_chave_produto(lf_base_detalhes, lf_chaves_produto).with_columns(
        [
            pl.col("chave_produto").cast(pl.Utf8),
            pl.col("ncm").cast(pl.Utf8),
            pl.col("cest").cast(pl.Utf8),
            pl.col("gtin").cast(pl.Utf8),
            pl.col("tipo_item").cast(pl.Utf8),
        ]
    )

    return (
        lf_indexada.group_by("codigo")
        .agg(
            [
                _lista_valor_auditavel_expr("descricao").unique().alias("lista_descricoes"),
                _lista_valor_auditavel_expr("ncm").unique().alias("lista_ncm"),
                _lista_valor_auditavel_expr("cest").unique().alias("lista_cest"),
                _lista_valor_auditavel_expr("gtin").unique().alias("lista_gtin"),
                _lista_valor_auditavel_expr("tipo_item").unique().alias("lista_tipo_item"),
                _lista_valor_auditavel_expr("chave_produto").unique().alias("lista_chave_produto"),
                pl.col("descricao").drop_nulls().n_unique().cast(pl.Int64).alias("qtd_descricoes"),
                pl.col("chave_produto").drop_nulls().n_unique().cast(pl.Int64).alias("qtd_grupos_descricao_afetados"),
                pl.len().cast(pl.Int64).alias("qtd_linhas"),
            ]
        )
        .filter(pl.col("qtd_descricoes") > 1)
        .with_columns(
            [
                pl.col("lista_descricoes").list.join("<<#>>").alias("lista_descricoes"),
                pl.col("lista_ncm").list.join(", ").alias("lista_ncm"),
                pl.col("lista_cest").list.join(", ").alias("lista_cest"),
                pl.col("lista_gtin").list.join(", ").alias("lista_gtin"),
                pl.col("lista_tipo_item").list.join(", ").alias("lista_tipo_item"),
                pl.col("lista_chave_produto").list.join(", ").alias("lista_chave_produto"),
            ]
        )
        .sort(["qtd_descricoes", "codigo"], descending=[True, False])
    )


__all__ = [
    "_aplicar_auto_consenso_por_descricao",
    "_construir_consensos_por_descricao",
    "_consenso_por_descricao_field",
    "construir_tabela_codigos_multidescricao",
    "construir_tabela_produtos_indexados",
    "construir_tabelas_analiticas",
]
