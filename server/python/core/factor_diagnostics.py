from __future__ import annotations

from typing import Any

import polars as pl


FATOR_MAXIMO = 1000.0
FATOR_MINIMO = 0.001
MAX_UNIDADES_POR_PRODUTO = 5
VARIACAO_FATOR_LIMITE = 5.0


def _issue(
    tipo: str,
    severidade: str,
    chave_produto: str,
    ano_referencia: int | None,
    unidade_origem: str,
    fator: float | None,
    detalhes: str,
    sugestao: str,
) -> dict[str, Any]:
    return {
        "tipo": tipo,
        "severidade": severidade,
        "chave_produto": chave_produto,
        "ano_referencia": ano_referencia,
        "unidade_origem": unidade_origem,
        "fator": fator,
        "detalhes": detalhes,
        "sugestao": sugestao,
    }


def _normalize_factor_frame(df_fatores: pl.DataFrame) -> pl.DataFrame:
    work = df_fatores.rename({col: col.lower() for col in df_fatores.columns})
    defaults: list[pl.Expr] = []
    for col_name, value in {
        "chave_produto": "",
        "ano_referencia": None,
        "unidade_origem": "",
        "fator": None,
        "editado_manual": False,
    }.items():
        if col_name not in work.columns:
            defaults.append(pl.lit(value).alias(col_name))
    if defaults:
        work = work.with_columns(defaults)

    return work.with_columns(
        [
            pl.col("chave_produto").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
            pl.col("ano_referencia").cast(pl.Int64, strict=False),
            pl.col("unidade_origem").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
            pl.col("fator").cast(pl.Float64, strict=False),
            pl.col("editado_manual").cast(pl.Boolean, strict=False).fill_null(False),
        ]
    )


def diagnosticar_fatores_conversao(df_fatores: pl.DataFrame) -> dict[str, Any]:
    work = _normalize_factor_frame(df_fatores)
    if work.is_empty():
        return {
            "stats": {
                "total_registros": 0,
                "produtos_unicos": 0,
                "anos_unicos": 0,
                "unidades_unicas": 0,
                "editados_manual": 0,
                "fatores_invalidos": 0,
                "fatores_extremos_altos": 0,
                "fatores_extremos_baixos": 0,
                "grupos_muitas_unidades": 0,
                "grupos_alta_variacao": 0,
            },
            "issues": [],
        }

    issues: list[dict[str, Any]] = []

    # ⚡ Bolt Optimization: Replace slow df.to_dicts() loops with fast native list zip iteration.
    invalidos = work.filter(pl.col("fator").is_null() | (pl.col("fator") <= 0))
    if not invalidos.is_empty():
        keys = invalidos.get_column("chave_produto").to_list()
        anos = invalidos.get_column("ano_referencia").to_list()
        unidades = invalidos.get_column("unidade_origem").to_list()
        fatores = invalidos.get_column("fator").to_list()
        for key, ano, uni, fat in zip(keys, anos, unidades, fatores):
            issues.append(
                _issue(
                    "FATOR_INVALIDO",
                    "critico",
                    str(key or ""),
                    ano,
                    str(uni or ""),
                    fat,
                    "Fator ausente, zero ou negativo.",
                    "Revisar fator manual e validar unidade de origem.",
                )
            )

    extremos_altos = work.filter(pl.col("fator") > FATOR_MAXIMO)
    if not extremos_altos.is_empty():
        keys = extremos_altos.get_column("chave_produto").to_list()
        anos = extremos_altos.get_column("ano_referencia").to_list()
        unidades = extremos_altos.get_column("unidade_origem").to_list()
        fatores = extremos_altos.get_column("fator").to_list()
        for key, ano, uni, fat in zip(keys, anos, unidades, fatores):
            issues.append(
                _issue(
                    "FATOR_EXTREMO_ALTO",
                    "critico",
                    str(key or ""),
                    ano,
                    str(uni or ""),
                    fat,
                    f"Fator acima de {FATOR_MAXIMO:.0f}.",
                    "Verificar se houve erro de unidade, embalagem ou digitacao de preco.",
                )
            )

    extremos_baixos = work.filter((pl.col("fator") > 0) & (pl.col("fator") < FATOR_MINIMO))
    if not extremos_baixos.is_empty():
        keys = extremos_baixos.get_column("chave_produto").to_list()
        anos = extremos_baixos.get_column("ano_referencia").to_list()
        unidades = extremos_baixos.get_column("unidade_origem").to_list()
        fatores = extremos_baixos.get_column("fator").to_list()
        for key, ano, uni, fat in zip(keys, anos, unidades, fatores):
            issues.append(
                _issue(
                    "FATOR_EXTREMO_BAIXO",
                    "alto",
                    str(key or ""),
                    ano,
                    str(uni or ""),
                    fat,
                    f"Fator abaixo de {FATOR_MINIMO:.3f}.",
                    "Revisar unidade de referencia e consistencia fisica da conversao.",
                )
            )

    unidades_vazias = work.filter(pl.col("unidade_origem") == "")
    if not unidades_vazias.is_empty():
        keys = unidades_vazias.get_column("chave_produto").to_list()
        anos = unidades_vazias.get_column("ano_referencia").to_list()
        fatores = unidades_vazias.get_column("fator").to_list()
        for key, ano, fat in zip(keys, anos, fatores):
            issues.append(
                _issue(
                    "UNIDADE_ORIGEM_VAZIA",
                    "medio",
                    str(key or ""),
                    ano,
                    "",
                    fat,
                    "Registro de fator sem unidade de origem informada.",
                    "Preencher a unidade antes de confiar no fator.",
                )
            )

    grupos_unidades = (
        work.filter(pl.col("unidade_origem") != "")
        .group_by(["chave_produto", "ano_referencia"])
        .agg(
            [
                pl.col("unidade_origem").n_unique().alias("qtd_unidades"),
                pl.col("unidade_origem").unique().sort().implode().alias("__lista_unidades"),
            ]
        )
        .filter(pl.col("qtd_unidades") > MAX_UNIDADES_POR_PRODUTO)
    )
    if not grupos_unidades.is_empty():
        keys = grupos_unidades.get_column("chave_produto").to_list()
        anos = grupos_unidades.get_column("ano_referencia").to_list()
        lista_uni = grupos_unidades.get_column("__lista_unidades").to_list()
        qtds = grupos_unidades.get_column("qtd_unidades").to_list()
        for key, ano, lst, qtd in zip(keys, anos, lista_uni, qtds):
            issues.append(
                _issue(
                    "MULTIPLAS_UNIDADES",
                    "medio",
                    str(key or ""),
                    ano,
                    ", ".join(lst or []),
                    None,
                    f"Produto/ano com {qtd} unidades diferentes.",
                    "Validar se falta agrupamento ou padronizacao de unidade.",
                )
            )

    grupos_variacao = (
        work.filter(pl.col("fator") > 0)
        .group_by(["chave_produto", "ano_referencia"])
        .agg(
            [
                pl.col("fator").max().alias("fator_max"),
                pl.col("fator").min().alias("fator_min"),
            ]
        )
        .with_columns((pl.col("fator_max") / pl.col("fator_min")).alias("variacao"))
        .filter(pl.col("variacao") > VARIACAO_FATOR_LIMITE)
    )
    if not grupos_variacao.is_empty():
        keys = grupos_variacao.get_column("chave_produto").to_list()
        anos = grupos_variacao.get_column("ano_referencia").to_list()
        variacoes = grupos_variacao.get_column("variacao").to_list()
        for key, ano, var in zip(keys, anos, variacoes):
            issues.append(
                _issue(
                    "ALTA_VARIACAO_FATORES",
                    "alto",
                    str(key or ""),
                    ano,
                    "",
                    var,
                    f"Variacao de {float(var or 0):.2f}x entre menor e maior fator.",
                    "Revisar se ha mistura de unidades ou produtos distintos sob a mesma chave.",
                )
            )

    severity_order = {"critico": 0, "alto": 1, "medio": 2, "baixo": 3}
    issues.sort(key=lambda item: (severity_order.get(str(item["severidade"]), 9), str(item["tipo"]), str(item["chave_produto"])))

    return {
        "stats": {
            "total_registros": int(work.height),
            "produtos_unicos": int(work["chave_produto"].n_unique()),
            "anos_unicos": int(work["ano_referencia"].drop_nulls().n_unique()),
            "unidades_unicas": int(work.filter(pl.col("unidade_origem") != "")["unidade_origem"].n_unique()),
            "editados_manual": int(work.filter(pl.col("editado_manual")).height),
            "fatores_invalidos": int(invalidos.height),
            "fatores_extremos_altos": int(extremos_altos.height),
            "fatores_extremos_baixos": int(extremos_baixos.height),
            "grupos_muitas_unidades": int(grupos_unidades.height),
            "grupos_alta_variacao": int(grupos_variacao.height),
        },
        "issues": issues[:200],
    }
