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

    invalidos = work.filter(pl.col("fator").is_null() | (pl.col("fator") <= 0)).select(
        ["chave_produto", "ano_referencia", "unidade_origem", "fator"]
    )
    for row in invalidos.to_dicts():
        issues.append(
            _issue(
                "FATOR_INVALIDO",
                "critico",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                str(row.get("unidade_origem") or ""),
                row.get("fator"),
                "Fator ausente, zero ou negativo.",
                "Revisar fator manual e validar unidade de origem.",
            )
        )

    extremos_altos = work.filter(pl.col("fator") > FATOR_MAXIMO).select(
        ["chave_produto", "ano_referencia", "unidade_origem", "fator"]
    )
    for row in extremos_altos.to_dicts():
        issues.append(
            _issue(
                "FATOR_EXTREMO_ALTO",
                "critico",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                str(row.get("unidade_origem") or ""),
                row.get("fator"),
                f"Fator acima de {FATOR_MAXIMO:.0f}.",
                "Verificar se houve erro de unidade, embalagem ou digitacao de preco.",
            )
        )

    extremos_baixos = work.filter((pl.col("fator") > 0) & (pl.col("fator") < FATOR_MINIMO)).select(
        ["chave_produto", "ano_referencia", "unidade_origem", "fator"]
    )
    for row in extremos_baixos.to_dicts():
        issues.append(
            _issue(
                "FATOR_EXTREMO_BAIXO",
                "alto",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                str(row.get("unidade_origem") or ""),
                row.get("fator"),
                f"Fator abaixo de {FATOR_MINIMO:.3f}.",
                "Revisar unidade de referencia e consistencia fisica da conversao.",
            )
        )

    unidades_vazias = work.filter(pl.col("unidade_origem") == "").select(
        ["chave_produto", "ano_referencia", "unidade_origem", "fator"]
    )
    for row in unidades_vazias.to_dicts():
        issues.append(
            _issue(
                "UNIDADE_ORIGEM_VAZIA",
                "medio",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                "",
                row.get("fator"),
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
    for row in grupos_unidades.to_dicts():
        issues.append(
            _issue(
                "MULTIPLAS_UNIDADES",
                "medio",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                ", ".join(row.get("__lista_unidades") or []),
                None,
                f"Produto/ano com {row.get('qtd_unidades')} unidades diferentes.",
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
    for row in grupos_variacao.to_dicts():
        issues.append(
            _issue(
                "ALTA_VARIACAO_FATORES",
                "alto",
                str(row.get("chave_produto") or ""),
                row.get("ano_referencia"),
                "",
                row.get("variacao"),
                f"Variacao de {float(row.get('variacao') or 0):.2f}x entre menor e maior fator.",
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
            "editados_manual": int(work.filter(pl.col("editado_manual") == True).height),
            "fatores_invalidos": int(invalidos.height),
            "fatores_extremos_altos": int(extremos_altos.height),
            "fatores_extremos_baixos": int(extremos_baixos.height),
            "grupos_muitas_unidades": int(grupos_unidades.height),
            "grupos_alta_variacao": int(grupos_variacao.height),
        },
        "issues": issues[:200],
    }
