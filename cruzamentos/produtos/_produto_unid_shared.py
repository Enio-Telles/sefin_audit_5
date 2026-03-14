from __future__ import annotations

import hashlib
import logging
from typing import Dict

import polars as pl


SCHEMA_FISCAL_PRODUTOS = {
    "codigo": pl.String,
    "descricao": pl.String,
    "descricao_ori": pl.String,
    "descr_compl": pl.String,
    "ncm": pl.Categorical,
    "cest": pl.Categorical,
    "gtin": pl.String,
    "unid": pl.Categorical,
    "tipo_item": pl.Categorical,
    "fonte": pl.Categorical,
}

MANUAL_MAP_COLUMNS = [
    "fonte",
    "codigo_original",
    "descricao_original",
    "tipo_item_original",
    "hash_manual_key",
    "codigo_novo",
    "descricao_nova",
    "ncm_novo",
    "cest_novo",
    "gtin_novo",
    "tipo_item_novo",
    "acao_manual",
]

DESCRIPTION_MANUAL_MAP_COLUMNS = [
    "tipo_regra",
    "descricao_origem",
    "descricao_destino",
    "descricao_par",
    "hash_descricoes_key",
    "chave_grupo_a",
    "chave_grupo_b",
    "score_origem",
    "acao_manual",
]

SOURCE_PRIORITY = {
    "EFD_0200": 0,
    "Bloco_H": 1,
    "EFD_C170": 2,
    "NFe": 3,
    "NFCe": 4,
}

AUTO_CONSENSO_FIELDS = ("tipo_item", "ncm", "cest", "gtin")

FONTE_PIPELINE_CONFIGS = {
    "nfe": {
        "nome": "NFe",
        "arquivo": "NFe_{cnpj}.parquet",
        "mapping": {
            "codigo": ["CPROD", "prod_cprod"],
            "descricao": ["XPROD", "prod_xprod"],
            "ncm": ["NCM", "prod_ncm"],
            "cest": ["CEST", "prod_cest"],
            "gtin": ["CEAN", "prod_cean"],
            "unid": ["UCOM", "prod_ucom"],
        },
    },
    "nfce": {
        "nome": "NFCe",
        "arquivo": "NFCe_{cnpj}.parquet",
        "mapping": {
            "codigo": ["CPROD", "prod_cprod"],
            "descricao": ["XPROD", "prod_xprod"],
            "ncm": ["NCM", "prod_ncm"],
            "cest": ["CEST", "prod_cest"],
            "gtin": ["CEAN", "prod_cean"],
            "unid": ["UCOM", "prod_ucom"],
        },
    },
    "c170": {
        "nome": "EFD_C170",
        "arquivo": "c170_simplificada_{cnpj}.parquet",
        "mapping": {
            "codigo": ["COD_ITEM", "cod_item"],
            "descricao": ["DESCR_ITEM", "descr_item"],
            "descr_compl": ["DESCR_COMPL", "descr_compl"],
            "unid": ["UNID", "unid"],
            "ncm": ["COD_NCM", "cod_ncm"],
            "cest": ["CEST", "cest"],
            "gtin": ["COD_BARRA", "cod_barra"],
            "tipo_item": ["TIPO_ITEM", "tipo_item"],
        },
    },
    "c0200": {
        "nome": "EFD_0200",
        "arquivo": "reg_0200_{cnpj}.parquet",
        "mapping": {
            "codigo": ["COD_ITEM", "cod_item"],
            "descricao": ["DESCR_ITEM", "descr_item"],
            "ncm": ["COD_NCM", "cod_ncm"],
            "unid": ["UNID_INV", "unid_inv"],
            "tipo_item": ["TIPO_ITEM", "tipo_item"],
            "gtin": ["COD_BARRA", "cod_barra"],
        },
    },
    "bloco_h": {
        "nome": "Bloco_H",
        "arquivo": "bloco_h_{cnpj}.parquet",
        "mapping": {
            "codigo": ["CODIGO_PRODUTO", "codigo_produto"],
            "descricao": ["DESCRICAO_PRODUTO", "descricao_produto"],
            "ncm": ["COD_NCM", "cod_ncm"],
            "cest": ["CEST", "cest"],
            "gtin": ["COD_BARRA", "cod_barra"],
            "unid": ["UNIDADE_MEDIDA", "unidade_medida"],
            "tipo_item": ["TIPO_ITEM", "tipo_item"],
        },
    },
}


def limpar_caracteres_especiais(expr: pl.Expr, manter_espacos: bool = False) -> pl.Expr:
    expr_limpa = expr.str.to_uppercase().str.strip_chars()
    expr_limpa = expr_limpa.str.replace_all(r"[^A-Z0-9\s/.-]", "")

    if not manter_espacos:
        expr_limpa = expr_limpa.str.replace_all(r"\s+", "")

    return expr_limpa


def _canon_expr(col_name: str, vazio: str = "(VAZIO)") -> pl.Expr:
    expr = pl.col(col_name).cast(pl.Utf8).fill_null("").str.strip_chars().str.to_uppercase()
    return pl.when(expr == "").then(pl.lit(vazio)).otherwise(expr)


def _hash_key_expr_from_cols(fonte_col: str, codigo_col: str, descricao_col: str, tipo_item_col: str) -> pl.Expr:
    payload = pl.concat_str(
        [
            _canon_expr(fonte_col),
            _canon_expr(codigo_col),
            _canon_expr(descricao_col),
            _canon_expr(tipo_item_col),
        ],
        separator="|",
    )
    return payload.map_elements(lambda x: hashlib.sha1(str(x).encode("utf-8")).hexdigest(), return_dtype=pl.Utf8)


def _ensure_columns_lazy(lf: pl.LazyFrame, cols: list[str]) -> pl.LazyFrame:
    present = set(lf.collect_schema().names())
    exprs = []
    for col in cols:
        if col not in present:
            exprs.append(pl.lit(None).cast(pl.Utf8).alias(col))
    if exprs:
        return lf.with_columns(exprs)
    return lf


def _lazy_empty_produtos_schema() -> pl.LazyFrame:
    return pl.DataFrame(schema=SCHEMA_FISCAL_PRODUTOS).lazy()


def _lista_valor_auditavel_expr(col_name: str) -> pl.Expr:
    expr = pl.col(col_name).cast(pl.Utf8).fill_null("(nulo)").str.strip_chars()
    expr = pl.when(expr == "").then(pl.lit("(nulo)")).otherwise(expr)
    return expr


def _normalized_text_expr(col_name: str) -> pl.Expr:
    return pl.col(col_name).cast(pl.Utf8).fill_null("").str.strip_chars()


def _manual_value_or_null_expr(col_name: str) -> pl.Expr:
    expr = _normalized_text_expr(col_name)
    return pl.when(expr == "").then(pl.lit(None)).otherwise(expr)


def _source_priority_expr(col_name: str = "fonte") -> pl.Expr:
    expr = pl.lit(len(SOURCE_PRIORITY))
    for fonte, prioridade in SOURCE_PRIORITY.items():
        expr = pl.when(pl.col(col_name).cast(pl.Utf8) == fonte).then(pl.lit(prioridade)).otherwise(expr)
    return expr.cast(pl.Int64)


def _resolver_coluna_origem(cols_presentes: list[str], coluna_mapeada) -> str | None:
    if not coluna_mapeada:
        return None

    candidatos = coluna_mapeada if isinstance(coluna_mapeada, (list, tuple)) else [coluna_mapeada]

    for candidato in candidatos:
        if candidato in cols_presentes:
            return candidato

    by_lower = {c.lower(): c for c in cols_presentes}
    for candidato in candidatos:
        resolved = by_lower.get(str(candidato).lower())
        if resolved:
            return resolved

    lower_cols = [c.lower() for c in cols_presentes]
    for candidato in candidatos:
        token = str(candidato).lower().strip()
        if not token:
            continue

        suffix_matches = [cols_presentes[i] for i, c in enumerate(lower_cols) if c.endswith(f"_{token}")]
        prod_matches = [c for c in suffix_matches if c.lower().startswith("prod_")]
        if len(prod_matches) == 1:
            return prod_matches[0]
        if len(suffix_matches) == 1:
            return suffix_matches[0]

        prefix_matches = [cols_presentes[i] for i, c in enumerate(lower_cols) if c.startswith(f"{token}_")]
        if len(prefix_matches) == 1:
            return prefix_matches[0]
    return None


def aplicar_mapeamento_e_schema(
    lf: pl.LazyFrame,
    mapping: Dict[str, object],
    fonte_nome: str,
) -> pl.LazyFrame:
    cols_presentes = lf.collect_schema().names()
    resolved_mapping = {
        destino: _resolver_coluna_origem(cols_presentes, origem)
        for destino, origem in mapping.items()
    }

    required = ("codigo", "descricao")
    required_missing = [k for k in required if not resolved_mapping.get(k)]
    logging.info(
        "[%s] Mapeamento resolvido: codigo=%s descricao=%s ncm=%s cest=%s gtin=%s unid=%s tipo_item=%s",
        fonte_nome,
        resolved_mapping.get("codigo"),
        resolved_mapping.get("descricao"),
        resolved_mapping.get("ncm"),
        resolved_mapping.get("cest"),
        resolved_mapping.get("gtin"),
        resolved_mapping.get("unid"),
        resolved_mapping.get("tipo_item"),
    )
    if required_missing:
        logging.warning(
            "[%s] Mapeamento invalido. Ausentes obrigatorias %s. Colunas disponiveis (amostra): %s",
            fonte_nome,
            required_missing,
            cols_presentes[:20],
        )
        return _lazy_empty_produtos_schema()

    missing_optional = [k for k, v in resolved_mapping.items() if v is None and k not in required]
    if missing_optional:
        logging.info("[%s] Colunas opcionais nao mapeadas: %s", fonte_nome, missing_optional)

    exprs = []
    for col_destino, dtype in SCHEMA_FISCAL_PRODUTOS.items():
        if col_destino == "fonte":
            exprs.append(pl.lit(fonte_nome).cast(pl.Categorical).alias("fonte"))
            continue

        if col_destino == "descricao_ori":
            col_origem_desc = resolved_mapping.get("descricao")
            if col_origem_desc:
                exprs.append(pl.col(col_origem_desc).cast(pl.String).alias("descricao_ori"))
            else:
                exprs.append(pl.lit(None).cast(pl.String).alias("descricao_ori"))
            continue

        col_origem = resolved_mapping.get(col_destino)
        if col_origem:
            expr = pl.col(col_origem)

            if col_destino in ["codigo", "ncm", "cest", "unid"]:
                expr = limpar_caracteres_especiais(expr.cast(pl.String), manter_espacos=False)
                if col_destino == "codigo":
                    expr = expr.str.strip_chars_start("0")
            elif col_destino in ["descricao", "descr_compl"]:
                expr = limpar_caracteres_especiais(expr.cast(pl.String), manter_espacos=True)
            elif col_destino == "gtin":
                gtin_limpo = expr.cast(pl.String).str.replace_all(r"[^0-9]", "")
                expr = pl.when(gtin_limpo.str.len_chars().is_in([8, 12, 13, 14])).then(gtin_limpo).otherwise(pl.lit(None))
            elif col_destino == "tipo_item":
                expr = expr.fill_null("(Vazio)").cast(pl.String).str.strip_chars()
                expr = pl.when(expr == "").then(pl.lit("(Vazio)")).otherwise(expr)

            exprs.append(expr.cast(dtype).alias(col_destino))
        else:
            exprs.append(pl.lit(None).cast(dtype).alias(col_destino))

    return lf.select(exprs).drop_nulls(subset=["codigo", "descricao"])


__all__ = [
    "AUTO_CONSENSO_FIELDS",
    "DESCRIPTION_MANUAL_MAP_COLUMNS",
    "FONTE_PIPELINE_CONFIGS",
    "MANUAL_MAP_COLUMNS",
    "SCHEMA_FISCAL_PRODUTOS",
    "SOURCE_PRIORITY",
    "_canon_expr",
    "_ensure_columns_lazy",
    "_hash_key_expr_from_cols",
    "_lazy_empty_produtos_schema",
    "_lista_valor_auditavel_expr",
    "_manual_value_or_null_expr",
    "_normalized_text_expr",
    "_resolver_coluna_origem",
    "_source_priority_expr",
    "aplicar_mapeamento_e_schema",
    "limpar_caracteres_especiais",
]
