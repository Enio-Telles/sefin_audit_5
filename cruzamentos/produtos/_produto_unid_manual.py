from __future__ import annotations

import hashlib
import logging
import os

import polars as pl

if __package__:
    from ._produto_unid_shared import (
        DESCRIPTION_MANUAL_MAP_COLUMNS,
        MANUAL_MAP_COLUMNS,
        _canon_expr,
        _ensure_columns_lazy,
        _hash_key_expr_from_cols,
        _manual_value_or_null_expr,
    )
else:
    from _produto_unid_shared import (
        DESCRIPTION_MANUAL_MAP_COLUMNS,
        MANUAL_MAP_COLUMNS,
        _canon_expr,
        _ensure_columns_lazy,
        _hash_key_expr_from_cols,
        _manual_value_or_null_expr,
    )


def _hash_descricao_rule(
    tipo_regra: str,
    descricao_origem: str,
    descricao_destino: str,
    descricao_par: str,
) -> str:
    payload = "|".join(
        [
            str(tipo_regra).strip().upper(),
            str(descricao_origem).strip().upper(),
            str(descricao_destino).strip().upper(),
            str(descricao_par).strip().upper(),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _blocked_pair_key(descricao_a: str, descricao_b: str) -> tuple[str, str]:
    desc_a = str(descricao_a).strip().upper()
    desc_b = str(descricao_b).strip().upper()
    return tuple(sorted((desc_a, desc_b)))


def _resolver_regras_descricoes(df_rules: pl.DataFrame) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    df_rules_unir = df_rules.filter(
        (pl.col("tipo_regra") == "UNIR_GRUPOS")
        & (pl.col("descricao_origem") != "")
        & (pl.col("descricao_destino") != "")
    )
    df_rules_block = df_rules.filter(
        (pl.col("tipo_regra") == "MANTER_SEPARADO")
        & (pl.col("descricao_origem") != "")
        & (pl.col("descricao_par") != "")
    )

    if df_rules_unir.is_empty():
        return [], []

    blocked_pairs = {
        _blocked_pair_key(row["descricao_origem"], row["descricao_par"])
        for row in df_rules_block.select(["descricao_origem", "descricao_par"]).to_dicts()
    }

    def _is_blocked_pair(descricao_a: str, descricao_b: str) -> bool:
        if not descricao_a or not descricao_b or descricao_a == descricao_b:
            return False
        return _blocked_pair_key(descricao_a, descricao_b) in blocked_pairs

    mapping = {
        str(row["descricao_origem"]): str(row["descricao_destino"])
        for row in df_rules_unir.select(["descricao_origem", "descricao_destino"]).to_dicts()
    }

    def _resolver_destino(descricao: str) -> str:
        current = descricao
        visited = set()
        while current in mapping and mapping[current] and mapping[current] != current and current not in visited:
            visited.add(current)
            current = mapping[current]
        return current

    accepted_members_by_destino: dict[str, set[str]] = {}
    resolved_rows: list[dict[str, str]] = []
    blocked_rows: list[dict[str, str]] = []

    for origem in sorted(mapping):
        destino_imediato = mapping.get(origem, "")
        destino_final = _resolver_destino(origem)
        if origem == destino_final:
            continue

        if _is_blocked_pair(origem, destino_final):
            blocked_rows.append(
                {
                    "tipo_regra": "UNIR_GRUPOS",
                    "descricao_origem": origem,
                    "descricao_destino": destino_imediato,
                    "descricao_destino_resolvido": destino_final,
                    "motivo_bloqueio": "MANTER_SEPARADO_DIRETO",
                    "descricao_bloqueante": destino_final,
                }
            )
            continue

        membros_destino = accepted_members_by_destino.setdefault(destino_final, {destino_final})
        blocked_member = next((membro for membro in membros_destino if _is_blocked_pair(origem, membro)), None)
        if blocked_member:
            blocked_rows.append(
                {
                    "tipo_regra": "UNIR_GRUPOS",
                    "descricao_origem": origem,
                    "descricao_destino": destino_imediato,
                    "descricao_destino_resolvido": destino_final,
                    "motivo_bloqueio": "MANTER_SEPARADO_CONVERGENCIA_GRUPO",
                    "descricao_bloqueante": blocked_member,
                }
            )
            continue

        membros_destino.add(origem)
        resolved_rows.append(
            {
                "descricao_origem": origem,
                "descricao_destino": destino_final,
            }
        )

    return resolved_rows, blocked_rows


def _normalize_mapa_descricoes_manual(df: pl.DataFrame, default_acao: str = "AGREGAR") -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in DESCRIPTION_MANUAL_MAP_COLUMNS})

    for col in DESCRIPTION_MANUAL_MAP_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    df = df.with_columns(
        [
            _canon_expr("tipo_regra", "UNIR_GRUPOS").alias("tipo_regra"),
            _canon_expr("descricao_origem", "").alias("descricao_origem"),
            _canon_expr("descricao_destino", "").alias("descricao_destino"),
            _canon_expr("descricao_par", "").alias("descricao_par"),
            _canon_expr("chave_grupo_a", "").alias("chave_grupo_a"),
            _canon_expr("chave_grupo_b", "").alias("chave_grupo_b"),
            pl.col("score_origem").cast(pl.Utf8).fill_null("").str.strip_chars().alias("score_origem"),
            _canon_expr("acao_manual", default_acao).alias("acao_manual"),
        ]
    )

    rows = []
    for row in df.select(DESCRIPTION_MANUAL_MAP_COLUMNS).to_dicts():
        tipo_regra = row.get("tipo_regra") or "UNIR_GRUPOS"
        descricao_origem = row.get("descricao_origem") or ""
        descricao_destino = row.get("descricao_destino") or ""
        descricao_par = row.get("descricao_par") or ""

        if tipo_regra == "MANTER_SEPARADO" and descricao_origem and descricao_par:
            descricao_origem, descricao_par = _blocked_pair_key(descricao_origem, descricao_par)
            descricao_destino = ""

        hash_key = row.get("hash_descricoes_key") or _hash_descricao_rule(
            tipo_regra,
            descricao_origem,
            descricao_destino,
            descricao_par,
        )
        rows.append(
            {
                "tipo_regra": tipo_regra,
                "descricao_origem": descricao_origem,
                "descricao_destino": descricao_destino,
                "descricao_par": descricao_par,
                "hash_descricoes_key": str(hash_key),
                "chave_grupo_a": row.get("chave_grupo_a") or "",
                "chave_grupo_b": row.get("chave_grupo_b") or "",
                "score_origem": row.get("score_origem") or "",
                "acao_manual": row.get("acao_manual") or default_acao,
            }
        )

    normalized = pl.DataFrame(rows).select(DESCRIPTION_MANUAL_MAP_COLUMNS)
    return normalized.unique(subset=["tipo_regra", "descricao_origem", "descricao_par"], keep="last")


def merge_mapa_descricoes_manual(
    mapa_path: str,
    df_novo: pl.DataFrame,
    default_acao: str = "AGREGAR",
) -> None:
    df_novo_norm = _normalize_mapa_descricoes_manual(df_novo, default_acao=default_acao)
    if os.path.exists(mapa_path):
        df_existente = pl.read_parquet(mapa_path)
        df_existente_norm = _normalize_mapa_descricoes_manual(df_existente, default_acao=default_acao)
        (
            pl.concat([df_existente_norm, df_novo_norm], how="diagonal_relaxed")
            .unique(subset=["tipo_regra", "descricao_origem", "descricao_par"], keep="last")
            .write_parquet(mapa_path)
        )
    else:
        df_novo_norm.write_parquet(mapa_path)


def aplicar_mapa_descricoes_manual(
    lf_base_detalhes: pl.LazyFrame,
    path_mapa_descricoes: str,
) -> pl.LazyFrame:
    if not os.path.exists(path_mapa_descricoes):
        return lf_base_detalhes

    logging.info("Aplicando Mapa Manual por Descricao...")
    df_rules = _normalize_mapa_descricoes_manual(
        pl.read_parquet(path_mapa_descricoes),
        default_acao="AGREGAR",
    )
    resolved_rows, blocked_rows = _resolver_regras_descricoes(df_rules)
    if not resolved_rows:
        if blocked_rows:
            logging.info(
                "Mapa manual por descricao: %s unioes bloqueadas por MANTER_SEPARADO.",
                len(blocked_rows),
            )
        return lf_base_detalhes
    if blocked_rows:
        direct_count = sum(1 for row in blocked_rows if row["motivo_bloqueio"] == "MANTER_SEPARADO_DIRETO")
        group_count = len(blocked_rows) - direct_count
        logging.info(
            "Mapa manual por descricao: %s unioes bloqueadas por MANTER_SEPARADO (%s diretas, %s por convergencia de grupo).",
            len(blocked_rows),
            direct_count,
            group_count,
        )

    if not resolved_rows:
        return lf_base_detalhes

    lf_rules = pl.DataFrame(resolved_rows).lazy()
    return (
        lf_base_detalhes.with_columns(_canon_expr("descricao", "").alias("__descricao_grupo_key"))
        .join(lf_rules, left_on="__descricao_grupo_key", right_on="descricao_origem", how="left")
        .with_columns(pl.coalesce(["descricao_destino", "descricao"]).alias("descricao"))
        .drop(["__descricao_grupo_key", "descricao_origem", "descricao_destino"], strict=False)
    )


def aplicar_mapa_revisao_manual(
    lf_base_detalhes: pl.LazyFrame,
    path_mapa_manual: str,
) -> pl.LazyFrame:
    if not os.path.exists(path_mapa_manual):
        return lf_base_detalhes

    logging.info("Aplicando Mapa de Revisao Manual...")
    lf_manual = pl.scan_parquet(path_mapa_manual)
    lf_manual = _ensure_columns_lazy(lf_manual, MANUAL_MAP_COLUMNS)
    lf_manual = lf_manual.with_columns(
        [
            _canon_expr("fonte", "").alias("fonte"),
            _canon_expr("codigo_original", "").alias("codigo_original"),
            _canon_expr("descricao_original").alias("descricao_original"),
            _canon_expr("tipo_item_original").alias("tipo_item_original"),
            _canon_expr("acao_manual", "AGREGAR").alias("acao_manual"),
            pl.when(pl.col("hash_manual_key").cast(pl.Utf8).fill_null("").str.strip_chars() == "")
            .then(_hash_key_expr_from_cols("fonte", "codigo_original", "descricao_original", "tipo_item_original"))
            .otherwise(pl.col("hash_manual_key").cast(pl.Utf8))
            .alias("hash_manual_key"),
        ]
    )
    lf_manual = lf_manual.select(
        [
            "hash_manual_key",
            "codigo_novo",
            "descricao_nova",
            "ncm_novo",
            "cest_novo",
            "gtin_novo",
            "tipo_item_novo",
        ]
    ).with_columns(
        [
            _manual_value_or_null_expr("codigo_novo").alias("codigo_novo"),
            _manual_value_or_null_expr("descricao_nova").alias("descricao_nova"),
            _manual_value_or_null_expr("ncm_novo").alias("ncm_novo"),
            _manual_value_or_null_expr("cest_novo").alias("cest_novo"),
            _manual_value_or_null_expr("gtin_novo").alias("gtin_novo"),
            _manual_value_or_null_expr("tipo_item_novo").alias("tipo_item_novo"),
        ]
    )

    lf_base_detalhes = lf_base_detalhes.with_columns(
        [
            _canon_expr("fonte", "").alias("__fonte_key"),
            _canon_expr("codigo", "").alias("__codigo_key"),
            _canon_expr("descricao_ori").alias("__descricao_ori_key"),
            _canon_expr("tipo_item").alias("__tipo_item_key"),
        ]
    ).with_columns(
        _hash_key_expr_from_cols(
            "__fonte_key", "__codigo_key", "__descricao_ori_key", "__tipo_item_key"
        ).alias("hash_manual_key")
    )

    return (
        lf_base_detalhes.join(
            lf_manual,
            on="hash_manual_key",
            how="left",
        )
        .with_columns(
            [
                pl.col("tipo_item_novo").is_not_null().alias("__manual_tipo_item_set"),
                pl.col("ncm_novo").is_not_null().alias("__manual_ncm_set"),
                pl.col("cest_novo").is_not_null().alias("__manual_cest_set"),
                pl.col("gtin_novo").is_not_null().alias("__manual_gtin_set"),
                pl.coalesce(["codigo_novo", "codigo"]).alias("codigo"),
                pl.coalesce(["descricao_nova", "descricao"]).alias("descricao"),
                pl.coalesce(["ncm_novo", "ncm"]).alias("ncm"),
                pl.coalesce(["cest_novo", "cest"]).alias("cest"),
                pl.coalesce(["gtin_novo", "gtin"]).alias("gtin"),
                pl.coalesce(["tipo_item_novo", "tipo_item"]).alias("tipo_item"),
            ]
        )
        .drop(
            [
                "codigo_novo",
                "descricao_nova",
                "ncm_novo",
                "cest_novo",
                "gtin_novo",
                "tipo_item_novo",
                "__fonte_key",
                "__codigo_key",
                "__descricao_ori_key",
                "__tipo_item_key",
            ]
        )
    )


def gerar_mapas_auditoria_manual(path_mapa_manual: str, diretorio_saida: str, cnpj: str) -> None:
    if not os.path.exists(path_mapa_manual):
        return

    df_manual = pl.read_parquet(path_mapa_manual)
    if "acao_manual" not in df_manual.columns:
        df_manual = df_manual.with_columns(pl.lit("AGREGAR").alias("acao_manual"))
    df_manual = df_manual.with_columns(_canon_expr("acao_manual", "AGREGAR").alias("acao_manual"))

    path_mapa_agregados = os.path.join(diretorio_saida, f"mapa_auditoria_agregados_{cnpj}.parquet")
    df_manual.filter(pl.col("acao_manual") == "AGREGAR").write_parquet(path_mapa_agregados)

    path_mapa_desagregados = os.path.join(diretorio_saida, f"mapa_auditoria_desagregados_{cnpj}.parquet")
    df_manual.filter(pl.col("acao_manual") == "DESAGREGAR").write_parquet(path_mapa_desagregados)


def gerar_mapa_auditoria_descricoes_manual(path_mapa_descricoes: str, diretorio_saida: str, cnpj: str) -> None:
    if not os.path.exists(path_mapa_descricoes):
        return

    df_descricoes = _normalize_mapa_descricoes_manual(
        pl.read_parquet(path_mapa_descricoes),
        default_acao="AGREGAR",
    )
    resolved_rows, blocked_rows = _resolver_regras_descricoes(df_descricoes)

    path_auditoria = os.path.join(diretorio_saida, f"mapa_auditoria_descricoes_{cnpj}.parquet")
    df_descricoes.write_parquet(path_auditoria)

    path_auditoria_aplicadas = os.path.join(
        diretorio_saida,
        f"mapa_auditoria_descricoes_aplicadas_{cnpj}.parquet",
    )
    pl.DataFrame(
        resolved_rows if resolved_rows else [],
        schema={
            "descricao_origem": pl.Utf8,
            "descricao_destino": pl.Utf8,
        },
    ).write_parquet(path_auditoria_aplicadas)

    path_auditoria_bloqueadas = os.path.join(
        diretorio_saida,
        f"mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet",
    )
    pl.DataFrame(
        blocked_rows if blocked_rows else [],
        schema={
            "tipo_regra": pl.Utf8,
            "descricao_origem": pl.Utf8,
            "descricao_destino": pl.Utf8,
            "descricao_destino_resolvido": pl.Utf8,
            "motivo_bloqueio": pl.Utf8,
            "descricao_bloqueante": pl.Utf8,
        },
    ).write_parquet(path_auditoria_bloqueadas)


__all__ = [
    "aplicar_mapa_descricoes_manual",
    "aplicar_mapa_revisao_manual",
    "gerar_mapa_auditoria_descricoes_manual",
    "gerar_mapas_auditoria_manual",
    "merge_mapa_descricoes_manual",
    "_resolver_regras_descricoes",
]
