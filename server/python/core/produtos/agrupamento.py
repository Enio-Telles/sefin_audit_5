from typing import Any
from pathlib import Path
from datetime import UTC, datetime
import polars as pl
from fastapi import HTTPException
import json
import re

from core.produto_runtime import (
    unificar_produtos_unidades,
    obter_status_vectorizacao
)
from core.produto_batch_lote import (
    construir_preview_unificacao_lote,
    filtrar_tabela_final_para_lote,
    ocultar_grupos_verificados,
    RULE_PRIORITY as BATCH_RULE_PRIORITY
)
from core.models import UnificacaoLotePreviewRequest
from core.produtos.utils import _load_cnpj_dirs, _similarity_score, _primary_value
from core.produtos.persistence import _gravar_status_analise

def _reprocessar_produtos(dir_analises: Path, cnpj_limpo: str) -> pl.DataFrame:
    df_result = unificar_produtos_unidades(cnpj_limpo, projeto_dir=_PROJETO_DIR)
    _gravar_status_analise(dir_analises, cnpj_limpo)
    return df_result


def _carregar_pares_preview_lote(
    cnpj_limpo: str,
    dir_analises: Path,
    df_agregados_filtrados: pl.DataFrame,
    engine: str,
    use_cache: bool,
    top_k: int,
    min_score: float,
) -> tuple[pl.DataFrame, str]:
    engine_norm = str(engine or "DOCUMENTAL").strip().upper()
    visible_keys = set(df_agregados_filtrados.get_column("chave_produto").cast(pl.Utf8).to_list()) if "chave_produto" in df_agregados_filtrados.columns else set()

    if engine_norm == "FAISS":
        pares_path = dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.parquet"
        if use_cache and pares_path.exists():
            df_pairs = pl.read_parquet(str(pares_path))
        else:
            df_pairs = construir_tabela_pares_descricoes_faiss(
                df_agregados_filtrados,
                top_k=max(2, min(int(top_k), 20)),
                min_score=max(0.30, min(float(min_score), 0.98)),
                batch_size=32,
            )
    elif engine_norm == "LIGHT":
        pares_path = dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.parquet"
        if use_cache and pares_path.exists():
            df_pairs = pl.read_parquet(str(pares_path))
        else:
            df_pairs = construir_tabela_pares_descricoes_light(
                df_agregados_filtrados,
                top_k=max(2, min(int(top_k), 20)),
                min_score=max(0.30, min(float(min_score), 0.98)),
            )
    else:
        df_pairs = construir_tabela_pares_descricoes_similares(df_agregados_filtrados)
        engine_norm = "DOCUMENTAL"

    if visible_keys and not df_pairs.is_empty():
        df_pairs = df_pairs.filter(
            pl.col("chave_produto_a").cast(pl.Utf8).is_in(sorted(visible_keys))
            & pl.col("chave_produto_b").cast(pl.Utf8).is_in(sorted(visible_keys))
        )
    return df_pairs, engine_norm


def _empty_batch_filters():
    return type("Filters", (), {"descricao_contains": "", "ncm_contains": "", "cest_contains": "", "show_verified": False})()


def _empty_batch_options():
    return type("Options", (), {"only_visible": True, "require_all_pairs_compatible": True, "max_component_size": 12})()


def _empty_batch_similarity():
    return type("Similarity", (), {"engine": "DOCUMENTAL", "use_cache": True, "top_k": 8, "min_score": 0.72})()


def _run_preview_unificacao_lote(req: UnificacaoLotePreviewRequest) -> dict[str, Any]:
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
    agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"
    if not agregados_path.exists():
        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "source_context": str(req.source_context or "REVISAO_FINAL"),
            "similarity_source": {"engine": "DOCUMENTAL", "use_cache": True, "top_k": 8, "min_score": 0.72},
            "rule_ids": list(BATCH_RULE_PRIORITY),
            "dataset_hash": None,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "resumo": {
                "total_rows_considered": 0,
                "total_candidate_pairs": 0,
                "total_components": 0,
                "total_proposals": 0,
                "by_rule": [],
            },
            "proposals": [],
        }

    filters = req.filters or _empty_batch_filters()
    options = req.options or _empty_batch_options()
    similarity = req.similarity_source or _empty_batch_similarity()
    requested_rules = [str(rule or "").strip() for rule in (req.rule_ids or list(BATCH_RULE_PRIORITY))]
    rule_ids = [rule for rule in BATCH_RULE_PRIORITY if rule in requested_rules]
    if not rule_ids:
        raise HTTPException(status_code=400, detail="Nenhuma regra de lote suportada foi informada.")

    df_agregados = pl.read_parquet(str(agregados_path))
    df_agregados = filtrar_tabela_final_para_lote(
        df_agregados,
        descricao_contains=str(filters.descricao_contains or ""),
        ncm_contains=str(filters.ncm_contains or ""),
        cest_contains=str(filters.cest_contains or ""),
    )
    if bool(options.only_visible):
        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        df_status = pl.read_parquet(str(status_path)) if status_path.exists() else None
        df_agregados = ocultar_grupos_verificados(df_agregados, df_status, bool(filters.show_verified))

    if df_agregados.is_empty():
        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "source_context": str(req.source_context or "REVISAO_FINAL"),
            "similarity_source": {
                "engine": str(similarity.engine or "DOCUMENTAL").strip().upper(),
                "use_cache": bool(similarity.use_cache),
                "top_k": int(similarity.top_k or 8),
                "min_score": float(similarity.min_score or 0.72),
            },
            "rule_ids": rule_ids,
            "dataset_hash": compute_file_sha1(agregados_path),
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "resumo": {
                "total_rows_considered": 0,
                "total_candidate_pairs": 0,
                "total_components": 0,
                "total_proposals": 0,
                "by_rule": [
                    {"rule_id": rule_id, "button_label": BATCH_RULE_CONFIG[rule_id]["button_label"], "proposal_count": 0, "group_count": 0}
                    for rule_id in rule_ids
                ],
            },
            "proposals": [],
        }

    df_pairs, engine_used = _carregar_pares_preview_lote(
        cnpj_limpo,
        dir_analises,
        df_agregados,
        engine=str(similarity.engine or "DOCUMENTAL"),
        use_cache=bool(similarity.use_cache),
        top_k=int(similarity.top_k or 8),
        min_score=float(similarity.min_score or 0.72),
    )
    preview = construir_preview_unificacao_lote(
        df_agregados,
        df_pairs,
        rule_ids=rule_ids,
        source_method=engine_used,
        require_all_pairs_compatible=bool(options.require_all_pairs_compatible),
        max_component_size=max(2, min(int(options.max_component_size or 12), 50)),
    )
    return {
        "success": True,
        "cnpj": cnpj_limpo,
        "source_context": str(req.source_context or "REVISAO_FINAL"),
        "similarity_source": {
            "engine": engine_used,
            "use_cache": bool(similarity.use_cache),
            "top_k": int(similarity.top_k or 8),
            "min_score": float(similarity.min_score or 0.72),
        },
        "rule_ids": rule_ids,
        "dataset_hash": compute_file_sha1(agregados_path),
        **preview,
    }


def _carregar_codigo_multidescricao_resumo(dir_analises: Path, cnpj_limpo: str, codigo_limpo: str) -> dict[str, Any]:
    path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"
    path_indexados = dir_analises / f"produtos_indexados_{cnpj_limpo}.parquet"

    resumo: dict[str, Any] = {}
    grupos_descricao: list[dict[str, Any]] = []
    opcoes_consenso: dict[str, list[dict[str, Any]]] = {"descricao": [], "ncm": [], "cest": [], "gtin": []}

    if path_codigos.exists():
        df_codigos = pl.read_parquet(str(path_codigos)).with_columns(pl.col("codigo").cast(pl.Utf8))
        row = df_codigos.filter(pl.col("codigo") == codigo_limpo)
        if not row.is_empty():
            resumo = row.to_dicts()[0]

    if path_indexados.exists():
        df_indexados = (
            pl.read_parquet(str(path_indexados))
            .with_columns(
                [
                    pl.col("codigo").cast(pl.Utf8),
                    pl.col("descricao").cast(pl.Utf8),
                    pl.col("descr_compl").cast(pl.Utf8),
                    pl.col("tipo_item").cast(pl.Utf8),
                    pl.col("ncm").cast(pl.Utf8),
                    pl.col("cest").cast(pl.Utf8),
                    pl.col("gtin").cast(pl.Utf8),
                    pl.col("lista_unidades").cast(pl.Utf8),
                    pl.col("lista_fontes").cast(pl.Utf8),
                ]
            )
            .filter(pl.col("codigo") == codigo_limpo)
        )

        if not df_indexados.is_empty():
            grupos_descricao = (
                df_indexados.group_by("descricao")
                .agg(
                    [
                        pl.sum("qtd_linhas").cast(pl.Int64).alias("qtd_linhas"),
                        pl.len().cast(pl.Int64).alias("qtd_combinacoes"),
                        pl.col("chave_produto").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_chave"),
                        pl.col("descr_compl").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_descr_compl"),
                        pl.col("tipo_item").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_tipo_item"),
                        pl.col("ncm").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_ncm"),
                        pl.col("cest").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_cest"),
                        pl.col("gtin").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_gtin"),
                        pl.col("lista_unidades").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_unidades"),
                        pl.col("lista_fontes").drop_nulls().cast(pl.Utf8).unique().sort().implode().alias("__lista_fontes"),
                    ]
                )
                .with_columns(
                    [
                        pl.col("__lista_chave").list.join(", ").alias("lista_chave_produto"),
                        pl.col("__lista_descr_compl").list.join(" | ").alias("lista_descr_compl"),
                        pl.col("__lista_tipo_item").list.join(", ").alias("lista_tipo_item"),
                        pl.col("__lista_ncm").list.join(", ").alias("lista_ncm"),
                        pl.col("__lista_cest").list.join(", ").alias("lista_cest"),
                        pl.col("__lista_gtin").list.join(", ").alias("lista_gtin"),
                        pl.col("__lista_unidades").list.join(" | ").alias("lista_unidades"),
                        pl.col("__lista_fontes").list.join(" | ").alias("lista_fontes"),
                    ]
                )
                .drop(
                    [
                        "__lista_chave",
                        "__lista_descr_compl",
                        "__lista_tipo_item",
                        "__lista_ncm",
                        "__lista_cest",
                        "__lista_gtin",
                        "__lista_unidades",
                        "__lista_fontes",
                    ]
                )
                .sort(["qtd_linhas", "descricao"], descending=[True, False])
                .to_dicts()
            )

            for field_name in ["descricao", "ncm", "cest", "gtin"]:
                df_opcoes = (
                    df_indexados.group_by(field_name)
                    .agg(pl.sum("qtd_linhas").cast(pl.Int64).alias("qtd_linhas"))
                    .rename({field_name: "valor"})
                    .with_columns(pl.col("valor").cast(pl.Utf8).fill_null(""))
                    .sort(["qtd_linhas", "valor"], descending=[True, False])
                )
                opcoes_consenso[field_name] = df_opcoes.to_dicts()

    return {
        "codigo": codigo_limpo,
        "resumo": resumo,
        "grupos_descricao": grupos_descricao,
        "opcoes_consenso": opcoes_consenso,
    }


def _carregar_detalhes_codigo(dir_analises: Path, cnpj_limpo: str, codigo: str) -> list[dict[str, Any]]:
    detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
    agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

    if not detalhes_path.exists():
        return []

    lf = pl.scan_parquet(str(detalhes_path))

    if str(codigo).startswith("ID_"):
        if agregados_path.exists():
            df_agregado = pl.read_parquet(str(agregados_path))
            row = df_agregado.filter(pl.col("chave_produto") == codigo)
            if not row.is_empty():
                descr = row["descricao"][0]
                return lf.filter(pl.col("descricao") == descr).collect().to_dicts()

    if str(codigo).upper().endswith("_AGR"):
        codigo_real = codigo.rsplit("_", 1)[0]
        codigo_norm = codigo_real.lstrip("0") or "0"
        return lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect().to_dicts()

    if "_" in codigo:
        parts = codigo.rsplit("_", 1)
        codigo_real = parts[0]
        tipo_item_val = parts[1]
        codigo_norm = codigo_real.lstrip("0") or "0"
        return (
            lf.filter((pl.col("codigo").str.replace("^0+", "") == codigo_norm) & (pl.col("tipo_item") == tipo_item_val))
            .collect()
            .to_dicts()
        )

    codigo_norm = codigo.lstrip("0") or "0"
    return lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect().to_dicts()


def _build_auto_separate_plan_backend(codigo: str, grupos: list[dict[str, Any]], modo: str) -> dict[str, Any]:
    ordered = sorted(
        grupos,
        key=lambda item: (-int(item.get("qtd_linhas") or 0), str(item.get("descricao") or "")),
    )
    if len(ordered) < 2:
        return {"eligible": False, "reason": "O codigo nao possui descricoes suficientes para auto-separacao."}

    all_cest_empty = all(not _primary_value(grupo.get("lista_cest")) for grupo in ordered)

    for i in range(len(ordered)):
        for j in range(i + 1, len(ordered)):
            grupo_a = ordered[i]
            grupo_b = ordered[j]
            score = _similarity_score(grupo_a.get("descricao"), grupo_b.get("descricao"))
            ncm_a = _primary_value(grupo_a.get("lista_ncm"))
            ncm_b = _primary_value(grupo_b.get("lista_ncm"))
            cest_a = _primary_value(grupo_a.get("lista_cest"))
            cest_b = _primary_value(grupo_b.get("lista_cest"))
            gtin_a = _primary_value(grupo_a.get("lista_gtin"))
            gtin_b = _primary_value(grupo_b.get("lista_gtin"))
            ncm_distinct = bool(ncm_a and ncm_b and ncm_a != ncm_b)
            cest_distinct = bool((cest_a or cest_b) and cest_a != cest_b)
            cest_compatible = all_cest_empty or cest_distinct
            gtin_distinct = bool(gtin_a and gtin_b and gtin_a != gtin_b)
            very_dissimilar = score <= 0.2

            criteria_matched = (
                very_dissimilar and ncm_distinct and cest_compatible and gtin_distinct
                if modo == "NCM_CEST_GTIN"
                else very_dissimilar and ncm_distinct and gtin_distinct
                if modo == "NCM_GTIN"
                else very_dissimilar and ncm_distinct
                if modo == "NCM_ONLY"
                else very_dissimilar
            )

            if not criteria_matched:
                reason = (
                    "As descricoes ainda tem similaridade relevante entre si."
                    if not very_dissimilar
                    else "Nem todos os pares possuem NCM e GTIN distintos e o CEST e distinto quando informado."
                    if modo == "NCM_CEST_GTIN"
                    else "Nem todos os pares possuem NCM e GTIN distintos e preenchidos."
                    if modo == "NCM_GTIN"
                    else "Nem todos os pares possuem NCM distinto e preenchido."
                    if modo == "NCM_ONLY"
                    else "As descricoes nao atingiram o criterio de separacao textual."
                )
                return {"eligible": False, "reason": reason}

    plan = [
        {
            "descricao": str(grupo.get("descricao") or ""),
            "codigo_novo": codigo if index == 0 else f"{codigo}_{index}",
            "descricao_nova": str(grupo.get("descricao") or ""),
            "ncm_novo": _primary_value(grupo.get("lista_ncm")),
            "cest_novo": _primary_value(grupo.get("lista_cest")),
            "gtin_novo": _primary_value(grupo.get("lista_gtin")),
        }
        for index, grupo in enumerate(ordered)
    ]
    return {"eligible": True, "plan": plan}
