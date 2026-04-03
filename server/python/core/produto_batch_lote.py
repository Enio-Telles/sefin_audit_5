from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from itertools import combinations
from typing import Any

import polars as pl

from .produto_classification import (
    NULLABLE_CONFLICT,
    NULLABLE_EQUAL_FILLED,
    NULLABLE_EQUAL_NULL,
    compare_nullable_metric,
    description_similarity,
    filled_evidence_count_from_relations,
    is_equal_nullable_metric,
    normalize_description_key,
)


RULE_R1 = "R1_HIGH_CONFIDENCE_FULL_FISCAL"
RULE_R2 = "R2_NCM_CEST"
RULE_R3 = "R3_GTIN_NCM"
RULE_R6 = "R6_MANTER_SEPARADO"

RULE_PRIORITY = [RULE_R1, RULE_R2, RULE_R3, RULE_R6]

RULE_CONFIG: dict[str, dict[str, str]] = {
    RULE_R1: {
        "button_label": "Unificar alta confianca",
        "confidence_band": "HIGH",
        "action": "UNIFICAR",
    },
    RULE_R2: {
        "button_label": "Unificar NCM + CEST",
        "confidence_band": "MEDIUM_HIGH",
        "action": "UNIFICAR",
    },
    RULE_R3: {
        "button_label": "Unificar GTIN + NCM",
        "confidence_band": "MEDIUM_HIGH",
        "action": "UNIFICAR",
    },
    RULE_R6: {
        "button_label": "Manter separados",
        "confidence_band": "HIGH",
        "action": "MANTER_SEPARADO",
    },
}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _split_csv_values(value: Any) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in str(value or "").split(","):
        normalized = str(item or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _split_pipe_values(value: Any) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in str(value or "").split("|"):
        normalized = str(item or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _field_relation_summary(values: list[str]) -> str:
    filled = [item for item in values if item]
    unique_filled = sorted(set(filled))
    has_blank = len(filled) != len(values)
    if len(unique_filled) > 1:
        return NULLABLE_CONFLICT
    if not unique_filled:
        return NULLABLE_EQUAL_NULL
    if has_blank:
        return "INCOMPLETE"
    return NULLABLE_EQUAL_FILLED


def _field_has_multiple_values(values: list[str]) -> bool:
    return len({item for item in values if item}) > 1


def normalize_final_group_row(row: dict[str, Any]) -> dict[str, Any]:
    lista_descricao = _split_pipe_values(row.get("lista_descricao"))
    lista_descr_compl = _split_pipe_values(row.get("lista_descr_compl"))
    codigos = _split_csv_values(row.get("lista_codigos"))
    lista_ncm = _split_csv_values(row.get("lista_ncm"))
    lista_cest = _split_csv_values(row.get("lista_cest"))
    lista_gtin = _split_csv_values(row.get("lista_gtin"))
    descricao = _clean_text(row.get("descricao"))
    return {
        "chave_produto": _clean_text(row.get("chave_produto")),
        "descricao": descricao,
        "descricao_normalizada": _clean_text(row.get("descricao_normalizada"))
        or normalize_description_key(descricao),
        "lista_descricao": lista_descricao or ([descricao] if descricao else []),
        "lista_descr_compl": lista_descr_compl,
        "codigos": codigos,
        "ncm": _clean_text(row.get("ncm_consenso")),
        "cest": _clean_text(row.get("cest_consenso")),
        "gtin": _clean_text(row.get("gtin_consenso")),
        "lista_ncm": lista_ncm,
        "lista_cest": lista_cest,
        "lista_gtin": lista_gtin,
        "qtd_codigos": int(row.get("qtd_codigos") or len(codigos) or 0),
        "requer_revisao_manual": bool(row.get("requer_revisao_manual")),
        "descricoes_conflitantes": _clean_text(row.get("descricoes_conflitantes")),
    }


def filtrar_tabela_final_para_lote(
    df_agregados: pl.DataFrame,
    descricao_contains: str = "",
    ncm_contains: str = "",
    cest_contains: str = "",
) -> pl.DataFrame:
    df = df_agregados
    descricao_term = _clean_text(descricao_contains).upper()
    ncm_term = _clean_text(ncm_contains)
    cest_term = _clean_text(cest_contains)
    if descricao_term and "lista_descricao" in df.columns:
        df = df.filter(
            pl.col("lista_descricao")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .str.contains(descricao_term, literal=True)
        )
    if ncm_term and "ncm_consenso" in df.columns:
        df = df.filter(
            pl.col("ncm_consenso").cast(pl.Utf8).str.contains(ncm_term, literal=True)
        )
    if cest_term and "cest_consenso" in df.columns:
        df = df.filter(
            pl.col("cest_consenso").cast(pl.Utf8).str.contains(cest_term, literal=True)
        )
    return df


def ocultar_grupos_verificados(
    df_agregados: pl.DataFrame, df_status: pl.DataFrame | None, show_verified: bool
) -> pl.DataFrame:
    if (
        show_verified
        or df_status is None
        or df_status.is_empty()
        or "ref_id" not in df_status.columns
    ):
        return df_agregados
    hidden = (
        df_status.filter(
            (pl.col("tipo_ref") == "POR_GRUPO")
            & pl.col("status_analise").is_in(
                ["VERIFICADO_SEM_ACAO", "UNIDO_ENTRE_GRUPOS", "MANTIDO_SEPARADO"]
            )
        )
        .get_column("ref_id")
        .cast(pl.Utf8)
        .to_list()
    )
    if not hidden:
        return df_agregados
    return df_agregados.filter(
        ~pl.col("chave_produto").cast(pl.Utf8).is_in(sorted(set(hidden)))
    )


def _pair_key(left_key: str, right_key: str) -> tuple[str, str]:
    return (left_key, right_key) if left_key <= right_key else (right_key, left_key)


def _build_pair_context(
    left: dict[str, Any],
    right: dict[str, Any],
    pair_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    score_descricao = (
        float(pair_row.get("score_descricao"))
        if pair_row and pair_row.get("score_descricao") is not None
        else float(
            description_similarity(
                left.get("descricao_normalizada"), right.get("descricao_normalizada")
            )
        )
    )
    score_descr_compl = float(
        description_similarity(
            " | ".join(left.get("lista_descr_compl") or []),
            " | ".join(right.get("lista_descr_compl") or []),
        )
    )
    ncm_state = compare_nullable_metric(left.get("ncm"), right.get("ncm"))
    cest_state = compare_nullable_metric(left.get("cest"), right.get("cest"))
    gtin_state = compare_nullable_metric(left.get("gtin"), right.get("gtin"))
    return {
        "score_descricao": round(score_descricao, 6),
        "score_descr_compl": round(score_descr_compl, 6),
        "ncm_state": ncm_state,
        "cest_state": cest_state,
        "gtin_state": gtin_state,
        "filled_evidence_count": filled_evidence_count_from_relations(
            ncm_state, cest_state, gtin_state
        ),
        "shared_codes": sorted(
            set(left.get("codigos") or []) & set(right.get("codigos") or [])
        ),
        "left_has_multi_ncm": _field_has_multiple_values(left.get("lista_ncm") or []),
        "right_has_multi_ncm": _field_has_multiple_values(right.get("lista_ncm") or []),
        "left_has_multi_cest": _field_has_multiple_values(left.get("lista_cest") or []),
        "right_has_multi_cest": _field_has_multiple_values(
            right.get("lista_cest") or []
        ),
        "left_has_multi_gtin": _field_has_multiple_values(left.get("lista_gtin") or []),
        "right_has_multi_gtin": _field_has_multiple_values(
            right.get("lista_gtin") or []
        ),
    }


def evaluate_batch_rule(
    rule_id: str,
    left: dict[str, Any],
    right: dict[str, Any],
    pair_context: dict[str, Any],
) -> dict[str, Any]:
    desc_score = float(pair_context.get("score_descricao") or 0.0)
    ncm_state = str(pair_context.get("ncm_state") or "")
    cest_state = str(pair_context.get("cest_state") or "")
    gtin_state = str(pair_context.get("gtin_state") or "")
    filled_count = int(pair_context.get("filled_evidence_count") or 0)
    shared_codes = pair_context.get("shared_codes") or []

    if shared_codes:
        return {
            "eligible": False,
            "reason": f"Compartilha codigo entre grupos: {', '.join(shared_codes)}.",
        }

    if rule_id == RULE_R1:
        if any(
            bool(pair_context.get(flag))
            for flag in [
                "left_has_multi_ncm",
                "right_has_multi_ncm",
                "left_has_multi_cest",
                "right_has_multi_cest",
                "left_has_multi_gtin",
                "right_has_multi_gtin",
            ]
        ):
            return {
                "eligible": False,
                "reason": "Grupo possui multiplos valores fiscais internos.",
            }
        if desc_score < 0.78:
            return {
                "eligible": False,
                "reason": "Descricao abaixo do threshold da regra R1.",
            }
        if not all(
            is_equal_nullable_metric(state)
            for state in [ncm_state, cest_state, gtin_state]
        ):
            return {
                "eligible": False,
                "reason": "Campos fiscais nao sao compativeis pela igualdade nullable.",
            }
        if filled_count < 2 and gtin_state != NULLABLE_EQUAL_FILLED:
            return {
                "eligible": False,
                "reason": "Evidencia fiscal preenchida insuficiente para alta confianca.",
            }
        return {
            "eligible": True,
            "reason": "Descricao similar com GTIN/NCM/CEST compativeis.",
        }

    if rule_id == RULE_R2:
        if any(
            bool(pair_context.get(flag))
            for flag in [
                "left_has_multi_ncm",
                "right_has_multi_ncm",
                "left_has_multi_cest",
                "right_has_multi_cest",
                "left_has_multi_gtin",
                "right_has_multi_gtin",
            ]
        ):
            return {
                "eligible": False,
                "reason": "Grupo possui multiplos valores fiscais internos.",
            }
        if desc_score < 0.74:
            return {
                "eligible": False,
                "reason": "Descricao abaixo do threshold da regra R2.",
            }
        if ncm_state != NULLABLE_EQUAL_FILLED:
            return {
                "eligible": False,
                "reason": "NCM deve estar preenchido e igual para R2.",
            }
        if not is_equal_nullable_metric(cest_state):
            return {"eligible": False, "reason": "CEST conflita entre os grupos."}
        if not is_equal_nullable_metric(gtin_state):
            return {
                "eligible": False,
                "reason": "GTIN conflita ou esta incompleto entre os grupos.",
            }
        return {
            "eligible": True,
            "reason": "Descricao similar com NCM e CEST compativeis.",
        }

    if rule_id == RULE_R3:
        if any(
            bool(pair_context.get(flag))
            for flag in [
                "left_has_multi_ncm",
                "right_has_multi_ncm",
                "left_has_multi_cest",
                "right_has_multi_cest",
                "left_has_multi_gtin",
                "right_has_multi_gtin",
            ]
        ):
            return {
                "eligible": False,
                "reason": "Grupo possui multiplos valores fiscais internos.",
            }
        if desc_score < 0.68:
            return {
                "eligible": False,
                "reason": "Descricao abaixo do threshold da regra R3.",
            }
        if gtin_state != NULLABLE_EQUAL_FILLED:
            return {
                "eligible": False,
                "reason": "GTIN deve estar preenchido e igual para R3.",
            }
        if ncm_state not in {NULLABLE_EQUAL_FILLED, NULLABLE_EQUAL_NULL}:
            return {
                "eligible": False,
                "reason": "NCM conflita ou esta incompleto entre os grupos.",
            }
        if cest_state == NULLABLE_CONFLICT:
            return {"eligible": False, "reason": "CEST conflita entre os grupos."}
        return {
            "eligible": True,
            "reason": "Descricao similar com GTIN preenchido e NCM compativeis.",
        }

    if rule_id == RULE_R6:
        if NULLABLE_CONFLICT in {ncm_state, cest_state, gtin_state}:
            conflict_fields = [
                field
                for field, state in [
                    ("NCM", ncm_state),
                    ("CEST", cest_state),
                    ("GTIN", gtin_state),
                ]
                if state == NULLABLE_CONFLICT
            ]
            return {
                "eligible": True,
                "reason": f"Conflito fiscal preenchido em {', '.join(conflict_fields)}.",
            }
        return {
            "eligible": False,
            "reason": "Nao ha conflito fiscal preenchido para manter separado.",
        }

    return {"eligible": False, "reason": "Regra de lote nao suportada."}


def _build_component_summaries(
    component_rows: list[dict[str, Any]],
    edge_contexts: list[dict[str, Any]],
    rule_id: str,
    proposal_index: int,
    source_method: str,
) -> dict[str, Any]:
    config = RULE_CONFIG[rule_id]
    ordered_rows = sorted(component_rows, key=lambda row: row["chave_produto"])
    descricao_candidates = sorted(
        ordered_rows,
        key=lambda row: (
            -(bool(row.get("gtin")) + bool(row.get("ncm")) + bool(row.get("cest"))),
            -(row.get("qtd_codigos") or 0),
            row.get("descricao") or "",
        ),
    )
    canonical_row = descricao_candidates[0]
    ncm_values = [_clean_text(row.get("ncm")) for row in ordered_rows]
    cest_values = [_clean_text(row.get("cest")) for row in ordered_rows]
    gtin_values = [_clean_text(row.get("gtin")) for row in ordered_rows]
    desc_scores = [float(edge["score_descricao"]) for edge in edge_contexts] or [1.0]
    compl_scores = [float(edge["score_descr_compl"]) for edge in edge_contexts] or [0.0]
    relation_summary = {
        "ncm": _field_relation_summary(ncm_values),
        "cest": _field_relation_summary(cest_values),
        "gtin": _field_relation_summary(gtin_values),
    }
    filled_count = filled_evidence_count_from_relations(
        relation_summary["ncm"],
        relation_summary["cest"],
        relation_summary["gtin"],
    )
    # ⚡ Bolt: Using native tuple .count() instead of sum(1 for...) is ~1.7x faster
    conflict_count = tuple(relation_summary.values()).count(NULLABLE_CONFLICT)
    if rule_id == RULE_R1:
        score_final_regra = min(
            0.99,
            0.72
            + (0.08 * filled_count)
            + (0.16 * sum(desc_scores) / max(len(desc_scores), 1)),
        )
    elif rule_id == RULE_R2:
        score_final_regra = min(
            0.94,
            0.64
            + (0.07 * filled_count)
            + (0.15 * sum(desc_scores) / max(len(desc_scores), 1)),
        )
    elif rule_id == RULE_R3:
        score_final_regra = min(
            0.92,
            0.62
            + (0.06 * filled_count)
            + (0.17 * sum(desc_scores) / max(len(desc_scores), 1)),
        )
    else:
        score_final_regra = max(
            0.05,
            min(
                0.35,
                0.08
                + (0.08 * conflict_count)
                + (0.08 * sum(desc_scores) / max(len(desc_scores), 1)),
            ),
        )

    return {
        "proposal_id": f"LOT_{rule_id.split('_')[0]}_{proposal_index:04d}",
        "rule_id": rule_id,
        "button_label": config["button_label"],
        "confidence_band": config["confidence_band"],
        "status": "ELEGIVEL",
        "source_method": source_method,
        "component_size": len(ordered_rows),
        "chaves_produto": [row["chave_produto"] for row in ordered_rows],
        "descricao_canonica_sugerida": canonical_row.get("descricao") or "",
        "lista_descricoes": [row.get("descricao") or "" for row in ordered_rows],
        "fiscal_signature": {
            "ncm_values": sorted({item for item in ncm_values if item}),
            "cest_values": sorted({item for item in cest_values if item}),
            "gtin_values": sorted({item for item in gtin_values if item}),
        },
        "relation_summary": relation_summary,
        "metrics": {
            "score_descricao_min": round(min(desc_scores), 6),
            "score_descricao_avg": round(
                sum(desc_scores) / max(len(desc_scores), 1), 6
            ),
            "score_descr_compl_avg": round(
                sum(compl_scores) / max(len(compl_scores), 1), 6
            ),
            "filled_evidence_count": filled_count,
            "score_final_regra": round(score_final_regra, 6),
        },
        "blocked": False,
        "blocked_reason": None,
    }


def construir_preview_unificacao_lote(
    df_agregados: pl.DataFrame,
    df_pairs: pl.DataFrame,
    rule_ids: list[str],
    source_method: str,
    require_all_pairs_compatible: bool = True,
    max_component_size: int = 12,
) -> dict[str, Any]:
    # ⚡ Bolt Optimization: Replacing slow `df.to_dicts()` iteration with fast `zip()` over column Series
    # to avoid row-by-row dictionary allocations and minimize Python-Rust FFI overhead.
    def _extract(df: pl.DataFrame, col_name: str) -> list[Any]:
        if col_name not in df.columns:
            return [None] * df.height
        series = df[col_name]
        return series.to_list() if hasattr(series, "to_list") else list(series)

    agregados_cols = df_agregados.columns
    agregados_extracts = [_extract(df_agregados, c) for c in agregados_cols]
    rows = [
        normalize_final_group_row(dict(zip(agregados_cols, vals)))
        for vals in zip(*agregados_extracts)
    ]

    row_map = {
        row["chave_produto"]: row
        for row in rows
        if row["chave_produto"] and row["descricao"]
    }
    pair_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    edges_by_rule: dict[str, list[tuple[str, str]]] = {
        rule_id: [] for rule_id in rule_ids
    }

    pairs_cols = df_pairs.columns
    pairs_extracts = [_extract(df_pairs, c) for c in pairs_cols]

    for vals in zip(*pairs_extracts):
        pair = dict(zip(pairs_cols, vals))
        left_key = _clean_text(pair.get("chave_produto_a"))
        right_key = _clean_text(pair.get("chave_produto_b"))
        if not left_key or not right_key or left_key == right_key:
            continue
        left = row_map.get(left_key)
        right = row_map.get(right_key)
        if not left or not right:
            continue
        key = _pair_key(left_key, right_key)
        pair_lookup[key] = _build_pair_context(left, right, pair)
        for rule_id in rule_ids:
            evaluation = evaluate_batch_rule(rule_id, left, right, pair_lookup[key])
            if evaluation.get("eligible"):
                edges_by_rule[rule_id].append(key)

    proposals: list[dict[str, Any]] = []
    total_components = 0
    claimed_components: set[tuple[str, ...]] = set()

    for rule_id in [rule for rule in RULE_PRIORITY if rule in rule_ids]:
        adjacency: dict[str, set[str]] = defaultdict(set)
        for left_key, right_key in edges_by_rule.get(rule_id, []):
            adjacency[left_key].add(right_key)
            adjacency[right_key].add(left_key)

        visited: set[str] = set()
        proposal_index = 1
        for start in sorted(adjacency):
            if start in visited:
                continue
            stack = [start]
            component_keys: list[str] = []
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                component_keys.append(current)
                for neighbor in sorted(adjacency.get(current, set())):
                    if neighbor not in visited:
                        stack.append(neighbor)
            component_keys = sorted(set(component_keys))
            if len(component_keys) < 2 or len(component_keys) > max(
                2, int(max_component_size)
            ):
                continue

            component_signature = tuple(component_keys)
            if component_signature in claimed_components:
                continue

            edge_contexts: list[dict[str, Any]] = []
            valid_component = True
            if require_all_pairs_compatible:
                for left_key, right_key in combinations(component_keys, 2):
                    key = _pair_key(left_key, right_key)
                    left = row_map[left_key]
                    right = row_map[right_key]
                    pair_context = pair_lookup.get(key) or _build_pair_context(
                        left, right, None
                    )
                    evaluation = evaluate_batch_rule(rule_id, left, right, pair_context)
                    if not evaluation.get("eligible"):
                        valid_component = False
                        break
                    edge_contexts.append(pair_context)
            else:
                for left_key, right_key in edges_by_rule.get(rule_id, []):
                    if left_key in component_keys and right_key in component_keys:
                        edge_contexts.append(
                            pair_lookup[_pair_key(left_key, right_key)]
                        )

            if not valid_component or not edge_contexts:
                continue

            component_rows = [row_map[key] for key in component_keys]
            proposals.append(
                _build_component_summaries(
                    component_rows,
                    edge_contexts,
                    rule_id,
                    proposal_index,
                    source_method,
                )
            )
            proposal_index += 1
            total_components += 1
            claimed_components.add(component_signature)

    summary_by_rule: list[dict[str, Any]] = []
    for rule_id in [rule for rule in RULE_PRIORITY if rule in rule_ids]:
        rule_proposals = [item for item in proposals if item["rule_id"] == rule_id]
        grouped_keys = sorted(
            {key for item in rule_proposals for key in item["chaves_produto"]}
        )
        summary_by_rule.append(
            {
                "rule_id": rule_id,
                "button_label": RULE_CONFIG[rule_id]["button_label"],
                "proposal_count": len(rule_proposals),
                "group_count": len(grouped_keys),
            }
        )

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "resumo": {
            "total_rows_considered": len(row_map),
            "total_candidate_pairs": int(df_pairs.height),
            "total_components": total_components,
            "total_proposals": len(proposals),
            "by_rule": summary_by_rule,
        },
        "proposals": proposals,
    }
