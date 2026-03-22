import difflib
from functools import lru_cache
import re
import os
import sys
import traceback
import logging
import hashlib
from datetime import UTC, datetime
import polars as pl
from pathlib import Path
from typing import Any
from fastapi import APIRouter, HTTPException, Query
from core.models import *
from core.produto_batch_lote import *
from core.produto_runtime import *
from core.utils import validar_cnpj
from ._utils import *

logger = logging.getLogger("sefin_audit_python")
router = APIRouter()

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


@router.post("/produtos/unificacao-lote/propostas")
async def preview_unificacao_lote(req: UnificacaoLotePreviewRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        return _run_preview_unificacao_lote(req)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[preview_unificacao_lote] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/unificacao-lote/aplicar")
async def aplicar_unificacao_lote(req: UnificacaoLoteApplyRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    action = _normalize_status_text(req.action)
    rule_id = _normalize_status_text(req.rule_id)
    if action not in {"UNIFICAR", "MANTER_SEPARADO"}:
        raise HTTPException(status_code=400, detail="Acao de lote nao suportada.")
    if not req.proposal_ids:
        raise HTTPException(status_code=400, detail="Nenhuma proposta foi informada.")
    try:
        preview_req = UnificacaoLotePreviewRequest(
            cnpj=req.cnpj,
            source_context=req.source_context,
            filters=req.filters,
            grouping_mode=req.grouping_mode,
            similarity_source=req.similarity_source,
            rule_ids=[rule_id],
            options=req.options,
        )
        preview = _run_preview_unificacao_lote(preview_req)
        proposals = {str(item.get("proposal_id")): item for item in preview.get("proposals", []) if str(item.get("rule_id")) == rule_id}
        selected_ids = [str(item).strip() for item in req.proposal_ids if str(item).strip()]
        selected_proposals = [proposals[item] for item in selected_ids if item in proposals]
        skipped = [{"proposal_id": item, "reason": "proposta nao encontrada ou nao elegivel com os filtros atuais"} for item in selected_ids if item not in proposals]

        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj_limpo}.parquet"
        regras: list[dict[str, Any]] = []

        if action == "UNIFICAR":
            for proposal in selected_proposals:
                canonical = _canon_text(proposal.get("descricao_canonica_sugerida"), "")
                if not canonical:
                    skipped.append({"proposal_id": proposal.get("proposal_id", ""), "reason": "descricao canonica ausente"})
                    continue
                for descricao in [str(item or "").strip() for item in proposal.get("lista_descricoes", [])]:
                    origem = _canon_text(descricao, "")
                    if not origem or origem == canonical:
                        continue
                    regras.append(
                        {
                            "tipo_regra": "UNIR_GRUPOS",
                            "descricao_origem": origem,
                            "descricao_destino": canonical,
                            "descricao_par": "",
                            "chave_grupo_a": "",
                            "chave_grupo_b": "",
                            "score_origem": str(proposal.get("metrics", {}).get("score_final_regra", "")),
                            "acao_manual": "AGREGAR",
                        }
                    )
            if regras:
                merge_mapa_descricoes_manual(str(mapa_descricoes_path), pl.DataFrame(regras), default_acao="AGREGAR")
                _reprocessar_produtos(dir_analises, cnpj_limpo)
            else:
                _gravar_status_analise(dir_analises, cnpj_limpo)
        else:
            for proposal in selected_proposals:
                descricoes = [str(item or "").strip() for item in proposal.get("lista_descricoes", []) if str(item or "").strip()]
                for index, origem in enumerate(descricoes):
                    for destino in descricoes[index + 1 :]:
                        origem_norm = _canon_text(origem, "")
                        destino_norm = _canon_text(destino, "")
                        if not origem_norm or not destino_norm or origem_norm == destino_norm:
                            continue
                        regras.append(
                            {
                                "tipo_regra": "MANTER_SEPARADO",
                                "descricao_origem": origem_norm,
                                "descricao_destino": "",
                                "descricao_par": destino_norm,
                                "chave_grupo_a": "",
                                "chave_grupo_b": "",
                                "score_origem": str(proposal.get("metrics", {}).get("score_final_regra", "")),
                                "acao_manual": "MANTER_SEPARADO",
                            }
                        )
            if regras:
                merge_mapa_descricoes_manual(str(mapa_descricoes_path), pl.DataFrame(regras), default_acao="MANTER_SEPARADO")
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            return {
                "success": True,
                "cnpj": cnpj_limpo,
                "action": action,
                "rule_id": rule_id,
                "applied_count": len(selected_proposals),
                "affected_groups_count": len({group for proposal in selected_proposals for group in proposal.get("chaves_produto", [])}),
                "skipped_count": len(skipped),
                "skipped": skipped,
                "status_updates_count": len({group for proposal in selected_proposals for group in proposal.get("chaves_produto", [])}),
                "mapa_manual_path": str(mapa_descricoes_path),
                "status_path": str(status_path),
            }

        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "action": action,
            "rule_id": rule_id,
            "applied_count": len(selected_proposals),
            "affected_groups_count": len({group for proposal in selected_proposals for group in proposal.get("chaves_produto", [])}),
            "skipped_count": len(skipped),
            "skipped": skipped,
            "status_updates_count": len({group for proposal in selected_proposals for group in proposal.get("chaves_produto", [])}),
            "mapa_manual_path": str(mapa_descricoes_path),
            "status_path": str(status_path),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[aplicar_unificacao_lote] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
