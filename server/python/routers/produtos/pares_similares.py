import re
import traceback
import logging
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

@router.get("/produtos/pares-grupos-similares")
async def get_pares_grupos_similares(
    cnpj: str = Query(...),
    metodo: str = Query("lexical"),
    forcar_recalculo: bool = Query(False),
    top_k: int = Query(8),
    min_score: float | None = Query(None),
    min_semantic_score: float = Query(0.32),
    page: int = Query(1),
    page_size: int = Query(50),
    search: str | None = Query(None),
    quick_filter: str = Query("TODOS"),
    sort_key: str = Query("PRIORIDADE"),
    show_analyzed: bool = Query(False),
):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    metodo_norm = str(metodo or "lexical").strip().lower()
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        page_norm = _normalize_page(page)
        page_size_norm = _normalize_page_size(page_size, default=50, max_size=200)
        quick_filter_norm = str(quick_filter or "TODOS").strip().upper()
        sort_key_norm = str(sort_key or "PRIORIDADE").strip().upper()
        search_term = str(search or "").strip().upper()
        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        def _lexical_cache_metadata(path: Path) -> dict[str, Any]:
            return {
                "metodo": "lexical",
                "engine": "lexical",
                "generated_at_utc": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat() if path.exists() else None,
                "modelo_vetorizacao": None,
            }

        params_top_k = max(2, min(int(top_k), 20))
        params_threshold = max(0.05, min(float(min_score if min_score is not None else min_semantic_score), 0.98))

        base_hash = None
        if agregados_path.exists():
            base_hash = compute_file_sha1(agregados_path)

        if metodo_norm == "faiss":
            status_vector = obter_status_vectorizacao()
            faiss_mode = (status_vector.get("modes") or {}).get("faiss") or {}
            pares_path = dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.parquet"
            metadata_path = dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.json"
            if not faiss_mode.get("available"):
                return {
                    "success": False,
                    "available": False,
                    "metodo": "faiss",
                    "message": faiss_mode.get("message") or status_vector["message"],
                    "file_path": str(pares_path),
                    "cache_metadata": read_vector_cache_metadata(metadata_path),
                    "data": [],
                    "page": page_norm,
                    "page_size": page_size_norm,
                    "total_file": 0,
                    "total_filtered": 0,
                    "total": 0,
                    "total_pages": 1,
                    "quick_filter_counts": {"todos": 0, "unirAutomatico": 0, "bloqueios": 0, "revisar": 0},
                }

            metadata = read_vector_cache_metadata(metadata_path)
            cache_ok = pares_path.exists() and cache_metadata_matches(
                metadata,
                metodo="faiss",
                input_base_hash=base_hash,
                top_k=params_top_k,
                min_semantic_score=params_threshold,
                model_name=str(faiss_mode.get("model_name") or "faiss"),
            )

            if (forcar_recalculo or not cache_ok) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_faiss(
                    df_agregados,
                    top_k=params_top_k,
                    min_score=params_threshold,
                    batch_size=32,
                ).write_parquet(str(pares_path))
                write_vector_cache_metadata(
                    metadata_path,
                    build_vector_cache_metadata(
                        metodo="faiss",
                        model_name=str(faiss_mode.get("model_name") or "faiss"),
                        engine=faiss_mode.get("engine") or "faiss",
                        input_base_hash=base_hash,
                        top_k=params_top_k,
                        min_semantic_score=params_threshold,
                        batch_size=32,
                    ),
                )
                metadata = read_vector_cache_metadata(metadata_path)

            if not pares_path.exists():
                return {
                    "success": True,
                    "available": True,
                    "metodo": "faiss",
                    "message": "Nenhum par FAISS gerado.",
                    "file_path": str(pares_path),
                    "cache_metadata": metadata,
                    "data": [],
                    "page": page_norm,
                    "page_size": page_size_norm,
                    "total_file": 0,
                    "total_filtered": 0,
                    "total": 0,
                    "total_pages": 1,
                    "quick_filter_counts": {"todos": 0, "unirAutomatico": 0, "bloqueios": 0, "revisar": 0},
                }

            df = pl.read_parquet(str(pares_path))
            selected_path = pares_path
            selected_metadata = metadata
            selected_message = "Pares FAISS carregados."
            selected_available = True
            selected_method = "faiss"
        elif metodo_norm == "light":
            light_path = dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.parquet"
            metadata_path = dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.json"
            metadata = read_vector_cache_metadata(metadata_path)
            cache_ok = light_path.exists() and cache_metadata_matches(
                metadata,
                metodo="light",
                input_base_hash=base_hash,
                top_k=params_top_k,
                min_semantic_score=params_threshold,
                model_name="CHAR_NGRAM_TFIDF_V1",
            )

            if (forcar_recalculo or not cache_ok) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_light(
                    df_agregados,
                    top_k=params_top_k,
                    min_score=params_threshold,
                ).write_parquet(str(light_path))
                write_vector_cache_metadata(
                    metadata_path,
                    build_vector_cache_metadata(
                        metodo="light",
                        model_name="CHAR_NGRAM_TFIDF_V1",
                        engine="light",
                        input_base_hash=base_hash,
                        top_k=params_top_k,
                        min_semantic_score=params_threshold,
                        batch_size=0,
                    ),
                )
                metadata = read_vector_cache_metadata(metadata_path)

            if not light_path.exists():
                return {
                    "success": True,
                    "available": True,
                    "metodo": "light",
                    "message": "Nenhuma sugestao leve gerada.",
                    "file_path": str(light_path),
                    "cache_metadata": metadata,
                    "data": [],
                    "page": page_norm,
                    "page_size": page_size_norm,
                    "total_file": 0,
                    "total_filtered": 0,
                    "total": 0,
                    "total_pages": 1,
                    "quick_filter_counts": {"todos": 0, "unirAutomatico": 0, "bloqueios": 0, "revisar": 0},
                }

            df = pl.read_parquet(str(light_path))
            selected_path = light_path
            selected_metadata = metadata
            selected_message = "Sugestoes leves carregadas."
            selected_available = True
            selected_method = "light"
        else:
            pares_path = dir_analises / f"pares_descricoes_similares_{cnpj_limpo}.parquet"

            if (forcar_recalculo or not pares_path.exists()) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_similares(df_agregados).write_parquet(str(pares_path))

            if not pares_path.exists():
                return {
                    "success": True,
                    "available": True,
                    "metodo": "lexical",
                    "message": "Nenhum par lexical gerado.",
                    "file_path": str(pares_path),
                    "cache_metadata": _lexical_cache_metadata(pares_path),
                    "data": [],
                    "page": page_norm,
                    "page_size": page_size_norm,
                    "total_file": 0,
                    "total_filtered": 0,
                    "total": 0,
                    "total_pages": 1,
                    "quick_filter_counts": {"todos": 0, "unirAutomatico": 0, "bloqueios": 0, "revisar": 0},
                }

            df = pl.read_parquet(str(pares_path))
            selected_path = pares_path
            selected_metadata = _lexical_cache_metadata(pares_path)
            selected_message = "Pares lexicais carregados."
            selected_available = True
            selected_method = "lexical"

        total_file = int(df.height)

        if not show_analyzed:
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            if status_path.exists():
                df_status = pl.read_parquet(str(status_path))
                hidden_groups = set(
                    df_status.filter(
                        (pl.col("tipo_ref") == "POR_GRUPO")
                        & pl.col("status_analise").is_in(["VERIFICADO_SEM_ACAO", "UNIDO_ENTRE_GRUPOS", "MANTIDO_SEPARADO"])
                    ).get_column("ref_id").cast(pl.Utf8).to_list()
                )
                if hidden_groups:
                    df = df.filter(
                        ~pl.col("chave_produto_a").cast(pl.Utf8).is_in(sorted(hidden_groups))
                        & ~pl.col("chave_produto_b").cast(pl.Utf8).is_in(sorted(hidden_groups))
                    )

        quick_filter_counts = {
            "todos": int(df.height),
            "unirAutomatico": int(df.filter(pl.col("uniao_automatica_elegivel") == True).height) if "uniao_automatica_elegivel" in df.columns else 0,
            "bloqueios": int(df.filter(pl.col("bloquear_uniao") == True).height) if "bloquear_uniao" in df.columns else 0,
            "revisar": int(df.filter(pl.col("recomendacao").cast(pl.Utf8) == "REVISAR").height) if "recomendacao" in df.columns else 0,
        }

        if quick_filter_norm == "UNIR_AUTOMATICO" and "uniao_automatica_elegivel" in df.columns:
            df = df.filter(pl.col("uniao_automatica_elegivel") == True)
        elif quick_filter_norm == "BLOQUEIOS" and "bloquear_uniao" in df.columns:
            df = df.filter(pl.col("bloquear_uniao") == True)
        elif quick_filter_norm == "REVISAR" and "recomendacao" in df.columns:
            df = df.filter(pl.col("recomendacao").cast(pl.Utf8) == "REVISAR")

        if search_term:
            searchable_columns = [
                "chave_produto_a",
                "descricao_a",
                "ncm_a",
                "cest_a",
                "gtin_a",
                "conflitos_a",
                "chave_produto_b",
                "descricao_b",
                "ncm_b",
                "cest_b",
                "gtin_b",
                "conflitos_b",
                "recomendacao",
                "motivo_recomendacao",
            ]
            exprs = [
                pl.col(col).cast(pl.Utf8).str.to_uppercase().str.contains(re.escape(search_term), literal=True)
                for col in searchable_columns
                if col in df.columns
            ]
            if exprs:
                combined = exprs[0]
                for expr in exprs[1:]:
                    combined = combined | expr
                df = df.filter(combined)

        if sort_key_norm == "SIMILARIDADE":
            sort_columns = [col for col in ["score_final", "score_descricao", "descricao_a"] if col in df.columns]
            descending = [True, True, False][: len(sort_columns)]
            df = df.sort(sort_columns, descending=descending)
        elif sort_key_norm == "RECOMENDACAO":
            sort_columns = [col for col in ["recomendacao", "score_final", "descricao_a"] if col in df.columns]
            descending = [False, True, False][: len(sort_columns)]
            df = df.sort(sort_columns, descending=descending)
        else:
            df = df.with_columns(
                pl.when(pl.col("bloquear_uniao") == True)
                .then(pl.lit(50))
                .when(pl.col("uniao_automatica_elegivel") == True)
                .then(pl.lit(40))
                .when(pl.col("recomendacao").cast(pl.Utf8) == "UNIR_SUGERIDO")
                .then(pl.lit(30))
                .when(pl.col("recomendacao").cast(pl.Utf8) == "SEPARAR_SUGERIDO")
                .then(pl.lit(20))
                .otherwise(pl.lit(10))
                .alias("__prioridade")
            ).sort(["__prioridade", "score_final", "score_descricao", "descricao_a"], descending=[True, True, True, False]).drop("__prioridade")

        total_filtered = int(df.height)
        paged_df, total, total_pages = _paginate_frame(df, page_norm, page_size_norm)

        return {
            "success": True,
            "available": selected_available,
            "metodo": selected_method,
            "message": selected_message,
            "file_path": str(selected_path),
            "cache_metadata": selected_metadata,
            "data": paged_df.to_dicts(),
            "page": min(page_norm, total_pages),
            "page_size": page_size_norm,
            "total_file": total_file,
            "total_filtered": total_filtered,
            "total": total,
            "total_pages": total_pages,
            "quick_filter_counts": quick_filter_counts,
        }
    except Exception as e:
        logger.error("[get_pares_grupos_similares] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
