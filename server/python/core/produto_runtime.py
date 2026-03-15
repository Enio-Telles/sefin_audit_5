from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger("sefin_audit_python")

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

_VECTOR_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_DETAIL_COLUMNS = [
    "fonte",
    "codigo",
    "descricao",
    "descr_compl",
    "tipo_item",
    "ncm",
    "cest",
    "gtin",
    "unid",
    "codigo_original",
    "descricao_original",
    "tipo_item_original",
    "hash_manual_key",
]


def produto_pipeline_em_modo_compatibilidade() -> bool:
    return True


def obter_runtime_produtos_status(dir_analises: Path, cnpj: str) -> dict[str, Any]:
    artefatos = {
        "produtos_agregados": dir_analises / f"produtos_agregados_{cnpj}.parquet",
        "base_detalhes": dir_analises / f"base_detalhes_produtos_{cnpj}.parquet",
        "produtos_indexados": dir_analises / f"produtos_indexados_{cnpj}.parquet",
        "codigos_multidescricao": dir_analises / f"codigos_multidescricao_{cnpj}.parquet",
        "status_analise": dir_analises / f"status_analise_produtos_{cnpj}.parquet",
        "pares_lexicais": dir_analises / f"pares_descricoes_similares_{cnpj}.parquet",
        "pares_semanticos": dir_analises / f"pares_descricoes_similares_semanticos_{cnpj}.parquet",
        "pares_hibridos": dir_analises / f"pares_descricoes_similares_hibridos_{cnpj}.parquet",
        "mapa_manual_unificacao": dir_analises / f"mapa_manual_unificacao_{cnpj}.parquet",
        "mapa_manual_descricoes": dir_analises / f"mapa_manual_descricoes_{cnpj}.parquet",
    }
    files: dict[str, dict[str, Any]] = {}
    for key, path in artefatos.items():
        info: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if path.exists():
            info["size_bytes"] = int(path.stat().st_size)
        files[key] = info

    return {
        "compat_mode": produto_pipeline_em_modo_compatibilidade(),
        "pipeline_legacy_removed": True,
        "files": files,
    }


def _canon_text(value: Any, vazio: str = "(VAZIO)") -> str:
    text = "" if value is None else str(value)
    text = text.strip().upper()
    return text if text else vazio


def _clean_value(value: Any) -> str:
    return str(value or "").strip()


def _clean_gtin(value: Any) -> str:
    text = _clean_value(value).upper()
    if text in {"SEM GTIN", "NO GTIN", "(NULL)", "(NULO)", "NULL", "NONE"}:
        return ""
    return _clean_value(value)


def _consensus(values: list[str]) -> str:
    counts: dict[str, int] = {}
    for value in values:
        text = _clean_value(value)
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _join_unique(values: list[str], sep: str = ", ") -> str:
    uniq = sorted({_clean_value(value) for value in values if _clean_value(value)})
    return sep.join(uniq)


def _normalize_similarity_text(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .replace("Á", "A")
        .replace("À", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )


def _normalize_similarity_tokens(value: Any) -> list[str]:
    return [
        token.strip()
        for token in re.sub(r"[^A-Z0-9 ]+", " ", _normalize_similarity_text(value)).split()
        if token.strip() and len(token.strip()) > 1
    ]


def _build_char_ngrams(value: Any, size: int = 3) -> set[str]:
    compact = re.sub(r"[^A-Z0-9]+", " ", _normalize_similarity_text(value))
    grams: set[str] = set()
    if len(compact) <= size:
        if compact.strip():
            grams.add(compact.strip())
        return grams
    for idx in range(0, len(compact) - size + 1):
        slice_value = compact[idx : idx + size].strip()
        if slice_value:
            grams.add(slice_value)
    return grams


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    return 0.0 if union == 0 else len(a & b) / union


def _similarity_score(a: Any, b: Any) -> float:
    token_score = _jaccard(set(_normalize_similarity_tokens(a)), set(_normalize_similarity_tokens(b)))
    ngram_score = _jaccard(_build_char_ngrams(a), _build_char_ngrams(b))
    return 0.6 * token_score + 0.4 * ngram_score


def _build_description_hash(origem: Any, destino: Any, descricao_par: Any, tipo_regra: Any) -> str:
    payload = "|".join(
        [
            _canon_text(tipo_regra, ""),
            _canon_text(origem, ""),
            _canon_text(destino, ""),
            _canon_text(descricao_par, ""),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _pick_col(df: pl.DataFrame, *candidates: str) -> str | None:
    lowered = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _metric_score(a: Any, b: Any) -> float:
    left = str(a or "").strip()
    right = str(b or "").strip()
    if left and right:
        return 1.0 if left == right else 0.0
    return 0.5


def _metric_equal(a: Any, b: Any) -> bool:
    left = str(a or "").strip()
    right = str(b or "").strip()
    return bool(left and right and left == right)


def _metric_conflict(a: Any, b: Any) -> bool:
    left = str(a or "").strip()
    right = str(b or "").strip()
    return bool(left and right and left != right)


def _count_codes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    return len([item for item in text.split(",") if str(item).strip()])


def _prepare_group_rows(df_agregados: pl.DataFrame) -> list[dict[str, Any]]:
    if df_agregados.is_empty():
        return []

    work = df_agregados
    requer_col = _pick_col(work, "requer_revisao_manual")
    if requer_col:
        work = work.filter(pl.col(requer_col) == True)

    chave_col = _pick_col(work, "chave_produto", "codigo_consenso", "codigo")
    descricao_col = _pick_col(work, "descricao", "descricao_consenso")
    if not chave_col or not descricao_col:
        return []

    ncm_col = _pick_col(work, "ncm_consenso", "ncm")
    cest_col = _pick_col(work, "cest_consenso", "cest")
    gtin_col = _pick_col(work, "gtin_consenso", "gtin")
    qtd_col = _pick_col(work, "qtd_codigos", "qtd_cod")
    conflitos_col = _pick_col(work, "descricoes_conflitantes", "lista_descricoes", "conflitos")

    rows: list[dict[str, Any]] = []
    for row in work.to_dicts():
        rows.append(
            {
                "chave_produto": str(row.get(chave_col) or "").strip(),
                "descricao": str(row.get(descricao_col) or "").strip(),
                "ncm": str(row.get(ncm_col) or "").strip() if ncm_col else "",
                "cest": str(row.get(cest_col) or "").strip() if cest_col else "",
                "gtin": str(row.get(gtin_col) or "").strip() if gtin_col else "",
                "qtd_codigos": _count_codes(row.get(qtd_col)) if qtd_col else 0,
                "conflitos": str(row.get(conflitos_col) or "").strip() if conflitos_col else "",
            }
        )
    return [row for row in rows if row["chave_produto"] and row["descricao"]]


def _classificar_par(
    score_descricao: float,
    score_ncm: float,
    score_cest: float,
    score_gtin: float,
    a: dict[str, Any],
    b: dict[str, Any],
) -> dict[str, Any]:
    score_final = (score_descricao + score_ncm + score_cest + score_gtin) / 4
    gtin_equal = _metric_equal(a["gtin"], b["gtin"])
    gtin_conflict = _metric_conflict(a["gtin"], b["gtin"])
    ncm_equal = _metric_equal(a["ncm"], b["ncm"])
    ncm_conflict = _metric_conflict(a["ncm"], b["ncm"])
    cest_equal = _metric_equal(a["cest"], b["cest"])
    cest_conflict = _metric_conflict(a["cest"], b["cest"])
    fiscal_conflict = sum(int(flag) for flag in [ncm_conflict, cest_conflict, gtin_conflict])

    recomendacao = "REVISAR"
    motivo = "Analise manual residual."
    uniao_auto = False
    bloquear = False

    if gtin_equal and score_descricao >= 0.45:
        recomendacao = "UNIR_AUTOMATICO_ELEGIVEL"
        motivo = "GTIN igual com similaridade textual suficiente."
        uniao_auto = True
    elif gtin_conflict or (score_descricao <= 0.15 and fiscal_conflict >= 2):
        recomendacao = "BLOQUEAR_UNIAO"
        motivo = "Conflito forte de GTIN ou divergencia textual/fiscal extrema."
        bloquear = True
    elif (ncm_equal and cest_equal and score_descricao >= 0.35) or score_final >= 0.72:
        recomendacao = "UNIR_SUGERIDO"
        motivo = "Afinidade fiscal consistente com similaridade suficiente."
    elif score_descricao <= 0.2 and (ncm_conflict or gtin_conflict):
        recomendacao = "SEPARAR_SUGERIDO"
        motivo = "Descricoes muito diferentes com divergencia fiscal relevante."

    return {
        "score_final": score_final,
        "recomendacao": recomendacao,
        "motivo_recomendacao": motivo,
        "uniao_automatica_elegivel": uniao_auto,
        "bloquear_uniao": bloquear,
    }


def construir_tabela_pares_descricoes_similares(df_agregados: pl.DataFrame) -> pl.DataFrame:
    rows = _prepare_group_rows(df_agregados)
    output: list[dict[str, Any]] = []
    for idx, left in enumerate(rows):
        for right in rows[idx + 1 :]:
            score_descricao = _similarity_score(left["descricao"], right["descricao"])
            score_ncm = _metric_score(left["ncm"], right["ncm"])
            score_cest = _metric_score(left["cest"], right["cest"])
            score_gtin = _metric_score(left["gtin"], right["gtin"])
            classificacao = _classificar_par(score_descricao, score_ncm, score_cest, score_gtin, left, right)
            output.append(
                {
                    "chave_produto_a": left["chave_produto"],
                    "descricao_a": left["descricao"],
                    "ncm_a": left["ncm"],
                    "cest_a": left["cest"],
                    "gtin_a": left["gtin"],
                    "qtd_codigos_a": left["qtd_codigos"],
                    "conflitos_a": left["conflitos"],
                    "chave_produto_b": right["chave_produto"],
                    "descricao_b": right["descricao"],
                    "ncm_b": right["ncm"],
                    "cest_b": right["cest"],
                    "gtin_b": right["gtin"],
                    "qtd_codigos_b": right["qtd_codigos"],
                    "conflitos_b": right["conflitos"],
                    "score_descricao": score_descricao,
                    "score_ncm": score_ncm,
                    "score_cest": score_cest,
                    "score_gtin": score_gtin,
                    "score_semantico": None,
                    "metodo_similaridade": "LEXICAL",
                    "modelo_vetorizacao": None,
                    "origem_par_hibrido": None,
                    **classificacao,
                }
            )
    return pl.DataFrame(output) if output else pl.DataFrame(schema={
        "chave_produto_a": pl.Utf8,
        "descricao_a": pl.Utf8,
        "ncm_a": pl.Utf8,
        "cest_a": pl.Utf8,
        "gtin_a": pl.Utf8,
        "qtd_codigos_a": pl.Int64,
        "conflitos_a": pl.Utf8,
        "chave_produto_b": pl.Utf8,
        "descricao_b": pl.Utf8,
        "ncm_b": pl.Utf8,
        "cest_b": pl.Utf8,
        "gtin_b": pl.Utf8,
        "qtd_codigos_b": pl.Int64,
        "conflitos_b": pl.Utf8,
        "score_descricao": pl.Float64,
        "score_ncm": pl.Float64,
        "score_cest": pl.Float64,
        "score_gtin": pl.Float64,
        "score_semantico": pl.Float64,
        "score_final": pl.Float64,
        "recomendacao": pl.Utf8,
        "motivo_recomendacao": pl.Utf8,
        "uniao_automatica_elegivel": pl.Boolean,
        "bloquear_uniao": pl.Boolean,
        "metodo_similaridade": pl.Utf8,
        "modelo_vetorizacao": pl.Utf8,
        "origem_par_hibrido": pl.Utf8,
    })


def obter_status_vectorizacao() -> dict[str, Any]:
    try:
        import numpy  # noqa: F401
        from sentence_transformers import SentenceTransformer  # noqa: F401
    except Exception as exc:
        return {
            "available": False,
            "message": f"Dependencias semanticas indisponiveis: {exc}",
            "engine": None,
            "model_name": _VECTOR_MODEL_NAME,
        }

    try:
        import faiss  # noqa: F401

        engine = "faiss"
    except Exception:
        engine = "numpy"

    return {
        "available": True,
        "message": "Vetorizacao semantica disponivel.",
        "engine": engine,
        "model_name": _VECTOR_MODEL_NAME,
    }


def compute_file_sha1(path: Path) -> str | None:
    if not path.exists():
        return None
    sha1 = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def build_vector_cache_metadata(
    metodo: str,
    model_name: str,
    engine: str | None,
    input_base_hash: str | None,
    top_k: int,
    min_semantic_score: float,
    batch_size: int,
) -> dict[str, Any]:
    from datetime import UTC, datetime

    return {
        "metodo": metodo,
        "modelo_vetorizacao": model_name,
        "engine": engine,
        "input_base_hash": input_base_hash,
        "top_k": int(top_k),
        "min_semantic_score": float(min_semantic_score),
        "batch_size": int(batch_size),
        "generated_at_utc": datetime.now(UTC).isoformat(),
    }


def read_vector_cache_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_vector_cache_metadata(path: Path, metadata: dict[str, Any]) -> None:
    path.write_text(json.dumps(metadata, ensure_ascii=True, indent=2), encoding="utf-8")


def cache_metadata_matches(
    metadata: dict[str, Any] | None,
    metodo: str,
    input_base_hash: str | None,
    top_k: int,
    min_semantic_score: float,
    model_name: str,
) -> bool:
    if not metadata:
        return False
    return (
        str(metadata.get("metodo") or "") == metodo
        and str(metadata.get("modelo_vetorizacao") or "") == model_name
        and str(metadata.get("input_base_hash") or "") == str(input_base_hash or "")
        and int(metadata.get("top_k") or 0) == int(top_k)
        and float(metadata.get("min_semantic_score") or 0.0) == float(min_semantic_score)
    )


def construir_tabela_pares_descricoes_semanticos(
    df_agregados: pl.DataFrame,
    top_k: int = 8,
    min_semantic_score: float = 0.32,
) -> pl.DataFrame:
    import numpy as np
    from sentence_transformers import SentenceTransformer

    lexical = construir_tabela_pares_descricoes_similares(df_agregados)
    rows = _prepare_group_rows(df_agregados)
    if not rows:
        return lexical

    descriptions = [row["descricao"] for row in rows]
    model = SentenceTransformer(_VECTOR_MODEL_NAME)
    embeddings = model.encode(descriptions, normalize_embeddings=True, show_progress_bar=False)
    embeddings_np = np.array(embeddings, dtype="float32")

    try:
        import faiss

        index = faiss.IndexFlatIP(embeddings_np.shape[1])
        index.add(embeddings_np)
        similarities, indices = index.search(embeddings_np, min(max(int(top_k), 2), len(rows)))
        engine = "FAISS"
    except Exception:
        similarities = np.matmul(embeddings_np, embeddings_np.T)
        indices = np.argsort(-similarities, axis=1)[:, : min(max(int(top_k), 2), len(rows))]
        similarities = np.take_along_axis(similarities, indices, axis=1)
        engine = "NUMPY"

    pair_map: dict[tuple[str, str], dict[str, Any]] = {}
    for i, left in enumerate(rows):
        for rank, j in enumerate(indices[i]):
            j = int(j)
            if i == j:
                continue
            score_sem = float(similarities[i][rank])
            if score_sem < float(min_semantic_score):
                continue
            right = rows[j]
            pair_key = tuple(sorted([left["chave_produto"], right["chave_produto"]]))
            score_descricao = _similarity_score(left["descricao"], right["descricao"])
            score_ncm = _metric_score(left["ncm"], right["ncm"])
            score_cest = _metric_score(left["cest"], right["cest"])
            score_gtin = _metric_score(left["gtin"], right["gtin"])
            classificacao = _classificar_par(score_sem, score_ncm, score_cest, score_gtin, left, right)
            candidate = {
                "chave_produto_a": left["chave_produto"],
                "descricao_a": left["descricao"],
                "ncm_a": left["ncm"],
                "cest_a": left["cest"],
                "gtin_a": left["gtin"],
                "qtd_codigos_a": left["qtd_codigos"],
                "conflitos_a": left["conflitos"],
                "chave_produto_b": right["chave_produto"],
                "descricao_b": right["descricao"],
                "ncm_b": right["ncm"],
                "cest_b": right["cest"],
                "gtin_b": right["gtin"],
                "qtd_codigos_b": right["qtd_codigos"],
                "conflitos_b": right["conflitos"],
                "score_descricao": score_descricao,
                "score_ncm": score_ncm,
                "score_cest": score_cest,
                "score_gtin": score_gtin,
                "score_semantico": score_sem,
                "score_final": (score_sem + score_ncm + score_cest + score_gtin) / 4,
                "recomendacao": classificacao["recomendacao"],
                "motivo_recomendacao": classificacao["motivo_recomendacao"],
                "uniao_automatica_elegivel": classificacao["uniao_automatica_elegivel"],
                "bloquear_uniao": classificacao["bloquear_uniao"],
                "metodo_similaridade": "SEMANTICO_FAISS" if engine == "FAISS" else "SEMANTICO_NUMPY",
                "modelo_vetorizacao": _VECTOR_MODEL_NAME,
                "origem_par_hibrido": "somente_semantico",
            }
            current = pair_map.get(pair_key)
            if current is None or float(candidate["score_final"]) > float(current["score_final"]):
                pair_map[pair_key] = candidate

    return pl.DataFrame(pair_map.values()) if pair_map else lexical.clear()


def construir_tabela_pares_descricoes_hibridos(df_lexical: pl.DataFrame, df_semantic: pl.DataFrame) -> pl.DataFrame:
    by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for source_name, df in [("somente_lexical", df_lexical), ("somente_semantico", df_semantic)]:
        if df.is_empty():
            continue
        for row in df.to_dicts():
            pair = tuple(sorted([str(row.get("chave_produto_a") or ""), str(row.get("chave_produto_b") or "")]))
            if not pair[0] or not pair[1]:
                continue
            current = by_pair.get(pair)
            if current is None:
                row["origem_par_hibrido"] = source_name
                by_pair[pair] = row
                continue
            merged = dict(current)
            merged["score_descricao"] = max(float(current.get("score_descricao") or 0.0), float(row.get("score_descricao") or 0.0))
            merged["score_semantico"] = max(float(current.get("score_semantico") or 0.0), float(row.get("score_semantico") or 0.0))
            merged["score_ncm"] = max(float(current.get("score_ncm") or 0.0), float(row.get("score_ncm") or 0.0))
            merged["score_cest"] = max(float(current.get("score_cest") or 0.0), float(row.get("score_cest") or 0.0))
            merged["score_gtin"] = max(float(current.get("score_gtin") or 0.0), float(row.get("score_gtin") or 0.0))
            semantic_weight = merged["score_semantico"] if merged["score_semantico"] else merged["score_descricao"]
            merged["score_final"] = (semantic_weight + merged["score_ncm"] + merged["score_cest"] + merged["score_gtin"]) / 4
            merged["origem_par_hibrido"] = "ambos"
            merged["metodo_similaridade"] = "HIBRIDO"
            by_pair[pair] = merged

    if not by_pair:
        return df_lexical.clear()

    rows: list[dict[str, Any]] = []
    for row in by_pair.values():
        semantic_base = float(row.get("score_semantico") or row.get("score_descricao") or 0.0)
        classificacao = _classificar_par(
            semantic_base,
            float(row.get("score_ncm") or 0.0),
            float(row.get("score_cest") or 0.0),
            float(row.get("score_gtin") or 0.0),
            {"ncm": row.get("ncm_a"), "cest": row.get("cest_a"), "gtin": row.get("gtin_a")},
            {"ncm": row.get("ncm_b"), "cest": row.get("cest_b"), "gtin": row.get("gtin_b")},
        )
        rows.append({**row, **classificacao, "score_final": max(float(row.get("score_final") or 0.0), classificacao["score_final"])})
    return pl.DataFrame(rows)


def _empty_produtos_result() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "chave_produto": pl.Utf8,
            "descricao": pl.Utf8,
            "requer_revisao_manual": pl.Boolean,
        }
    )


def unificar_produtos_unidades(cnpj: str, projeto_dir: Path | None = None) -> pl.DataFrame:
    import importlib.util

    base_dir = projeto_dir or Path(__file__).resolve().parents[3]
    config_path = base_dir / "config.py"
    spec = importlib.util.spec_from_file_location("sefin_config_local", str(config_path))
    sefin_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sefin_config)
    dir_parquet, dir_analises, _ = sefin_config.obter_diretorios_cnpj(cnpj)
    produtos_path = dir_analises / f"produtos_agregados_{cnpj}.parquet"
    base_detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj}.parquet"
    indexados_path = dir_analises / f"produtos_indexados_{cnpj}.parquet"
    codigos_path = dir_analises / f"codigos_multidescricao_{cnpj}.parquet"
    variacoes_path = dir_analises / f"variacoes_produtos_{cnpj}.parquet"

    logger.warning("[produto_runtime] reconstruindo pipeline de produtos em modo de compatibilidade ativa para %s", cnpj)

    df_base = _carregar_base_detalhes(dir_parquet)
    if df_base.is_empty():
        empty = _empty_produtos_result()
        empty.write_parquet(str(produtos_path))
        pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS}).write_parquet(str(base_detalhes_path))
        _build_produtos_indexados(pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS}), empty).write_parquet(str(indexados_path))
        _build_codigos_multidescricao(pl.DataFrame(schema={"codigo": pl.Utf8})).write_parquet(str(codigos_path))
        _build_variacoes_produtos(pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})).write_parquet(str(variacoes_path))
        return empty

    df_base = _aplicar_mapas_manuais(df_base, dir_analises, cnpj)
    df_agregados = _build_produtos_agregados(df_base)
    df_indexados = _build_produtos_indexados(df_base, df_agregados)
    df_codigos = _build_codigos_multidescricao(df_indexados)
    df_variacoes = _build_variacoes_produtos(df_base)

    df_base.write_parquet(str(base_detalhes_path))
    df_agregados.write_parquet(str(produtos_path))
    df_indexados.write_parquet(str(indexados_path))
    df_codigos.write_parquet(str(codigos_path))
    df_variacoes.write_parquet(str(variacoes_path))
    return df_agregados


def _normalize_mapa_descricoes_manual(df: pl.DataFrame, default_acao: str = "AGREGAR") -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in DESCRIPTION_MANUAL_MAP_COLUMNS})

    for col in DESCRIPTION_MANUAL_MAP_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").cast(pl.Utf8).alias(col))

    rows: list[dict[str, str]] = []
    for row in df.select(DESCRIPTION_MANUAL_MAP_COLUMNS).to_dicts():
        tipo_regra = _canon_text(row.get("tipo_regra"), "")
        origem = _canon_text(row.get("descricao_origem"), "")
        destino = _canon_text(row.get("descricao_destino"), "")
        descricao_par = _canon_text(row.get("descricao_par"), "")
        rows.append(
            {
                "tipo_regra": tipo_regra,
                "descricao_origem": origem,
                "descricao_destino": destino,
                "descricao_par": descricao_par,
                "hash_descricoes_key": str(
                    row.get("hash_descricoes_key")
                    or _build_description_hash(origem, destino, descricao_par, tipo_regra)
                ),
                "chave_grupo_a": _canon_text(row.get("chave_grupo_a"), ""),
                "chave_grupo_b": _canon_text(row.get("chave_grupo_b"), ""),
                "score_origem": str(row.get("score_origem") or "").strip(),
                "acao_manual": _canon_text(row.get("acao_manual"), default_acao),
            }
        )
    return pl.DataFrame(rows).select(DESCRIPTION_MANUAL_MAP_COLUMNS).unique(subset=["hash_descricoes_key"], keep="last")


def merge_mapa_descricoes_manual(mapa_path: str | Path, df_novo: pl.DataFrame, default_acao: str = "AGREGAR") -> None:
    mapa_path = Path(mapa_path)
    novo = _normalize_mapa_descricoes_manual(df_novo, default_acao=default_acao)
    if mapa_path.exists():
        existente = _normalize_mapa_descricoes_manual(pl.read_parquet(str(mapa_path)), default_acao=default_acao)
        merged = pl.concat([existente, novo], how="diagonal_relaxed").unique(subset=["hash_descricoes_key"], keep="last")
        merged.write_parquet(str(mapa_path))
    else:
        novo.write_parquet(str(mapa_path))


def _source_frame_rows(path: Path, fonte: str, mappings: dict[str, str]) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})
    df = pl.read_parquet(str(path))
    rows: list[dict[str, Any]] = []
    for row in df.to_dicts():
        codigo = _clean_value(row.get(mappings.get("codigo", "")))
        descricao = _clean_value(row.get(mappings.get("descricao", "")))
        if not codigo or not descricao:
            continue
        tipo_item = _clean_value(row.get(mappings.get("tipo_item", "")))
        rows.append(
            {
                "fonte": fonte,
                "codigo": codigo,
                "descricao": descricao,
                "descr_compl": _clean_value(row.get(mappings.get("descr_compl", ""))),
                "tipo_item": tipo_item,
                "ncm": _clean_value(row.get(mappings.get("ncm", ""))),
                "cest": _clean_value(row.get(mappings.get("cest", ""))),
                "gtin": _clean_gtin(row.get(mappings.get("gtin", ""))),
                "unid": _clean_value(row.get(mappings.get("unid", ""))),
                "codigo_original": codigo,
                "descricao_original": descricao,
                "tipo_item_original": tipo_item,
                "hash_manual_key": "",
            }
        )
    return pl.DataFrame(rows).select(_DETAIL_COLUMNS) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})


def _carregar_base_detalhes(dir_parquet: Path) -> pl.DataFrame:
    frames = [
        _source_frame_rows(
            dir_parquet / next((p.name for p in dir_parquet.glob("NFe_*.parquet")), "__missing__"),
            "NFE",
            {
                "codigo": "prod_cprod",
                "descricao": "prod_xprod",
                "ncm": "prod_ncm",
                "cest": "prod_cest",
                "gtin": "prod_cean",
                "unid": "prod_ucom",
            },
        ),
        _source_frame_rows(
            dir_parquet / next((p.name for p in dir_parquet.glob("NFCe_*.parquet")), "__missing__"),
            "NFCE",
            {
                "codigo": "prod_cprod",
                "descricao": "prod_xprod",
                "ncm": "prod_ncm",
                "cest": "prod_cest",
                "gtin": "prod_cean",
                "unid": "prod_ucom",
            },
        ),
        _source_frame_rows(
            dir_parquet / next((p.name for p in dir_parquet.glob("c170_simplificada_*.parquet")), "__missing__"),
            "C170",
            {
                "codigo": "cod_item",
                "descricao": "descr_item",
                "descr_compl": "descr_compl",
                "tipo_item": "tipo_item",
                "ncm": "cod_ncm",
                "cest": "cest",
                "gtin": "cod_barra",
                "unid": "unid",
            },
        ),
        _source_frame_rows(
            dir_parquet / next((p.name for p in dir_parquet.glob("reg_0200_*.parquet")), "__missing__"),
            "REG0200",
            {
                "codigo": "cod_item",
                "descricao": "descr_item",
                "tipo_item": "tipo_item",
                "ncm": "cod_ncm",
                "cest": "cest",
                "gtin": "cod_barra",
                "unid": "unid_inv",
            },
        ),
        _source_frame_rows(
            dir_parquet / next((p.name for p in dir_parquet.glob("bloco_h_*.parquet")), "__missing__"),
            "BLOCO_H",
            {
                "codigo": "codigo_produto",
                "descricao": "descricao_produto",
                "descr_compl": "obs_complementar",
                "tipo_item": "tipo_item",
                "ncm": "cod_ncm",
                "cest": "cest",
                "gtin": "cod_barra",
                "unid": "unidade_medida",
            },
        ),
    ]
    non_empty = [frame for frame in frames if not frame.is_empty()]
    if not non_empty:
        return pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})
    df = pl.concat(non_empty, how="diagonal_relaxed")
    def expr_canon(col_name: str, default_empty: str = "(VAZIO)") -> pl.Expr:
        expr = pl.col(col_name).fill_null("").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        return pl.when(expr == "").then(pl.lit(default_empty)).otherwise(expr)

    return df.with_columns(
        [
            pl.col("fonte").cast(pl.Utf8),
            pl.col("codigo").cast(pl.Utf8),
            pl.col("descricao").cast(pl.Utf8),
            pl.col("descr_compl").cast(pl.Utf8).fill_null(""),
            pl.col("tipo_item").cast(pl.Utf8).fill_null(""),
            pl.col("ncm").cast(pl.Utf8).fill_null(""),
            pl.col("cest").cast(pl.Utf8).fill_null(""),
            pl.col("gtin").cast(pl.Utf8).fill_null(""),
            pl.col("unid").cast(pl.Utf8).fill_null(""),
            pl.col("codigo_original").cast(pl.Utf8),
            pl.col("descricao_original").cast(pl.Utf8),
            pl.col("tipo_item_original").cast(pl.Utf8).fill_null(""),
        ]
    ).with_columns(
        pl.concat_str(
            [
                expr_canon("fonte", ""),
                expr_canon("codigo_original", ""),
                expr_canon("descricao_original", "(VAZIO)"),
                expr_canon("tipo_item_original", "(VAZIO)"),
            ],
            separator="|",
        ).map_elements(
            lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest(),
            return_dtype=pl.Utf8,
        ).alias("hash_manual_key")
    )


def _resolve_description_unions(mapa_descricoes_path: Path) -> dict[str, str]:
    if not mapa_descricoes_path.exists():
        return {}
    df = _normalize_mapa_descricoes_manual(pl.read_parquet(str(mapa_descricoes_path)))
    parent: dict[str, str] = {}
    for row in df.to_dicts():
        if row.get("tipo_regra") != "UNIR_GRUPOS":
            continue
        origem = _canon_text(row.get("descricao_origem"), "")
        destino = _canon_text(row.get("descricao_destino"), "")
        if origem and destino:
            parent[origem] = destino

    def resolve(text: str) -> str:
        seen: set[str] = set()
        current = text
        while current in parent and current not in seen:
            seen.add(current)
            current = parent[current]
        return current

    return {key: resolve(key) for key in list(parent)}


def _aplicar_mapas_manuais(df_base: pl.DataFrame, dir_analises: Path, cnpj: str) -> pl.DataFrame:
    if df_base.is_empty():
        return df_base

    mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj}.parquet"
    mapa_manual_path = dir_analises / f"mapa_manual_unificacao_{cnpj}.parquet"

    unions = _resolve_description_unions(mapa_descricoes_path)
    if unions:
        canon_expr = pl.col("descricao").fill_null("").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        canon_expr = pl.when(canon_expr == "").then(pl.lit("")).otherwise(canon_expr)
        clean_expr = pl.col("descricao").fill_null("").cast(pl.Utf8).str.strip_chars()

        try:
            df_base = df_base.with_columns(
                canon_expr.replace_strict(unions, default=clean_expr).alias("descricao")
            )
        except Exception:
            try:
                # Fallback for older Polars versions
                df_base = df_base.with_columns(
                    canon_expr.replace(unions, default=clean_expr).alias("descricao")
                )
            except Exception:
                # Fallback for even older Polars versions (< 0.19)
                df_base = df_base.with_columns(
                    canon_expr.map_dict(unions, default=clean_expr).alias("descricao")
                )

    if mapa_manual_path.exists():
        mapa = pl.read_parquet(str(mapa_manual_path))
        for col in [
            "hash_manual_key",
            "codigo_novo",
            "descricao_nova",
            "ncm_novo",
            "cest_novo",
            "gtin_novo",
            "tipo_item_novo",
        ]:
            if col not in mapa.columns:
                mapa = mapa.with_columns(pl.lit("").alias(col))
        mapa = mapa.select(
            [
                pl.col("hash_manual_key").cast(pl.Utf8),
                pl.col("codigo_novo").cast(pl.Utf8).alias("__codigo_novo"),
                pl.col("descricao_nova").cast(pl.Utf8).alias("__descricao_nova"),
                pl.col("ncm_novo").cast(pl.Utf8).alias("__ncm_novo"),
                pl.col("cest_novo").cast(pl.Utf8).alias("__cest_novo"),
                pl.col("gtin_novo").cast(pl.Utf8).alias("__gtin_novo"),
                pl.col("tipo_item_novo").cast(pl.Utf8).alias("__tipo_item_novo"),
            ]
        ).unique(subset=["hash_manual_key"], keep="last")
        df_base = df_base.join(mapa, on="hash_manual_key", how="left").with_columns(
            [
                pl.when(pl.col("__codigo_novo").is_not_null() & (pl.col("__codigo_novo") != ""))
                .then(pl.col("__codigo_novo"))
                .otherwise(pl.col("codigo"))
                .alias("codigo"),
                pl.when(pl.col("__descricao_nova").is_not_null() & (pl.col("__descricao_nova") != ""))
                .then(pl.col("__descricao_nova"))
                .otherwise(pl.col("descricao"))
                .alias("descricao"),
                pl.when(pl.col("__ncm_novo").is_not_null() & (pl.col("__ncm_novo") != ""))
                .then(pl.col("__ncm_novo"))
                .otherwise(pl.col("ncm"))
                .alias("ncm"),
                pl.when(pl.col("__cest_novo").is_not_null() & (pl.col("__cest_novo") != ""))
                .then(pl.col("__cest_novo"))
                .otherwise(pl.col("cest"))
                .alias("cest"),
                pl.when(pl.col("__gtin_novo").is_not_null() & (pl.col("__gtin_novo") != ""))
                .then(pl.col("__gtin_novo"))
                .otherwise(pl.col("gtin"))
                .alias("gtin"),
                pl.when(pl.col("__tipo_item_novo").is_not_null() & (pl.col("__tipo_item_novo") != ""))
                .then(pl.col("__tipo_item_novo"))
                .otherwise(pl.col("tipo_item"))
                .alias("tipo_item"),
            ]
        ).drop([c for c in df_base.columns if c.startswith("__")], strict=False)
    return df_base


def _build_produtos_agregados(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "chave_produto": pl.Utf8,
                "descricao": pl.Utf8,
                "lista_descricao": pl.Utf8,
                "qtd_descricoes": pl.Int64,
                "qtd_codigos": pl.Int64,
                "ncm_consenso": pl.Utf8,
                "cest_consenso": pl.Utf8,
                "gtin_consenso": pl.Utf8,
                "tipo_item_consenso": pl.Utf8,
                "codigo_consenso": pl.Utf8,
                "lista_unid": pl.Utf8,
                "descricoes_conflitantes": pl.Utf8,
                "requer_revisao_manual": pl.Boolean,
            }
        )

    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(
        df_base.group_by("descricao").agg(
            [
                pl.col("codigo").drop_nulls().cast(pl.Utf8).alias("__codigos"),
                pl.col("ncm").drop_nulls().cast(pl.Utf8).alias("__ncm"),
                pl.col("cest").drop_nulls().cast(pl.Utf8).alias("__cest"),
                pl.col("gtin").drop_nulls().cast(pl.Utf8).alias("__gtin"),
                pl.col("tipo_item").drop_nulls().cast(pl.Utf8).alias("__tipo_item"),
                pl.col("unid").drop_nulls().cast(pl.Utf8).alias("__unid"),
            ]
        ).sort("descricao").to_dicts(),
        start=1,
    ):
        codigos = [item for item in row.get("__codigos", []) if _clean_value(item)]
        ncm_values = [item for item in row.get("__ncm", []) if _clean_value(item)]
        cest_values = [item for item in row.get("__cest", []) if _clean_value(item)]
        gtin_values = [item for item in row.get("__gtin", []) if _clean_value(item)]
        tipo_values = [item for item in row.get("__tipo_item", []) if _clean_value(item)]
        unid_values = [item for item in row.get("__unid", []) if _clean_value(item)]
        conflicts = []
        if len(set(codigos)) > 1:
            conflicts.append("CODIGO")
        if len(set(ncm_values)) > 1:
            conflicts.append("NCM")
        if len(set(cest_values)) > 1:
            conflicts.append("CEST")
        if len(set(gtin_values)) > 1:
            conflicts.append("GTIN")
        if len(set(tipo_values)) > 1:
            conflicts.append("TIPO_ITEM")
        rows.append(
            {
                "chave_produto": f"ID_{idx:04d}",
                "descricao": _clean_value(row.get("descricao")),
                "lista_descricao": _clean_value(row.get("descricao")),
                "qtd_descricoes": 1,
                "qtd_codigos": len(set(codigos)),
                "ncm_consenso": _consensus(ncm_values),
                "cest_consenso": _consensus(cest_values),
                "gtin_consenso": _consensus(gtin_values),
                "tipo_item_consenso": _consensus(tipo_values),
                "codigo_consenso": _consensus(codigos),
                "lista_unid": _join_unique(unid_values),
                "descricoes_conflitantes": ", ".join(conflicts),
                "requer_revisao_manual": bool(conflicts),
            }
        )
    return pl.DataFrame(rows)


def _build_produtos_indexados(df_base: pl.DataFrame, df_agregados: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "chave_produto": pl.Utf8,
                "codigo": pl.Utf8,
                "descricao": pl.Utf8,
                "descr_compl": pl.Utf8,
                "tipo_item": pl.Utf8,
                "ncm": pl.Utf8,
                "cest": pl.Utf8,
                "gtin": pl.Utf8,
                "lista_unidades": pl.Utf8,
                "lista_fontes": pl.Utf8,
                "qtd_linhas": pl.Int64,
            }
        )
    chave_map = df_agregados.select(["descricao", "chave_produto"])
    joined = df_base.join(chave_map, on="descricao", how="left")
    return (
        joined.group_by(["chave_produto", "codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"])
        .agg(
            [
                pl.len().cast(pl.Int64).alias("qtd_linhas"),
                pl.col("unid").drop_nulls().cast(pl.Utf8).unique().sort().alias("__unidades"),
                pl.col("fonte").drop_nulls().cast(pl.Utf8).unique().sort().alias("__fontes"),
            ]
        )
        .with_columns(
            [
                pl.col("__unidades").list.join(", ").alias("lista_unidades"),
                pl.col("__fontes").list.join(", ").alias("lista_fontes"),
            ]
        )
        .drop(["__unidades", "__fontes"])
        .sort(["chave_produto", "codigo", "descricao"])
    )


def _build_codigos_multidescricao(df_indexados: pl.DataFrame) -> pl.DataFrame:
    if df_indexados.is_empty():
        return pl.DataFrame(
            schema={
                "codigo": pl.Utf8,
                "qtd_descricoes": pl.Int64,
                "lista_descricoes": pl.Utf8,
                "lista_ncm": pl.Utf8,
                "lista_cest": pl.Utf8,
                "lista_gtin": pl.Utf8,
                "lista_tipo_item": pl.Utf8,
                "lista_chave_produto": pl.Utf8,
                "qtd_grupos_descricao_afetados": pl.Int64,
                "lista_descr_compl": pl.Utf8,
            }
        )
    grouped = (
        df_indexados.group_by("codigo")
        .agg(
            [
                pl.col("descricao").drop_nulls().cast(pl.Utf8).unique().sort().alias("__descricoes"),
                pl.col("ncm").drop_nulls().cast(pl.Utf8).unique().sort().alias("__ncm"),
                pl.col("cest").drop_nulls().cast(pl.Utf8).unique().sort().alias("__cest"),
                pl.col("gtin").drop_nulls().cast(pl.Utf8).unique().sort().alias("__gtin"),
                pl.col("tipo_item").drop_nulls().cast(pl.Utf8).unique().sort().alias("__tipo"),
                pl.col("chave_produto").drop_nulls().cast(pl.Utf8).unique().sort().alias("__chaves"),
                pl.col("descr_compl").drop_nulls().cast(pl.Utf8).unique().sort().alias("__compl"),
            ]
        )
        .with_columns(pl.col("__descricoes").list.len().cast(pl.Int64).alias("qtd_descricoes"))
        .filter(pl.col("qtd_descricoes") > 1)
        .with_columns(
            [
                pl.col("__descricoes").list.join("<<#>>").alias("lista_descricoes"),
                pl.col("__ncm").list.join(", ").alias("lista_ncm"),
                pl.col("__cest").list.join(", ").alias("lista_cest"),
                pl.col("__gtin").list.join(", ").alias("lista_gtin"),
                pl.col("__tipo").list.join(", ").alias("lista_tipo_item"),
                pl.col("__chaves").list.join(", ").alias("lista_chave_produto"),
                pl.col("__compl").list.join(" | ").alias("lista_descr_compl"),
                pl.col("__chaves").list.len().cast(pl.Int64).alias("qtd_grupos_descricao_afetados"),
            ]
        )
        .drop(["__descricoes", "__ncm", "__cest", "__gtin", "__tipo", "__chaves", "__compl"])
        .sort("codigo")
    )
    return grouped


def _build_variacoes_produtos(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(schema={"descricao": pl.Utf8, "qtd_codigos": pl.Int64, "qtd_ncm": pl.Int64, "qtd_gtin": pl.Int64})
    return (
        df_base.group_by("descricao")
        .agg(
            [
                pl.col("codigo").n_unique().cast(pl.Int64).alias("qtd_codigos"),
                pl.col("ncm").n_unique().cast(pl.Int64).alias("qtd_ncm"),
                pl.col("gtin").n_unique().cast(pl.Int64).alias("qtd_gtin"),
            ]
        )
        .filter((pl.col("qtd_codigos") > 1) | (pl.col("qtd_ncm") > 1) | (pl.col("qtd_gtin") > 1))
        .sort(["qtd_codigos", "qtd_ncm", "qtd_gtin"], descending=[True, True, True])
    )
