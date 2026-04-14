from __future__ import annotations
from collections import Counter, defaultdict
import hashlib
import importlib.util
import json
import logging
import math
import re
from pathlib import Path
from typing import Any

import difflib
from functools import lru_cache

import polars as pl
from core.produto_classification import (
    choose_consensus as doc_choose_consensus,
    choose_standard_code as doc_choose_standard_code,
    classify_group_pair as doc_classify_group_pair,
    clean_cest as doc_clean_cest,
    clean_gtin as doc_clean_gtin,
    clean_ncm as doc_clean_ncm,
    normalize_description_key as doc_normalize_description_key,
    normalize_unit as doc_normalize_unit,
)

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
_LIGHT_VECTOR_MODEL_NAME = "CHAR_NGRAM_TFIDF_V1"
_FAISS_VECTOR_MODEL_NAME = _VECTOR_MODEL_NAME
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


def obter_runtime_produtos_status(dir_analises: Path, cnpj: str) -> dict[str, Any]:
    artefatos = {
        "produtos_agregados": dir_analises / f"produtos_agregados_{cnpj}.parquet",
        "base_detalhes": dir_analises / f"base_detalhes_produtos_{cnpj}.parquet",
        "status_analise": dir_analises / f"status_analise_produtos_{cnpj}.parquet",
        "mapa_agregados": dir_analises / f"mapa_auditoria_agregados_{cnpj}.parquet",
        "mapa_desagregados": dir_analises
        / f"mapa_auditoria_desagregados_{cnpj}.parquet",
        "pares_sugeridos_light": dir_analises
        / f"pares_descricoes_similares_light_{cnpj}.parquet",
        "pares_sugeridos_faiss": dir_analises
        / f"pares_descricoes_similares_faiss_{cnpj}.parquet",
    }
    files: dict[str, dict[str, Any]] = {}
    for key, path in artefatos.items():
        info: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if path.exists():
            info["size_bytes"] = int(path.stat().st_size)
        files[key] = info

    return {
        "files": files,
    }


def _canon_text(value: Any, vazio: str = "(VAZIO)") -> str:
    text = "" if value is None else str(value)
    text = text.strip().upper()
    return text if text else vazio


def _clean_value(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value or "").strip()


def _clean_gtin(value: Any) -> str:
    return doc_clean_gtin(value)


def _consensus(values: list[str]) -> str:
    return doc_choose_consensus(values)


def _join_unique(values: list[str], sep: str = ", ") -> str:
    uniq = sorted({_clean_value(value) for value in values if _clean_value(value)})
    return sep.join(uniq)


_STOP_WORDS = {
    "DE",
    "DA",
    "DO",
    "DAS",
    "DOS",
    "E",
    "COM",
    "SEM",
    "PARA",
    "UN",
    "PCT",
    "CX",
    "KG",
    "LT",
    "ML",
    "GR",
    "PC",
    "LATA",
    "LITRO",
    "LITROS",
    "GARRAFA",
}
_RE_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")


@lru_cache(maxsize=10000)
def _normalize_similarity_text(value: str) -> str:
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


@lru_cache(maxsize=10000)
def _normalize_similarity_tokens(value: str) -> tuple[str, ...]:
    clean_text = _RE_NON_ALNUM.sub(" ", _normalize_similarity_text(value))
    return tuple(
        token
        for token in clean_text.split()
        if len(token) > 1 and token not in _STOP_WORDS
    )


@lru_cache(maxsize=10000)
def _normalize_similarity_tokens_set(value: str) -> frozenset[str]:
    return frozenset(_normalize_similarity_tokens(value))


@lru_cache(maxsize=10000)
def _normalize_ngram_text(value: str) -> str:
    # Use precompiled regex and string split/join to remove extra spaces efficiently
    return " ".join(_RE_NON_ALNUM.sub(" ", _normalize_similarity_text(value)).split())


@lru_cache(maxsize=10000)
def _char_ngrams(value: str, size: int = 3) -> tuple[str, ...]:
    text = _normalize_ngram_text(value)
    if not text:
        return tuple()
    padded = f"  {text}  "
    if len(padded) <= size:
        return (padded,)
    return tuple(padded[idx : idx + size] for idx in range(len(padded) - size + 1))


@lru_cache(maxsize=10000)
def _char_ngram_norm(value: str, size: int = 3) -> float:
    grams = _char_ngrams(value, size=size)
    if not grams:
        return 0.0
    counts = Counter(grams)
    return math.hypot(*counts.values())


@lru_cache(maxsize=10000)
def _char_ngram_cosine(a: str, b: str, size: int = 3) -> float:
    grams_a = _char_ngrams(a, size=size)
    grams_b = _char_ngrams(b, size=size)
    if not grams_a and not grams_b:
        return 1.0
    if not grams_a or not grams_b:
        return 0.0
    counts_a = Counter(grams_a)
    counts_b = Counter(grams_b)
    dot = sum([v * counts_b.get(k, 0) for k, v in counts_a.items()])
    norm_a = _char_ngram_norm(a, size=size)
    norm_b = _char_ngram_norm(b, size=size)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


@lru_cache(maxsize=10000)
def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 1.0
    intersection = len(a & b)
    union = len(a | b)
    return 0.0 if union == 0 else intersection / union


@lru_cache(maxsize=10000)
def _sequence_match(a: str, b: str) -> float:
    str_a = " ".join(_normalize_similarity_tokens(a))
    str_b = " ".join(_normalize_similarity_tokens(b))
    if not str_a and not str_b:
        return 1.0
    if not str_a or not str_b:
        return 0.0
    return difflib.SequenceMatcher(None, str_a, str_b).ratio()


@lru_cache(maxsize=10000)
def _similarity_score(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0

    a_str = str(a)
    b_str = str(b)

    token_score = _jaccard(
        _normalize_similarity_tokens_set(a_str), _normalize_similarity_tokens_set(b_str)
    )
    sequence_score = _sequence_match(a_str, b_str)

    return 0.4 * token_score + 0.6 * sequence_score


def _build_description_hash(
    origem: Any, destino: Any, descricao_par: Any, tipo_regra: Any
) -> str:
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


def _is_valid_gtin_candidate(value: Any) -> bool:
    text = _clean_gtin(value)
    return len(text) > 7 and text.isdigit()


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

    chave_col = _pick_col(df_agregados, "chave_produto", "codigo_consenso", "codigo")
    descricao_col = _pick_col(df_agregados, "descricao", "descricao_consenso")
    if not chave_col or not descricao_col:
        return []

    ncm_col = _pick_col(df_agregados, "ncm_consenso", "ncm")
    cest_col = _pick_col(df_agregados, "cest_consenso", "cest")
    gtin_col = _pick_col(df_agregados, "gtin_consenso", "gtin")
    conflitos_col = _pick_col(
        df_agregados, "descricoes_conflitantes", "lista_descricoes", "conflitos"
    )
    descricao_norm_col = _pick_col(df_agregados, "descricao_normalizada")
    lista_codigos_col = _pick_col(df_agregados, "lista_codigos")
    lista_descr_compl_col = _pick_col(df_agregados, "lista_descr_compl")

    rows: list[dict[str, Any]] = []

    # ⚡ Bolt Optimization: Replacing slow `df.to_dicts()` iteration with fast `zip()` over column Series.
    # Avoiding row-by-row dictionary allocations dramatically reduces FFI overhead for large DataFrames.
    def _extract(col_name: str | None) -> list[Any]:
        if not col_name:
            return [None] * df_agregados.height
        series = df_agregados[col_name]
        return series.to_list() if hasattr(series, "to_list") else list(series)

    chaves = _extract(chave_col)
    descricoes = _extract(descricao_col)
    ncms = _extract(ncm_col)
    cests = _extract(cest_col)
    gtins = _extract(gtin_col)
    conflitos_vals = _extract(conflitos_col)
    desc_norms = _extract(descricao_norm_col)
    lista_codigos_vals = _extract(lista_codigos_col)
    descr_compl_vals = _extract(lista_descr_compl_col)

    for (
        c_chave,
        c_desc,
        c_ncm,
        c_cest,
        c_gtin,
        c_conf,
        c_desc_n,
        c_cods,
        c_compl,
    ) in zip(
        chaves,
        descricoes,
        ncms,
        cests,
        gtins,
        conflitos_vals,
        desc_norms,
        lista_codigos_vals,
        descr_compl_vals,
        strict=False,
    ):
        raw_codes = str(c_cods or "").strip() if lista_codigos_col else ""
        codes = [item.strip() for item in raw_codes.split(",") if item.strip()]
        rows.append(
            {
                "chave_produto": str(c_chave or "").strip(),
                "descricao": str(c_desc or "").strip(),
                "descricao_normalizada": str(
                    c_desc_n or doc_normalize_description_key(c_desc)
                ).strip(),
                "ncm": str(c_ncm or "").strip() if ncm_col else "",
                "cest": str(c_cest or "").strip() if cest_col else "",
                "gtin": str(c_gtin or "").strip() if gtin_col else "",
                "lista_descr_compl": (
                    str(c_compl or "").strip() if lista_descr_compl_col else ""
                ),
                "codigos": codes,
                "qtd_codigos": len(codes),
                "conflitos": str(c_conf or "").strip() if conflitos_col else "",
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
    left = {
        "descricao_normalizada": a.get("descricao_normalizada")
        or a.get("descricao")
        or "",
        "ncm": a.get("ncm") or "",
        "cest": a.get("cest") or "",
        "gtin": a.get("gtin") or "",
        "codigos": a.get("codigos") or [],
    }
    right = {
        "descricao_normalizada": b.get("descricao_normalizada")
        or b.get("descricao")
        or "",
        "ncm": b.get("ncm") or "",
        "cest": b.get("cest") or "",
        "gtin": b.get("gtin") or "",
        "codigos": b.get("codigos") or [],
    }
    result = doc_classify_group_pair(left, right)
    # Preserve externally expected signature while delegating to the new deterministic engine.
    result["score_descricao"] = (
        score_descricao if score_descricao is not None else result["score_descricao"]
    )
    result["score_ncm"] = score_ncm if score_ncm is not None else result["score_ncm"]
    result["score_cest"] = (
        score_cest if score_cest is not None else result["score_cest"]
    )
    result["score_gtin"] = (
        score_gtin if score_gtin is not None else result["score_gtin"]
    )
    return result


def construir_tabela_pares_descricoes_similares(
    df_agregados: pl.DataFrame,
) -> pl.DataFrame:
    rows = _prepare_group_rows(df_agregados)
    output: list[dict[str, Any]] = []
    for idx, left in enumerate(rows):
        for right in rows[idx + 1 :]:
            classificacao = doc_classify_group_pair(left, right)
            score_descricao = classificacao["score_descricao"]
            score_ncm = classificacao["score_ncm"]
            score_cest = classificacao["score_cest"]
            score_gtin = classificacao["score_gtin"]
            shared_codes = sorted(set(left["codigos"]) & set(right["codigos"]))
            should_emit = (
                bool(shared_codes)
                or bool(classificacao["uniao_automatica_elegivel"])
                or classificacao["recomendacao"]
                in {"UNIR_SUGERIDO", "BLOQUEAR_UNIAO", "SEPARAR_SUGERIDO"}
            )
            if not should_emit:
                continue
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
                    "metodo_similaridade": "DOCUMENTAL",
                    "modelo_vetorizacao": None,
                    "origem_par_hibrido": None,
                    **classificacao,
                }
            )
    return (
        pl.DataFrame(output)
        if output
        else pl.DataFrame(
            schema={
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
            }
        )
    )


def _module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


@lru_cache(maxsize=1)
def _load_sentence_transformer_model(model_name: str):
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_name, device="cpu")


def _semantic_runtime_available() -> bool:
    return (
        _module_available("faiss")
        and _module_available("sentence_transformers")
        and _module_available("numpy")
    )


def obter_status_vectorizacao() -> dict[str, Any]:
    faiss_available = _semantic_runtime_available()
    semantic_message = (
        "Busca vetorial com FAISS e embeddings semanticos disponivel sob demanda."
        if faiss_available
        else "FAISS e/ou sentence-transformers indisponiveis neste runtime."
    )
    return {
        "available": faiss_available,
        "light_available": True,
        "message": "Modo leve e FAISS ficam desligados ate serem solicitados explicitamente.",
        "engine": "light",
        "model_name": _LIGHT_VECTOR_MODEL_NAME,
        "modes": {
            "light": {
                "available": True,
                "message": "Vetorizacao leve por char n-grams disponivel sob demanda.",
                "model_name": _LIGHT_VECTOR_MODEL_NAME,
                "engine": "light",
            },
            "faiss": {
                "available": faiss_available,
                "message": semantic_message,
                "model_name": _FAISS_VECTOR_MODEL_NAME,
                "engine": "faiss",
            },
        },
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
        and float(metadata.get("min_semantic_score") or 0.0)
        == float(min_semantic_score)
    )


def _build_faiss_vector_text(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("descricao") or "").strip(),
        str(row.get("lista_descr_compl") or "").strip(),
    ]
    ncm = str(row.get("ncm") or "").strip()
    cest = str(row.get("cest") or "").strip()
    if ncm:
        parts.append(f"NCM {ncm}")
    if cest:
        parts.append(f"CEST {cest}")
    return " | ".join(part for part in parts if part)


def _encode_faiss_rows(rows: list[dict[str, Any]], batch_size: int = 32):
    import numpy as np

    texts = [_build_faiss_vector_text(row) for row in rows]
    model = _load_sentence_transformer_model(_FAISS_VECTOR_MODEL_NAME)
    vectors = model.encode(
        texts,
        batch_size=max(4, int(batch_size)),
        show_progress_bar=False,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )
    return np.asarray(vectors, dtype="float32")


def _search_faiss_neighbors(vectors, top_k: int):
    import faiss
    import numpy as np

    arr = np.asarray(vectors, dtype="float32")
    if arr.ndim != 2 or arr.shape[0] == 0:
        return np.empty((0, 0), dtype="float32"), np.empty((0, 0), dtype="int64")
    faiss.normalize_L2(arr)
    k = max(2, min(int(top_k) + 1, int(arr.shape[0])))
    index = faiss.IndexFlatIP(int(arr.shape[1]))
    index.add(arr)
    return index.search(arr, k)


def construir_tabela_pares_descricoes_faiss(
    df_agregados: pl.DataFrame,
    top_k: int = 8,
    min_score: float = 0.62,
    batch_size: int = 32,
) -> pl.DataFrame:
    rows = _prepare_group_rows(df_agregados)
    if not rows:
        return construir_tabela_pares_descricoes_similares(df_agregados)
    if not _semantic_runtime_available():
        raise RuntimeError(
            "FAISS e/ou sentence-transformers nao estao disponiveis neste runtime."
        )

    vectors = _encode_faiss_rows(rows, batch_size=batch_size)
    scores, neighbors = _search_faiss_neighbors(vectors, top_k=top_k)
    threshold = max(0.05, min(float(min_score), 0.98))

    output: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for idx, left in enumerate(rows):
        for score, candidate_idx in zip(scores[idx].tolist(), neighbors[idx].tolist()):
            if candidate_idx == idx or candidate_idx < 0:
                continue
            if candidate_idx < idx:
                continue
            semantic_score = max(0.0, min(float(score), 1.0))
            right = rows[int(candidate_idx)]
            pair_key = (left["chave_produto"], right["chave_produto"])
            if pair_key in seen_pairs:
                continue
            if semantic_score < threshold:
                continue

            seen_pairs.add(pair_key)
            score_descricao = max(
                _similarity_score(
                    str(left.get("descricao") or ""), str(right.get("descricao") or "")
                ),
                _char_ngram_cosine(
                    _build_faiss_vector_text(left),
                    _build_faiss_vector_text(right),
                    size=3,
                ),
            )
            classificacao = _classificar_par(
                score_descricao=score_descricao,
                score_ncm=_metric_score(left.get("ncm"), right.get("ncm")),
                score_cest=_metric_score(left.get("cest"), right.get("cest")),
                score_gtin=_metric_score(left.get("gtin"), right.get("gtin")),
                a=left,
                b=right,
            )

            if (
                semantic_score >= max(0.82, threshold)
                and not classificacao.get("bloquear_uniao")
                and not classificacao.get("uniao_automatica_elegivel")
                and str(classificacao.get("recomendacao") or "") == "REVISAR"
                and float(classificacao.get("score_ncm") or 0.0) >= 0.5
            ):
                classificacao["recomendacao"] = "UNIR_SUGERIDO"
                classificacao["motivo_recomendacao"] = (
                    "Alta proximidade semantica via FAISS com dados fiscais compativeis."
                )
                classificacao["score_final"] = max(
                    float(classificacao.get("score_final") or 0.0),
                    round(0.55 + (semantic_score * 0.35), 6),
                )

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
                    "score_ncm": _metric_score(left.get("ncm"), right.get("ncm")),
                    "score_cest": _metric_score(left.get("cest"), right.get("cest")),
                    "score_gtin": _metric_score(left.get("gtin"), right.get("gtin")),
                    "score_semantico": semantic_score,
                    "metodo_similaridade": "FAISS_VECTOR",
                    "modelo_vetorizacao": _FAISS_VECTOR_MODEL_NAME,
                    "origem_par_hibrido": "faiss_cosine",
                    **classificacao,
                    "score_final": max(
                        float(classificacao.get("score_final") or 0.0), semantic_score
                    ),
                }
            )

    if output:
        return pl.DataFrame(output).sort(
            ["score_final", "score_semantico", "score_descricao"],
            descending=[True, True, True],
        )

    return pl.DataFrame(
        schema={
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
        }
    )


def _build_light_vector_text(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("descricao") or "").strip(),
        str(row.get("lista_descr_compl") or "").strip(),
        " ".join(_normalize_similarity_tokens(str(row.get("descricao") or ""))),
    ]
    return " | ".join(part for part in parts if part)


def _build_light_block_keys(row: dict[str, Any], vector_text: str) -> set[str]:
    tokens = _normalize_similarity_tokens(vector_text)
    keys: set[str] = set()
    if tokens:
        keys.add(f"T1:{tokens[0]}")
        if len(tokens) > 1:
            keys.add(f"T2:{tokens[0]}|{tokens[1]}")
    ncm = str(row.get("ncm") or "").strip()
    cest = str(row.get("cest") or "").strip()
    gtin = _clean_gtin(row.get("gtin"))
    if len(ncm) >= 4:
        keys.add(f"N4:{ncm[:4]}")
    if cest:
        keys.add(f"C:{cest}")
    if _is_valid_gtin_candidate(gtin):
        keys.add(f"G:{gtin}")
    if not keys:
        keys.add(f"FALLBACK:{(vector_text[:6] or '(vazio)')}")
    return keys


def _score_light_pair(
    left: dict[str, Any], right: dict[str, Any], threshold: float
) -> tuple[float, bool]:
    left_text = _build_light_vector_text(left)
    right_text = _build_light_vector_text(right)
    score_char = _char_ngram_cosine(left_text, right_text, size=3)
    score_desc = _similarity_score(
        str(left.get("descricao") or ""), str(right.get("descricao") or "")
    )
    score_compl = _similarity_score(
        str(left.get("lista_descr_compl") or ""),
        str(right.get("lista_descr_compl") or ""),
    )
    base_score = (0.55 * score_char) + (0.35 * score_desc) + (0.10 * score_compl)

    gtin_equal = _metric_equal(
        left.get("gtin"), right.get("gtin")
    ) and _is_valid_gtin_candidate(left.get("gtin"))
    gtin_conflict = (
        _metric_conflict(left.get("gtin"), right.get("gtin"))
        and _is_valid_gtin_candidate(left.get("gtin"))
        and _is_valid_gtin_candidate(right.get("gtin"))
    )
    ncm_prefix_equal = bool(
        str(left.get("ncm") or "").strip()
        and str(right.get("ncm") or "").strip()
        and str(left.get("ncm") or "")[:4] == str(right.get("ncm") or "")[:4]
    )
    cest_equal = _metric_equal(left.get("cest"), right.get("cest"))

    adjusted = base_score
    if gtin_equal:
        adjusted = max(adjusted, 0.995)
    else:
        if ncm_prefix_equal:
            adjusted += 0.05
        if cest_equal:
            adjusted += 0.03
        if gtin_conflict:
            adjusted -= 0.20
        elif (
            _metric_conflict(left.get("ncm"), right.get("ncm")) and not ncm_prefix_equal
        ):
            adjusted -= 0.08

    adjusted = max(0.0, min(1.0, adjusted))
    should_emit = adjusted >= float(threshold) or gtin_equal or gtin_conflict
    return adjusted, should_emit


def construir_tabela_pares_descricoes_light(
    df_agregados: pl.DataFrame,
    top_k: int = 8,
    min_score: float = 0.72,
) -> pl.DataFrame:
    rows = _prepare_group_rows(df_agregados)
    if not rows:
        return construir_tabela_pares_descricoes_similares(df_agregados)

    max_candidates_per_group = 400
    threshold = max(0.30, min(float(min_score), 0.98))
    indexed_rows: list[dict[str, Any]] = []
    block_index: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        vector_text = _build_light_vector_text(row)
        row_copy = dict(row)
        row_copy["__vector_text"] = vector_text
        row_copy["__block_keys"] = _build_light_block_keys(row, vector_text)
        indexed_rows.append(row_copy)
        for key in row_copy["__block_keys"]:
            block_index[key].append(idx)

    output: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    top_k_norm = max(2, min(int(top_k), 20))
    for idx, left in enumerate(indexed_rows):
        candidate_indexes: set[int] = set()
        for key in left["__block_keys"]:
            related = block_index.get(key) or []
            if len(related) > max_candidates_per_group:
                continue
            candidate_indexes.update(
                candidate for candidate in related if candidate > idx
            )

        scored_candidates: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for candidate_idx in candidate_indexes:
            right = indexed_rows[candidate_idx]
            light_score, should_emit = _score_light_pair(left, right, threshold)
            if not should_emit:
                continue
            scored_candidates.append((light_score, left, right))

        scored_candidates.sort(key=lambda item: item[0], reverse=True)
        for light_score, row_left, row_right in scored_candidates[:top_k_norm]:
            pair_key = (row_left["chave_produto"], row_right["chave_produto"])
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            classificacao = doc_classify_group_pair(row_left, row_right)
            output.append(
                {
                    "chave_produto_a": row_left["chave_produto"],
                    "descricao_a": row_left["descricao"],
                    "ncm_a": row_left["ncm"],
                    "cest_a": row_left["cest"],
                    "gtin_a": row_left["gtin"],
                    "qtd_codigos_a": row_left["qtd_codigos"],
                    "conflitos_a": row_left["conflitos"],
                    "chave_produto_b": row_right["chave_produto"],
                    "descricao_b": row_right["descricao"],
                    "ncm_b": row_right["ncm"],
                    "cest_b": row_right["cest"],
                    "gtin_b": row_right["gtin"],
                    "qtd_codigos_b": row_right["qtd_codigos"],
                    "conflitos_b": row_right["conflitos"],
                    "score_descricao": light_score,
                    "score_ncm": _metric_score(row_left["ncm"], row_right["ncm"]),
                    "score_cest": _metric_score(row_left["cest"], row_right["cest"]),
                    "score_gtin": _metric_score(row_left["gtin"], row_right["gtin"]),
                    "score_semantico": light_score,
                    "metodo_similaridade": "LIGHT_VECTOR",
                    "modelo_vetorizacao": _LIGHT_VECTOR_MODEL_NAME,
                    "origem_par_hibrido": "char_ngrams_tfidf",
                    **classificacao,
                    "score_final": max(
                        float(classificacao.get("score_final") or 0.0), light_score
                    ),
                }
            )

    if output:
        return pl.DataFrame(output).sort(
            ["score_final", "score_descricao"], descending=[True, True]
        )

    return pl.DataFrame(
        schema={
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
        }
    )


def _empty_produtos_result() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "chave_produto": pl.Utf8,
            "descricao": pl.Utf8,
            "requer_revisao_manual": pl.Boolean,
        }
    )


def _cleanup_legacy_produto_artifacts(dir_analises: Path, cnpj: str) -> None:
    legacy_paths = [
        dir_analises / f"produtos_indexados_{cnpj}.parquet",
        dir_analises / f"codigos_multidescricao_{cnpj}.parquet",
        dir_analises / f"variacoes_produtos_{cnpj}.parquet",
        dir_analises / f"pares_descricoes_similares_{cnpj}.parquet",
        dir_analises / f"pares_descricoes_similares_semanticos_{cnpj}.parquet",
        dir_analises / f"pares_descricoes_similares_semanticos_{cnpj}.json",
        dir_analises / f"pares_descricoes_similares_hibridos_{cnpj}.parquet",
        dir_analises / f"pares_descricoes_similares_hibridos_{cnpj}.json",
        dir_analises / f"mapa_auditoria_descricoes_{cnpj}.parquet",
        dir_analises / f"mapa_auditoria_descricoes_aplicadas_{cnpj}.parquet",
        dir_analises / f"mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet",
    ]
    for path in legacy_paths:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            logger.warning(
                "[produto_runtime] nao foi possivel remover artefato legado: %s", path
            )


def unificar_produtos_unidades(
    cnpj: str, projeto_dir: Path | None = None
) -> pl.DataFrame:
    import importlib.util

    base_dir = projeto_dir or Path(__file__).resolve().parents[3]
    config_path = base_dir / "config.py"
    spec = importlib.util.spec_from_file_location(
        "sefin_config_local", str(config_path)
    )
    sefin_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sefin_config)
    dir_parquet, dir_analises, _ = sefin_config.obter_diretorios_cnpj(cnpj)
    produtos_path = dir_analises / f"produtos_agregados_{cnpj}.parquet"
    base_detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj}.parquet"
    indexados_path = dir_analises / f"produtos_indexados_{cnpj}.parquet"
    codigos_path = dir_analises / f"codigos_multidescricao_{cnpj}.parquet"
    variacoes_path = dir_analises / f"variacoes_produtos_{cnpj}.parquet"

    _cleanup_legacy_produto_artifacts(dir_analises, cnpj)

    logger.info(
        "[produto_runtime] reconstruindo pipeline atual de produtos para %s", cnpj
    )

    df_base = _carregar_base_detalhes(dir_parquet)
    if df_base.is_empty():
        empty = _empty_produtos_result()
        empty.write_parquet(str(produtos_path))
        pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS}).write_parquet(
            str(base_detalhes_path)
        )
        _build_produtos_indexados(
            pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS}), empty
        ).write_parquet(str(indexados_path))
        _build_codigos_multidescricao(
            pl.DataFrame(schema={"codigo": pl.Utf8})
        ).write_parquet(str(codigos_path))
        _build_variacoes_produtos(
            pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})
        ).write_parquet(str(variacoes_path))
        _cleanup_legacy_produto_artifacts(dir_analises, cnpj)
        return empty

    df_base = _aplicar_mapas_manuais(df_base, dir_analises, cnpj)
    df_base = _aplicar_desagregacao_codigos(df_base)
    df_agregados = _build_produtos_agregados(df_base)
    df_indexados = _build_produtos_indexados(df_base, df_agregados)
    df_codigos = _build_codigos_multidescricao(df_indexados)
    df_variacoes = _build_variacoes_produtos(df_base)

    df_base.write_parquet(str(base_detalhes_path))
    df_agregados.write_parquet(str(produtos_path))
    df_indexados.write_parquet(str(indexados_path))
    df_codigos.write_parquet(str(codigos_path))
    df_variacoes.write_parquet(str(variacoes_path))
    _cleanup_legacy_produto_artifacts(dir_analises, cnpj)
    return df_agregados


def _normalize_mapa_descricoes_manual(
    df: pl.DataFrame, default_acao: str = "AGREGAR"
) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in DESCRIPTION_MANUAL_MAP_COLUMNS})

    for col in DESCRIPTION_MANUAL_MAP_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").cast(pl.Utf8).alias(col))

    rows: list[dict[str, str]] = []

    # ⚡ Bolt Optimization: Replace slow `df.to_dicts()` iteration with fast `zip()` over column Series lists.
    # By extracting list primitives natively and bypassing per-row Python dict allocations via FFI,
    # we significantly speed up row-level parsing logic for large datasets.
    cols_data = [
        df.get_column("tipo_regra").to_list(),
        df.get_column("descricao_origem").to_list(),
        df.get_column("descricao_destino").to_list(),
        df.get_column("descricao_par").to_list(),
        df.get_column("hash_descricoes_key").to_list(),
        df.get_column("chave_grupo_a").to_list(),
        df.get_column("chave_grupo_b").to_list(),
        df.get_column("score_origem").to_list(),
        df.get_column("acao_manual").to_list(),
    ]

    for (
        r_tipo,
        r_origem,
        r_destino,
        r_par,
        r_hash,
        r_chave_a,
        r_chave_b,
        r_score,
        r_acao,
    ) in zip(*cols_data):
        tipo_regra = _canon_text(r_tipo, "")
        origem = _canon_text(r_origem, "")
        destino = _canon_text(r_destino, "")
        descricao_par = _canon_text(r_par, "")
        rows.append(
            {
                "tipo_regra": tipo_regra,
                "descricao_origem": origem,
                "descricao_destino": destino,
                "descricao_par": descricao_par,
                "hash_descricoes_key": str(
                    r_hash
                    or _build_description_hash(
                        origem, destino, descricao_par, tipo_regra
                    )
                ),
                "chave_grupo_a": _canon_text(r_chave_a, ""),
                "chave_grupo_b": _canon_text(r_chave_b, ""),
                "score_origem": str(r_score or "").strip(),
                "acao_manual": _canon_text(r_acao, default_acao),
            }
        )
    return (
        pl.DataFrame(rows)
        .select(DESCRIPTION_MANUAL_MAP_COLUMNS)
        .unique(subset=["hash_descricoes_key"], keep="last")
    )


def merge_mapa_descricoes_manual(
    mapa_path: str | Path, df_novo: pl.DataFrame, default_acao: str = "AGREGAR"
) -> None:
    mapa_path = Path(mapa_path)
    novo = _normalize_mapa_descricoes_manual(df_novo, default_acao=default_acao)
    if mapa_path.exists():
        existente = _normalize_mapa_descricoes_manual(
            pl.read_parquet(str(mapa_path)), default_acao=default_acao
        )
        merged = pl.concat([existente, novo], how="diagonal_relaxed").unique(
            subset=["hash_descricoes_key"], keep="last"
        )
        merged.write_parquet(str(mapa_path))
    else:
        novo.write_parquet(str(mapa_path))


def _source_frame_rows(
    path: Path, fonte: str, mappings: dict[str, str]
) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})
    df = pl.read_parquet(str(path))
    rows: list[dict[str, Any]] = []

    # ⚡ Bolt Optimization: Replacing slow `df.to_dicts()` iteration with fast `zip()` over column Series.
    # This avoids massive Python dictionary allocation overhead per row while crossing the Rust FFI boundary,
    # resulting in a significant performance boost for large dataset reads.
    cod_col = mappings.get("codigo", "")
    desc_col = mappings.get("descricao", "")
    tipo_col = mappings.get("tipo_item", "")
    compl_col = mappings.get("descr_compl", "")
    ncm_col = mappings.get("ncm", "")
    cest_col = mappings.get("cest", "")
    gtin_col = mappings.get("gtin", "")
    unid_col = mappings.get("unid", "")

    # Retrieve columns safely (fallback to empty series if missing)
    cod_series = (
        df.get_column(cod_col)
        if cod_col in df.columns
        else pl.Series([None] * df.height)
    )
    desc_series = (
        df.get_column(desc_col)
        if desc_col in df.columns
        else pl.Series([None] * df.height)
    )
    tipo_series = (
        df.get_column(tipo_col)
        if tipo_col in df.columns
        else pl.Series([None] * df.height)
    )
    compl_series = (
        df.get_column(compl_col)
        if compl_col in df.columns
        else pl.Series([None] * df.height)
    )
    ncm_series = (
        df.get_column(ncm_col)
        if ncm_col in df.columns
        else pl.Series([None] * df.height)
    )
    cest_series = (
        df.get_column(cest_col)
        if cest_col in df.columns
        else pl.Series([None] * df.height)
    )
    gtin_series = (
        df.get_column(gtin_col)
        if gtin_col in df.columns
        else pl.Series([None] * df.height)
    )
    unid_series = (
        df.get_column(unid_col)
        if unid_col in df.columns
        else pl.Series([None] * df.height)
    )

    for cod, desc, tipo, compl, ncm, cest, gtin, unid in zip(
        cod_series,
        desc_series,
        tipo_series,
        compl_series,
        ncm_series,
        cest_series,
        gtin_series,
        unid_series,
    ):
        codigo = _clean_value(cod)
        descricao = _clean_value(desc)
        if not codigo or not descricao:
            continue
        tipo_item = _clean_value(tipo)
        rows.append(
            {
                "fonte": fonte,
                "codigo": codigo,
                "descricao": descricao,
                "descr_compl": _clean_value(compl),
                "tipo_item": tipo_item,
                "ncm": doc_clean_ncm(ncm),
                "cest": doc_clean_cest(cest),
                "gtin": _clean_gtin(gtin),
                "unid": doc_normalize_unit(unid),
                "codigo_original": codigo,
                "descricao_original": descricao,
                "tipo_item_original": tipo_item,
                "hash_manual_key": "",
            }
        )
    return (
        pl.DataFrame(rows).select(_DETAIL_COLUMNS)
        if rows
        else pl.DataFrame(schema={c: pl.Utf8 for c in _DETAIL_COLUMNS})
    )


def _carregar_base_detalhes(dir_parquet: Path) -> pl.DataFrame:
    frames = [
        _source_frame_rows(
            dir_parquet
            / next((p.name for p in dir_parquet.glob("NFe_*.parquet")), "__missing__"),
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
            dir_parquet
            / next((p.name for p in dir_parquet.glob("NFCe_*.parquet")), "__missing__"),
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
            dir_parquet
            / next(
                (p.name for p in dir_parquet.glob("c170_simplificada_*.parquet")),
                "__missing__",
            ),
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
            dir_parquet
            / next(
                (p.name for p in dir_parquet.glob("bloco_h_*.parquet")), "__missing__"
            ),
            "BLOCO_H",
            {
                "codigo": "codigo_produto",
                "descricao": "descricao_produto",
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
        expr = (
            pl.col(col_name)
            .fill_null("")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
        )
        return pl.when(expr == "").then(pl.lit(default_empty)).otherwise(expr)

    df = df.with_columns(
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
        ).alias("__temp_concat")
    )

    # ⚡ Bolt Optimization: apply hashlib.sha1 only to unique values to minimize
    # map_elements overhead and FFI boundary crossing, making it significantly faster
    # for dataframes with repetitive items.
    unique_df = (
        df.select("__temp_concat")
        .unique()
        .with_columns(
            pl.col("__temp_concat")
            .map_elements(
                lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest(),
                return_dtype=pl.Utf8,
            )
            .alias("hash_manual_key")
        )
    )

    return df.join(unique_df, on="__temp_concat", how="left").drop("__temp_concat")


def _aplicar_desagregacao_codigos(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return df_base

    # ⚡ Bolt Optimization: map_elements only on unique descriptions to prevent massive FFI overhead per row
    unique_desc = (
        df_base.select("descricao")
        .drop_nulls()
        .unique()
        .with_columns(
            pl.col("descricao")
            .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
            .alias("descricao_normalizada")
        )
    )
    work = df_base.join(unique_desc, on="descricao", how="left")

    code_groups = (
        work.group_by("codigo")
        .agg(
            pl.col("descricao_normalizada")
            .drop_nulls()
            .unique()
            .sort()
            .alias("__descricoes_norm")
        )
        .filter(pl.col("__descricoes_norm").list.len() > 1)
    )
    if code_groups.is_empty():
        return work.drop("descricao_normalizada")

    replacements: dict[str, str] = {}
    for cod, desc_norm_list in zip(
        code_groups["codigo"], code_groups["__descricoes_norm"]
    ):
        codigo = str(cod or "").strip()
        raw_list = (
            desc_norm_list.to_list()
            if hasattr(desc_norm_list, "to_list")
            else desc_norm_list
        )
        groups = [
            str(item or "").strip()
            for item in (raw_list or [])
            if str(item or "").strip()
        ]
        for index, descricao_norm in enumerate(groups, start=1):
            replacements[f"{codigo}|{descricao_norm}"] = (
                f"{codigo}_SEPARADO_{index:02d}"
            )

    replacements_df = pl.DataFrame(
        {
            "__desagregacao_key": list(replacements.keys()),
            "__codigo_desagregado": list(replacements.values()),
        },
        schema={"__desagregacao_key": pl.Utf8, "__codigo_desagregado": pl.Utf8},
    )

    return (
        work.with_columns(
            pl.concat_str(["codigo", "descricao_normalizada"], separator="|").alias(
                "__desagregacao_key"
            )
        )
        .join(replacements_df, on="__desagregacao_key", how="left")
        .with_columns(pl.col("__codigo_desagregado").fill_null(""))
        .with_columns(
            pl.when(pl.col("__codigo_desagregado") != "")
            .then(pl.col("__codigo_desagregado"))
            .otherwise(pl.col("codigo"))
            .alias("codigo")
        )
        .drop(
            ["descricao_normalizada", "__desagregacao_key", "__codigo_desagregado"],
            strict=False,
        )
    )


def _resolve_description_unions(mapa_descricoes_path: Path) -> dict[str, str]:
    if not mapa_descricoes_path.exists():
        return {}
    df = _normalize_mapa_descricoes_manual(pl.read_parquet(str(mapa_descricoes_path)))
    parent: dict[str, str] = {}
    if {"tipo_regra", "descricao_origem", "descricao_destino"}.issubset(df.columns):
        # ⚡ Bolt Optimization: Replacing slow `df.to_dicts()` iteration with fast `zip()` over column Series lists.
        for tipo, desc_origem, desc_destino in zip(
            df.get_column("tipo_regra").to_list(),
            df.get_column("descricao_origem").to_list(),
            df.get_column("descricao_destino").to_list(),
        ):
            if tipo != "UNIR_GRUPOS":
                continue
            origem = _canon_text(desc_origem, "")
            destino = _canon_text(desc_destino, "")
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


def _aplicar_mapas_manuais(
    df_base: pl.DataFrame, dir_analises: Path, cnpj: str
) -> pl.DataFrame:
    if df_base.is_empty():
        return df_base

    mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj}.parquet"
    mapa_manual_path = dir_analises / f"mapa_manual_unificacao_{cnpj}.parquet"

    unions = _resolve_description_unions(mapa_descricoes_path)
    if unions:
        canon_expr = (
            pl.col("descricao")
            .fill_null("")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
        )
        canon_expr = pl.when(canon_expr == "").then(pl.lit("")).otherwise(canon_expr)
        clean_expr = pl.col("descricao").fill_null("").cast(pl.Utf8).str.strip_chars()

        try:
            df_base = df_base.with_columns(
                canon_expr.replace_strict(unions, default=clean_expr).alias("descricao")
            )
        except (AttributeError, TypeError, Exception):
            # Fallback for older Polars versions using join instead of map_elements
            # Convert unions dict to DataFrame and do a left join
            unions_df = pl.DataFrame(
                {
                    "__canon_desc": list(unions.keys()),
                    "__replaced_desc": list(unions.values()),
                },
                schema={"__canon_desc": pl.Utf8, "__replaced_desc": pl.Utf8},
            )
            df_base = (
                df_base.with_columns(
                    canon_expr.alias("__canon_desc"), clean_expr.alias("__clean_desc")
                )
                .join(unions_df, on="__canon_desc", how="left")
                .with_columns(
                    pl.coalesce(["__replaced_desc", "__clean_desc"]).alias("descricao")
                )
                .drop(["__canon_desc", "__clean_desc", "__replaced_desc"], strict=False)
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
        df_base = (
            df_base.join(mapa, on="hash_manual_key", how="left")
            .with_columns(
                [
                    pl.when(
                        pl.col("__codigo_novo").is_not_null()
                        & (pl.col("__codigo_novo") != "")
                    )
                    .then(pl.col("__codigo_novo"))
                    .otherwise(pl.col("codigo"))
                    .alias("codigo"),
                    pl.when(
                        pl.col("__descricao_nova").is_not_null()
                        & (pl.col("__descricao_nova") != "")
                    )
                    .then(pl.col("__descricao_nova"))
                    .otherwise(pl.col("descricao"))
                    .alias("descricao"),
                    pl.when(
                        pl.col("__ncm_novo").is_not_null()
                        & (pl.col("__ncm_novo") != "")
                    )
                    .then(pl.col("__ncm_novo"))
                    .otherwise(pl.col("ncm"))
                    .alias("ncm"),
                    pl.when(
                        pl.col("__cest_novo").is_not_null()
                        & (pl.col("__cest_novo") != "")
                    )
                    .then(pl.col("__cest_novo"))
                    .otherwise(pl.col("cest"))
                    .alias("cest"),
                    pl.when(
                        pl.col("__gtin_novo").is_not_null()
                        & (pl.col("__gtin_novo") != "")
                    )
                    .then(pl.col("__gtin_novo"))
                    .otherwise(pl.col("gtin"))
                    .alias("gtin"),
                    pl.when(
                        pl.col("__tipo_item_novo").is_not_null()
                        & (pl.col("__tipo_item_novo") != "")
                    )
                    .then(pl.col("__tipo_item_novo"))
                    .otherwise(pl.col("tipo_item"))
                    .alias("tipo_item"),
                ]
            )
            .drop([c for c in df_base.columns if c.startswith("__")], strict=False)
        )
    return df_base


def _build_produtos_agregados(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "chave_produto": pl.Utf8,
                "descricao": pl.Utf8,
                "descricao_normalizada": pl.Utf8,
                "lista_descricao": pl.Utf8,
                "lista_descr_compl": pl.Utf8,
                "qtd_descricoes": pl.Int64,
                "qtd_codigos": pl.Int64,
                "lista_codigos": pl.Utf8,
                "lista_ncm": pl.Utf8,
                "lista_cest": pl.Utf8,
                "lista_gtin": pl.Utf8,
                "ncm_consenso": pl.Utf8,
                "cest_consenso": pl.Utf8,
                "gtin_consenso": pl.Utf8,
                "tipo_item_consenso": pl.Utf8,
                "codigo_consenso": pl.Utf8,
                "codigo_padrao": pl.Utf8,
                "lista_unid": pl.Utf8,
                "descricoes_conflitantes": pl.Utf8,
                "requer_revisao_manual": pl.Boolean,
            }
        )
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in df_base.to_dicts():
        desc_norm = doc_normalize_description_key(row.get("descricao"))
        if not desc_norm:
            continue
        buckets.setdefault(desc_norm, []).append(row)

    grouped_rows: list[dict[str, Any]] = []
    for index, (desc_norm, rows) in enumerate(sorted(buckets.items()), start=1):
        descricoes = sorted(
            {
                _clean_value(row.get("descricao"))
                for row in rows
                if _clean_value(row.get("descricao"))
            }
        )
        descr_compls = sorted(
            {
                _clean_value(row.get("descr_compl"))
                for row in rows
                if _clean_value(row.get("descr_compl"))
            }
        )
        codigos = sorted(
            {
                _clean_value(row.get("codigo"))
                for row in rows
                if _clean_value(row.get("codigo"))
            }
        )
        ncms = sorted(
            {
                _clean_value(row.get("ncm"))
                for row in rows
                if _clean_value(row.get("ncm"))
            }
        )
        cests = sorted(
            {
                _clean_value(row.get("cest"))
                for row in rows
                if _clean_value(row.get("cest"))
            }
        )
        gtins = sorted(
            {
                _clean_value(row.get("gtin"))
                for row in rows
                if _clean_value(row.get("gtin"))
            }
        )
        tipos = sorted(
            {
                _clean_value(row.get("tipo_item"))
                for row in rows
                if _clean_value(row.get("tipo_item"))
            }
        )
        unidades = sorted(
            {
                _clean_value(row.get("unid"))
                for row in rows
                if _clean_value(row.get("unid"))
            }
        )

        conflitos: list[str] = []
        if len(descricoes) > 1:
            conflitos.append("DESCRICAO")
        if len(codigos) > 1:
            conflitos.append("CODIGO")
        if len(ncms) > 1:
            conflitos.append("NCM")
        if len(cests) > 1:
            conflitos.append("CEST")
        if len(gtins) > 1:
            conflitos.append("GTIN")
        if len(tipos) > 1:
            conflitos.append("TIPO_ITEM")

        codigo_padrao = doc_choose_standard_code(rows)
        grouped_rows.append(
            {
                "chave_produto": f"ID_{index:04d}",
                "descricao": _consensus(descricoes)
                or (descricoes[0] if descricoes else ""),
                "descricao_normalizada": desc_norm,
                "lista_descricao": " | ".join(descricoes),
                "lista_descr_compl": " | ".join(descr_compls),
                "qtd_descricoes": len(descricoes),
                "qtd_codigos": len(codigos),
                "lista_codigos": ", ".join(codigos),
                "lista_ncm": ", ".join(ncms),
                "lista_cest": ", ".join(cests),
                "lista_gtin": ", ".join(gtins),
                "ncm_consenso": _consensus(ncms),
                "cest_consenso": _consensus(cests),
                "gtin_consenso": _consensus(gtins),
                "tipo_item_consenso": _consensus(tipos),
                "codigo_consenso": codigo_padrao,
                "codigo_padrao": codigo_padrao,
                "lista_unid": ", ".join(unidades),
                "descricoes_conflitantes": ", ".join(conflitos),
                "requer_revisao_manual": bool(conflitos),
            }
        )

    return pl.DataFrame(grouped_rows).select(
        "chave_produto",
        "descricao",
        "descricao_normalizada",
        "lista_descricao",
        "lista_descr_compl",
        "qtd_descricoes",
        "qtd_codigos",
        "lista_codigos",
        "lista_ncm",
        "lista_cest",
        "lista_gtin",
        "ncm_consenso",
        "cest_consenso",
        "gtin_consenso",
        "tipo_item_consenso",
        "codigo_consenso",
        "codigo_padrao",
        "lista_unid",
        "descricoes_conflitantes",
        "requer_revisao_manual",
    )


def _build_produtos_indexados(
    df_base: pl.DataFrame, df_agregados: pl.DataFrame
) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "chave_produto": pl.Utf8,
                "codigo": pl.Utf8,
                "descricao": pl.Utf8,
                "descricao_normalizada": pl.Utf8,
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
    # ⚡ Bolt Optimization: map_elements only on unique descriptions to prevent massive FFI overhead per row
    unique_desc = (
        df_base.select("descricao")
        .drop_nulls()
        .unique()
        .with_columns(
            pl.col("descricao")
            .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
            .alias("descricao_normalizada")
        )
    )
    df_base_norm = df_base.join(unique_desc, on="descricao", how="left")
    joined = df_base_norm.join(
        df_agregados.select(["descricao_normalizada", "chave_produto"]),
        on="descricao_normalizada",
        how="left",
    )
    return (
        joined.group_by(
            [
                "chave_produto",
                "codigo",
                "descricao",
                "descricao_normalizada",
                "descr_compl",
                "tipo_item",
                "ncm",
                "cest",
                "gtin",
            ]
        )
        .agg(
            [
                pl.len().cast(pl.Int64).alias("qtd_linhas"),
                pl.col("unid")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__unidades"),
                pl.col("fonte")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__fontes"),
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
                "qtd_descricoes_normalizadas": pl.Int64,
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

    # ⚡ Bolt Optimization: map_elements only on unique descriptions to prevent massive FFI overhead per row
    if "descricao_normalizada" not in df_indexados.columns:
        unique_desc = (
            df_indexados.select("descricao")
            .drop_nulls()
            .unique()
            .with_columns(
                pl.col("descricao")
                .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
                .alias("descricao_normalizada")
            )
        )
        df_indexados = df_indexados.join(unique_desc, on="descricao", how="left")
    else:
        missing_norm = df_indexados.filter(
            pl.col("descricao_normalizada").is_null()
            | (pl.col("descricao_normalizada").cast(pl.Utf8) == "")
        )
        if not missing_norm.is_empty():
            unique_desc = (
                missing_norm.select("descricao")
                .drop_nulls()
                .unique()
                .with_columns(
                    pl.col("descricao")
                    .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
                    .alias("__new_descricao_normalizada")
                )
            )
            df_indexados = (
                df_indexados.join(unique_desc, on="descricao", how="left")
                .with_columns(
                    pl.when(
                        pl.col("descricao_normalizada").is_null()
                        | (pl.col("descricao_normalizada").cast(pl.Utf8) == "")
                    )
                    .then(pl.col("__new_descricao_normalizada"))
                    .otherwise(pl.col("descricao_normalizada").cast(pl.Utf8))
                    .alias("descricao_normalizada")
                )
                .drop("__new_descricao_normalizada")
            )

    grouped = (
        df_indexados.group_by("codigo")
        .agg(
            [
                pl.col("descricao")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__descricoes"),
                pl.col("descricao_normalizada")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__descricoes_norm"),
                pl.col("ncm").drop_nulls().cast(pl.Utf8).unique().sort().alias("__ncm"),
                pl.col("cest")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__cest"),
                pl.col("gtin")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__gtin"),
                pl.col("tipo_item")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__tipo"),
                pl.col("chave_produto")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__chaves"),
                pl.col("descr_compl")
                .drop_nulls()
                .cast(pl.Utf8)
                .unique()
                .sort()
                .alias("__compl"),
            ]
        )
        .with_columns(
            [
                pl.col("__descricoes")
                .list.len()
                .cast(pl.Int64)
                .alias("qtd_descricoes"),
                pl.col("__descricoes_norm")
                .list.len()
                .cast(pl.Int64)
                .alias("qtd_descricoes_normalizadas"),
            ]
        )
        .filter(pl.col("qtd_descricoes_normalizadas") > 1)
        .with_columns(
            [
                pl.col("__descricoes").list.join("<<#>>").alias("lista_descricoes"),
                pl.col("__ncm").list.join(", ").alias("lista_ncm"),
                pl.col("__cest").list.join(", ").alias("lista_cest"),
                pl.col("__gtin").list.join(", ").alias("lista_gtin"),
                pl.col("__tipo").list.join(", ").alias("lista_tipo_item"),
                pl.col("__chaves").list.join(", ").alias("lista_chave_produto"),
                pl.col("__compl").list.join(" | ").alias("lista_descr_compl"),
                pl.col("__chaves")
                .list.len()
                .cast(pl.Int64)
                .alias("qtd_grupos_descricao_afetados"),
            ]
        )
        .drop(
            [
                "__descricoes",
                "__descricoes_norm",
                "__ncm",
                "__cest",
                "__gtin",
                "__tipo",
                "__chaves",
                "__compl",
            ]
        )
        .sort("codigo")
    )
    return grouped


def _build_variacoes_produtos(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "descricao": pl.Utf8,
                "qtd_codigos": pl.Int64,
                "qtd_ncm": pl.Int64,
                "qtd_gtin": pl.Int64,
            }
        )

    # ⚡ Bolt Optimization: map_elements only on unique descriptions to prevent massive FFI overhead per row
    unique_desc = (
        df_base.select("descricao")
        .drop_nulls()
        .unique()
        .with_columns(
            pl.col("descricao")
            .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
            .alias("descricao_normalizada")
        )
    )
    df_base_norm = df_base.join(unique_desc, on="descricao", how="left")

    return (
        df_base_norm.group_by("descricao_normalizada")
        .agg(
            [
                pl.col("descricao")
                .drop_nulls()
                .cast(pl.Utf8)
                .mode()
                .first()
                .alias("descricao"),
                pl.col("codigo").n_unique().cast(pl.Int64).alias("qtd_codigos"),
                pl.col("ncm").n_unique().cast(pl.Int64).alias("qtd_ncm"),
                pl.col("gtin").n_unique().cast(pl.Int64).alias("qtd_gtin"),
            ]
        )
        .filter(
            (pl.col("qtd_codigos") > 1)
            | (pl.col("qtd_ncm") > 1)
            | (pl.col("qtd_gtin") > 1)
        )
        .sort(["qtd_codigos", "qtd_ncm", "qtd_gtin"], descending=[True, True, True])
    )


def produto_pipeline_em_modo_compatibilidade() -> bool:
    return False
