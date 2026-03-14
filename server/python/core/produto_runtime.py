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


def produto_pipeline_em_modo_compatibilidade() -> bool:
    return True


def _canon_text(value: Any, vazio: str = "(VAZIO)") -> str:
    text = "" if value is None else str(value)
    text = text.strip().upper()
    return text if text else vazio


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
    _, dir_analises, _ = sefin_config.obter_diretorios_cnpj(cnpj)
    produtos_path = dir_analises / f"produtos_agregados_{cnpj}.parquet"
    if produtos_path.exists():
        logger.warning(
            "[produto_runtime] usando modo de compatibilidade sem pipeline legado de produtos para %s",
            cnpj,
        )
        return pl.read_parquet(str(produtos_path))
    logger.warning(
        "[produto_runtime] pipeline legado de produtos removido e nenhum parquet agregado existe para %s",
        cnpj,
    )
    return _empty_produtos_result()


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
