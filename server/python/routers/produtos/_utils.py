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
from core.models import (
    AutoSepararResidualRequest,
    DesfazerManualCodigoRequest,
    DesfazerManualDescricoesRequest,
    DescricaoManualMapItem,
    ProdutoAnaliseStatusRequest,
    ProdutoUnidRequest,
    RevisaoManualSubmitRequest,
    ResolverManualDescricoesRequest,
    ResolverManualUnificarRequest,
    ResolverManualDesagregarRequest,
    ResolverManualMultiDetalhesRequest,
    UnificacaoLoteApplyRequest,
    UnificacaoLotePreviewRequest,
)
from core.produto_batch_lote import (
    RULE_CONFIG as BATCH_RULE_CONFIG,
    RULE_PRIORITY as BATCH_RULE_PRIORITY,
    construir_preview_unificacao_lote,
    filtrar_tabela_final_para_lote,
    ocultar_grupos_verificados,
)
from core.produto_runtime import (
    _normalize_mapa_descricoes_manual,
    build_vector_cache_metadata,
    cache_metadata_matches,
    compute_file_sha1,
    construir_tabela_pares_descricoes_faiss,
    construir_tabela_pares_descricoes_light,
    construir_tabela_pares_descricoes_similares,
    merge_mapa_descricoes_manual,
    obter_runtime_produtos_status,
    obter_status_vectorizacao,
    read_vector_cache_metadata,
    unificar_produtos_unidades,
    write_vector_cache_metadata,
)
from core.utils import validar_cnpj

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python", tags=["produto_unid"])

# Get project root from environment or handle it
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent

# Ensure project root is in Python path for cruzamentos imports
if str(_PROJETO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJETO_DIR))

_MANUAL_MAP_COLUMNS = [
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

_DESCRIPTION_HISTORY_COLUMNS = [
    "snapshot_seq",
    "snapshot_ts_utc",
    "snapshot_label",
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

_VERIFICADOS_COLUMNS = [
    "tipo_ref",
    "ref_id",
    "ref_id_aux",
    "descricao_ref",
    "contexto_tela",
    "status_analise",
    "dt_evento",
]

_STATUS_ANALISE_COLUMNS = [
    "tipo_ref",
    "ref_id",
    "ref_id_aux",
    "descricao_ref",
    "contexto_tela",
    "status_analise",
    "origem_status",
    "dt_ultima_acao",
]


def _normalize_page(value: Any) -> int:
    try:
        return max(1, int(value))
    except Exception:
        return 1


def _normalize_page_size(value: Any, default: int = 50, max_size: int = 200) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(1, min(parsed, max_size))


def _paginate_frame(df: pl.DataFrame, page: int, page_size: int) -> tuple[pl.DataFrame, int, int]:
    total = int(df.height)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    offset = (page - 1) * page_size
    return df.slice(offset, page_size), total, total_pages


def _load_cnpj_dirs(cnpj_limpo: str) -> tuple[Path, Path, Path]:
    import importlib.util

    _config_path = _PROJETO_DIR / "config.py"
    _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
    _sefin_config = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_sefin_config)
    return _sefin_config.obter_diretorios_cnpj(cnpj_limpo)


def _canon_text(value: Any, vazio: str = "(VAZIO)") -> str:
    text = "" if value is None else str(value)
    text = text.strip().upper()
    return text if text else vazio



import difflib
from functools import lru_cache

_STOP_WORDS = {"DE", "DA", "DO", "DAS", "DOS", "E", "COM", "SEM", "PARA", "UN", "PCT", "CX", "KG", "LT", "ML", "GR", "PC", "LATA", "LITRO", "LITROS", "GARRAFA"}

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
    clean_text = re.sub(r"[^A-Z0-9 ]+", " ", _normalize_similarity_text(value))
    return tuple(
        token for token in clean_text.split()
        if len(token) > 1 and token not in _STOP_WORDS
    )

@lru_cache(maxsize=10000)
def _normalize_similarity_tokens_set(value: str) -> frozenset[str]:
    return frozenset(_normalize_similarity_tokens(value))


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
    if not a and not b: return 1.0
    if not a or not b: return 0.0

    a_str = str(a)
    b_str = str(b)

    token_score = _jaccard(_normalize_similarity_tokens_set(a_str), _normalize_similarity_tokens_set(b_str))
    sequence_score = _sequence_match(a_str, b_str)

    return 0.4 * token_score + 0.6 * sequence_score


def _primary_value(value: Any) -> str:
    return str((str(value or "").split(",")[0])).strip()


def _resumir_motivos_ignorados(motivos_ignorados: list[dict[str, str]]) -> list[dict[str, Any]]:
    contagem: dict[str, int] = {}
    amostras: dict[str, list[str]] = {}
    for item in motivos_ignorados:
        motivo = str(item.get("motivo") or "Nao elegivel").strip() or "Nao elegivel"
        codigo = str(item.get("codigo") or "").strip()
        contagem[motivo] = contagem.get(motivo, 0) + 1
        if codigo:
            amostras.setdefault(motivo, [])
            if codigo not in amostras[motivo] and len(amostras[motivo]) < 5:
                amostras[motivo].append(codigo)
    return [
        {"motivo": motivo, "qtd_codigos": qtd, "codigos_amostra": amostras.get(motivo, [])}
        for motivo, qtd in sorted(contagem.items(), key=lambda pair: (-pair[1], pair[0]))
    ]


def _build_manual_hash(
    fonte: Any,
    codigo_original: Any,
    descricao_original: Any,
    tipo_item_original: Any,
) -> str:
    payload = "|".join(
        [
            _canon_text(fonte),
            _canon_text(codigo_original),
            _canon_text(descricao_original),
            _canon_text(tipo_item_original),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_manual_decisions(df: pl.DataFrame, default_acao: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in _MANUAL_MAP_COLUMNS})

    for col in _MANUAL_MAP_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    df = df.with_columns(
        [
            pl.col("fonte").cast(pl.Utf8).fill_null(""),
            pl.col("codigo_original").cast(pl.Utf8).fill_null(""),
            pl.col("descricao_original").cast(pl.Utf8).fill_null(""),
            pl.col("tipo_item_original").cast(pl.Utf8).fill_null(""),
            pl.col("codigo_novo").cast(pl.Utf8).fill_null(""),
            pl.col("descricao_nova").cast(pl.Utf8).fill_null(""),
            pl.col("ncm_novo").cast(pl.Utf8).fill_null(""),
            pl.col("cest_novo").cast(pl.Utf8).fill_null(""),
            pl.col("gtin_novo").cast(pl.Utf8).fill_null(""),
            pl.col("tipo_item_novo").cast(pl.Utf8).fill_null(""),
            pl.when(pl.col("acao_manual").is_null() | (pl.col("acao_manual").cast(pl.Utf8).str.strip_chars() == ""))
            .then(pl.lit(default_acao))
            .otherwise(pl.col("acao_manual").cast(pl.Utf8))
            .alias("acao_manual"),
        ]
    )

    rows = []
    for row in df.select(_MANUAL_MAP_COLUMNS).to_dicts():
        fonte = _canon_text(row.get("fonte"), "")
        codigo_original = _canon_text(row.get("codigo_original"), "")
        descricao_original = _canon_text(row.get("descricao_original"))
        tipo_item_original = _canon_text(row.get("tipo_item_original"))
        hash_manual_key = row.get("hash_manual_key") or _build_manual_hash(
            fonte,
            codigo_original,
            descricao_original,
            tipo_item_original,
        )
        rows.append(
            {
                "fonte": fonte,
                "codigo_original": codigo_original,
                "descricao_original": descricao_original,
                "tipo_item_original": tipo_item_original,
                "hash_manual_key": str(hash_manual_key),
                "codigo_novo": _canon_text(row.get("codigo_novo"), ""),
                "descricao_nova": _canon_text(row.get("descricao_nova"), ""),
                "ncm_novo": _canon_text(row.get("ncm_novo"), ""),
                "cest_novo": _canon_text(row.get("cest_novo"), ""),
                "gtin_novo": _canon_text(row.get("gtin_novo"), ""),
                "tipo_item_novo": _canon_text(row.get("tipo_item_novo"), ""),
                "acao_manual": _canon_text(row.get("acao_manual"), default_acao),
            }
        )

    normalized = pl.DataFrame(rows).select(_MANUAL_MAP_COLUMNS)
    return normalized.unique(subset=["hash_manual_key"], keep="last")


def _merge_manual_map(mapa_path: Path, df_novo: pl.DataFrame, default_acao: str) -> None:
    df_novo_norm = _normalize_manual_decisions(df_novo, default_acao=default_acao)
    if mapa_path.exists():
        df_existente = pl.read_parquet(str(mapa_path))
        df_existente_norm = _normalize_manual_decisions(df_existente, default_acao=default_acao)
        df_merge = pl.concat([df_existente_norm, df_novo_norm], how="diagonal_relaxed").unique(
            subset=["hash_manual_key"], keep="last"
        )
        df_merge.write_parquet(mapa_path)
    else:
        df_novo_norm.write_parquet(mapa_path)


def _snapshot_mapa_descricoes_history(history_path: Path, mapa_df: pl.DataFrame, snapshot_label: str) -> int:
    snapshot_seq = 1
    if history_path.exists():
        try:
            df_history = pl.read_parquet(str(history_path))
            if "snapshot_seq" in df_history.columns and df_history.height > 0:
                snapshot_seq = int(df_history["snapshot_seq"].max()) + 1
        except Exception:
            snapshot_seq = 1

    snapshot_ts = datetime.now(UTC).isoformat()
    if mapa_df.is_empty():
        df_snapshot = pl.DataFrame(schema={c: pl.Utf8 for c in _DESCRIPTION_HISTORY_COLUMNS}).with_columns(
            [
                pl.lit(snapshot_seq).cast(pl.Int64).alias("snapshot_seq"),
                pl.lit(snapshot_ts).alias("snapshot_ts_utc"),
                pl.lit(snapshot_label).alias("snapshot_label"),
            ]
        ).select(_DESCRIPTION_HISTORY_COLUMNS)
    else:
        df_snapshot = mapa_df.with_columns(
            [
                pl.lit(snapshot_seq).cast(pl.Int64).alias("snapshot_seq"),
                pl.lit(snapshot_ts).alias("snapshot_ts_utc"),
                pl.lit(snapshot_label).alias("snapshot_label"),
            ]
        ).select(_DESCRIPTION_HISTORY_COLUMNS)

    if history_path.exists():
        df_history = pl.read_parquet(str(history_path))
        pl.concat([df_history, df_snapshot], how="diagonal_relaxed").write_parquet(str(history_path))
    else:
        df_snapshot.write_parquet(str(history_path))

    return snapshot_seq


def _descricao_rule_matches(row: dict[str, Any], descricoes_set: set[str]) -> bool:
    tipo_regra = str(row.get("tipo_regra") or "").strip().upper()
    origem = _canon_text(row.get("descricao_origem"), "")
    destino = _canon_text(row.get("descricao_destino"), "")
    descricao_par = _canon_text(row.get("descricao_par"), "")
    if tipo_regra == "UNIR_GRUPOS":
        return origem in descricoes_set and destino in descricoes_set
    if tipo_regra == "MANTER_SEPARADO":
        return origem in descricoes_set and descricao_par in descricoes_set
    return False


def _normalize_status_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _normalizar_mapa_verificados(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={c: pl.Utf8 for c in _VERIFICADOS_COLUMNS})

    for col in _VERIFICADOS_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))

    rows = []
    for row in df.select(_VERIFICADOS_COLUMNS).to_dicts():
        rows.append(
            {
                "tipo_ref": _normalize_status_text(row.get("tipo_ref")),
                "ref_id": _normalize_status_text(row.get("ref_id")),
                "ref_id_aux": _normalize_status_text(row.get("ref_id_aux")),
                "descricao_ref": str(row.get("descricao_ref") or "").strip(),
                "contexto_tela": _normalize_status_text(row.get("contexto_tela")),
                "status_analise": _normalize_status_text(row.get("status_analise"), "VERIFICADO_SEM_ACAO"),
                "dt_evento": str(row.get("dt_evento") or datetime.now(UTC).isoformat()).strip(),
            }
        )
    return pl.DataFrame(rows).select(_VERIFICADOS_COLUMNS).unique(subset=["tipo_ref", "ref_id", "ref_id_aux"], keep="last")


def _gravar_status_analise(dir_analises: Path, cnpj_limpo: str) -> Path:
    mapa_verificados_path = dir_analises / f"mapa_verificados_produtos_{cnpj_limpo}.parquet"
    mapa_agregados_path = dir_analises / f"mapa_auditoria_agregados_{cnpj_limpo}.parquet"
    mapa_desagregados_path = dir_analises / f"mapa_auditoria_desagregados_{cnpj_limpo}.parquet"
    mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj_limpo}.parquet"
    agregados_produtos_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"
    status_path = dir_analises / f"status_analise_produtos_{cnpj_limpo}.parquet"

    status_rows: list[dict[str, str]] = []
    descricao_to_grupo: dict[str, tuple[str, str]] = {}

    if agregados_produtos_path.exists():
        df_grupos = pl.read_parquet(str(agregados_produtos_path))
        if {"descricao", "chave_produto"}.issubset(df_grupos.columns):
            for row in df_grupos.select(["descricao", "chave_produto"]).to_dicts():
                descricao = _normalize_status_text(row.get("descricao"))
                chave = _normalize_status_text(row.get("chave_produto"))
                if descricao and chave:
                    descricao_to_grupo[descricao] = (chave, str(row.get("descricao") or "").strip())

    if mapa_verificados_path.exists():
        df_verificados = _normalizar_mapa_verificados(pl.read_parquet(str(mapa_verificados_path)))
        status_rows.extend(
            df_verificados.with_columns(
                [
                    pl.lit("MAPA_VERIFICADOS").alias("origem_status"),
                    pl.col("dt_evento").alias("dt_ultima_acao"),
                ]
            ).select(_STATUS_ANALISE_COLUMNS).to_dicts()
        )

    if mapa_agregados_path.exists():
        df_agregados = pl.read_parquet(str(mapa_agregados_path))
        for row in df_agregados.to_dicts():
            descricao_nova = _normalize_status_text(row.get("descricao_nova"))
            grupo = descricao_to_grupo.get(descricao_nova)
            if not grupo:
                continue
            status_rows.append(
                {
                    "tipo_ref": "POR_GRUPO",
                    "ref_id": grupo[0],
                    "ref_id_aux": "",
                    "descricao_ref": grupo[1],
                    "contexto_tela": "CONSOLIDACAO_SELECAO",
                    "status_analise": "CONSOLIDADO",
                    "origem_status": "MAPA_AUDITORIA_AGREGADOS",
                    "dt_ultima_acao": datetime.now(UTC).isoformat(),
                }
            )

    if mapa_desagregados_path.exists():
        df_desagregados = pl.read_parquet(str(mapa_desagregados_path))
        for row in df_desagregados.to_dicts():
            codigo_original = _normalize_status_text(row.get("codigo_original"))
            if codigo_original:
                status_rows.append(
                    {
                        "tipo_ref": "POR_CODIGO",
                        "ref_id": codigo_original,
                        "ref_id_aux": "",
                        "descricao_ref": str(row.get("descricao_original") or "").strip(),
                        "contexto_tela": "REVISAO_RESIDUAL",
                        "status_analise": "SEPARADO",
                        "origem_status": "MAPA_AUDITORIA_DESAGREGADOS",
                        "dt_ultima_acao": datetime.now(UTC).isoformat(),
                    }
                )

    if mapa_descricoes_path.exists():
        df_descricoes = pl.read_parquet(str(mapa_descricoes_path))
        for row in df_descricoes.to_dicts():
            tipo_regra = _normalize_status_text(row.get("tipo_regra"))
            descricao_origem = _normalize_status_text(row.get("descricao_origem"))
            descricao_destino = _normalize_status_text(row.get("descricao_destino"))
            descricao_par = _normalize_status_text(row.get("descricao_par"))
            if tipo_regra == "UNIR_GRUPOS":
                for descricao in [descricao_origem, descricao_destino]:
                    grupo = descricao_to_grupo.get(descricao)
                    if not grupo:
                        continue
                    status_rows.append(
                        {
                            "tipo_ref": "POR_GRUPO",
                            "ref_id": grupo[0],
                            "ref_id_aux": "",
                            "descricao_ref": grupo[1],
                            "contexto_tela": "DECISAO_ENTRE_GRUPOS",
                            "status_analise": "UNIDO_ENTRE_GRUPOS",
                            "origem_status": "MAPA_MANUAL_DESCRICOES",
                            "dt_ultima_acao": datetime.now(UTC).isoformat(),
                        }
                    )
            elif tipo_regra == "MANTER_SEPARADO":
                for descricao, descricao_aux in [(descricao_origem, descricao_par), (descricao_par, descricao_origem)]:
                    grupo = descricao_to_grupo.get(descricao)
                    grupo_aux = descricao_to_grupo.get(descricao_aux)
                    if not grupo:
                        continue
                    status_rows.append(
                        {
                            "tipo_ref": "POR_GRUPO",
                            "ref_id": grupo[0],
                            "ref_id_aux": grupo_aux[0] if grupo_aux else "",
                            "descricao_ref": grupo[1],
                            "contexto_tela": "DECISAO_ENTRE_GRUPOS",
                            "status_analise": "MANTIDO_SEPARADO",
                            "origem_status": "MAPA_MANUAL_DESCRICOES",
                            "dt_ultima_acao": datetime.now(UTC).isoformat(),
                        }
                    )

    if status_rows:
        df_status = pl.DataFrame(status_rows).with_columns(
            pl.when(pl.col("status_analise") == "MANTIDO_SEPARADO")
            .then(pl.lit(50))
            .when(pl.col("status_analise") == "UNIDO_ENTRE_GRUPOS")
            .then(pl.lit(40))
            .when(pl.col("status_analise") == "CONSOLIDADO")
            .then(pl.lit(30))
            .when(pl.col("status_analise") == "SEPARADO")
            .then(pl.lit(20))
            .when(pl.col("status_analise") == "VERIFICADO_SEM_ACAO")
            .then(pl.lit(10))
            .otherwise(pl.lit(0))
            .alias("__prioridade")
        ).sort(["__prioridade", "dt_ultima_acao"], descending=[True, True]).unique(
            subset=["tipo_ref", "ref_id", "ref_id_aux"], keep="first"
        ).drop("__prioridade").select(_STATUS_ANALISE_COLUMNS)
    else:
        df_status = pl.DataFrame(schema={c: pl.Utf8 for c in _STATUS_ANALISE_COLUMNS})

    df_status.write_parquet(str(status_path))
    return status_path


def _reprocessar_produtos(dir_analises: Path, cnpj_limpo: str) -> pl.DataFrame:
    df_result = unificar_produtos_unidades(cnpj_limpo, projeto_dir=_PROJETO_DIR)
    _gravar_status_analise(dir_analises, cnpj_limpo)
    return df_result


def _resumir_status_analise(dir_analises: Path, cnpj_limpo: str, df_status: pl.DataFrame) -> dict[str, int]:
    codigos_multidescricao_path = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"
    produtos_agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

    def _count_distinct(tipo_ref: str, statuses: list[str]) -> int:
        if df_status.is_empty():
            return 0
        subset = df_status.filter(
            (pl.col("tipo_ref") == tipo_ref) & pl.col("status_analise").is_in(statuses)
        )
        if subset.is_empty():
            return 0
        return int(subset.select(pl.col("ref_id").n_unique()).item())

    verificados = _count_distinct("POR_CODIGO", ["VERIFICADO_SEM_ACAO"]) + _count_distinct(
        "POR_GRUPO", ["VERIFICADO_SEM_ACAO"]
    )
    consolidados = _count_distinct("POR_GRUPO", ["CONSOLIDADO"])
    separados = _count_distinct("POR_CODIGO", ["SEPARADO"])
    decididos_entre_grupos = _count_distinct("POR_GRUPO", ["UNIDO_ENTRE_GRUPOS", "MANTIDO_SEPARADO"])

    codigos_analisados: set[str] = set()
    grupos_analisados: set[str] = set()
    if not df_status.is_empty():
        for row in df_status.select(["tipo_ref", "ref_id", "status_analise"]).to_dicts():
            tipo_ref = _normalize_status_text(row.get("tipo_ref"))
            ref_id = _normalize_status_text(row.get("ref_id"))
            status = _normalize_status_text(row.get("status_analise"))
            if not ref_id:
                continue
            if tipo_ref == "POR_CODIGO" and status in {"SEPARADO", "VERIFICADO_SEM_ACAO"}:
                codigos_analisados.add(ref_id)
            if tipo_ref == "POR_GRUPO" and status in {
                "CONSOLIDADO",
                "UNIDO_ENTRE_GRUPOS",
                "MANTIDO_SEPARADO",
                "VERIFICADO_SEM_ACAO",
            }:
                grupos_analisados.add(ref_id)

    pendentes_codigos = 0
    if codigos_multidescricao_path.exists():
        df_codigos = pl.read_parquet(str(codigos_multidescricao_path))
        if "codigo" in df_codigos.columns:
            pendentes_codigos = sum(
                1
                for value in df_codigos.get_column("codigo").to_list()
                if _normalize_status_text(value) not in codigos_analisados
            )

    pendentes_grupos = 0
    if produtos_agregados_path.exists():
        df_grupos = pl.read_parquet(str(produtos_agregados_path))
        if {"chave_produto", "requer_revisao_manual"}.issubset(df_grupos.columns):
            df_pendentes_grupo = df_grupos.filter(pl.col("requer_revisao_manual") == True)
            pendentes_grupos = sum(
                1
                for value in df_pendentes_grupo.get_column("chave_produto").to_list()
                if _normalize_status_text(value) not in grupos_analisados
            )

    return {
        "pendentes": int(pendentes_codigos + pendentes_grupos),
        "verificados": int(verificados),
        "consolidados": int(consolidados),
        "separados": int(separados),
        "decididos_entre_grupos": int(decididos_entre_grupos),
    }
