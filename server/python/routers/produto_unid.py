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
    construir_tabela_pares_descricoes_hibridos,
    construir_tabela_pares_descricoes_semanticos,
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


@router.get("/produtos/revisao-manual")
async def get_produtos_revisao_manual(cnpj: str = Query(...)):
    """Retorna os produtos que requerem revisao manual para o CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not agregados_path.exists():
            return {"success": True, "data": []}

        df = pl.scan_parquet(str(agregados_path)).filter(pl.col("requer_revisao_manual") == True).collect()

        return {"success": True, "data": df.to_dicts()}
    except Exception as e:
        logger.error("[get_produtos_revisao_manual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/revisao-final")
async def get_produtos_revisao_final(cnpj: str = Query(...)):
    """Retorna metadados da tabela final de produtos ja desagregada para a tela unica de revisao."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not agregados_path.exists():
            return {
                "success": True,
                "available": False,
                "file_path": str(agregados_path),
                "summary": {
                    "total_grupos": 0,
                    "grupos_revisao_manual": 0,
                    "grupos_com_gtin": 0,
                    "grupos_com_cest": 0,
                },
            }

        df = pl.read_parquet(str(agregados_path))
        expected_columns = {
            "lista_codigos",
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "codigo_padrao",
            "lista_descr_compl",
        }
        if not expected_columns.issubset(set(df.columns)):
            logger.info(
                "[get_produtos_revisao_final] parquet com schema antigo detectado para %s; regenerando tabela final.",
                cnpj_limpo,
            )
            df = unificar_produtos_unidades(cnpj_limpo, projeto_dir=_PROJETO_DIR)

        return {
            "success": True,
            "available": True,
            "file_path": str(agregados_path),
            "summary": {
                "total_grupos": int(df.height),
                "grupos_revisao_manual": int(df.filter(pl.col("requer_revisao_manual") == True).height)
                if "requer_revisao_manual" in df.columns
                else 0,
                "grupos_com_gtin": int(df.filter(pl.col("gtin_consenso").cast(pl.Utf8) != "").height)
                if "gtin_consenso" in df.columns
                else 0,
                "grupos_com_cest": int(df.filter(pl.col("cest_consenso").cast(pl.Utf8) != "").height)
                if "cest_consenso" in df.columns
                else 0,
            },
        }
    except Exception as e:
        logger.error("[get_produtos_revisao_final] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


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
        elif metodo_norm == "semantic":
            status_vector = obter_status_vectorizacao()
            semantic_mode = (status_vector.get("modes") or {}).get("semantic") or {}
            pares_path = dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.parquet"
            metadata_path = dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.json"
            if not semantic_mode.get("available"):
                return {
                    "success": False,
                    "available": False,
                    "metodo": "semantic",
                    "message": semantic_mode.get("message") or status_vector["message"],
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
                metodo="semantic",
                input_base_hash=base_hash,
                top_k=params_top_k,
                min_semantic_score=params_threshold,
                model_name=str(semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
            )

            if (forcar_recalculo or not cache_ok) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_semanticos(
                    df_agregados,
                    top_k=params_top_k,
                    min_semantic_score=params_threshold,
                ).write_parquet(str(pares_path))
                write_vector_cache_metadata(
                    metadata_path,
                    build_vector_cache_metadata(
                        metodo="semantic",
                        model_name=str(semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
                        engine=semantic_mode.get("engine") or status_vector.get("engine"),
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
                    "metodo": "semantic",
                    "message": "Nenhum par semantico gerado.",
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
            selected_message = "Pares semanticos carregados."
            selected_available = True
            selected_method = "semantic"
        elif metodo_norm == "hybrid":
            status_vector = obter_status_vectorizacao()
            hybrid_mode = (status_vector.get("modes") or {}).get("hybrid") or {}
            semantic_mode = (status_vector.get("modes") or {}).get("semantic") or {}
            lexical_path = dir_analises / f"pares_descricoes_similares_{cnpj_limpo}.parquet"
            semantic_path = dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.parquet"
            hybrid_path = dir_analises / f"pares_descricoes_similares_hibridos_{cnpj_limpo}.parquet"
            metadata_path = dir_analises / f"pares_descricoes_similares_hibridos_{cnpj_limpo}.json"
            if not hybrid_mode.get("available"):
                return {
                    "success": False,
                    "available": False,
                    "metodo": "hybrid",
                    "message": hybrid_mode.get("message") or status_vector["message"],
                    "file_path": str(hybrid_path),
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

            hybrid_metadata = read_vector_cache_metadata(metadata_path)
            semantic_metadata_path = dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.json"
            semantic_metadata = read_vector_cache_metadata(semantic_metadata_path)

            if (forcar_recalculo or not lexical_path.exists()) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_similares(df_agregados).write_parquet(str(lexical_path))

            semantic_cache_ok = semantic_path.exists() and cache_metadata_matches(
                semantic_metadata,
                metodo="semantic",
                input_base_hash=base_hash,
                top_k=params_top_k,
                min_semantic_score=params_threshold,
                model_name=str(semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
            )
            if (forcar_recalculo or not semantic_cache_ok) and agregados_path.exists():
                df_agregados = pl.read_parquet(str(agregados_path))
                construir_tabela_pares_descricoes_semanticos(
                    df_agregados,
                    top_k=params_top_k,
                    min_semantic_score=params_threshold,
                ).write_parquet(str(semantic_path))
                write_vector_cache_metadata(
                    semantic_metadata_path,
                    build_vector_cache_metadata(
                        metodo="semantic",
                        model_name=str(semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
                        engine=semantic_mode.get("engine") or status_vector.get("engine"),
                        input_base_hash=base_hash,
                        top_k=params_top_k,
                        min_semantic_score=params_threshold,
                        batch_size=32,
                    ),
                )
                semantic_metadata = read_vector_cache_metadata(semantic_metadata_path)

            hybrid_cache_ok = hybrid_path.exists() and cache_metadata_matches(
                hybrid_metadata,
                metodo="hybrid",
                input_base_hash=base_hash,
                top_k=params_top_k,
                min_semantic_score=params_threshold,
                model_name=str(hybrid_mode.get("model_name") or semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
            )
            if (forcar_recalculo or not hybrid_cache_ok) and lexical_path.exists() and semantic_path.exists():
                df_lexical = pl.read_parquet(str(lexical_path))
                df_semantic = pl.read_parquet(str(semantic_path))
                construir_tabela_pares_descricoes_hibridos(df_lexical, df_semantic).write_parquet(str(hybrid_path))
                write_vector_cache_metadata(
                    metadata_path,
                    build_vector_cache_metadata(
                        metodo="hybrid",
                        model_name=str(hybrid_mode.get("model_name") or semantic_mode.get("model_name") or status_vector.get("model_name") or "semantic"),
                        engine=hybrid_mode.get("engine") or status_vector.get("engine"),
                        input_base_hash=base_hash,
                        top_k=params_top_k,
                        min_semantic_score=params_threshold,
                        batch_size=32,
                    ),
                )
                hybrid_metadata = read_vector_cache_metadata(metadata_path)

            if not hybrid_path.exists():
                return {
                    "success": True,
                    "available": True,
                    "metodo": "hybrid",
                    "message": "Nenhum par hibrido gerado.",
                    "file_path": str(hybrid_path),
                    "cache_metadata": hybrid_metadata,
                    "data": [],
                    "page": page_norm,
                    "page_size": page_size_norm,
                    "total_file": 0,
                    "total_filtered": 0,
                    "total": 0,
                    "total_pages": 1,
                    "quick_filter_counts": {"todos": 0, "unirAutomatico": 0, "bloqueios": 0, "revisar": 0},
                }

            df = pl.read_parquet(str(hybrid_path))
            selected_path = hybrid_path
            selected_metadata = hybrid_metadata
            selected_message = "Pares hibridos carregados."
            selected_available = True
            selected_method = "hybrid"
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


@router.get("/produtos/vectorizacao-status")
async def get_vectorizacao_status(cnpj: str = Query(...)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"
        current_base_hash = compute_file_sha1(agregados_path) if agregados_path.exists() else None
        faiss_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.json")
        light_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.json")
        semantic_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.json")
        hybrid_cache = read_vector_cache_metadata(dir_analises / f"pares_descricoes_similares_hibridos_{cnpj_limpo}.json")
        return {
            "success": True,
            "status": obter_status_vectorizacao(),
            "current_base_hash": current_base_hash,
            "caches": {
                "faiss": {
                    **faiss_cache,
                    "stale": bool(current_base_hash and faiss_cache and faiss_cache.get("input_base_hash") != current_base_hash),
                },
                "light": {
                    **light_cache,
                    "stale": bool(current_base_hash and light_cache and light_cache.get("input_base_hash") != current_base_hash),
                },
                "semantic": {
                    **semantic_cache,
                    "stale": bool(current_base_hash and semantic_cache and semantic_cache.get("input_base_hash") != current_base_hash),
                },
                "hybrid": {
                    **hybrid_cache,
                    "stale": bool(current_base_hash and hybrid_cache and hybrid_cache.get("input_base_hash") != current_base_hash),
                },
            },
        }
    except Exception as e:
        logger.error("[get_vectorizacao_status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/runtime-status")
async def get_runtime_produtos_status(cnpj: str = Query(...)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "runtime": obter_runtime_produtos_status(dir_analises, cnpj_limpo),
        }
    except Exception as e:
        logger.error("[get_runtime_produtos_status] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/rebuild-runtime")
async def rebuild_runtime_produtos(req: ProdutoUnidRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        df = _reprocessar_produtos(dir_analises, cnpj_limpo)
        runtime = obter_runtime_produtos_status(dir_analises, cnpj_limpo)
        return {
            "success": True,
            "message": "Pipeline de produtos reconstruido com sucesso.",
            "rows": int(df.height),
            "runtime": runtime,
        }
    except Exception as e:
        logger.error("[rebuild_runtime_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/vectorizacao-clear-cache")
async def clear_vectorizacao_cache(cnpj: str = Query(...), metodo: str = Query("all")):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    metodo_norm = str(metodo or "all").strip().lower()
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if metodo_norm not in {"faiss", "light", "semantic", "hybrid", "all"}:
        raise HTTPException(status_code=400, detail="Metodo invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        targets: list[Path] = []
        if metodo_norm in {"faiss", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_faiss_{cnpj_limpo}.json",
                ]
            )
        if metodo_norm in {"light", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_light_{cnpj_limpo}.json",
                ]
            )
        if metodo_norm in {"semantic", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_semanticos_{cnpj_limpo}.json",
                ]
            )
        if metodo_norm in {"hybrid", "all"}:
            targets.extend(
                [
                    dir_analises / f"pares_descricoes_similares_hibridos_{cnpj_limpo}.parquet",
                    dir_analises / f"pares_descricoes_similares_hibridos_{cnpj_limpo}.json",
                ]
            )

        removed: list[str] = []
        for target in targets:
            if target.exists():
                target.unlink()
                removed.append(str(target))

        return {
            "success": True,
            "message": "Cache vetorizado removido.",
            "removed": removed,
        }
    except Exception as e:
        logger.error("[clear_vectorizacao_cache] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/codigos-multidescricao")
async def get_produtos_codigos_multidescricao(
    cnpj: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(50),
    sort_column: str | None = Query(None),
    sort_direction: str = Query("desc"),
    show_verified: bool = Query(False),
):
    """Retorna os codigos que aparecem com multiplas descricoes para o CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        page_norm = _normalize_page(page)
        page_size_norm = _normalize_page_size(page_size, default=50, max_size=200)
        _, dir_analises, _ = _load_cnpj_dirs(cnpj_limpo)
        path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"

        if not path_codigos.exists():
            return {
                "success": True,
                "file_path": str(path_codigos),
                "data": [],
                "page": page_norm,
                "page_size": page_size_norm,
                "total": 0,
                "total_pages": 1,
                "summary": {"total_codigos": 0, "total_descricoes": 0, "total_grupos": 0},
            }

        df = pl.read_parquet(str(path_codigos))
        df = df.with_columns(pl.lit("").alias("status_analise"))

        if not show_verified:
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            if status_path.exists():
                df_status = pl.read_parquet(str(status_path))
                df_codigo_status = (
                    df_status.filter(pl.col("tipo_ref") == "POR_CODIGO")
                    .select(
                        [
                            pl.col("ref_id").cast(pl.Utf8).alias("codigo"),
                            pl.col("status_analise").cast(pl.Utf8),
                        ]
                    )
                    .unique(subset=["codigo"], keep="last")
                )
                df = df.join(df_codigo_status, on="codigo", how="left", suffix="_joined").with_columns(
                    pl.coalesce([pl.col("status_analise_joined"), pl.col("status_analise")]).alias("status_analise")
                ).drop("status_analise_joined", strict=False)
                verified_codes = set(
                    df_status.filter(
                        (pl.col("tipo_ref") == "POR_CODIGO") & (pl.col("status_analise") == "VERIFICADO_SEM_ACAO")
                    ).get_column("ref_id").cast(pl.Utf8).to_list()
                )
                if verified_codes:
                    df = df.filter(~pl.col("codigo").is_in(sorted(verified_codes)))
        else:
            status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
            if status_path.exists():
                df_status = pl.read_parquet(str(status_path))
                df_codigo_status = (
                    df_status.filter(pl.col("tipo_ref") == "POR_CODIGO")
                    .select(
                        [
                            pl.col("ref_id").cast(pl.Utf8).alias("codigo"),
                            pl.col("status_analise").cast(pl.Utf8),
                        ]
                    )
                    .unique(subset=["codigo"], keep="last")
                )
                df = df.join(df_codigo_status, on="codigo", how="left", suffix="_joined").with_columns(
                    pl.coalesce([pl.col("status_analise_joined"), pl.col("status_analise")]).alias("status_analise")
                ).drop("status_analise_joined", strict=False)

        summary = {
            "total_codigos": int(df.height),
            "total_descricoes": int(df.select(pl.sum("qtd_descricoes")).item() or 0) if "qtd_descricoes" in df.columns else 0,
            "total_grupos": int(df.select(pl.sum("qtd_grupos_descricao_afetados")).item() or 0)
            if "qtd_grupos_descricao_afetados" in df.columns
            else 0,
        }

        sort_col = str(sort_column or "").strip()
        sort_desc = str(sort_direction or "desc").lower() != "asc"
        sortable = set(df.columns)
        if sort_col in sortable:
            df = df.sort(sort_col, descending=sort_desc, nulls_last=True)
        elif "qtd_descricoes" in sortable:
            df = df.sort("qtd_descricoes", descending=True, nulls_last=True)

        paged_df, total, total_pages = _paginate_frame(df, page_norm, page_size_norm)

        return {
            "success": True,
            "file_path": str(path_codigos),
            "data": paged_df.to_dicts(),
            "page": min(page_norm, total_pages),
            "page_size": page_size_norm,
            "total": total,
            "total_pages": total_pages,
            "summary": summary,
        }
    except Exception as e:
        logger.error("[get_produtos_codigos_multidescricao] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


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


@router.get("/produtos/codigo-multidescricao-resumo")
async def get_codigo_multidescricao_resumo(cnpj: str = Query(...), codigo: str = Query(...)):
    """Retorna um resumo indexado do codigo multidescricao para uso direto nos popups."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    codigo_limpo = str(codigo or "").strip()
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if not codigo_limpo:
        raise HTTPException(status_code=400, detail="Codigo invalido")

    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        payload = _carregar_codigo_multidescricao_resumo(dir_analises, cnpj_limpo, codigo_limpo)
        return {"success": True, **payload}
    except Exception as e:
        logger.error("[get_codigo_multidescricao_resumo] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/status-analise")
async def get_status_analise_produtos(cnpj: str = Query(...), include_data: bool = Query(True)):
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        df_status = pl.read_parquet(str(status_path)) if status_path.exists() else pl.DataFrame(schema={c: pl.Utf8 for c in _STATUS_ANALISE_COLUMNS})
        return {
            "success": True,
            "file_path": str(status_path),
            "data": df_status.to_dicts() if include_data else [],
            "resumo": _resumir_status_analise(dir_analises, cnpj_limpo, df_status),
        }
    except Exception as e:
        logger.error("[get_status_analise_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/marcar-verificado")
async def marcar_produto_verificado(req: ProdutoAnaliseStatusRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_verificados_path = dir_analises / f"mapa_verificados_produtos_{cnpj_limpo}.parquet"

        df_novo = pl.DataFrame(
            [
                {
                    "tipo_ref": _normalize_status_text(req.tipo_ref),
                    "ref_id": _normalize_status_text(req.ref_id),
                    "ref_id_aux": _normalize_status_text(req.ref_id_aux),
                    "descricao_ref": str(req.descricao_ref or "").strip(),
                    "contexto_tela": _normalize_status_text(req.contexto_tela),
                    "status_analise": "VERIFICADO_SEM_ACAO",
                    "dt_evento": datetime.now(UTC).isoformat(),
                }
            ]
        )

        if mapa_verificados_path.exists():
            df_existente = _normalizar_mapa_verificados(pl.read_parquet(str(mapa_verificados_path)))
            df_merge = pl.concat([df_existente, _normalizar_mapa_verificados(df_novo)], how="diagonal_relaxed").unique(
                subset=["tipo_ref", "ref_id", "ref_id_aux"], keep="last"
            )
        else:
            df_merge = _normalizar_mapa_verificados(df_novo)

        df_merge.write_parquet(str(mapa_verificados_path))
        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        return {
            "success": True,
            "mensagem": "Item marcado como verificado.",
            "arquivo": str(mapa_verificados_path),
            "status_file": str(status_path),
        }
    except Exception as e:
        logger.error("[marcar_produto_verificado] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/desfazer-verificado")
async def desfazer_produto_verificado(req: ProdutoAnaliseStatusRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_verificados_path = dir_analises / f"mapa_verificados_produtos_{cnpj_limpo}.parquet"

        removed = 0
        if mapa_verificados_path.exists():
            df_existente = _normalizar_mapa_verificados(pl.read_parquet(str(mapa_verificados_path)))
            before = df_existente.height
            df_existente = df_existente.filter(
                ~(
                    (pl.col("tipo_ref") == _normalize_status_text(req.tipo_ref))
                    & (pl.col("ref_id") == _normalize_status_text(req.ref_id))
                    & (pl.col("ref_id_aux") == _normalize_status_text(req.ref_id_aux))
                )
            )
            removed = before - df_existente.height
            df_existente.write_parquet(str(mapa_verificados_path))

        status_path = _gravar_status_analise(dir_analises, cnpj_limpo)
        return {
            "success": True,
            "mensagem": "Marcacao de verificado removida.",
            "qtd_removidos": removed,
            "status_file": str(status_path),
        }
    except Exception as e:
        logger.error("[desfazer_produto_verificado] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/auto-separar-residual")
async def auto_separar_residual(req: AutoSepararResidualRequest):
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    modo = str(req.modo or "").strip().upper()
    modos_validos = {"NCM_CEST_GTIN", "NCM_GTIN", "NCM_ONLY", "TEXT_ONLY"}
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if modo not in modos_validos:
        raise HTTPException(status_code=400, detail="Modo invalido")

    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        codigos_path = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        if not codigos_path.exists():
            return {
                "status": "sucesso",
                "preview": bool(req.preview),
                "modo": modo,
                "qtd_codigos_avaliados": 0,
                "qtd_codigos_elegiveis": 0,
                "qtd_codigos_aplicados": 0,
                "qtd_codigos_ignorados": 0,
                "motivos_ignorados": [],
                "resumo_motivos_ignorados": [],
            }

        df_codigos = pl.read_parquet(str(codigos_path)).with_columns(pl.col("codigo").cast(pl.Utf8))
        requested = [str(item).strip() for item in (req.codigos or []) if str(item).strip()]
        codigos = requested or [str(item).strip() for item in df_codigos.get_column("codigo").to_list() if str(item).strip()]

        motivos_ignorados: list[dict[str, str]] = []
        decisoes_batch: list[dict[str, Any]] = []
        qtd_elegiveis = 0
        qtd_aplicados = 0

        for codigo in codigos:
            payload = _carregar_codigo_multidescricao_resumo(dir_analises, cnpj_limpo, codigo)
            avaliacao = _build_auto_separate_plan_backend(codigo, payload.get("grupos_descricao", []), modo)
            if not avaliacao.get("eligible"):
                motivos_ignorados.append({"codigo": codigo, "motivo": str(avaliacao.get("reason") or "Nao elegivel")})
                continue

            qtd_elegiveis += 1
            if req.preview:
                continue

            detalhes = _carregar_detalhes_codigo(dir_analises, cnpj_limpo, codigo)
            if not detalhes:
                motivos_ignorados.append({"codigo": codigo, "motivo": "Nao foi possivel carregar os detalhes brutos do codigo."})
                continue

            destino_por_descricao = {
                _normalize_similarity_text(item.get("descricao")): item
                for item in avaliacao.get("plan", [])
            }

            try:
                for item in detalhes:
                    descricao_base = _normalize_similarity_text(item.get("descricao") or item.get("descricao_original"))
                    destino = destino_por_descricao.get(descricao_base)
                    if not destino:
                        raise ValueError(
                            f"Descricao sem destino automatico: {str(item.get('descricao') or item.get('descricao_original') or '')}"
                        )
                    fonte = item.get("fonte", "")
                    codigo_original = item.get("codigo_original", item.get("codigo", ""))
                    descricao_original = item.get("descricao_original", item.get("descricao_ori", item.get("descricao", "")))
                    tipo_item_original = item.get("tipo_item_original", item.get("tipo_item", ""))
                    decisoes_batch.append(
                        {
                            "fonte": fonte,
                            "codigo_original": codigo_original,
                            "descricao_original": descricao_original,
                            "tipo_item_original": tipo_item_original,
                            "hash_manual_key": _build_manual_hash(fonte, codigo_original, descricao_original, tipo_item_original),
                            "codigo_novo": destino.get("codigo_novo", ""),
                            "descricao_nova": destino.get("descricao_nova", ""),
                            "ncm_novo": destino.get("ncm_novo", ""),
                            "cest_novo": destino.get("cest_novo", ""),
                            "gtin_novo": destino.get("gtin_novo", ""),
                            "tipo_item_novo": item.get("tipo_item", ""),
                            "acao_manual": "DESAGREGAR",
                        }
                    )
                qtd_aplicados += 1
            except Exception as detail_error:
                motivos_ignorados.append({"codigo": codigo, "motivo": str(detail_error)})

        if not req.preview and decisoes_batch:
            _merge_manual_map(mapa_path, pl.DataFrame(decisoes_batch), default_acao="DESAGREGAR")
            _reprocessar_produtos(dir_analises, cnpj_limpo)

        return {
            "status": "sucesso",
            "preview": bool(req.preview),
            "modo": modo,
            "qtd_codigos_avaliados": len(codigos),
            "qtd_codigos_elegiveis": qtd_elegiveis,
            "qtd_codigos_aplicados": qtd_aplicados if not req.preview else 0,
            "qtd_codigos_ignorados": len(codigos) - (qtd_elegiveis if req.preview else qtd_aplicados),
            "motivos_ignorados": motivos_ignorados if req.preview else motivos_ignorados[:100],
            "resumo_motivos_ignorados": _resumir_motivos_ignorados(motivos_ignorados),
        }
    except Exception as e:
        logger.error("[auto_separar_residual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/produtos/detalhes-codigo")
async def get_detalhes_produto(cnpj: str = Query(...), codigo: str = Query(...)):
    """Retorna as linhas originais (fontes) associadas a um codigo master ou chave_produto."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        return {"success": True, "codigo": codigo, "itens": _carregar_detalhes_codigo(dir_analises, cnpj_limpo, str(codigo))}
    except Exception as e:
        logger.error("[get_detalhes_produto] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/detalhes-multi-codigo")
async def get_detalhes_multi_produtos(req: ResolverManualMultiDetalhesRequest):
    """Retorna as linhas originais (fontes) associadas a multiplos codigos master."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not detalhes_path.exists():
            return {"success": True, "data": []}

        lf = pl.scan_parquet(str(detalhes_path))
        df_agregado = pl.read_parquet(str(agregados_path)) if agregados_path.exists() else None
        filters = []
        for c in req.codigos:
            if str(c).startswith("ID_") and df_agregado is not None:
                row = df_agregado.filter(pl.col("chave_produto") == c)
                if not row.is_empty():
                    descr = row["descricao"][0]
                    filters.append(pl.col("descricao") == descr)
                    continue

            if str(c).upper().endswith("_AGR"):
                cod_real_raw = c.rsplit("_", 1)[0]
                cod_real = cod_real_raw.lstrip("0") if cod_real_raw.lstrip("0") else "0"
                filters.append(pl.col("codigo").str.replace("^0+", "") == cod_real)
            elif "_" in c:
                parts = c.rsplit("_", 1)
                cod_real = parts[0].lstrip("0") if parts[0].lstrip("0") else "0"
                tipo_val = parts[1]
                filters.append(
                    (pl.col("codigo").str.replace("^0+", "") == cod_real)
                    & (pl.col("tipo_item") == tipo_val)
                )
            else:
                cod_norm = c.lstrip("0") if c.lstrip("0") else "0"
                filters.append(pl.col("codigo").str.replace("^0+", "") == cod_norm)

        if filters:
            final_filter = filters[0]
            for f in filters[1:]:
                final_filter = final_filter | f
            df = lf.filter(final_filter).collect()
        else:
            df = pl.DataFrame(schema=lf.collect_schema())

        return {"success": True, "itens": df.to_dicts()}
    except Exception as e:
        logger.error("[get_detalhes_multi_produtos] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/revisao-manual/submit")
async def submit_revisao_manual(req: RevisaoManualSubmitRequest):
    """Grava as decisoes de revisao manual e roda o script de unificacao de produtos."""
    from core.models import RevisaoManualSubmitRequest

    if not isinstance(req, RevisaoManualSubmitRequest):
        pass

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        decisoes = [item.dict() for item in req.decisoes]
        df_novo = pl.DataFrame(decisoes)
        _merge_manual_map(mapa_path, df_novo, default_acao="AGREGAR")

        logger.info("Revisoes gravadas com sucesso no arquivo %s", mapa_path.name)
        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {"success": True, "message": "Revisoes aplicadas com sucesso."}
    except Exception as e:
        logger.error("[submit_revisao_manual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/resolver-manual-unificar")
async def resolver_manual_unificar(req: ResolverManualUnificarRequest):
    """Processa a unificacao de produtos e executa o motor."""
    from core.models import ResolverManualUnificarRequest

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        decisoes = []
        for item in req.itens:
            fonte = item.get("fonte", "")
            codigo_original = item.get("codigo_original", item.get("codigo", ""))
            descricao_original = item.get(
                "descricao_original",
                item.get("descricao_ori", item.get("descricao", "")),
            )
            tipo_item_original = item.get("tipo_item_original", item.get("tipo_item", ""))
            decisao = {
                "fonte": fonte,
                "codigo_original": codigo_original,
                "descricao_original": descricao_original,
                "tipo_item_original": tipo_item_original,
                "hash_manual_key": _build_manual_hash(fonte, codigo_original, descricao_original, tipo_item_original),
                "codigo_novo": req.decisao.get("codigo", ""),
                "descricao_nova": req.decisao.get("descricao", ""),
                "ncm_novo": req.decisao.get("ncm", ""),
                "cest_novo": req.decisao.get("cest", ""),
                "gtin_novo": req.decisao.get("gtin", ""),
                "tipo_item_novo": req.decisao.get("tipo_item", ""),
                "acao_manual": "AGREGAR",
            }
            decisoes.append(decisao)

        df_novo = pl.DataFrame(decisoes)
        _merge_manual_map(mapa_path, df_novo, default_acao="AGREGAR")

        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {"status": "sucesso", "mensagem": "Unificacao aplicada com sucesso."}
    except Exception as e:
        logger.error("[resolver_manual_unificar] Erro: %s\n%s", e, traceback.format_exc())
        return {"status": "erro", "mensagem": str(e)}


@router.post("/produtos/resolver-manual-desagregar")
async def resolver_manual_desagregar(req: ResolverManualDesagregarRequest):
    """Processa a desagregacao de produtos e executa o motor."""
    from core.models import ResolverManualDesagregarRequest

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        decisoes = []
        for item in req.itens_decididos:
            fonte = item.get("fonte", "")
            codigo_original = item.get("codigo_original", item.get("codigo", ""))
            descricao_original = item.get(
                "descricao_original",
                item.get("descricao_ori", item.get("descricao", "")),
            )
            tipo_item_original = item.get("tipo_item_original", item.get("tipo_item", ""))
            decisao = {
                "fonte": fonte,
                "codigo_original": codigo_original,
                "descricao_original": descricao_original,
                "tipo_item_original": tipo_item_original,
                "hash_manual_key": _build_manual_hash(fonte, codigo_original, descricao_original, tipo_item_original),
                "codigo_novo": item.get("codigo_novo", ""),
                "descricao_nova": item.get("descricao_nova", ""),
                "ncm_novo": item.get("ncm_novo", ""),
                "cest_novo": item.get("cest_novo", ""),
                "gtin_novo": item.get("gtin_novo", ""),
                "tipo_item_novo": item.get("tipo_item_novo", item.get("tipo_item", "")),
                "acao_manual": "DESAGREGAR",
            }
            decisoes.append(decisao)

        df_novo = pl.DataFrame(decisoes)
        _merge_manual_map(mapa_path, df_novo, default_acao="DESAGREGAR")

        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {"status": "sucesso", "mensagem": "Desagregacao aplicada com sucesso."}
    except Exception as e:
        logger.error("[resolver_manual_desagregar] Erro: %s\n%s", e, traceback.format_exc())
        return {"status": "erro", "mensagem": str(e)}


@router.post("/produtos/resolver-manual-descricoes")
async def resolver_manual_descricoes(req: ResolverManualDescricoesRequest):
    """Grava regras de unificacao/separacao por descricao e executa o motor."""
    from core.models import ResolverManualDescricoesRequest

    if not isinstance(req, ResolverManualDescricoesRequest):
        pass

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj_limpo}.parquet"
        history_path = dir_analises / f"mapa_manual_descricoes_historico_{cnpj_limpo}.parquet"

        if mapa_descricoes_path.exists():
            df_before = _normalize_mapa_descricoes_manual(
                pl.read_parquet(str(mapa_descricoes_path)),
                default_acao="AGREGAR",
            )
        else:
            df_before = pl.DataFrame(
                schema={
                    "tipo_regra": pl.Utf8,
                    "descricao_origem": pl.Utf8,
                    "descricao_destino": pl.Utf8,
                    "descricao_par": pl.Utf8,
                    "hash_descricoes_key": pl.Utf8,
                    "chave_grupo_a": pl.Utf8,
                    "chave_grupo_b": pl.Utf8,
                    "score_origem": pl.Utf8,
                    "acao_manual": pl.Utf8,
                }
            )
        _snapshot_mapa_descricoes_history(history_path, df_before, "before_merge")

        regras = [item.dict() for item in req.regras]
        df_novo = pl.DataFrame(regras) if regras else pl.DataFrame(schema={c: pl.Utf8 for c in [
            "tipo_regra",
            "descricao_origem",
            "descricao_destino",
            "descricao_par",
            "chave_grupo_a",
            "chave_grupo_b",
            "score_origem",
            "acao_manual",
        ]})

        merge_mapa_descricoes_manual(str(mapa_descricoes_path), df_novo, default_acao="AGREGAR")
        df_after = _normalize_mapa_descricoes_manual(
            pl.read_parquet(str(mapa_descricoes_path)),
            default_acao="AGREGAR",
        )
        _snapshot_mapa_descricoes_history(history_path, df_after, "after_merge")
        _reprocessar_produtos(dir_analises, cnpj_limpo)

        return {
            "status": "sucesso",
            "mensagem": "Mapa manual de descricoes aplicado com sucesso.",
            "arquivo": str(mapa_descricoes_path),
            "qtd_regras": len(regras),
        }
    except Exception as e:
        logger.error("[resolver_manual_descricoes] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/desfazer-decisao-codigo")
async def desfazer_decisao_codigo(req: DesfazerManualCodigoRequest):
    """Remove decisoes manuais por item associadas a um codigo original e reprocessa os produtos."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    codigo_limpo = _canon_text(req.codigo, "")
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if not codigo_limpo:
        raise HTTPException(status_code=400, detail="Codigo invalido")

    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"

        if not mapa_path.exists():
            return {
                "status": "sucesso",
                "mensagem": f"Nenhuma decisao manual encontrada para o codigo {codigo_limpo}.",
                "qtd_regras_removidas": 0,
            }

        df_existente = _normalize_manual_decisions(pl.read_parquet(str(mapa_path)), default_acao="AGREGAR")
        total_antes = df_existente.height
        df_restante = df_existente.filter(pl.col("codigo_original") != codigo_limpo)
        removidas = total_antes - df_restante.height

        if removidas == 0:
            return {
                "status": "sucesso",
                "mensagem": f"Nenhuma decisao manual encontrada para o codigo {codigo_limpo}.",
                "qtd_regras_removidas": 0,
            }

        if df_restante.is_empty():
            mapa_path.unlink(missing_ok=True)
        else:
            df_restante.write_parquet(str(mapa_path))

        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {
            "status": "sucesso",
            "mensagem": f"{removidas} decisao(oes) manual(is) removida(s) para o codigo {codigo_limpo}.",
            "qtd_regras_removidas": removidas,
        }
    except Exception as e:
        logger.error("[desfazer_decisao_codigo] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/produtos/desfazer-manual-descricoes")
async def desfazer_manual_descricoes(req: DesfazerManualDescricoesRequest):
    """Restaura o estado anterior das regras por descricao entre as descricoes selecionadas e reprocessa os produtos."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    descricoes = sorted({_canon_text(descricao, "") for descricao in req.descricoes if _canon_text(descricao, "")})
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")
    if len(descricoes) < 2:
        raise HTTPException(status_code=400, detail="Informe pelo menos duas descricoes.")

    try:
        import importlib.util

        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)

        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_descricoes_path = dir_analises / f"mapa_manual_descricoes_{cnpj_limpo}.parquet"
        history_path = dir_analises / f"mapa_manual_descricoes_historico_{cnpj_limpo}.parquet"

        if not mapa_descricoes_path.exists():
            return {
                "status": "sucesso",
                "mensagem": "Nenhuma regra manual de descricoes encontrada.",
                "qtd_regras_removidas": 0,
            }

        df_existente = _normalize_mapa_descricoes_manual(
            pl.read_parquet(str(mapa_descricoes_path)),
            default_acao="AGREGAR",
        )
        descricoes_set = set(descricoes)
        rows_atuais = df_existente.to_dicts()
        regras_atuais_alvo = [row for row in rows_atuais if _descricao_rule_matches(row, descricoes_set)]

        if not regras_atuais_alvo:
            return {
                "status": "sucesso",
                "mensagem": "Nenhuma regra manual encontrada entre as descricoes selecionadas.",
                "qtd_regras_removidas": 0,
            }

        if not history_path.exists():
            return {
                "status": "sucesso",
                "mensagem": "Historico nao encontrado. Nao e possivel reconstruir o estado anterior.",
                "qtd_regras_removidas": 0,
            }

        df_history = pl.read_parquet(str(history_path))
        if df_history.is_empty() or "snapshot_seq" not in df_history.columns:
            return {
                "status": "sucesso",
                "mensagem": "Historico vazio. Nao e possivel reconstruir o estado anterior.",
                "qtd_regras_removidas": 0,
            }

        current_seq = int(df_history["snapshot_seq"].max())
        regras_alvo_norm = _normalize_mapa_descricoes_manual(pl.DataFrame(regras_atuais_alvo), default_acao="AGREGAR")
        regras_alvo_set = {
            (
                row["tipo_regra"],
                row["descricao_origem"],
                row["descricao_destino"],
                row["descricao_par"],
            )
            for row in regras_alvo_norm.to_dicts()
        }

        prev_subset_rows: list[dict[str, Any]] = []
        previous_seq_found: int | None = None
        for snapshot_seq in sorted(
            {
                int(value)
                for value in df_history["snapshot_seq"].to_list()
                if int(value) < current_seq
            },
            reverse=True,
        ):
            df_snapshot = (
                df_history.filter(pl.col("snapshot_seq") == snapshot_seq)
                .drop(["snapshot_seq", "snapshot_ts_utc", "snapshot_label"], strict=False)
            )
            df_snapshot_norm = _normalize_mapa_descricoes_manual(df_snapshot, default_acao="AGREGAR")
            subset_rows = [
                row
                for row in df_snapshot_norm.to_dicts()
                if _descricao_rule_matches(row, descricoes_set)
            ]
            subset_set = {
                (
                    row["tipo_regra"],
                    row["descricao_origem"],
                    row["descricao_destino"],
                    row["descricao_par"],
                )
                for row in subset_rows
            }
            if subset_set != regras_alvo_set:
                prev_subset_rows = subset_rows
                previous_seq_found = snapshot_seq
                break

        rows_restantes = [row for row in rows_atuais if not _descricao_rule_matches(row, descricoes_set)]
        rows_reconstruidas = rows_restantes + prev_subset_rows
        removidas = len(regras_atuais_alvo)

        _snapshot_mapa_descricoes_history(history_path, df_existente, "before_restore")

        if rows_reconstruidas:
            df_reconstruido = _normalize_mapa_descricoes_manual(
                pl.DataFrame(rows_reconstruidas),
                default_acao="AGREGAR",
            )
            df_reconstruido.write_parquet(str(mapa_descricoes_path))
            _snapshot_mapa_descricoes_history(history_path, df_reconstruido, f"after_restore_from_{previous_seq_found or 0}")
        else:
            mapa_descricoes_path.unlink(missing_ok=True)
            _snapshot_mapa_descricoes_history(
                history_path,
                pl.DataFrame(
                    schema={
                        "tipo_regra": pl.Utf8,
                        "descricao_origem": pl.Utf8,
                        "descricao_destino": pl.Utf8,
                        "descricao_par": pl.Utf8,
                        "hash_descricoes_key": pl.Utf8,
                        "chave_grupo_a": pl.Utf8,
                        "chave_grupo_b": pl.Utf8,
                        "score_origem": pl.Utf8,
                        "acao_manual": pl.Utf8,
                    }
                ),
                f"after_restore_from_{previous_seq_found or 0}",
            )

        _reprocessar_produtos(dir_analises, cnpj_limpo)
        return {
            "status": "sucesso",
            "mensagem": (
                f"{removidas} regra(s) manual(is) revertida(s) com base no snapshot anterior "
                f"{previous_seq_found if previous_seq_found is not None else 'vazio'}."
            ),
            "qtd_regras_removidas": removidas,
        }
    except Exception as e:
        logger.error("[desfazer_manual_descricoes] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
