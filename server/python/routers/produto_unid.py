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
    DesfazerManualCodigoRequest,
    DesfazerManualDescricoesRequest,
    DescricaoManualMapItem,
    ProdutoUnidRequest,
    RevisaoManualSubmitRequest,
    ResolverManualDescricoesRequest,
    ResolverManualUnificarRequest,
    ResolverManualDesagregarRequest,
    ResolverManualMultiDetalhesRequest,
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


def _canon_text(value: Any, vazio: str = "(VAZIO)") -> str:
    text = "" if value is None else str(value)
    text = text.strip().upper()
    return text if text else vazio


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


@router.get("/produtos/codigos-multidescricao")
async def get_produtos_codigos_multidescricao(cnpj: str = Query(...)):
    """Retorna os codigos que aparecem com multiplas descricoes para o CNPJ."""
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
        path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"

        if not path_codigos.exists():
            return {"success": True, "file_path": str(path_codigos), "data": []}

        df = pl.read_parquet(str(path_codigos))
        return {"success": True, "file_path": str(path_codigos), "data": df.to_dicts()}
    except Exception as e:
        logger.error("[get_produtos_codigos_multidescricao] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


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
        path_codigos = dir_analises / f"codigos_multidescricao_{cnpj_limpo}.parquet"
        path_indexados = dir_analises / f"produtos_indexados_{cnpj_limpo}.parquet"

        resumo = {}
        grupos_descricao = []
        opcoes_consenso = {"descricao": [], "ncm": [], "cest": [], "gtin": []}

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
            "success": True,
            "codigo": codigo_limpo,
            "resumo": resumo,
            "grupos_descricao": grupos_descricao,
            "opcoes_consenso": opcoes_consenso,
        }
    except Exception as e:
        logger.error("[get_codigo_multidescricao_resumo] Erro: %s\n%s", e, traceback.format_exc())
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
        detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
        agregados_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"

        if not detalhes_path.exists():
            return {"success": True, "data": []}

        lf = pl.scan_parquet(str(detalhes_path))

        if str(codigo).startswith("ID_"):
            if agregados_path.exists():
                df_agregado = pl.read_parquet(str(agregados_path))
                row = df_agregado.filter(pl.col("chave_produto") == codigo)
                if not row.is_empty():
                    descr = row["descricao"][0]
                    df = lf.filter(pl.col("descricao") == descr).collect()
                    return {"success": True, "codigo": codigo, "itens": df.to_dicts()}

        # _agr must be treated as product code suffix, not tipo_item split.
        if str(codigo).upper().endswith("_AGR"):
            codigo_real = codigo.rsplit("_", 1)[0]
            codigo_norm = codigo_real.lstrip("0")
            if not codigo_norm:
                codigo_norm = "0"
            df = lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect()
        elif "_" in codigo:
            parts = codigo.rsplit("_", 1)
            codigo_real = parts[0]
            tipo_item_val = parts[1]

            codigo_norm = codigo_real.lstrip("0")
            if not codigo_norm:
                codigo_norm = "0"

            df = lf.filter(
                (pl.col("codigo").str.replace("^0+", "") == codigo_norm)
                & (pl.col("tipo_item") == tipo_item_val)
            ).collect()
        else:
            codigo_norm = codigo.lstrip("0")
            if not codigo_norm:
                codigo_norm = "0"
            df = lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect()

        return {"success": True, "codigo": codigo, "itens": df.to_dicts()}
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
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades

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
        unificar_produtos_unidades(cnpj_limpo)
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
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades

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

        unificar_produtos_unidades(cnpj_limpo)
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
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades

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

        unificar_produtos_unidades(cnpj_limpo)
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
        from cruzamentos.produtos.produto_unid import (
            merge_mapa_descricoes_manual,
            unificar_produtos_unidades,
        )
        from cruzamentos.produtos._produto_unid_manual import _normalize_mapa_descricoes_manual

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
        unificar_produtos_unidades(cnpj_limpo)

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
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades

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

        unificar_produtos_unidades(cnpj_limpo)
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
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades
        from cruzamentos.produtos._produto_unid_manual import _normalize_mapa_descricoes_manual

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

        unificar_produtos_unidades(cnpj_limpo)
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
