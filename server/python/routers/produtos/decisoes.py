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
from .multidescricao import _carregar_codigo_multidescricao_resumo
from .detalhes import _carregar_detalhes_codigo

logger = logging.getLogger("sefin_audit_python")
router = APIRouter()

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
