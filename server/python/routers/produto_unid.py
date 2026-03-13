import re
import os
import sys
import traceback
import logging
import polars as pl
from pathlib import Path
from typing import Optional, Any
from fastapi import APIRouter, HTTPException, Query
from core.models import ProdutoUnidRequest, RevisaoManualSubmitRequest, ResolverManualUnificarRequest, ResolverManualDesagregarRequest, ResolverManualMultiDetalhesRequest
from core.utils import validar_cnpj

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python", tags=["produto_unid"])

# Get project root from environment or handle it
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent

# Ensure project root is in Python path for cruzamentos imports
if str(_PROJETO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJETO_DIR))

@router.get("/produtos/revisao-manual")
async def get_produtos_revisao_manual(cnpj: str = Query(...)):
    """Retorna os produtos que requerem revisão manual para o CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
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

@router.get("/produtos/detalhes-codigo")
async def get_detalhes_produto(cnpj: str = Query(...), codigo: str = Query(...)):
    """Retorna as linhas originais (fontes) associadas a um código master ou chave_produto."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
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
        
        # Se for uma chave_produto gerada (ID_0001), precisamos descobrir qual a descrição/código ela representa
        if str(codigo).startswith("ID_"):
            if agregados_path.exists():
                df_agregado = pl.read_parquet(str(agregados_path))
                row = df_agregado.filter(pl.col("chave_produto") == codigo)
                if not row.is_empty():
                    # No novo formato, o agrupamento é por Descrição.
                    descr = row["descricao"][0]
                    # Retorna todos os itens com essa descrição na base de detalhes
                    df = lf.filter(pl.col("descricao") == descr).collect()
                    return {"success": True, "codigo": codigo, "itens": df.to_dicts()}

        # Lógica original (decomposição por código real)
        if "_" in codigo:
            parts = codigo.rsplit("_", 1)
            codigo_real = parts[0]
            tipo_item_val = parts[1]
            
            codigo_norm = codigo_real.lstrip("0")
            if not codigo_norm: codigo_norm = "0"
            
            df = lf.filter(
                (pl.col("codigo").str.replace("^0+", "") == codigo_norm) &
                (pl.col("tipo_item") == tipo_item_val)
            ).collect()
        else:
            codigo_norm = codigo.lstrip("0")
            if not codigo_norm: codigo_norm = "0"
            df = lf.filter(pl.col("codigo").str.replace("^0+", "") == codigo_norm).collect()
        
        return {"success": True, "codigo": codigo, "itens": df.to_dicts()}
    except Exception as e:
        logger.error("[get_detalhes_produto] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/produtos/detalhes-multi-codigo")
async def get_detalhes_multi_produtos(req: ResolverManualMultiDetalhesRequest):
    """Retorna as linhas originais (fontes) associadas a múltiplos códigos master."""
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
    try:
        import importlib.util
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        
        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
        
        if not detalhes_path.exists():
            return {"success": True, "data": []}
            
        lf = pl.scan_parquet(str(detalhes_path))
        filters = []
        for c in req.codigos:
            if "_" in c:
                parts = c.rsplit("_", 1)
                cod_real = parts[0].lstrip("0") if parts[0].lstrip("0") else "0"
                tipo_val = parts[1]
                filters.append(
                    (pl.col("codigo").str.replace("^0+", "") == cod_real) &
                    (pl.col("tipo_item") == tipo_val)
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
    """Grava as decisões de revisão manual e roda o script de unificação de produtos."""
    from core.models import RevisaoManualSubmitRequest
    if not isinstance(req, RevisaoManualSubmitRequest):
        # FastAPI handles this usually, but being safe if called differently
        pass

    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
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
        
        if mapa_path.exists():
            df_existente = pl.read_parquet(str(mapa_path))
            df_merge = pl.concat([df_existente, df_novo], how="diagonal_relaxed").unique(
                subset=["fonte", "codigo_original", "descricao_original", "tipo_item_original"], 
                keep="last"
            )
            df_merge.write_parquet(mapa_path)
        else:
            df_novo.write_parquet(mapa_path)
            
        logger.info(f"Revisões gravadas com sucesso no arquivo {mapa_path.name}")
        unificar_produtos_unidades(cnpj_limpo)
        return {"success": True, "message": "Revisões aplicadas com sucesso."}
    except Exception as e:
        logger.error("[submit_revisao_manual] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/produtos/resolver-manual-unificar")
async def resolver_manual_unificar(req: ResolverManualUnificarRequest):
    """Processa a unificação de produtos e executa o motor."""
    from core.models import ResolverManualUnificarRequest
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
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
            decisao = {
                "fonte": item.get("fonte", ""),
                "codigo_original": item.get("codigo_original", item.get("codigo", "")),
                "descricao_original": item.get("descricao_original", item.get("descricao", "")),
                "tipo_item_original": item.get("tipo_item", ""),
                "codigo_novo": req.decisao.get("codigo", ""),
                "descricao_nova": req.decisao.get("descricao", ""),
                "ncm_novo": req.decisao.get("ncm", ""),
                "cest_novo": req.decisao.get("cest", ""),
                "gtin_novo": req.decisao.get("gtin", ""),
                "tipo_item_novo": req.decisao.get("tipo_item", "")
            }
            decisoes.append(decisao)
            
        df_novo = pl.DataFrame(decisoes)
        
        if mapa_path.exists():
            df_existente = pl.read_parquet(str(mapa_path))
            df_merge = pl.concat([df_existente, df_novo], how="diagonal_relaxed").unique(
                subset=["fonte", "codigo_original", "descricao_original", "tipo_item_original"], 
                keep="last"
            )
            df_merge.write_parquet(mapa_path)
        else:
            df_novo.write_parquet(mapa_path)
            
        unificar_produtos_unidades(cnpj_limpo)
        return {"status": "sucesso", "mensagem": "Unificação aplicada com sucesso."}
    except Exception as e:
        logger.error("[resolver_manual_unificar] Erro: %s\n%s", e, traceback.format_exc())
        return {"status": "erro", "mensagem": str(e)}

@router.post("/produtos/resolver-manual-desagregar")
async def resolver_manual_desagregar(req: ResolverManualDesagregarRequest):
    """Processa a desagregação de produtos e executa o motor."""
    from core.models import ResolverManualDesagregarRequest
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")
    try:
        import importlib.util
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades
        
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        
        _, dir_analises, _ = _sefin_config.obter_diretorios_cnpj(cnpj_limpo)
        mapa_path = dir_analises / f"mapa_manual_unificacao_{cnpj_limpo}.parquet"
        
        df_novo = pl.DataFrame(req.itens_decididos)
        
        if mapa_path.exists():
            df_existente = pl.read_parquet(str(mapa_path))
            df_merge = pl.concat([df_existente, df_novo], how="diagonal_relaxed").unique(
                subset=["fonte", "codigo_original", "descricao_original", "tipo_item_original"], 
                keep="last"
            )
            df_merge.write_parquet(mapa_path)
        else:
            df_novo.write_parquet(mapa_path)
            
        unificar_produtos_unidades(cnpj_limpo)
        return {"status": "sucesso", "mensagem": "Desagregação aplicada com sucesso."}
    except Exception as e:
        logger.error("[resolver_manual_desagregar] Erro: %s\n%s", e, traceback.format_exc())
        return {"status": "erro", "mensagem": str(e)}