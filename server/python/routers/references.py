import logging
import polars as pl
import re
from pathlib import Path
from fastapi import APIRouter, HTTPException

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python/references", tags=["references"])

_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent
_REF_DIR = _PROJETO_DIR / "referencias"

@router.get("/ncm/{codigo}")
async def get_ncm_details(codigo: str):
    """Busca detalhes do NCM (Capitulo, Posicao, Descricao)."""
    ncm_path = _REF_DIR / "NCM" / "tabela_ncm.parquet"
    ncm_posicao_path = _REF_DIR / "NCM" / "ncm_posicao.parquet"
    ncm_capitulos_path = _REF_DIR / "NCM" / "ncm_capitulos.parquet"
    if not ncm_path.exists():
        ncm_path = _REF_DIR / "NCM" / "ncm_tabela.parquet"

    if not ncm_path.exists():
        raise HTTPException(status_code=404, detail="Tabela NCM nao encontrada.")

    try:
        codigo_limpo = re.sub(r"[^0-9]", "", codigo)
        df = pl.read_parquet(str(ncm_path))

        cols = df.columns

        def get_col(options):
            for opt in options:
                if opt in cols:
                    return opt
            return options[0]

        def is_invalid(v):
            return v is None or str(v).strip().lower() in ["none", "nan", "null", ""]

        col_codigo = get_col(["Codigo_NCM", "codigo"])
        col_cap = get_col(["Capitulo", "capitulo"])
        col_descr_cap = get_col(["Descr_Capitulo", "descr_capitulo"])
        col_pos = get_col(["Posicao", "posicao"])
        col_descr_pos = get_col(["Descr_Posicao", "descr_posicao"])
        col_desc = get_col(["Descricao", "descricao"])

        df = df.with_columns(
            [
                pl.col(col_codigo).cast(pl.Utf8).fill_null("").str.replace_all(r"[^0-9]", "").alias("__codigo_norm"),
                pl.col(col_pos).cast(pl.Utf8).fill_null("").str.replace_all(r"[^0-9]", "").alias("__pos_norm"),
                pl.col(col_descr_pos).cast(pl.Utf8).fill_null("").alias("__descr_pos_txt"),
            ]
        )

        res = df.filter(pl.col("__codigo_norm") == codigo_limpo)
        if res.is_empty():
            res = df.filter(pl.col("__codigo_norm").str.starts_with(codigo_limpo[:4]))
            if res.is_empty():
                raise HTTPException(status_code=404, detail="NCM nao localizado.")

        res = res.with_columns(l=pl.col("__codigo_norm").str.len_chars()).sort("l", descending=True)
        item = res.to_dicts()[0]

        ncm_val = re.sub(r"[^0-9]", "", str(item.get(col_codigo, "")))
        capitulo = str(item.get(col_cap) or ncm_val[:2])
        posicao = str(item.get(col_pos) or ncm_val[:4])

        descr_capitulo = item.get(col_descr_cap) or ""
        descr_posicao = item.get(col_descr_pos) or ""

        if is_invalid(descr_capitulo) and ncm_capitulos_path.exists():
            try:
                df_cap = pl.read_parquet(str(ncm_capitulos_path)).with_columns(
                    pl.col("NCM").cast(pl.Utf8).fill_null("").str.replace_all(r"[^0-9]", "").alias("__cap_norm")
                )
                cap_res = df_cap.filter(pl.col("__cap_norm") == ncm_val[:2]).head(1)
                if not cap_res.is_empty():
                    descr_capitulo = cap_res.row(0, named=True).get("Descrição", "")
            except Exception:
                pass

        if is_invalid(descr_posicao):
            pos_prefix = ncm_val[:4]

            exact_code_pos = (
                df.filter(
                    (pl.col("__codigo_norm") == codigo_limpo)
                    & (pl.col("__descr_pos_txt").str.strip_chars() != "")
                )
                .head(1)
            )
            if not exact_code_pos.is_empty():
                descr_posicao = exact_code_pos.row(0, named=True).get(col_descr_pos)

            if is_invalid(descr_posicao):
                pos_res = (
                    df.filter(
                        (pl.col("__pos_norm") == pos_prefix)
                        & (pl.col("__descr_pos_txt").str.strip_chars() != "")
                    )
                    .head(1)
                )
                if not pos_res.is_empty():
                    descr_posicao = pos_res.row(0, named=True).get(col_descr_pos)

            if is_invalid(descr_posicao):
                code_res = (
                    df.filter(
                        (pl.col("__codigo_norm") == pos_prefix)
                        & (pl.col("__descr_pos_txt").str.strip_chars() != "")
                    )
                    .head(1)
                )
                if not code_res.is_empty():
                    descr_posicao = code_res.row(0, named=True).get(col_descr_pos)

            if is_invalid(descr_posicao) and ncm_posicao_path.exists():
                try:
                    df_pos = pl.read_parquet(str(ncm_posicao_path)).with_columns(
                        pl.col("NCM").cast(pl.Utf8).fill_null("").str.replace_all(r"[^0-9]", "").alias("__pos_norm")
                    )
                    pos_ref = df_pos.filter(pl.col("__pos_norm") == pos_prefix).head(1)
                    if not pos_ref.is_empty():
                        descr_posicao = pos_ref.row(0, named=True).get("Descrição", "")
                except Exception:
                    pass

        d_cap = descr_capitulo if not is_invalid(descr_capitulo) else ""
        d_pos = descr_posicao if not is_invalid(descr_posicao) else ""

        item_desc = str(item.get(col_desc, "")).strip()
        item_desc = re.sub(r"^[-\s]+", "", item_desc)

        formatted_desc = (
            f"Capitulo: {capitulo} - {d_cap}\n"
            f"Posicao: {posicao} - {d_pos}\n"
            f"NCM: {item.get(col_codigo)} - {item_desc}"
        )

        return {
            "success": True,
            "data": {
                "codigo": item.get(col_codigo),
                "capitulo": capitulo,
                "descr_capitulo": descr_capitulo,
                "posicao": posicao,
                "descr_posicao": descr_posicao,
                "descricao": formatted_desc,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[references] Erro NCM: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cest/{codigo}")
async def get_cest_details(codigo: str):
    """Busca detalhes do CEST (Segmento, NCMs associados, Descrição)."""
    cest_path = _REF_DIR / "CEST" / "cest.parquet"
    seg_path = _REF_DIR / "CEST" / "segmentos_mercadorias.parquet"
    
    if not cest_path.exists():
        raise HTTPException(status_code=404, detail="Tabela CEST não encontrada.")
    
    try:
        codigo_limpo = re.sub(r"[^0-9]", "", codigo)
        df_cest = pl.read_parquet(str(cest_path))
        
        # Filtra pelo CEST (normalizando a coluna na comparação)
        res_cest = df_cest.filter(pl.col("CEST").str.replace_all(r"[^0-9]", "") == codigo_limpo)
        
        if res_cest.is_empty():
            # Tenta busca parcial se não achar exato (ex: 4 ou 7 dígitos)
            res_cest = df_cest.filter(pl.col("CEST").str.replace_all(r"[^0-9]", "").str.starts_with(codigo_limpo[:4]))
            if res_cest.is_empty():
                raise HTTPException(status_code=404, detail="CEST não localizado.")
        
        # Coleta todas as descrições e NCMs associados a este CEST
        descricoes = res_cest.select("DESCRICAO").unique().to_series().to_list()
        ncms = res_cest.select("NCM").unique().to_series().to_list()
        
        # Extrai segmento (2 primeiros dígitos do CEST)
        cest_row = res_cest.row(0, named=True)
        cest_val = re.sub(r"[^0-9]", "", str(cest_row.get("CEST", "")))
        segmento_id = cest_val[:2]
        
        # Busca nome do segmento
        segmento_nome = "Não localizado"
        if seg_path.exists():
            df_seg = pl.read_parquet(str(seg_path))
            res_seg = df_seg.filter(pl.col("Codigo_Segmento") == segmento_id)
            if not res_seg.is_empty():
                segmento_nome = res_seg.row(0, named=True).get("Nome_Segmento", "Não localizado")
        
        # Formatação Segmento: Codigo_Segmento - Nome_Segmento
        seg_formatado = f"Segmento: {segmento_id} - {segmento_nome}"
        
        # Buscar descrição da posição do NCM associado
        ncm_associado = str(ncms[0]) if ncms else ""
        descr_pos_ncm = ""
        if ncm_associado:
            try:
                # Normaliza NCM associado e pega os 4 primeiros dígitos da posição
                ncm_pos_query = re.sub(r"[^0-9]", "", ncm_associado)[:4]
                ncm_ref_path = _REF_DIR / "NCM" / "tabela_ncm.parquet"
                if ncm_ref_path.exists():
                    df_ncm = pl.read_parquet(str(ncm_ref_path))
                    # Busca a posição normalizando a coluna Posicao
                    pos_row = df_ncm.filter(pl.col("Posicao").str.replace_all(r"[^0-9]", "") == ncm_pos_query).head(1)
                    if not pos_row.is_empty():
                        descr_pos_ncm = pos_row.row(0, named=True).get("Descr_Posicao", "")
            except:
                pass
        
        # Formatação Descrição: Segmento na linha 1; Descrição na linha 2; NCMs na linha 3
        ncm_info = f"{ncm_associado} ({descr_pos_ncm})" if descr_pos_ncm else ncm_associado
        full_desc = f"{seg_formatado}\nDescrição: {descricoes[0] if descricoes else ''}\nNCMs: {ncm_info}"
        
        return {
            "success": True,
            "data": {
                "codigo": codigo,
                "segmento": seg_formatado,
                "nome_segmento": segmento_nome,
                "descricoes": [full_desc],
                "ncms_associados": ncms
            }
        }
    except Exception as e:
        logger.error(f"[references] Erro CEST: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
