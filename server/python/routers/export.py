import logging
import re
import sys
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from io import BytesIO
import polars as pl
from core.models import ExcelExportRequest
from core.utils import _write_excel_with_format
from routers.filesystem import _is_path_allowed

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python/export", tags=["export"])

# Get project root for config access
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(_PROJETO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJETO_DIR))

@router.post("/excel")
async def export_to_excel(request: ExcelExportRequest):
    """Exporta arquivos Parquet para Excel com formatação padrão."""
    output_path = Path(request.output_dir)
    try:
        output_path = output_path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho de saída inválido")
    if not _is_path_allowed(output_path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho de saída não permitido")

    output_path.mkdir(parents=True, exist_ok=True)
    results = []
    for source in request.source_files:
        source_path = Path(source)
        try:
            source_path = source_path.resolve()
        except Exception:
            results.append({"file": source, "status": "error", "message": "Caminho inválido"})
            continue
        if not _is_path_allowed(source_path):
            results.append({"file": source, "status": "error", "message": "Acesso não permitido"})
            continue

        if not source_path.exists():
            results.append({"file": source, "status": "error", "message": "Arquivo não encontrado"})
            continue
        try:
            df = pl.read_parquet(str(source_path))
            if df.is_empty():
                results.append({"file": source, "status": "skipped", "message": "Sem dados"})
                continue
            excel_path = output_path / (source_path.stem + ".xlsx")
            import pandas as pd
            with pd.ExcelWriter(str(excel_path), engine="xlsxwriter") as writer:
                _write_excel_with_format(df.to_pandas(), writer)
            results.append({"file": source, "output": str(excel_path), "rows": len(df), "status": "success"})
        except Exception as e:
            results.append({"file": source, "status": "error", "message": str(e)})
    return {"success": True, "results": results}


@router.get("/excel-download")
async def export_excel_download(file_path: str = Query(...)):
    """Exporta um Parquet para Excel e retorna como download."""
    source = Path(file_path)
    try:
        source = source.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_path_allowed(source):
        raise HTTPException(status_code=403, detail="Acesso não permitido")

    if not source.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    try:
        import pandas as pd
        import tempfile
        df = pl.read_parquet(str(source))
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with pd.ExcelWriter(tmp_path, engine="xlsxwriter") as writer:
                _write_excel_with_format(df.to_pandas(), writer)
            with open(tmp_path, "rb") as f:
                buffer = BytesIO(f.read())
            buffer.seek(0)
            return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f'attachment; filename="{source.stem}.xlsx"'})
        finally:
            if Path(tmp_path).exists(): Path(tmp_path).unlink()
    except Exception as e:
        logger.error("[export_excel_download] Erro: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/revisao-manual-excel")
async def download_revisao_manual_excel(cnpj: str = Query(...)):
    """Gera e envia um Excel formatado com os produtos que requerem revisão manual."""
    import pandas as pd

    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo:
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    # Carrega config para obter paths do CNPJ
    try:
        from core.config_loader import get_config_var
        obter_diretorios_cnpj = get_config_var("obter_diretorios_cnpj")
        _, dir_analises, _ = obter_diretorios_cnpj(cnpj_limpo)
    except Exception as e:
        logger.error("[revisao_manual_excel] Erro ao carregar config: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro na configuração: {e}")

    parquet_path = dir_analises / f"produtos_agregados_{cnpj_limpo}.parquet"
    if not parquet_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Arquivo de produtos não encontrado. Execute a unificação de produtos primeiro."
        )

    try:
        df = pl.read_parquet(str(parquet_path))

        # Filtra apenas os que requerem revisão
        if "requer_revisao_manual" in df.columns:
            df_revisao = df.filter(pl.col("requer_revisao_manual"))
        else:
            df_revisao = df

        if df_revisao.is_empty():
            raise HTTPException(
                status_code=404,
                detail="Nenhum produto requer revisão manual para este CNPJ."
            )

        # Gera o Excel em memória
        buffer = BytesIO()
        df_pandas = df_revisao.to_pandas()

        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            _write_excel_with_format(df_pandas, writer, sheet_name="Revisão Manual")

            # Acessa o workbook e worksheet para formatação extra
            workbook = writer.book
            worksheet = writer.sheets["Revisão Manual"]

            # Formato de cabeçalho destacado
            header_fmt = workbook.add_format({
                "bold": True,
                "bg_color": "#FFC000",
                "border": 1,
                "text_wrap": True,
                "valign": "vcenter",
                "align": "center",
            })

            # Reescreve os cabeçalhos com formatação
            for col_num, col_name in enumerate(df_pandas.columns):
                worksheet.write(0, col_num, col_name, header_fmt)

            # Ajusta largura das colunas baseado no conteúdo
            for col_num, col_name in enumerate(df_pandas.columns):
                max_len = max(
                    df_pandas[col_name].astype(str).str.len().max() or 10,
                    len(col_name)
                )
                worksheet.set_column(col_num, col_num, min(max_len + 2, 60))

        buffer.seek(0)
        filename = f"revisao_manual_produtos_{cnpj_limpo}.xlsx"

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[revisao_manual_excel] Erro: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
