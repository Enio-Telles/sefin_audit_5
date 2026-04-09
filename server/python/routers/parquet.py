import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from core.utils import _human_size
from core.models import (
    ParquetReadRequest,
    ParquetWriteRequest,
    ParquetAddRowRequest,
    ParquetAddColumnRequest,
)
import polars as pl
import logging

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python/parquet", tags=["parquet"])

# Whitelist de diretórios (reuso de lógica mínima do filesystem)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def _allowed_paths() -> list[Path]:
    bases = [
        _PROJECT_ROOT,
        _PROJECT_ROOT / "CNPJ",
        _PROJECT_ROOT / "consultas_fonte",
        _PROJECT_ROOT / "referencias",
    ]
    extra = os.getenv("ALLOWED_BASE_DIRS", "").strip()
    if extra:
        import re as _re

        for raw in _re.split(r"[;,]", extra):
            raw = raw.strip()
            if raw:
                try:
                    bases.append(Path(raw))
                except Exception:
                    pass
    norm = []
    for p in bases:
        try:
            norm.append(p.resolve())
        except Exception:
            pass
    # dedup
    seen = set()
    uniq = []
    for p in norm:
        s = str(p)
        if s not in seen:
            seen.add(s)
            uniq.append(p)
    return uniq


def _is_allowed(p: Path) -> bool:
    try:
        rp = p.resolve()
    except Exception:
        return False
    for base in _allowed_paths():
        try:
            if hasattr(rp, "is_relative_to") and rp.is_relative_to(base):  # type: ignore[attr-defined]
                return True
            import os as _os

            if _os.path.commonpath([str(rp), str(base)]) == str(base):
                return True
        except Exception:
            continue
    return False


@router.post("/read")
async def read_parquet(request: ParquetReadRequest):
    """Lê um arquivo Parquet com suporte a paginação, filtros e ordenação usando processamento Lazy."""
    path = Path(request.file_path)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")

    try:
        # 1. Inicia leitura Lazy
        lf = pl.scan_parquet(str(path))

        # 2. Obtém total original (sem filtros)
        total_rows_all = lf.select(pl.len()).collect().item()

        # 3. Aplica Filtros
        if request.filters:
            for col, val in request.filters.items():
                if val:
                    # Tenta converter para String para busca por substring case-insensitive
                    # Verificamos se a coluna existe antes de aplicar
                    if col in lf.collect_schema().names():
                        lf = lf.filter(
                            pl.col(col).cast(pl.Utf8).str.contains(f"(?i){val}")
                        )

        # 4. Obtém total filtrado (antes da paginação)
        filtered_rows = lf.select(pl.len()).collect().item()

        # 5. Aplica Ordenação
        if request.sort_column and request.sort_column in lf.collect_schema().names():
            lf = lf.sort(
                request.sort_column, descending=(request.sort_direction == "desc")
            )

        # 6. Aplica Paginação (Slice) e Coleta
        start = (request.page - 1) * request.page_size
        df_page = lf.slice(start, request.page_size).collect()

        import math

        total_pages = (
            math.ceil(filtered_rows / request.page_size) if request.page_size > 0 else 1
        )

        return {
            "columns": df_page.columns,
            "dtypes": {col: str(dtype) for col, dtype in df_page.schema.items()},
            "rows": df_page.to_dicts(),
            "total_rows": total_rows_all,  # Total global do arquivo
            "filtered_rows": filtered_rows,  # Total que bate com os filtros
            "page": request.page,
            "page_size": request.page_size,
            "total_pages": total_pages,
            "file_name": path.name,
        }
    except Exception as e:
        logger.error(f"Erro ao ler parquet {path}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/unique-values")
async def get_unique_values(file_path: str = Query(...), column: str = Query(...)):
    """Retorna valores únicos de uma coluna."""
    path = Path(file_path)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    try:
        df = pl.read_parquet(str(path))
        if column not in df.columns:
            raise HTTPException(
                status_code=400, detail=f"Coluna '{column}' não encontrada"
            )
        unique_vals = (
            df.select(pl.col(column)).unique().sort(column).to_series().to_list()
        )
        return {"values": [str(v) for v in unique_vals]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/write-cell")
async def write_parquet_cell(request: ParquetWriteRequest):
    """Atualiza uma única célula em um arquivo Parquet."""
    path = Path(request.file_path)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    try:
        df = pl.read_parquet(str(path))
        if request.column not in df.columns:
            raise HTTPException(
                status_code=400, detail=f"Coluna '{request.column}' não encontrada"
            )

        # Converte valor para o tipo correto da coluna
        dtype = df.schema[request.column]
        new_val = request.value
        try:
            if dtype in (pl.Int64, pl.Int32):
                new_val = int(request.value)
            elif dtype in (pl.Float64, pl.Float32):
                new_val = float(request.value)
            elif dtype == pl.Boolean:
                new_val = request.value.lower() in ("true", "1", "yes")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Valor '{request.value}' inválido para tipo {dtype}",
            )

        # Polars DataFrames são imutáveis, então criamos um novo com a alteração
        df_list = df.to_dicts()
        if 0 <= request.row_index < len(df_list):
            df_list[request.row_index][request.column] = new_val
            df_updated = pl.from_dicts(df_list, schema=df.schema)
            df_updated.write_parquet(str(path))
            return {"success": True}
        else:
            raise HTTPException(
                status_code=400, detail="Índice de linha fora dos limites"
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-row")
async def add_parquet_row(request: ParquetAddRowRequest):
    """Adiciona uma linha vazia ao arquivo Parquet."""
    path = Path(request.file_path)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    try:
        df = pl.read_parquet(str(path))
        new_row = {col: None for col in df.columns}
        df_new = pl.from_dicts([new_row], schema=df.schema)
        df_updated = pl.concat([df, df_new])
        df_updated.write_parquet(str(path))
        return {"success": True, "new_index": len(df_updated) - 1}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-column")
async def add_parquet_column(request: ParquetAddColumnRequest):
    """Adiciona uma nova coluna ao arquivo Parquet."""
    path = Path(request.file_path)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    try:
        df = pl.read_parquet(str(path))
        if request.column_name in df.columns:
            raise HTTPException(
                status_code=400, detail=f"Coluna '{request.column_name}' já existe"
            )
        df_updated = df.with_columns(
            pl.lit(request.default_value).alias(request.column_name)
        )
        df_updated.write_parquet(str(path))
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_parquet_files(directory: str = Query(...)):
    """Lista todos os arquivos Parquet em um diretório."""
    path = Path(directory)
    try:
        path = path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not path.exists() or not path.is_dir():
        return {"files": [], "count": 0, "directory": directory}
    try:
        files = []
        for f in path.glob("*.parquet"):
            stats = f.stat()

            # Obtém metadados leves (evita coletar o arquivo todo)
            rows = 0
            columns = 0
            try:
                # Ler o schema é instantâneo e não carrega dados
                lf = pl.scan_parquet(str(f))
                columns = len(lf.collect_schema().names())
                # ler apenas pl.len() é rápido para recuperar a contagem de linhas
                rows = lf.select(pl.len()).collect().item()
            except Exception as e:
                logger.error(f"Erro ao ler metadados do Parquet {f.name}: {e}")
                pass

            files.append(
                {
                    "name": f.name,
                    "path": str(f.absolute()),
                    "size": stats.st_size,
                    "size_human": _human_size(stats.st_size),
                    "rows": rows,
                    "columns": columns,
                    "modified": stats.st_mtime,
                    "relative_path": f.name,
                }
            )
        sorted_files = sorted(files, key=lambda x: x["name"])
        return {
            "files": sorted_files,
            "count": len(sorted_files),
            "directory": directory,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_parquet(file: UploadFile = File(...), directory: str = Query(...)):
    """Upload de arquivo Parquet para um diretório."""
    dir_path = Path(directory)
    try:
        dir_path = dir_path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_allowed(dir_path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / Path(file.filename).name
    content = await file.read()
    await run_in_threadpool(file_path.write_bytes, content)
    try:
        df = pl.read_parquet(str(file_path))
        return {
            "success": True,
            "file_path": str(file_path),
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns,
        }
    except Exception as e:
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=400, detail=f"Arquivo inválido: {str(e)}")
