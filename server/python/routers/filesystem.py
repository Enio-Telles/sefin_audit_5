import os
import re
import traceback
import logging
import polars as pl
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from core.utils import ler_sql, extrair_parametros_sql, _human_size, validar_cnpj

logger = logging.getLogger("sefin_audit_python")
from core.audit_artifacts_service import obter_arquivos_auditoria
router = APIRouter(prefix="/api/python", tags=["filesystem"])

# Get project root from environment or handle it
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent


# Diretórios permitidos (whitelist) para navegação/listagem
# Pode ser extendido via variável de ambiente ALLOWED_BASE_DIRS (separado por ';' ou ',')
def _get_allowed_dirs() -> list[Path]:
    import importlib.util

    allowed: list[Path] = []
    # Diretórios padrões do projeto
    allowed.append(_PROJETO_DIR)
    allowed.append(_PROJETO_DIR / "consultas_fonte")
    allowed.append(_PROJETO_DIR / "CNPJ")
    allowed.append(_PROJETO_DIR / "referencias")
    # Diretórios definidos em config.py (se existirem)
    try:
        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _cfg = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_cfg)  # type: ignore
        for name in ("DIR_SQL", "DIR_CNPJS"):
            p = getattr(_cfg, name, None)
            if p:
                try:
                    allowed.append(Path(p))
                except Exception:
                    pass
    except Exception:
        pass
    # Diretórios adicionais via ambiente
    extra = os.getenv("ALLOWED_BASE_DIRS", "").strip()
    if extra:
        for raw in re.split(r"[;,]", extra):
            raw = raw.strip()
            if not raw:
                continue
            try:
                allowed.append(Path(raw))
            except Exception:
                continue
    # Normaliza para caminhos absolutos resolvidos
    norm: list[Path] = []
    for p in allowed:
        try:
            norm.append(p.resolve())
        except Exception:
            pass
    # Remove duplicatas preservando ordem
    seen = set()
    uniq = []
    for p in norm:
        s = str(p)
        if s not in seen:
            seen.add(s)
            uniq.append(p)
    return uniq


def _is_path_allowed(target: Path) -> bool:
    try:
        t = target.resolve()
    except Exception:
        return False
    allowed = _get_allowed_dirs()
    for base in allowed:
        try:
            # Python 3.9+: Path.is_relative_to
            if hasattr(t, "is_relative_to") and t.is_relative_to(base):  # type: ignore[attr-defined]
                return True
            # Fallback: commonpath
            import os as _os

            if _os.path.commonpath([str(t), str(base)]) == str(base):
                return True
        except Exception:
            continue
    return False


def _clamp_parent(parent: Path | None) -> str | None:
    if parent is None:
        return None
    try:
        p = parent.resolve()
    except Exception:
        return None
    return str(p) if _is_path_allowed(p) else None


@router.get("/project/paths")
async def get_project_paths():
    """Retorna os caminhos base do projeto (alinhado ao client)."""
    return {
        "projeto_dir": str(_PROJETO_DIR),
        "consultas_fonte": str((_PROJETO_DIR / "consultas_fonte").resolve()),
        "consultas_fonte_auxiliares": str(
            (_PROJETO_DIR / "consultas_fonte" / "auxiliares").resolve()
        ),
        "cruzamentos": str((_PROJETO_DIR / "cruzamentos").resolve()),
        "referencias": str((_PROJETO_DIR / "referencias").resolve()),
    }


@router.get("/filesystem/browse")
async def browse_filesystem(path: str = Query("")):
    """Navega pelo sistema de arquivos (diretórios e arquivos .parquet ou .sql)."""
    try:
        target = _PROJETO_DIR if not path else Path(path)
        # Normaliza e valida contra whitelist
        try:
            target = target.resolve()
        except Exception:
            raise HTTPException(status_code=400, detail="Caminho inválido")
        if not _is_path_allowed(target):
            raise HTTPException(
                status_code=403, detail="Acesso ao caminho não permitido"
            )
        if not target.exists():
            raise HTTPException(status_code=404, detail="Caminho não encontrado")
        if target.is_file():
            target = target.parent

        parent = _clamp_parent(target.parent if target != _PROJETO_DIR else None)
        entries = []
        for item in target.iterdir():
            try:
                if item.name.startswith((".", "_")):
                    continue
                if not _is_path_allowed(item):
                    continue
                if item.is_dir():
                    entries.append(
                        {
                            "name": item.name,
                            "path": str(item.resolve()),
                            "type": "directory",
                        }
                    )
                elif item.is_file() and item.suffix.lower() in (
                    ".parquet",
                    ".sql",
                    ".xlsx",
                    ".docx",
                    ".html",
                    ".txt",
                    ".pdf",
                ):
                    stats = item.stat()
                    entries.append(
                        {
                            "name": item.name,
                            "path": str(item.resolve()),
                            "type": "file",
                            "size": stats.st_size,
                            "human_size": _human_size(stats.st_size),
                            "modified": stats.st_mtime,
                        }
                    )
            except Exception:
                continue
        entries.sort(key=lambda x: x["name"].lower())
        return {"current": str(target), "parent": parent, "entries": entries}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filesystem/sql-queries")
async def list_sql_queries(path: str = Query("")):
    """Lista todos os arquivos .sql em um diretório ou arquivo único."""
    if not path:
        return {"queries": []}
    target_path = Path(path)
    try:
        target_path = target_path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_path_allowed(target_path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not target_path.exists():
        return {"queries": []}
    try:
        queries = []
        files = (
            [target_path]
            if target_path.is_file() and target_path.suffix.lower() == ".sql"
            else list(target_path.glob("*.sql"))
            if target_path.is_dir()
            else []
        )
        for file in files:
            try:
                sql_content = ler_sql(file)
                params_set = extrair_parametros_sql(sql_content)
                params_list = [
                    p for p in params_set if p.lower() not in ("cnpj", "cnpj_raiz")
                ]
            except Exception:
                params_list = []
            queries.append(
                {
                    "id": str(file.resolve()),
                    "name": file.stem,
                    "description": f"Arquivo SQL: {file.name}",
                    "parameters": params_list,
                }
            )
        return {"queries": sorted(queries, key=lambda x: x["name"])}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao ler consultas SQL: {str(e)}"
        )


@router.get("/filesystem/auxiliary-queries")
async def list_auxiliary_queries(path: str = Query("")):
    """Lista todos os arquivos .sql auxiliares em um diretório."""
    if not path:
        return {"queries": [], "count": 0}
    target_path = Path(path)
    try:
        target_path = target_path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Caminho inválido")
    if not _is_path_allowed(target_path):
        raise HTTPException(status_code=403, detail="Acesso ao caminho não permitido")
    if not target_path.exists() or not target_path.is_dir():
        return {"queries": [], "count": 0}
    try:
        queries = []
        for file in target_path.glob("*.sql"):
            try:
                sql_content = ler_sql(file)
                params_set = extrair_parametros_sql(sql_content)
                params_list = [
                    p for p in params_set if p.lower() not in ("cnpj", "cnpj_raiz")
                ]
            except Exception:
                params_list = []
            queries.append(
                {
                    "id": str(file.resolve()),
                    "name": file.stem,
                    "description": f"Tabela auxiliar: {file.name}",
                    "parameters": params_list,
                }
            )
        sorted_queries = sorted(queries, key=lambda x: x["name"])
        return {"queries": sorted_queries, "count": len(sorted_queries)}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao ler consultas auxiliares: {str(e)}"
        )


@router.get("/auditoria/consultas")
async def listar_consultas_disponiveis():
    """Lista as consultas disponíveis no projeto (arquivos SQL)."""
    try:
        import importlib.util

        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        DIR_SQL = getattr(_sefin_config, "DIR_SQL", _PROJETO_DIR / "consultas_fonte")
        if not DIR_SQL.exists():
            return {"success": True, "consultas": []}
        sql_files = sorted(DIR_SQL.glob("*.sql"))
        return {
            "success": True,
            "consultas": [{"id": f.name, "nome": f.stem} for f in sql_files],
        }
    except Exception as e:
        logger.error("[listar_consultas] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validate-cnpj")
async def validate_cnpj_endpoint(cnpj: str = Query(...)):
    """Valida um CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    is_valid = validar_cnpj(cnpj_limpo)
    return {"cnpj": cnpj, "cnpj_limpo": cnpj_limpo, "valid": is_valid}


@router.get("/auditoria/historico")
async def listar_historico():
    """Lista todos os CNPJs que já possuem pastas criadas."""
    try:
        import importlib.util

        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        DIR_CNPJS = getattr(_sefin_config, "DIR_CNPJS", _PROJETO_DIR / "CNPJ")

        historico = []
        if DIR_CNPJS.exists():
            for d in DIR_CNPJS.iterdir():
                if d.is_dir() and d.name != "sem_cnpj":
                    cnpj = d.name
                    dir_parquet, dir_analises, dir_relatorios = (
                        d / "arquivos_parquet",
                        d / "analises",
                        d / "relatorios",
                    )
                    qtd_parquets = (
                        len(list(dir_parquet.glob("*.parquet")))
                        if dir_parquet.exists()
                        else 0
                    )
                    qtd_analises = (
                        len(list(dir_analises.glob("*.parquet")))
                        if dir_analises.exists()
                        else 0
                    )
                    qtd_relatorios = len(
                        [
                            f
                            for f in (
                                dir_relatorios.iterdir()
                                if dir_relatorios.exists()
                                else []
                            )
                            if f.is_file()
                        ]
                    )

                    last_mod = 0
                    for subdir in [dir_parquet, dir_analises, dir_relatorios]:
                        if subdir.exists():
                            for f in subdir.rglob("*"):
                                if f.is_file():
                                    last_mod = max(last_mod, f.stat().st_mtime)

                    razao_social = None
                    cadastrais_file = dir_parquet / f"dados_cadastrais_{cnpj}.parquet"
                    if cadastrais_file.exists():
                        try:
                            df_cadastrais = (
                                pl.scan_parquet(str(cadastrais_file))
                                .select("razao_social")
                                .collect()
                            )
                            if not df_cadastrais.is_empty():
                                razao_social = re.sub(
                                    r"<[^>]+>", "", str(df_cadastrais[0, 0])
                                ).strip()
                        except Exception:
                            pass

                    if qtd_parquets > 0 or qtd_analises > 0 or qtd_relatorios > 0:
                        historico.append(
                            {
                                "cnpj": cnpj,
                                "razao_social": razao_social,
                                "qtd_parquets": qtd_parquets,
                                "qtd_analises": qtd_analises,
                                "qtd_relatorios": qtd_relatorios,
                                "ultima_modificacao": datetime.fromtimestamp(
                                    last_mod
                                ).isoformat()
                                if last_mod > 0
                                else None,
                            }
                        )
        historico.sort(key=lambda x: x["ultima_modificacao"] or "", reverse=True)
        return {"success": True, "historico": historico}
    except Exception as e:
        logger.error("[historico] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/auditoria/historico/{cnpj}")
async def detalhes_historico_cnpj(cnpj: str):
    """Detalhes do histórico para um CNPJ específico: lista arquivos extraídos, análises e relatórios."""
    try:
        cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
        if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
            raise HTTPException(status_code=400, detail="CNPJ inválido")

        import importlib.util

        _spec = importlib.util.spec_from_file_location(
            "sefin_config", str(_PROJETO_DIR / "config.py")
        )
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        DIR_CNPJS = getattr(_sefin_config, "DIR_CNPJS", _PROJETO_DIR / "CNPJ")

        base_dir = (
            (DIR_CNPJS / cnpj_limpo)
            if isinstance(DIR_CNPJS, Path)
            else Path(DIR_CNPJS) / cnpj_limpo
        )
        if not base_dir.exists() or not base_dir.is_dir():
            raise HTTPException(status_code=404, detail="CNPJ não encontrado")

        dir_parquet = base_dir / "arquivos_parquet"
        dir_analises = base_dir / "analises"
        dir_relatorios = base_dir / "relatorios"

        arquivos = obter_arquivos_auditoria(cnpj_limpo, dir_parquet, dir_analises, dir_relatorios)

        import json
        etapas_salvas = []
        erros_salvos = []
        status_file = dir_analises / "status_pipeline.json"
        if status_file.exists():
            try:
                with open(status_file, "r") as f:
                    data = json.load(f)
                    etapas_salvas = data.get("etapas", [])
                    erros_salvos = data.get("erros", [])
            except Exception:
                pass

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "etapas": etapas_salvas,
            "erros": erros_salvos,
            **arquivos,
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[historico/{cnpj}] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


