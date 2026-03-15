import re
with open("server/python/routers/analysis.py", "r", encoding="utf-8") as f:
    content = f.read()

# Achando o bloco do endpoint
regex = r"@router\.post\(\"/auditoria/pipeline\"\)\nasync def audit_pipeline\(req: AuditPipelineRequest\):\n(.*?)(?=^\@router\.post|\Z)"
match = re.search(regex, content, re.DOTALL | re.MULTILINE)
if match:
    original_func_body = match.group(1)

    # Dividindo a função original em duas partes: Setup (até o try) e Pipeline (try até o except HTTPException)
    setup_part = original_func_body.split("try:\n")[0]
    pipeline_part = original_func_body.split("        # ETAPA 1: Extração\n")[1].split("        except HTTPException:\n")[0]

    new_bg_func = f"""
import traceback
import json
from fastapi import BackgroundTasks

async def run_audit_pipeline_bg(req: AuditPipelineRequest, cnpj_limpo: str, dir_parquet, dir_analises, dir_relatorios):
    try:
        import os
        from dotenv import load_dotenv
        import keyring
        import oracledb

        # ETAPA 1: Extração
{pipeline_part}
        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({{"status": "concluido", "arquivos": len(arquivos_extraidos), "detalhes": "Verifique a aba de arquivos gerados"}}, f)
    except Exception as e:
        logger.error(f"[pipeline bg] Erro: {{e}}\\n{{traceback.format_exc()}}")
        status_file = dir_analises / "status_pipeline.json"
        try:
            with open(status_file, "w") as f:
                json.dump({{"status": "erro", "motivo": str(e)}}, f)
        except Exception:
            pass

@router.post("/auditoria/pipeline")
async def audit_pipeline(req: AuditPipelineRequest, background_tasks: BackgroundTasks):
{setup_part}    try:
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        obter_diretorios_cnpj = _sefin_config.obter_diretorios_cnpj
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        background_tasks.add_task(run_audit_pipeline_bg, req, cnpj_limpo, dir_parquet, dir_analises, dir_relatorios)

        return {{
            "success": True,
            "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente.",
            "cnpj": cnpj_limpo,
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios)
        }}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[pipeline] Erro ao agendar: %s\\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
"""
    content = content[:match.start()] + new_bg_func + content[match.end():]

with open("server/python/routers/analysis.py", "w", encoding="utf-8") as f:
    f.write(content)
