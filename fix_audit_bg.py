import re

with open('server/python/routers/analysis.py', 'r') as f:
    content = f.read()

# Modify `run_audit_pipeline_bg`
# Add {"status": "executando"} at the beginning of try
target_start = """async def run_audit_pipeline_bg(req: AuditPipelineRequest, cnpj_limpo: str, dir_parquet, dir_analises, dir_relatorios, dir_sql):
    try:
        import os"""

replacement_start = """async def run_audit_pipeline_bg(req: AuditPipelineRequest, cnpj_limpo: str, dir_parquet, dir_analises, dir_relatorios, dir_sql):
    try:
        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({"status": "executando", "message": "Auditoria em andamento."}, f)

        import os"""

content = content.replace(target_start, replacement_start)

# Change "concluido" to "concluida" for consistency in frontend matching
target_end = """        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({"status": "concluido", "arquivos": len(arquivos_extraidos), "detalhes": "Verifique a aba de arquivos gerados"}, f)"""

replacement_end = """        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({"status": "concluida", "arquivos": len(arquivos_extraidos), "detalhes": "Verifique a aba de arquivos gerados"}, f)"""

content = content.replace(target_end, replacement_end)

with open('server/python/routers/analysis.py', 'w') as f:
    f.write(content)
