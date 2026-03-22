import re

with open('server/python/routers/analysis.py', 'r') as f:
    content = f.read()

# 1. Update `audit_pipeline` (POST)
target_chunk = """        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        background_tasks.add_task(run_audit_pipeline_bg, req, cnpj_limpo, dir_parquet, dir_analises, dir_relatorios, DIR_SQL)

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "etapas": [
                {"etapa": "Extração de Dados", "status": "agendada"},
                {"etapa": "Cruzamentos e Análises", "status": "agendada"},
                {"etapa": "Análise de Produtos", "status": "agendada"},
                {"etapa": "Geração de Relatórios", "status": "agendada"}
            ],
            "arquivos_extraidos": [],
            "arquivos_analises": [],
            "arquivos_produtos": [],
            "arquivos_relatorios": [],
            "erros": [],
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
            "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente."
        }"""

replacement_chunk = """        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({"status": "agendada", "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente."}, f)

        background_tasks.add_task(run_audit_pipeline_bg, req, cnpj_limpo, dir_parquet, dir_analises, dir_relatorios, DIR_SQL)

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "job_status": "agendada",
            "etapas": [
                {"etapa": "Extração de Dados", "status": "agendada"},
                {"etapa": "Cruzamentos e Análises", "status": "agendada"},
                {"etapa": "Análise de Produtos", "status": "agendada"},
                {"etapa": "Geração de Relatórios", "status": "agendada"}
            ],
            "arquivos_extraidos": [],
            "arquivos_analises": [],
            "arquivos_produtos": [],
            "arquivos_relatorios": [],
            "erros": [],
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
            "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente."
        }"""

content = content.replace(target_chunk, replacement_chunk)

with open('server/python/routers/analysis.py', 'w') as f:
    f.write(content)
