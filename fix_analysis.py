import re

with open("server/python/routers/analysis.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Aplicar a substituição de PyArrow
extra = """
                    cursor.execute(sql, bind_vars)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)}, strict=False)
                    df = normalizar_colunas(df)
                    parquet_name = f"{query_name}_{cnpj_limpo}.parquet"
                    parquet_path = dir_parquet / parquet_name
                    df.write_parquet(str(parquet_path))
                    arquivos_extraidos.append({"name": parquet_name, "path": str(parquet_path), "rows": len(rows)})
"""

extra_new = """
                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    import uuid
                    import os

                    cursor.execute(sql, bind_vars)
                    columns = [desc[0] for desc in cursor.description]

                    parquet_name = f"{query_name}_{cnpj_limpo}.parquet"
                    parquet_path = dir_parquet / parquet_name

                    tmp_parquet_path = f"{parquet_path}.tmp_{uuid.uuid4()}"
                    writer = None
                    total_rows = 0
                    try:
                        while True:
                            chunk = cursor.fetchmany(100000)
                            if not chunk: break

                            data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(columns)}

                            table = pa.Table.from_pydict(data_dict)
                            if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                            writer.write_table(table)
                            total_rows += len(chunk)

                        if writer: writer.close()
                        elif total_rows == 0:
                            empty_cols = {col.lower(): [] for col in columns}
                            empty = pa.Table.from_pydict(empty_cols)
                            pq.write_table(empty, tmp_parquet_path)

                        os.replace(tmp_parquet_path, str(parquet_path))
                    except Exception:
                        if writer: writer.close()
                        if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                        raise

                    arquivos_extraidos.append({"name": parquet_name, "path": str(parquet_path), "rows": total_rows})
"""
content = content.replace(extra.strip(), extra_new.strip())

# 2. Desacoplar para background mantendo ETAPA 3 e sem erros de sintaxe
regex = r"@router\.post\(\"/auditoria/pipeline\"\)\nasync def audit_pipeline\(req: AuditPipelineRequest\):\n(.*?)(?=^\@router\.post|\Z)"
match = re.search(regex, content, re.DOTALL | re.MULTILINE)
if match:
    func_body = match.group(1)

    # Separar o bloco inicial (até obter diretórios)
    split_target = "        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)\n"
    parts = func_body.split(split_target)

    setup_part = parts[0] + split_target

    # O restante é a lógica do try-except que vamos extrair e retornar no return original
    # Precisamos capturar o final (o dict return) para extraí-lo da função e não retornar no router.
    logic_part = parts[1]

    # Localizar o return dict gigante
    return_dict_start = logic_part.find("        return {")
    return_dict_end = logic_part.find("        }\n", return_dict_start) + 10

    # A lógica da background task deve ir do "ETAPA 1" até antes do return_dict
    bg_logic = logic_part[:return_dict_start]

    # A exceção final também faz parte do router, a gente precisa injetar o background task no setup

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

{bg_logic}
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
{setup_part}
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
    # Substituir a função original pela versão refatorada
    content = content[:match.start()] + new_bg_func + content[match.end():]

with open("server/python/routers/analysis.py", "w", encoding="utf-8") as f:
    f.write(content)
