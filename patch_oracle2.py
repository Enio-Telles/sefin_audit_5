import re

with open("server/python/routers/oracle.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Add imports
import_replacement = """from core.utils import validar_cnpj, ler_sql, extrair_parametros_sql
from core.execution_trace import build_execution_trace, ExecutionTimer, ExecutionTraceStore
from core.sqlite_audit_repo import SqliteAuditRepo"""
content = content.replace("from core.utils import validar_cnpj, ler_sql, extrair_parametros_sql, normalizar_colunas", import_replacement)

# Remove unused polars
content = content.replace("import polars as pl\n", "")

# 2. Modify extract_oracle_data start
extract_start_pattern = r'''@router\.post\("/extract"\)
async def extract_oracle_data\(request: ExtractionRequest\):
    """Extrai dados do Oracle por CNPJ e salva em Parquet\."""
    cnpj_limpo = re\.sub\(r"\[\^0-9\]", "", request\.cnpj\) if request\.cnpj else ""
    if request\.cnpj and not validar_cnpj\(cnpj_limpo\):
        raise HTTPException\(status_code=400, detail="CNPJ inválido"\)

    output_path = Path\(request\.output_dir\) / cnpj_limpo if cnpj_limpo else Path\(request\.output_dir\)
    output_path\.mkdir\(parents=True, exist_ok=True\)

    results = \[\]
    try:'''

extract_start_replacement = '''@router.post("/extract")
async def extract_oracle_data(request: ExtractionRequest):
    """Extrai dados do Oracle por CNPJ e salva em Parquet."""
    cnpj_limpo = re.sub(r"[^0-9]", "", request.cnpj) if request.cnpj else ""
    if request.cnpj and not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    output_path = Path(request.output_dir) / cnpj_limpo if cnpj_limpo else Path(request.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    trace = build_execution_trace(scope="oracle_extraction", cnpj=cnpj_limpo, **request.model_dump(exclude={"connection"}))
    sqlite_repo = SqliteAuditRepo()

    results = []
    try:'''

content = re.sub(extract_start_pattern, extract_start_replacement, content)

# 3. Add Precheck timer
precheck_pattern = r"""        import oracledb
        from db_manager import DatabaseManager
        _oracle_network_precheck\(request\.connection\.host, request\.connection\.port\)"""

precheck_replacement = """        import oracledb
        from db_manager import DatabaseManager
        with ExecutionTimer(trace, stage="network_precheck"):
            _oracle_network_precheck(request.connection.host, request.connection.port)"""
content = re.sub(precheck_pattern, precheck_replacement, content)

# 4. Add DB connection timer
db_conn_pattern = r"""        dsn = oracledb\.makedsn\(request\.connection\.host, request\.connection\.port, service_name=request\.connection\.service\)
        db_manager = DatabaseManager\(dsn=dsn, user=request\.connection\.user, password=request\.connection\.password\)

        with db_manager\.get_connection\(\) as conexao:"""

db_conn_replacement = """        dsn = oracledb.makedsn(request.connection.host, request.connection.port, service_name=request.connection.service)
        db_manager = DatabaseManager(dsn=dsn, user=request.connection.user, password=request.connection.password)

        with ExecutionTimer(trace, stage="db_connection"):
            conexao = db_manager.get_connection()
            conexao.__enter__()

        try:"""
content = re.sub(db_conn_pattern, db_conn_replacement, content)


# We need to make sure conexao.__exit__ is called at the end of the extraction
return_pattern = r"""        return \{"success": True, "results": results, "output_dir": str\(output_path\)\}

    except ImportError:"""

return_replacement = """
        finally:
            conexao.__exit__(None, None, None)

        ExecutionTraceStore(output_path / "_audit" / trace.execution_id).save(trace)
        try:
            sqlite_repo.save_trace(trace)
        except Exception as e:
            logger.error(f"Erro ao salvar trace no SQLite: {e}")

        return {"success": True, "results": results, "output_dir": str(output_path), "execution_id": trace.execution_id, "trace_dir": str(output_path / "_audit" / trace.execution_id)}

    except ImportError as e:
        trace.add_event("error", "error", str(e))
        try: sqlite_repo.save_trace(trace)
        except: pass
"""
content = re.sub(return_pattern, return_replacement, content)


# Catching the main exception block
exception_pattern = r"""    except Exception as e:
        raise HTTPException\(status_code=500, detail=str\(e\)\)"""
exception_replacement = """    except Exception as e:
        trace.add_event("error", "error", str(e))
        try: sqlite_repo.save_trace(trace)
        except: pass
        raise HTTPException(status_code=500, detail=str(e))"""
content = re.sub(exception_pattern, exception_replacement, content)

# 5. Main queries execution timer and artifact addition
main_query_loop_pattern = r"""            for query_path in request\.queries:
                query_file = Path\(query_path\)
                query_name = query_file\.stem if query_file\.exists\(\) else query_path
                try:"""
main_query_loop_replacement = """            for query_path in request.queries:
                query_file = Path(query_path)
                query_name = query_file.stem if query_file.exists() else query_path
                with ExecutionTimer(trace, stage="execute_query", query=query_name):
                  try:"""
content = re.sub(main_query_loop_pattern, main_query_loop_replacement, content)


# Parquet addition in main queries
main_parquet_pattern = r"""                    results\.append\(\{
                        "query": query_name,
                        "rows": total_rows,
                        "columns": len\(columns\),
                        "file": str\(parquet_path\),
                        "status": "success",
                    \}\)
                except Exception as e:"""
main_parquet_replacement = """                    trace.add_artifact("parquet", str(parquet_path), query=query_name, rows=total_rows, columns=len(columns))
                    results.append({
                        "query": query_name,
                        "rows": total_rows,
                        "columns": len(columns),
                        "file": str(parquet_path),
                        "status": "success",
                    })
                  except Exception as e:"""
content = re.sub(main_parquet_pattern, main_parquet_replacement, content)


# 6. Aux queries execution timer and artifact addition
aux_query_loop_pattern = r"""                    for aux_file in aux_sql_files:
                        aux_name = aux_file\.stem
                        try:"""
aux_query_loop_replacement = """                    for aux_file in aux_sql_files:
                        aux_name = aux_file.stem
                        with ExecutionTimer(trace, stage="execute_aux_query", query=aux_name):
                          try:"""
content = re.sub(aux_query_loop_pattern, aux_query_loop_replacement, content)


# Parquet addition in aux queries
aux_parquet_pattern = r"""                            results\.append\(\{
                                "query": f"\[AUX\] \{aux_name\}",
                                "rows": total_rows,
                                "columns": len\(aux_columns\),
                                "file": str\(aux_parquet_path\),
                                "status": "success",
                            \}\)
                        except Exception as e:"""
aux_parquet_replacement = """                            trace.add_artifact("parquet", str(aux_parquet_path), query=f"[AUX] {aux_name}", rows=total_rows, columns=len(aux_columns))
                            results.append({
                                "query": f"[AUX] {aux_name}",
                                "rows": total_rows,
                                "columns": len(aux_columns),
                                "file": str(aux_parquet_path),
                                "status": "success",
                            })
                          except Exception as e:"""
content = re.sub(aux_parquet_pattern, aux_parquet_replacement, content)


with open("server/python/routers/oracle.py", "w", encoding="utf-8") as f:
    f.write(content)
