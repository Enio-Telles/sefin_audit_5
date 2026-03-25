import re
import os
import uuid
from pathlib import Path
from core.models import ExtractionRequest
from core.utils import ler_sql, extrair_parametros_sql
from core.execution_trace import build_execution_trace, ExecutionTimer, ExecutionTraceStore
from core.sqlite_audit_repo import SqliteAuditRepo
from core.job_manager import job_manager
import logging

logger = logging.getLogger("sefin_audit_python.oracle_extraction")

def run_oracle_extraction_job(job_id: str, request: ExtractionRequest):
    """
    Função de serviço que executa a extração em background.
    Mantém o trace de execução e atualiza o job state conforme progride.
    """
    try:
        import oracledb
        from db_manager import DatabaseManager
    except ImportError:
        job_manager.update_job(job_id, status="error", error_message="Driver Oracle (oracledb) não instalado", stage="failed")
        raise ImportError("Driver Oracle (oracledb) não instalado")

    cnpj_limpo = re.sub(r"[^0-9]", "", request.cnpj) if request.cnpj else ""
    output_path = Path(request.output_dir) / cnpj_limpo if cnpj_limpo else Path(request.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    trace = build_execution_trace(
        scope="oracle_extraction",
        cnpj=cnpj_limpo,
        **request.model_dump(exclude={"connection"}),
    )
    sqlite_repo = SqliteAuditRepo()

    job_manager.update_job(
        job_id,
        execution_id=trace.execution_id,
        output_dir=str(output_path),
        stage="init",
        message="Iniciando extração",
        total_queries=len(request.queries) + (1 if request.include_auxiliary and request.auxiliary_queries_dir else 0) # Estimativa inicial para auxiliares
    )

    results = []
    completed_queries = 0

    try:
        if job_manager.is_cancelled(job_id):
             raise InterruptedError("Job cancelado pelo usuário")

        job_manager.update_job(job_id, stage="db_connection", message="Conectando ao banco de dados")
        dsn = oracledb.makedsn(
            request.connection.host,
            request.connection.port,
            service_name=request.connection.service,
        )
        db_mgr = DatabaseManager(
            dsn=dsn, user=request.connection.user, password=request.connection.password
        )

        with ExecutionTimer(trace, stage="db_connection"):
            conexao = db_mgr.get_connection()
            conexao.__enter__()

        try:
            for query_path in request.queries:
                if job_manager.is_cancelled(job_id):
                     raise InterruptedError("Job cancelado pelo usuário")

                query_file = Path(query_path)
                query_name = query_file.stem if query_file.exists() else query_path

                job_manager.update_job(job_id, stage="execute_query", message=f"Executando consulta {query_name}")

                with ExecutionTimer(trace, stage="execute_query", query=query_name):
                    try:
                        sql = ler_sql(query_file) if query_file.exists() else query_path
                        params = extrair_parametros_sql(sql)
                        bind_vars = {p: None for p in params}
                        if cnpj_limpo:
                            for p in params:
                                if p.lower() == "cnpj":
                                    bind_vars[p] = cnpj_limpo
                                elif p.lower() == "cnpj_raiz":
                                    bind_vars[p] = cnpj_limpo[:8]

                        if request.parameters:
                            for p in params:
                                val = None
                                if p in request.parameters:
                                    val = request.parameters[p]
                                elif p.lower() in request.parameters:
                                    val = request.parameters[p.lower()]
                                elif p.upper() in request.parameters:
                                    val = request.parameters[p.upper()]

                                if val is not None:
                                    bind_vars[p] = val

                        for p in list(bind_vars.keys()):
                            if bind_vars[p] is None and p.lower() not in ("cnpj", "cnpj_raiz"):
                                bind_vars[p] = ""

                        import pyarrow as pa
                        import pyarrow.parquet as pq

                        with conexao.cursor() as cursor:
                            cursor.execute(sql, bind_vars)
                            columns = [desc[0] for desc in cursor.description]

                            parquet_name = f"{query_name}_{cnpj_limpo}.parquet" if cnpj_limpo else f"{query_name}.parquet"
                            parquet_path = output_path / parquet_name
                            tmp_parquet_path = f"{parquet_path}.tmp_{uuid.uuid4()}"

                            writer = None
                            total_rows = 0

                            try:
                                while True:
                                    if job_manager.is_cancelled(job_id):
                                         raise InterruptedError("Job cancelado pelo usuário durante o fetch")

                                    chunk = cursor.fetchmany(100000)
                                    if not chunk:
                                        break

                                    if request.normalize_columns:
                                        data_dict = {
                                            col.lower(): [row[i] for row in chunk]
                                            for i, col in enumerate(columns)
                                        }
                                    else:
                                        data_dict = {
                                            col: [row[i] for row in chunk]
                                            for i, col in enumerate(columns)
                                        }

                                    table = pa.Table.from_pydict(data_dict)
                                    if writer is None:
                                        writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                    writer.write_table(table)
                                    total_rows += len(chunk)

                                    # Opcional: atualizar progresso baseando em rows (se possível estimar)
                                    job_manager.update_job(job_id, message=f"Lendo dados de {query_name}: {total_rows} linhas...")

                                if writer:
                                    writer.close()
                                elif total_rows == 0:
                                    empty_cols = {col.lower() if request.normalize_columns else col: [] for col in columns}
                                    empty = pa.Table.from_pydict(empty_cols)
                                    pq.write_table(empty, tmp_parquet_path)

                                os.replace(tmp_parquet_path, str(parquet_path))
                            except Exception:
                                if writer:
                                    writer.close()
                                if os.path.exists(tmp_parquet_path):
                                    os.remove(tmp_parquet_path)
                                raise

                        trace.add_artifact(
                            "parquet",
                            str(parquet_path),
                            query=query_name,
                            rows=total_rows,
                            columns=len(columns),
                        )
                        results.append({
                            "query": query_name,
                            "rows": total_rows,
                            "columns": len(columns),
                            "file": str(parquet_path),
                            "status": "success",
                        })
                    except Exception as e:
                        if isinstance(e, InterruptedError):
                             raise
                        results.append({
                            "query": query_name,
                            "status": "error",
                            "message": str(e),
                        })

                completed_queries += 1
                job_manager.update_job(
                     job_id,
                     completed_queries=completed_queries,
                     results=results,
                     progress=(completed_queries / job_manager.get_job(job_id).total_queries) * 100
                )

            # Consultas auxiliares
            if request.include_auxiliary and request.auxiliary_queries_dir:
                 if job_manager.is_cancelled(job_id):
                     raise InterruptedError("Job cancelado pelo usuário")

                 aux_sql_dir = Path(request.auxiliary_queries_dir)
                 if aux_sql_dir.exists() and aux_sql_dir.is_dir():
                     aux_sql_files = list(aux_sql_dir.glob("*.sql"))

                     job_manager.update_job(
                          job_id,
                          total_queries=len(request.queries) + len(aux_sql_files),
                          message=f"Iniciando {len(aux_sql_files)} consultas auxiliares..."
                     )

                     aux_output_path = output_path / "tabelas_auxiliares"
                     aux_output_path.mkdir(parents=True, exist_ok=True)

                     for aux_file in aux_sql_files:
                         if job_manager.is_cancelled(job_id):
                              raise InterruptedError("Job cancelado pelo usuário")

                         aux_name = aux_file.stem
                         job_manager.update_job(job_id, stage="execute_aux_query", message=f"Executando auxiliar {aux_name}")

                         with ExecutionTimer(trace, stage="execute_aux_query", query=aux_name):
                             try:
                                 aux_sql = ler_sql(aux_file)
                                 aux_params = extrair_parametros_sql(aux_sql)
                                 aux_bind = {p: None for p in aux_params}
                                 if cnpj_limpo:
                                     for p in aux_params:
                                         if p.lower() == "cnpj":
                                             aux_bind[p] = cnpj_limpo
                                         elif p.lower() == "cnpj_raiz":
                                             aux_bind[p] = cnpj_limpo[:8]

                                 if request.parameters:
                                     for p in aux_params:
                                         val = None
                                         if p in request.parameters:
                                             val = request.parameters[p]
                                         elif p.lower() in request.parameters:
                                             val = request.parameters[p.lower()]
                                         elif p.upper() in request.parameters:
                                             val = request.parameters[p.upper()]

                                         if val is not None:
                                             aux_bind[p] = val

                                 for p in list(aux_bind.keys()):
                                     if aux_bind[p] is None and p.lower() not in ("cnpj", "cnpj_raiz"):
                                         aux_bind[p] = ""

                                 import pyarrow as pa
                                 import pyarrow.parquet as pq

                                 with conexao.cursor() as cursor:
                                     cursor.execute(aux_sql, aux_bind)
                                     aux_columns = [desc[0] for desc in cursor.description]

                                     aux_parquet_name = f"{aux_name}.parquet"
                                     aux_parquet_path = aux_output_path / aux_parquet_name

                                     tmp_parquet_path = f"{aux_parquet_path}.tmp_{uuid.uuid4()}"
                                     writer = None
                                     total_rows = 0
                                     try:
                                         while True:
                                             if job_manager.is_cancelled(job_id):
                                                  raise InterruptedError("Job cancelado pelo usuário durante o fetch auxiliar")

                                             chunk = cursor.fetchmany(100000)
                                             if not chunk:
                                                 break

                                             if request.normalize_columns:
                                                 data_dict = {
                                                     col.lower(): [row[i] for row in chunk]
                                                     for i, col in enumerate(aux_columns)
                                                 }
                                             else:
                                                 data_dict = {
                                                     col: [row[i] for row in chunk]
                                                     for i, col in enumerate(aux_columns)
                                                 }

                                             table = pa.Table.from_pydict(data_dict)
                                             if writer is None:
                                                 writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                             writer.write_table(table)
                                             total_rows += len(chunk)

                                             job_manager.update_job(job_id, message=f"Lendo dados de {aux_name}: {total_rows} linhas...")

                                         if writer:
                                             writer.close()
                                         elif total_rows == 0:
                                             empty_cols = {col.lower() if request.normalize_columns else col: [] for col in aux_columns}
                                             empty = pa.Table.from_pydict(empty_cols)
                                             pq.write_table(empty, tmp_parquet_path)

                                         os.replace(tmp_parquet_path, str(aux_parquet_path))
                                     except Exception:
                                         if writer:
                                             writer.close()
                                         if os.path.exists(tmp_parquet_path):
                                             os.remove(tmp_parquet_path)
                                         raise

                                 trace.add_artifact(
                                     "parquet",
                                     str(aux_parquet_path),
                                     query=f"[AUX] {aux_name}",
                                     rows=total_rows,
                                     columns=len(aux_columns),
                                 )
                                 results.append({
                                     "query": f"[AUX] {aux_name}",
                                     "rows": total_rows,
                                     "columns": len(aux_columns),
                                     "file": str(aux_parquet_path),
                                     "status": "success",
                                 })
                             except Exception as e:
                                 if isinstance(e, InterruptedError):
                                      raise
                                 results.append({
                                     "query": f"[AUX] {aux_name}",
                                     "status": "error",
                                     "message": str(e),
                                 })

                         completed_queries += 1
                         job_manager.update_job(
                              job_id,
                              completed_queries=completed_queries,
                              results=results,
                              progress=(completed_queries / job_manager.get_job(job_id).total_queries) * 100
                         )

        finally:
            conexao.__exit__(None, None, None)

        # Finalização bem-sucedida (o próprio job_manager atualizará o status final para success)
        job_manager.update_job(job_id, message="Processamento concluído", results=results)

    except InterruptedError as e:
         trace.add_event("cancelled", "error", str(e))
         job_manager.update_job(job_id, status="cancelled", stage="cancelled", message=str(e), results=results)
         # Re-raise para que o _run_job_wrapper não mude para success
         raise
    except Exception as e:
         trace.add_event("error", "error", str(e))
         # Re-raise para que o _run_job_wrapper lide com o status de erro e exception logging
         raise
    finally:
        # Sempre salva o trace no final, independentemente de erro/cancelamento
        try:
             ExecutionTraceStore(output_path / "_audit" / trace.execution_id).save(trace)
        except Exception as e:
             logger.error(f"Erro ao salvar ExecutionTrace JSON: {e}")
        try:
             sqlite_repo.save_trace(trace)
        except Exception as e:
             logger.error(f"Erro ao salvar trace no SQLite: {e}")
