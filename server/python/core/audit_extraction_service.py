import os
import uuid
from pathlib import Path
import logging

from core.utils import ler_sql, extrair_parametros_sql

logger = logging.getLogger("sefin_audit_python")

def executar_extracao_sql(
    conexao,
    cnpj_limpo: str,
    dir_parquet: Path,
    dir_sql: Path,
    data_limite_processamento: str = None
) -> tuple[list[dict], list[str]]:
    """
    Executa os arquivos .sql do dir_sql, escreve os parquets no dir_parquet,
    retorna lista de arquivos_extraidos e erros.
    """
    if not isinstance(dir_sql, Path):
        dir_sql = Path(dir_sql)
    if not dir_sql.exists() or not dir_sql.is_dir():
        raise Exception(f"Diretório SQL inválido ou não encontrado: {dir_sql}")

    sql_files = sorted(dir_sql.glob("*.sql"))
    if not sql_files:
        raise Exception(f"Nenhum arquivo .sql encontrado no diretório: {dir_sql}")

    arquivos_extraidos = []
    erros = []

    with conexao.cursor() as cursor:
        cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")

        import pyarrow as pa
        import pyarrow.parquet as pq

        for sql_file in [f for f in sql_files if f.is_file()]:
            query_name = sql_file.stem
            try:
                sql = ler_sql(sql_file)
                params = extrair_parametros_sql(sql)
                bind_vars = {p: "" for p in params}
                for p in params:
                    if p.lower() == "cnpj":
                        bind_vars[p] = cnpj_limpo
                    elif p.lower() == "cnpj_raiz":
                        bind_vars[p] = cnpj_limpo[:8]
                    elif (
                        p.lower() == "data_limite_processamento"
                        and data_limite_processamento
                    ):
                        bind_vars[p] = data_limite_processamento

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
                        if not chunk:
                            break

                        data_dict = {
                            col.lower(): [row[i] for row in chunk]
                            for i, col in enumerate(columns)
                        }

                        table = pa.Table.from_pydict(data_dict)
                        if writer is None:
                            writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                        writer.write_table(table)
                        total_rows += len(chunk)

                    if writer:
                        writer.close()
                    elif total_rows == 0:
                        empty_cols = {col.lower(): [] for col in columns}
                        empty = pa.Table.from_pydict(empty_cols)
                        pq.write_table(empty, tmp_parquet_path)

                    os.replace(tmp_parquet_path, str(parquet_path))
                except Exception:
                    if writer:
                        writer.close()
                    if os.path.exists(tmp_parquet_path):
                        os.remove(tmp_parquet_path)
                    raise

                arquivos_extraidos.append(
                    {
                        "name": parquet_name,
                        "path": str(parquet_path),
                        "rows": total_rows,
                    }
                )
            except Exception as e:
                erros.append(f"Extração {query_name}: {str(e)}")

    return arquivos_extraidos, erros
