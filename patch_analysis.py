import re
with open("server/python/routers/analysis.py", "r", encoding="utf-8") as f:
    content = f.read()

# Substituir pipeline de extração
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

with open("server/python/routers/analysis.py", "w", encoding="utf-8") as f:
    f.write(content)
