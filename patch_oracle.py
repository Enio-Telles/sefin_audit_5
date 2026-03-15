import re
with open("server/python/routers/oracle.py", "r", encoding="utf-8") as f:
    content = f.read()

# Substituir Extração Genérica (linhas ~124)
extra_gen = """
                    with conexao.cursor() as cursor:
                        cursor.execute(sql, bind_vars)
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()

                    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)}, strict=False)
                    if request.normalize_columns:
                        df = normalizar_colunas(df)

                    parquet_name = f"{query_name}_{cnpj_limpo}.parquet" if cnpj_limpo else f"{query_name}.parquet"
                    parquet_path = output_path / parquet_name
                    df.write_parquet(str(parquet_path))

                    results.append({
                        "query": query_name,
                        "rows": len(rows),
"""

extra_gen_new = """
                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    import uuid
                    import os

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
                                chunk = cursor.fetchmany(100000)
                                if not chunk: break

                                if request.normalize_columns:
                                    data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(columns)}
                                else:
                                    data_dict = {col: [row[i] for row in chunk] for i, col in enumerate(columns)}

                                table = pa.Table.from_pydict(data_dict)
                                if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                writer.write_table(table)
                                total_rows += len(chunk)

                            if writer: writer.close()
                            elif total_rows == 0:
                                empty_cols = {col.lower() if request.normalize_columns else col: [] for col in columns}
                                empty = pa.Table.from_pydict(empty_cols)
                                pq.write_table(empty, tmp_parquet_path)

                            os.replace(tmp_parquet_path, str(parquet_path))
                        except Exception:
                            if writer: writer.close()
                            if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                            raise

                    results.append({
                        "query": query_name,
                        "rows": total_rows,
"""

content = content.replace(extra_gen.strip(), extra_gen_new.strip())


# Substituir Extração Auxiliares (linhas ~185)
extra_aux = """
                            with conexao.cursor() as cursor:
                                cursor.execute(aux_sql, aux_bind)
                                aux_columns = [desc[0] for desc in cursor.description]
                                aux_rows = cursor.fetchall()
                            aux_df = pl.DataFrame({col: [row[i] for row in aux_rows] for i, col in enumerate(aux_columns)}, strict=False)
                            if request.normalize_columns:
                                aux_df = normalizar_colunas(aux_df)
                            aux_parquet_name = f"{aux_name}.parquet"
                            aux_parquet_path = aux_output_path / aux_parquet_name
                            aux_df.write_parquet(str(aux_parquet_path))
                            results.append({
                                "query": f"[AUX] {aux_name}",
                                "rows": len(aux_rows),
"""

extra_aux_new = """
                            import pyarrow as pa
                            import pyarrow.parquet as pq
                            import uuid
                            import os

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
                                        chunk = cursor.fetchmany(100000)
                                        if not chunk: break

                                        if request.normalize_columns:
                                            data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(aux_columns)}
                                        else:
                                            data_dict = {col: [row[i] for row in chunk] for i, col in enumerate(aux_columns)}

                                        table = pa.Table.from_pydict(data_dict)
                                        if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                        writer.write_table(table)
                                        total_rows += len(chunk)

                                    if writer: writer.close()
                                    elif total_rows == 0:
                                        empty_cols = {col.lower() if request.normalize_columns else col: [] for col in aux_columns}
                                        empty = pa.Table.from_pydict(empty_cols)
                                        pq.write_table(empty, tmp_parquet_path)

                                    os.replace(tmp_parquet_path, str(aux_parquet_path))
                                except Exception:
                                    if writer: writer.close()
                                    if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                                    raise

                            results.append({
                                "query": f"[AUX] {aux_name}",
                                "rows": total_rows,
"""

content = content.replace(extra_aux.strip(), extra_aux_new.strip())

with open("server/python/routers/oracle.py", "w", encoding="utf-8") as f:
    f.write(content)
