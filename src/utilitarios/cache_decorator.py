import functools
import polars as pl
from pathlib import Path
import hashlib
import time
import re
import os
from typing import Callable, Any, Optional

def cached_transform(cache_dir: Path | str, func_name: Optional[str] = None):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal func_name
            if func_name is None:
                func_name = func.__name__

            # Find CNPJ to separate cache by CNPJ
            cnpj_val = "global"
            if "cnpj" in kwargs:
                cnpj_val = kwargs["cnpj"]
            elif len(args) > 0 and isinstance(args[0], str) and len(args[0]) >= 14:
                cnpj_val = args[0]

            base_cache = Path(cache_dir) / cnpj_val
            base_cache.mkdir(parents=True, exist_ok=True)

            cache_path = base_cache / f"{func_name}_cache.parquet"
            meta_path = base_cache / f"{func_name}_cache.meta"

            # Intelligent hash: include func_name, args (like CNPJ), and LazyFrame data dependencies
            hash_input = hashlib.sha256()
            hash_input.update(func_name.encode())

            for arg in args:
                if isinstance(arg, pl.LazyFrame):
                    plan = arg.explain()
                    hash_input.update(plan.encode())
                    # Invalida o cache se os arquivos upstream mudarem (extrai caminhos de Parquet SCAN)
                    for match in re.finditer(r'Parquet SCAN (.*?\.parquet)', plan):
                        filepath = match.group(1).strip()
                        if os.path.exists(filepath):
                            hash_input.update(str(os.path.getmtime(filepath)).encode())
                            hash_input.update(str(os.path.getsize(filepath)).encode())
                elif isinstance(arg, pl.DataFrame):
                    hash_input.update(str(arg.schema).encode())
                    hash_input.update(str(arg.height).encode())
                else:
                    hash_input.update(str(arg).encode())

            for k, v in kwargs.items():
                hash_input.update(str(k).encode())
                if isinstance(v, pl.LazyFrame):
                    plan = v.explain()
                    hash_input.update(plan.encode())
                    for match in re.finditer(r'Parquet SCAN (.*?\.parquet)', plan):
                        filepath = match.group(1).strip()
                        if os.path.exists(filepath):
                            hash_input.update(str(os.path.getmtime(filepath)).encode())
                            hash_input.update(str(os.path.getsize(filepath)).encode())
                elif isinstance(v, pl.DataFrame):
                    hash_input.update(str(v.schema).encode())
                    hash_input.update(str(v.height).encode())
                else:
                    hash_input.update(str(v).encode())

            hash_hex = hash_input.hexdigest()

            # Reutiliza resultado cacheado
            if cache_path.exists() and meta_path.exists():
                 with open(meta_path, 'r') as m:
                      saved_hash = m.read().strip()
                 if saved_hash == hash_hex:
                      print(f"   [CACHE HIT] Carregando resultado de {func_name}")
                      return pl.scan_parquet(cache_path)

            print(f"   [CACHE MISS] Executando plano lógico de {func_name}")

            result = func(*args, **kwargs)

            if isinstance(result, pl.LazyFrame):
                start_time = time.time()
                # Coleta e descarta para mensurar o tempo de execução
                # Se for muito rapido, retornamos o result original (LazyFrame puro) para
                # nao quebrar a otimizacao global. Se for demorado (>30s), salvamos.
                df_to_save = result.collect(streaming=True)
                exec_time = time.time() - start_time

                if exec_time > 30.0:
                    print(f"   [CACHE] Processamento levou {exec_time:.2f}s (>30s). Salvando intermediário...")
                    tmp_path = cache_path.with_suffix('.tmp')
                    df_to_save.write_parquet(tmp_path, compression="snappy")
                    import shutil
                    if hasattr(os, 'replace'):
                        os.replace(tmp_path, cache_path)
                    else:
                        shutil.move(tmp_path, cache_path)

                    with open(meta_path, 'w') as m:
                        m.write(hash_hex)

                    return pl.scan_parquet(cache_path)
                else:
                    # Rápido o suficiente. Joga fora os dados coletados e retorna a expressao lazy
                    # permitindo que o Polars otimize o DAG inteiro no final.
                    print(f"   [CACHE] Processamento levou {exec_time:.2f}s (<30s). Ignorando cache.")
                    return result
            else:
                # Eager result timing
                start_time = time.time()
                df_to_save = result
                exec_time = time.time() - start_time # Takes basically 0
                return result

        return wrapper
    return decorator
