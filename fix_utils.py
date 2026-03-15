import re

with open("server/python/core/utils.py", "r", encoding="utf-8") as f:
    content = f.read()

# O código que vamos remover (pois o PyArrow já faz isso atômicamente via o loop):
atomic_func_str = """
def write_parquet_atomic(df: pl.DataFrame, final_path: str):
    tmp = f"{final_path}.tmp_{uuid.uuid4()}"
    df.write_parquet(tmp)
    os.replace(tmp, final_path)
"""

content = content.replace(atomic_func_str, "")

# Remover os imports se estiverem sozinhos
content = content.replace("import os\nimport uuid\n", "")

with open("server/python/core/utils.py", "w", encoding="utf-8") as f:
    f.write(content)
