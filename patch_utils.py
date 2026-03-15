import os
import uuid
from pathlib import Path

def patch_utils():
    with open("server/python/core/utils.py", "r", encoding="utf-8") as f:
        content = f.read()

    # Adicionar os imports e a função write_parquet_atomic logo após os imports
    if "def write_parquet_atomic" not in content:
        import_block = "import os\nimport uuid\n"
        if "import os" not in content:
            content = content.replace("import polars as pl\n", import_block + "import polars as pl\n")

        atomic_func = """
def write_parquet_atomic(df: pl.DataFrame, final_path: str):
    tmp = f"{final_path}.tmp_{uuid.uuid4()}"
    df.write_parquet(tmp)
    os.replace(tmp, final_path)

"""
        content = content.replace("logger = logging.getLogger(\"sefin_audit_python\")\n", "logger = logging.getLogger(\"sefin_audit_python\")\n" + atomic_func)

        with open("server/python/core/utils.py", "w", encoding="utf-8") as f:
            f.write(content)

patch_utils()
