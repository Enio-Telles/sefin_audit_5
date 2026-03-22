import tempfile
from pathlib import Path
import os
import shutil

TMP_DIR = tempfile.TemporaryDirectory()
base_path = Path(TMP_DIR.name)

DIR_SQL = str(base_path / "sql")
DIR_PARQUET = str(base_path / "parquet")
DIR_ANALISES = str(base_path / "analises")
DIR_RELATORIOS = str(base_path / "relatorios")
DIR_CONSULTAS = str(base_path / "consultas")

DIR_CNPJS = Path.cwd() / "CNPJ"
DIR_CNPJS.mkdir(parents=True, exist_ok=True)
_test_dir = DIR_CNPJS / "00000000000191"
_test_dir.mkdir(parents=True, exist_ok=True)
(_test_dir / "arquivos_parquet").mkdir(parents=True, exist_ok=True)
(_test_dir / "analises").mkdir(parents=True, exist_ok=True)
(_test_dir / "relatorios").mkdir(parents=True, exist_ok=True)

# Also create mock data so products routing doesn't raise 500 when it expects parquets to do math on
import polars as pl
try:
    df_empty = pl.DataFrame({"descricao": [], "chave_produto": [], "requer_revisao_manual": []})
    df_empty.write_parquet(_test_dir / "analises" / "produtos_agregados_00000000000191.parquet")

    df_empty2 = pl.DataFrame({"tipo_ref": [], "status_analise": [], "ref_id": []})
    df_empty2.write_parquet(_test_dir / "analises" / "status_analise_produtos_00000000000191.parquet")

    df_empty3 = pl.DataFrame({"codigo": []})
    df_empty3.write_parquet(_test_dir / "analises" / "codigos_multidescricao_00000000000191.parquet")
except Exception as e:
    pass

def obter_diretorios_cnpj(cnpj: str):
    # API endpoints directly use _load_cnpj_dirs -> obter_diretorios_cnpj
    # For testing and avoiding "name is not defined" or 500s because of empty path lists,
    # let's map directly to the real test dir.
    base = DIR_CNPJS / cnpj
    base.mkdir(parents=True, exist_ok=True)
    dir_parquet = base / "arquivos_parquet"
    dir_analises = base / "analises"
    dir_relatorios = base / "relatorios"
    dir_parquet.mkdir(parents=True, exist_ok=True)
    dir_analises.mkdir(parents=True, exist_ok=True)
    dir_relatorios.mkdir(parents=True, exist_ok=True)

    # We must also create empty parquets here inside the new dirs dynamically if they don't exist
    try:
        import polars as pl
        p1 = dir_analises / f"produtos_agregados_{cnpj}.parquet"
        if not p1.exists():
            pl.DataFrame({"descricao": [], "chave_produto": [], "requer_revisao_manual": []}).write_parquet(p1)
        p2 = dir_analises / f"status_analise_produtos_{cnpj}.parquet"
        if not p2.exists():
            pl.DataFrame({"tipo_ref": [], "status_analise": [], "ref_id": []}).write_parquet(p2)
        p3 = dir_analises / f"codigos_multidescricao_{cnpj}.parquet"
        if not p3.exists():
            pl.DataFrame({"codigo": []}).write_parquet(p3)
    except:
        pass

    return dir_parquet, dir_analises, dir_relatorios
