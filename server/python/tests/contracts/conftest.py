import pytest
import sys
from pathlib import Path
import polars as pl

# Ensure 'server/python' is in sys.path so 'api' can be imported easily
python_dir = Path(__file__).parent.parent.parent.resolve()
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

@pytest.fixture(autouse=True)
def setup_mock_config(monkeypatch, tmp_path):
    config_py = tmp_path / "config.py"

    config_content = """
from pathlib import Path

def obter_diretorios_cnpj(cnpj):
    base_dir = Path(f"/tmp/auditoria_test/{cnpj}")
    dir_parquet = base_dir / "parquet"
    dir_analises = base_dir / "analises"
    dir_relatorios = base_dir / "relatorios"

    dir_parquet.mkdir(parents=True, exist_ok=True)
    dir_analises.mkdir(parents=True, exist_ok=True)
    dir_relatorios.mkdir(parents=True, exist_ok=True)

    return dir_parquet, dir_analises, dir_relatorios

DIR_SQL = Path("/tmp/sql_test")
DIR_SQL.mkdir(parents=True, exist_ok=True)

DIR_CNPJS = Path("/tmp/auditoria_test")
DIR_CNPJS.mkdir(parents=True, exist_ok=True)
"""
    config_py.write_text(config_content)

    cnpj_dir = Path("/tmp/auditoria_test/11222333000181")
    (cnpj_dir / "arquivos_parquet").mkdir(parents=True, exist_ok=True)
    (cnpj_dir / "analises").mkdir(parents=True, exist_ok=True)
    (cnpj_dir / "relatorios").mkdir(parents=True, exist_ok=True)

    import routers.analysis
    import routers.filesystem
    import routers.produtos._utils
    import routers.produtos.revisao
    import routers.produtos.status
    import routers.produtos.multidescricao
    import routers.produtos.vectorizacao

    routers.analysis._PROJETO_DIR = tmp_path
    routers.filesystem._PROJETO_DIR = tmp_path
    routers.produtos._utils._PROJETO_DIR = tmp_path
    routers.produtos.revisao._PROJETO_DIR = tmp_path
    routers.produtos.status._PROJETO_DIR = tmp_path
    routers.produtos.multidescricao._PROJETO_DIR = tmp_path
    routers.produtos.vectorizacao._PROJETO_DIR = tmp_path

    # Provide missing functions that usually come from _utils
    routers.produtos.revisao._load_cnpj_dirs = lambda cnpj: (cnpj_dir / "arquivos_parquet", cnpj_dir / "analises", cnpj_dir / "relatorios")
    routers.produtos.status._load_cnpj_dirs = lambda cnpj: (cnpj_dir / "arquivos_parquet", cnpj_dir / "analises", cnpj_dir / "relatorios")
    routers.produtos.status._gravar_status_analise = lambda dir_analises, cnpj_limpo: dir_analises / "status.parquet"
    routers.produtos.status._resumir_status_analise = lambda dir_analises, cnpj_limpo, df: {"pendentes": 0}

    routers.produtos.multidescricao._load_cnpj_dirs = lambda cnpj: (cnpj_dir / "arquivos_parquet", cnpj_dir / "analises", cnpj_dir / "relatorios")
    routers.produtos.multidescricao._normalize_page = lambda page: max(1, page)
    routers.produtos.multidescricao._normalize_page_size = lambda page_size, default=50, max_size=200: min(max(1, page_size), max_size)

    # create a mock status.parquet
    pl.DataFrame({"test": [1]}).write_parquet(str(cnpj_dir / "analises" / "status.parquet"))

    yield
