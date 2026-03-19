import os
from pathlib import Path

from dotenv import load_dotenv

APP_NAME = "Fiscal Parquet Analyzer"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load environment variables from .env file in the project root
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    load_dotenv(env_path, override=False, encoding="latin-1")
DATA_ROOT = PROJECT_ROOT / "workspace"
CONSULTAS_ROOT = DATA_ROOT / "consultas"
APP_STATE_ROOT = DATA_ROOT / "app_state"
REGISTRY_FILE = APP_STATE_ROOT / "cnpjs.json"
AGGREGATION_LOG_FILE = APP_STATE_ROOT / "operacoes_agregacao.jsonl"
PIPELINE_SCRIPT = PROJECT_ROOT / "pipeline_oracle_parquet.py"
SQL_DIR = PROJECT_ROOT / "sql"
FUNCOES_ROOT = Path(r"c:\funcoes")
EXTRA_SQL_DIRS = [FUNCOES_ROOT / "consultas_fonte"]
DEFAULT_PAGE_SIZE = 200
MAX_DOCX_ROWS = 500

CNPJ_ROOT = FUNCOES_ROOT / "CNPJ"
CONSULTAS_FONTE_DIR = FUNCOES_ROOT / "consultas_fonte"
TABELA_PRODUTOS_DIR = FUNCOES_ROOT / "funcoes_tabelas" / "tabela_produtos"

for path in [DATA_ROOT, CONSULTAS_ROOT, APP_STATE_ROOT, SQL_DIR, CNPJ_ROOT]:
    path.mkdir(parents=True, exist_ok=True)
