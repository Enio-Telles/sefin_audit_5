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
PIPELINE_SCRIPT = PROJECT_ROOT / "src" / "orquestrador.py"
SQL_DIR = PROJECT_ROOT / "sql"

# Definir os diretórios principais baseados na PROJECT_ROOT (sem caminhos fixos)
CNPJ_ROOT = PROJECT_ROOT / "dados" / "CNPJ"
CONSULTAS_FONTE_DIR = PROJECT_ROOT / "sql"
TABELA_PRODUTOS_DIR = PROJECT_ROOT / "src" / "transformacao" / "analise_produtos"
CFOP_BI_PATH = PROJECT_ROOT / "dados" / "referencias" / "cfop" / "cfop_bi.parquet"
DIR_REFERENCIAS = PROJECT_ROOT / "dados" / "referencias"

DEFAULT_PAGE_SIZE = 200
MAX_DOCX_ROWS = 500

for path in [DATA_ROOT, CONSULTAS_ROOT, APP_STATE_ROOT, SQL_DIR, CNPJ_ROOT]:
    path.mkdir(parents=True, exist_ok=True)

# Alias para retrocompatibilidade com scripts que buscam FUNCOES_ROOT
FUNCOES_ROOT = PROJECT_ROOT
