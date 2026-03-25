import logging
import os
import sys
import uvicorn
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Adiciona o diretório atual ao sys.path para garantir que os módulos locais sejam encontrados
_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.append(str(_CURRENT_DIR))
# Garante que a raiz do projeto esteja no sys.path para imports (ex.: 'cruzamentos')
_PROJECT_ROOT = _CURRENT_DIR.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.append(str(_PROJECT_ROOT))

from routers import oracle, parquet, analysis, reports, filesystem, export, produto_unid, references, jobs

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("sefin_audit_python")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando API Python SEFIN Audit Tool...")
    yield
    logger.info("Encerrando API Python...")

app = FastAPI(
    title="SEFIN Audit Tool - Python API",
    description="API de alto desempenho para extração e processamento de dados fiscais (Polars/Oracle).",
    version="2.0.0",
    lifespan=lifespan
)

# CORS
# CORS configurável por ambiente (se não definido, restringe a localhost por segurança)
allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Inclusão dos Routers
app.include_router(oracle.router)
app.include_router(parquet.router)
app.include_router(analysis.router)
app.include_router(reports.router)
app.include_router(filesystem.router)
app.include_router(export.router)
app.include_router(produto_unid.router)
app.include_router(references.router)
app.include_router(jobs.router)

@app.get("/health")
async def health_check():
    return {"status": "ok", "version": "2.0.0", "engine": "python"}

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Requisição: {request.method} {request.url.path}")
    response = await call_next(request)
    return response

if __name__ == "__main__":
    port = int(os.getenv("PYTHON_API_PORT", 8000))
    host = os.getenv("PYTHON_API_HOST", "127.0.0.1")
    logger.info(f"Servidor rodando em http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
