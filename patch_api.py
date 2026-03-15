with open("server/python/api.py", "r", encoding="utf-8") as f:
    content = f.read()

log_setup = """
# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("sefin_audit_python")
"""

log_setup_new = """
# Configuração de Logging
import logging.handlers
import traceback
log_dir = _CURRENT_DIR / "logs"
log_dir.mkdir(exist_ok=True)
file_handler = logging.handlers.RotatingFileHandler(log_dir / "api_python.log", maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), file_handler]
)
logger = logging.getLogger("sefin_audit_python")
"""

content = content.replace(log_setup.strip(), log_setup_new.strip())


exc_setup = """
app = FastAPI(
"""

exc_setup_new = """
from fastapi.responses import JSONResponse
from fastapi import Request

app = FastAPI(
"""
content = content.replace(exc_setup.strip(), exc_setup_new.strip())

exc_handler = """
# CORS
"""

exc_handler_new = """
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global Error in {request.url.path}: {exc}\\n{traceback.format_exc()}")
    return JSONResponse(status_code=500, content={"detail": "Erro interno do servidor. Consulte os logs.", "success": False})

# CORS
"""

content = content.replace(exc_handler.strip(), exc_handler_new.strip())

with open("server/python/api.py", "w", encoding="utf-8") as f:
    f.write(content)
