import re
import os
import uuid
import keyring
import socket
from pathlib import Path
from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv, set_key
from core.models import OracleConnectionConfig, ExtractionRequest
from core.utils import validar_cnpj
import logging
from core.job_manager import job_manager
from services.oracle_extraction_service import run_oracle_extraction_job

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python/oracle", tags=["oracle"])

# Get project root from environment or handle it
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent


def _oracle_network_precheck(host: str, port: int) -> None:
    """Valida DNS e conectividade TCP basica antes do connect do Oracle."""
    host_clean = str(host or "").strip()
    if not host_clean:
        raise ValueError("Host Oracle nao informado.")

    try:
        socket.getaddrinfo(host_clean, port)
    except socket.gaierror as exc:
        raise ConnectionError(
            f"Host Oracle nao resolvido por DNS: {host_clean}. Verifique hostname, VPN ou DNS da rede corporativa."
        ) from exc

    try:
        with socket.create_connection((host_clean, int(port)), timeout=3):
            pass
    except TimeoutError as exc:
        raise ConnectionError(
            f"Host Oracle resolvido, mas a porta {port} nao respondeu em tempo habil para {host_clean}. Verifique rede, VPN ou firewall."
        ) from exc
    except OSError as exc:
        raise ConnectionError(
            f"Host Oracle resolvido, mas a porta {port} nao esta acessivel em {host_clean}. Verifique rede, VPN, firewall ou disponibilidade do listener Oracle."
        ) from exc


@router.post("/test-connection")
async def test_oracle_connection(config: OracleConnectionConfig):
    """Testa conexão com o banco Oracle."""
    try:
        import oracledb
        from db_manager import DatabaseManager

        _oracle_network_precheck(config.host, config.port)
        dsn = oracledb.makedsn(config.host, config.port, service_name=config.service)
        db_manager = DatabaseManager(
            dsn=dsn, user=config.user, password=config.password
        )
        with db_manager.get_connection():
            pass  # NLS alter and connection verified in manager
        return {"success": True, "message": "Conexão estabelecida com sucesso"}
    except ImportError:
        return {
            "success": False,
            "message": "Driver Oracle (oracledb) não instalado. Instale com: pip install oracledb",
        }
    except Exception as e:
        return {"success": False, "message": f"Erro de conexão: {str(e)}"}


@router.post("/extract")
async def extract_oracle_data(request: ExtractionRequest):
    """Cria um job de extração de dados do Oracle por CNPJ."""
    cnpj_limpo = re.sub(r"[^0-9]", "", request.cnpj) if request.cnpj else ""
    if request.cnpj and not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    try:
         # Mantem o precheck rapido para nao enfileirar job inutil se a rede estiver fora
         _oracle_network_precheck(request.connection.host, request.connection.port)
    except Exception as e:
         raise HTTPException(status_code=502, detail=f"Erro de rede pré-extração: {str(e)}")

    job_id = str(uuid.uuid4())
    job_state = job_manager.submit_job(
         job_id,
         "oracle_extraction",
         run_oracle_extraction_job,
         request
    )

    return {
        "success": True,
        "job_id": job_id,
        "status": job_state.status,
        "message": "Job de extração iniciado",
        "job_info": job_state.model_dump()
    }


@router.get("/credentials")
async def get_oracle_credentials():
    """Indica a presença de credenciais salvas sem expor a senha ao cliente."""
    try:
        env_path = _PROJETO_DIR / ".env"
        load_dotenv(dotenv_path=str(env_path), override=True)
        saved_user = os.getenv("SAVED_ORACLE_USER", "").strip().strip("'").strip('"')
        if not saved_user:
            return {"success": True, "has_credentials": False}
        password = keyring.get_password("sefin_audit_tool", saved_user)
        if not password:
            return {"success": True, "has_credentials": False, "user": saved_user}
        # Não retornar a senha por segurança
        return {"success": True, "has_credentials": True, "user": saved_user}
    except Exception as e:
        return {"success": False, "message": str(e), "has_credentials": False}


@router.post("/save-credentials")
async def save_oracle_credentials(config: OracleConnectionConfig):
    """Salva o usuário no .env local e a senha de forma criptografada no Windows Credential Manager."""
    if not config.user or not config.password:
        raise HTTPException(status_code=400, detail="Usuário e senha são obrigatórios")
    try:
        env_path = _PROJETO_DIR / ".env"
        load_dotenv(dotenv_path=str(env_path), override=True)
        saved_user = os.getenv("SAVED_ORACLE_USER", "").strip().strip("'").strip('"')
        # Apenas re-escreve o .env se o usuario mudou
        if saved_user != config.user.strip().strip("'").strip('"'):
            set_key(str(env_path), "SAVED_ORACLE_USER", config.user)

        keyring.set_password("sefin_audit_tool", config.user, config.password)
        return {
            "success": True,
            "message": "Credenciais salvas com sucesso no Cofre do Windows",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao salvar credenciais: {str(e)}"
        )


@router.delete("/clear-credentials")
async def clear_oracle_credentials():
    """Remove a senha do Windows Credential Manager e o usuário do .env."""
    try:
        env_path = _PROJETO_DIR / ".env"
        load_dotenv(dotenv_path=str(env_path), override=True)
        saved_user = os.getenv("SAVED_ORACLE_USER", "").strip().strip("'").strip('"')
        if saved_user:
            try:
                keyring.delete_password("sefin_audit_tool", saved_user)
            except keyring.errors.PasswordDeleteError:
                pass
            set_key(str(env_path), "SAVED_ORACLE_USER", "")
        return {"success": True, "message": "Credenciais removidas com sucesso"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao remover credenciais: {str(e)}"
        )
