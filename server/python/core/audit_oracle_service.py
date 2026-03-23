import os
import logging
from pathlib import Path

logger = logging.getLogger("sefin_audit_python")

def obter_conexao_oracle(projeto_dir: Path):
    """
    Carrega credenciais do Oracle e retorna uma conexão ativa.
    Levanta ValueError ou Exception em caso de erro.
    """
    from dotenv import load_dotenv
    import keyring
    import oracledb

    load_dotenv(dotenv_path=str(projeto_dir / ".env"), override=True)
    saved_user = os.getenv("SAVED_ORACLE_USER", "").strip().strip("'").strip('"')
    if not saved_user:
        raise ValueError("Credenciais Oracle ausentes.")

    saved_password = keyring.get_password("sefin_audit_tool", saved_user)
    if not saved_password:
        raise ValueError("Senha Oracle ausente no Cofre.")

    dsn = oracledb.makedsn("exa01-scan.sefin.ro.gov.br", 1521, service_name="sefindw")
    conexao = oracledb.connect(user=saved_user, password=saved_password, dsn=dsn)

    return conexao
