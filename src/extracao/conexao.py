"""
Módulo de conexão Oracle refatorado.
Usa credenciais do arquivo .env na raiz do projeto.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import oracledb
from rich import print as rprint

# Localiza a raiz do projeto (assumindo que este arquivo está em src/extracao/)
RAIZ_PROJETO = Path(__file__).parent.parent.parent
env_path = RAIZ_PROJETO / '.env'
load_dotenv(dotenv_path=env_path, encoding='latin-1', override=True)

HOST = os.getenv("ORACLE_HOST", 'exa01-scan.sefin.ro.gov.br').strip()
PORTA = int(os.getenv("ORACLE_PORT", '1521').strip())
SERVICO = os.getenv("ORACLE_SERVICE", 'sefindw').strip()

def conectar(usuario=None, senha=None):
    """
    Estabelece conexão com o banco Oracle.
    """
    if usuario is None:
        usuario = os.getenv("DB_USER")
    if senha is None:
        senha = os.getenv("DB_PASSWORD")
    
    if usuario:
        usuario = usuario.strip()
    if senha:
        senha = senha.strip()
    
    if not usuario or not senha:
        rprint("[red]Erro:[/red] Credenciais não encontradas no .env")
        return None
    
    try:
        dsn = oracledb.makedsn(HOST, PORTA, service_name=SERVICO)
        
        # Log de debug opcional
        # rprint(f"[cyan]DEBUG: Conectando a {HOST}:{PORTA}/{SERVICO}[/cyan]")
        
        conexao = oracledb.connect(user=usuario, password=senha, dsn=dsn)
        
        # Configuração de sessão para evitar problemas com decimais
        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")

        rprint("[green]=> Conectado ao Oracle[/green]")
        return conexao
    except Exception as e:
        rprint(f"[red]Erro de conexão Oracle:[/red] {e}")
        return None
