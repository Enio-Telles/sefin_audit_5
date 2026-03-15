import re
import os
import keyring
import socket
from pathlib import Path
from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv, set_key
from core.models import OracleConnectionConfig, ExtractionRequest
from core.utils import validar_cnpj, ler_sql, extrair_parametros_sql, normalizar_colunas
import polars as pl
import logging

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
        db_manager = DatabaseManager(dsn=dsn, user=config.user, password=config.password)
        with db_manager.get_connection() as conexao:
            pass # NLS alter and connection verified in manager
        return {"success": True, "message": "Conexão estabelecida com sucesso"}
    except ImportError:
        return {"success": False, "message": "Driver Oracle (oracledb) não instalado. Instale com: pip install oracledb"}
    except Exception as e:
        return {"success": False, "message": f"Erro de conexão: {str(e)}"}


@router.post("/extract")
async def extract_oracle_data(request: ExtractionRequest):
    """Extrai dados do Oracle por CNPJ e salva em Parquet."""
    cnpj_limpo = re.sub(r"[^0-9]", "", request.cnpj) if request.cnpj else ""
    if request.cnpj and not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    output_path = Path(request.output_dir) / cnpj_limpo if cnpj_limpo else Path(request.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results = []
    try:
        import oracledb
        from db_manager import DatabaseManager
        _oracle_network_precheck(request.connection.host, request.connection.port)
        dsn = oracledb.makedsn(request.connection.host, request.connection.port, service_name=request.connection.service)
        db_manager = DatabaseManager(dsn=dsn, user=request.connection.user, password=request.connection.password)
        
        with db_manager.get_connection() as conexao:
            for query_path in request.queries:
                query_file = Path(query_path)
                query_name = query_file.stem if query_file.exists() else query_path
                try:
                    sql = ler_sql(query_file) if query_file.exists() else query_path
                    params = extrair_parametros_sql(sql)
                    bind_vars = {p: None for p in params}
                    if cnpj_limpo:
                        for p in params:
                            if p.lower() == "cnpj":
                                bind_vars[p] = cnpj_limpo
                            elif p.lower() == "cnpj_raiz":
                                bind_vars[p] = cnpj_limpo[:8]

                    if request.parameters:
                        for p in params:
                            val = None
                            if p in request.parameters:
                                val = request.parameters[p]
                            elif p.lower() in request.parameters:
                                val = request.parameters[p.lower()]
                            elif p.upper() in request.parameters:
                                val = request.parameters[p.upper()]
                            
                            if val is not None:
                                bind_vars[p] = val

                    # Substitui None por "" para evitar DPY-4010 em parâmetros opcionais
                    for p in list(bind_vars.keys()):
                        if bind_vars[p] is None and p.lower() not in ("cnpj", "cnpj_raiz"):
                            bind_vars[p] = ""

                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    import uuid
                    import os

                    with conexao.cursor() as cursor:
                        cursor.execute(sql, bind_vars)
                        columns = [desc[0] for desc in cursor.description]

                        parquet_name = f"{query_name}_{cnpj_limpo}.parquet" if cnpj_limpo else f"{query_name}.parquet"
                        parquet_path = output_path / parquet_name

                        tmp_parquet_path = f"{parquet_path}.tmp_{uuid.uuid4()}"
                        writer = None
                        total_rows = 0
                        try:
                            while True:
                                chunk = cursor.fetchmany(100000)
                                if not chunk: break

                                if request.normalize_columns:
                                    data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(columns)}
                                else:
                                    data_dict = {col: [row[i] for row in chunk] for i, col in enumerate(columns)}

                                table = pa.Table.from_pydict(data_dict)
                                if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                writer.write_table(table)
                                total_rows += len(chunk)

                            if writer: writer.close()
                            elif total_rows == 0:
                                empty_cols = {col.lower() if request.normalize_columns else col: [] for col in columns}
                                empty = pa.Table.from_pydict(empty_cols)
                                pq.write_table(empty, tmp_parquet_path)

                            os.replace(tmp_parquet_path, str(parquet_path))
                        except Exception:
                            if writer: writer.close()
                            if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                            raise

                    results.append({
                        "query": query_name,
                        "rows": total_rows,
                        "columns": len(columns),
                        "file": str(parquet_path),
                        "status": "success",
                    })
                except Exception as e:
                    results.append({
                        "query": query_name,
                        "status": "error",
                        "message": str(e),
                    })

            if request.include_auxiliary and request.auxiliary_queries_dir:
                aux_sql_dir = Path(request.auxiliary_queries_dir)
                if aux_sql_dir.exists() and aux_sql_dir.is_dir():
                    aux_output_path = Path(request.output_dir) / "tabelas_auxiliares"
                    aux_output_path.mkdir(parents=True, exist_ok=True)
                    aux_sql_files = list(aux_sql_dir.glob("*.sql"))
                    logger.info("[extract] Executando %d consultas auxiliares de '%s'", len(aux_sql_files), aux_sql_dir)
                    for aux_file in aux_sql_files:
                        aux_name = aux_file.stem
                        try:
                            aux_sql = ler_sql(aux_file)
                            aux_params = extrair_parametros_sql(aux_sql)
                            aux_bind = {p: None for p in aux_params}
                            if cnpj_limpo:
                                for p in aux_params:
                                    if p.lower() == "cnpj":
                                        aux_bind[p] = cnpj_limpo
                                    elif p.lower() == "cnpj_raiz":
                                        aux_bind[p] = cnpj_limpo[:8]

                            if request.parameters:
                                for p in aux_params:
                                    val = None
                                    if p in request.parameters:
                                        val = request.parameters[p]
                                    elif p.lower() in request.parameters:
                                        val = request.parameters[p.lower()]
                                    elif p.upper() in request.parameters:
                                        val = request.parameters[p.upper()]
                                    
                                    if val is not None:
                                        aux_bind[p] = val

                            # Substitui None por "" para evitar DPY-4010 em parâmetros opcionais
                            for p in list(aux_bind.keys()):
                                if aux_bind[p] is None and p.lower() not in ("cnpj", "cnpj_raiz"):
                                    aux_bind[p] = ""

                            import pyarrow as pa
                            import pyarrow.parquet as pq
                            import uuid
                            import os

                            with conexao.cursor() as cursor:
                                cursor.execute(aux_sql, aux_bind)
                                aux_columns = [desc[0] for desc in cursor.description]

                                aux_parquet_name = f"{aux_name}.parquet"
                                aux_parquet_path = aux_output_path / aux_parquet_name

                                tmp_parquet_path = f"{aux_parquet_path}.tmp_{uuid.uuid4()}"
                                writer = None
                                total_rows = 0
                                try:
                                    while True:
                                        chunk = cursor.fetchmany(100000)
                                        if not chunk: break

                                        if request.normalize_columns:
                                            data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(aux_columns)}
                                        else:
                                            data_dict = {col: [row[i] for row in chunk] for i, col in enumerate(aux_columns)}

                                        table = pa.Table.from_pydict(data_dict)
                                        if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                                        writer.write_table(table)
                                        total_rows += len(chunk)

                                    if writer: writer.close()
                                    elif total_rows == 0:
                                        empty_cols = {col.lower() if request.normalize_columns else col: [] for col in aux_columns}
                                        empty = pa.Table.from_pydict(empty_cols)
                                        pq.write_table(empty, tmp_parquet_path)

                                    os.replace(tmp_parquet_path, str(aux_parquet_path))
                                except Exception:
                                    if writer: writer.close()
                                    if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                                    raise

                            results.append({
                                "query": f"[AUX] {aux_name}",
                                "rows": total_rows,
                                "columns": len(aux_columns),
                                "file": str(aux_parquet_path),
                                "status": "success",
                            })
                        except Exception as e:
                            results.append({
                                "query": f"[AUX] {aux_name}",
                                "status": "error",
                                "message": str(e),
                            })
        return {"success": True, "results": results, "output_dir": str(output_path)}

    except ImportError:
        raise HTTPException(status_code=500, detail="Driver Oracle (oracledb) não instalado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        return {"success": True, "message": "Credenciais salvas com sucesso no Cofre do Windows"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar credenciais: {str(e)}")


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
        raise HTTPException(status_code=500, detail=f"Erro ao remover credenciais: {str(e)}")
