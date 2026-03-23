from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Optional, Any, Union
import polars as pl
import duckdb
import importlib.util

# Localiza o diretório raiz e o config
_current_dir = Path(__file__).resolve().parent
_root_dir = _current_dir.parent.parent
_config_path = _root_dir / "config.py"

# Importação dinâmica do config para evitar problemas de path circular
_spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
_sefin_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sefin_config)

obter_diretorios_cnpj = _sefin_config.obter_diretorios_cnpj
DIR_CNPJS = _sefin_config.DIR_CNPJS

def carregar_tabelas_multi_cnpj(
    tabela_nome: str, 
    cnpjs: Optional[List[str]] = None,
    pasta: str = "analises"
) -> pl.LazyFrame:
    """
    Carrega a mesma tabela de múltiplos CNPJs e as concatena em um único LazyFrame.
    
    Args:
        tabela_nome: Nome base do arquivo parquet (ex: 'dim_produto' ou 'reg_0200').
        cnpjs: Lista de CNPJs (apenas números). Se None, tenta carregar de todos os diretórios em DIR_CNPJS.
        pasta: Subpasta dentro do CNPJ onde procurar ('analises' ou 'arquivos_parquet').
        
    Returns:
        Um pl.LazyFrame contendo os dados de todos os CNPJs com a coluna 'cnpj_origem'.
    """
    if cnpjs is None:
        # Lista todos os subdiretórios que parecem CNPJs (apenas números)
        cnpjs = [d.name for d in DIR_CNPJS.iterdir() if d.is_dir() and d.name.isdigit()]

    frames = []
    for cnpj in cnpjs:
        dir_parquet, dir_analises, _ = obter_diretorios_cnpj(cnpj)
        
        diretorio_busca = dir_analises if pasta == "analises" else dir_parquet
        
        # Procura por arquivos que começam com o nome da tabela
        # O sistema salva como: {tabela_nome}_{cnpj}.parquet
        padrão = f"{tabela_nome}_{cnpj}.parquet"
        caminho_arquivo = diretorio_busca / padrão
        
        # Fallback se não tiver o CNPJ no nome (algumas tabelas base podem não ter)
        if not caminho_arquivo.exists():
            caminho_arquivo = diretorio_busca / f"{tabela_nome}.parquet"
            
        if caminho_arquivo.exists():
            lf = pl.scan_parquet(caminho_arquivo).with_columns(
                pl.lit(cnpj).alias("cnpj_origem")
            )
            frames.append(lf)
            
    if not frames:
        raise FileNotFoundError(f"Nenhum arquivo '{tabela_nome}' encontrado para os CNPJs fornecidos em '{pasta}'.")
        
    return pl.concat(frames, how="diagonal_relaxed")

def criar_contexto_sql(tabelas: Dict[str, Union[pl.DataFrame, pl.LazyFrame, Path]]) -> duckdb.DuckDBPyConnection:
    """
    Cria uma conexão DuckDB e registra as tabelas Polars para consulta SQL.
    
    Args:
        tabelas: Dicionário mapeando { "nome_tabela_sql": objeto_polars_ou_path }
        
    Example:
        >>> lf_produtos = carregar_tabelas_multi_cnpj("dim_produto")
        >>> con = criar_contexto_sql({"produtos": lf_produtos})
        >>> res = con.execute("SELECT cnpj_origem, count(*) FROM produtos GROUP BY 1").pl()
    """
    con = duckdb.connect(database=":memory:")
    
    for nome, obj in tabelas.items():
        # Validação básica de identificador SQL para evitar injeção no 'nome'
        if not all(c.isalnum() or c == '_' for c in nome):
            raise ValueError(f"Nome de tabela inválido: {nome}. Use apenas letras, números e underscores.")

        if isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            # DuckDB consegue ler DataFrames e LazyFrames do Polars diretamente
            con.register(nome, obj)
        elif isinstance(obj, (str, Path)):
            # Se for um path, usa Polars para escanear com segurança e registra no DuckDB
            # Isso evita injeção de SQL no path via f-strings
            con.register(nome, pl.scan_parquet(str(obj)))
            
    return con
