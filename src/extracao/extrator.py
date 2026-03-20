import re
import concurrent.futures
import polars as pl
from pathlib import Path
from rich import print as rprint

from .conexao import conectar
from .leitor_sql import ler_sql
from ..utilitarios.validacao import validar_cnpj
from ..utilitarios.parquet_utils import salvar_para_parquet

def processar_arquivo_sql(arq_sql: Path, cnpj_limpo: str, data_limite: str, consultas_dir: Path, pasta_saida_base: Path):
    """
    Executa uma consulta SQL no Oracle e salva o resultado em Parquet.
    Cada thread abre e fecha sua própria conexão para garantir thread-safety.
    """
    try:
        # Abertura de conexão local dentro da thread (conforme Tarefa 2)
        with conectar() as conexao:
            with conexao.cursor() as cursor:
                cursor.arraysize = 5000 
                
                sql_txt = ler_sql(arq_sql)
                if not sql_txt:
                    return True

                cursor.prepare(sql_txt)
                nomes_binds = [b.upper() for b in cursor.bindnames()]

                binds = {}
                if "CNPJ" in nomes_binds:
                    binds["CNPJ"] = cnpj_limpo
                else:
                    rprint(f"[yellow]⚠️ {arq_sql.name} ignorado (sem bind :CNPJ)[/yellow]")
                    return True

                if "DATA_LIMITE_PROCESSAMENTO" in nomes_binds:
                    binds["DATA_LIMITE_PROCESSAMENTO"] = data_limite

                cursor.execute(None, binds)
                colunas = [col[0] for col in cursor.description]
                dados = cursor.fetchall()

                if not dados:
                    rprint(f"[yellow]  Zero linhas para {arq_sql.name}[/yellow]")
                    return True

                df = pl.DataFrame(dados, schema=colunas, orient="row")
                
                # Caminhos Unificados (conforme Tarefa 1)
                # arq_sql pode estar em subpastas dentro de consultas_dir
                caminho_relativo = arq_sql.relative_to(consultas_dir)
                # Salva em pasta_sa_base/tabelas_brutas/subpastas
                arquivo_saida = pasta_saida_base / caminho_relativo.parent / f"{arq_sql.stem}_{cnpj_limpo}.parquet"
                
                # Garantia de criação de diretórios
                arquivo_saida.parent.mkdir(parents=True, exist_ok=True)

                return salvar_para_parquet(df, arquivo_saida)

    except Exception as e:
        rprint(f"[red]  ❌ Erro em {arq_sql.name}: {e}[/red]")
        return False

def extrair_por_cnpj(cnpj: str, data_limite: str = None, pasta_consultas: Path = None, pasta_base_saida: Path = None):
    """
    Orquestra a extração de múltiplos arquivos SQL para um CNPJ usando ThreadPoolExecutor.
    """
    if not validar_cnpj(cnpj):
        rprint(f"[red]CNPJ {cnpj} inválido[/red]")
        return False

    cnpj_limpo = re.sub(r'[^0-9]', '', cnpj)
    
    # Destino definitivo: no diretório do CNPJ dentro de 'tabelas_brutas' (Tarefa 1)
    pasta_saida_cnpj = pasta_base_saida / cnpj_limpo / "tabelas_brutas"
    pasta_saida_cnpj.mkdir(parents=True, exist_ok=True)
    
    arquivos_sql = list(pasta_consultas.rglob("*.sql"))
    if not arquivos_sql:
        rprint("[yellow]Nenhuma consulta encontrada[/yellow]")
        return False

    rprint(f"[bold cyan]Extraindo {len(arquivos_sql)} consultas para {cnpj_limpo}...[/bold cyan]")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futuros = {
            executor.submit(processar_arquivo_sql, arq, cnpj_limpo, data_limite, pasta_consultas, pasta_saida_cnpj): arq 
            for arq in arquivos_sql
        }
        
        for futuro in concurrent.futures.as_completed(futuros):
            futuro.result()

    return True
