import argparse
import polars as pl
from pathlib import Path
from rich import print as rprint
import sys
pl.enable_string_cache()

# Adiciona a raiz do projeto (c:/funcoes) ao sys.path para permitir imports absolutos
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Importar constantes do config
from src.config import SQL_DIR, CNPJ_ROOT as DIR_DADOS, DIR_REFERENCIAS

# Importar funções de transformação
from src.extracao.extrator import extrair_por_cnpj
from src.transformacao.analise_produtos.documentos import processar_tabela_documentos
from src.transformacao.analise_produtos.itens import processar_tabela_itens
from src.transformacao.analise_produtos.produtos import processar_tabela_produtos
from src.transformacao.analise_produtos.enriquecimento import enriquecer_itens_com_referencias
from src.utilitarios.parquet_utils import salvar_para_parquet

def executar_extrair(cnpj: str, data_limite: str, caminho_sql: Path, caminho_dados_raiz: Path) -> bool:
    """Etapa 1: Extração de Dados Brutos."""
    rprint(f"[cyan]Extraindo dados do Oracle em {caminho_dados_raiz / cnpj / 'tabelas_brutas'}... [/cyan]")
    return extrair_por_cnpj(
        cnpj=cnpj, 
        data_limite=data_limite, 
        pasta_consultas=caminho_sql, 
        pasta_base_saida=caminho_dados_raiz
    )

def executar_processar(cnpj: str, caminho_dados_raiz: Path) -> bool:
    """Etapa 2: Transformação e Consolidação."""
    rprint("[yellow]Iniciando processamento e consolidação de tabelas...[/yellow]")
    pasta_tabelas = caminho_dados_raiz / cnpj / "tabelas_brutas"
    pasta_analises = caminho_dados_raiz / cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)
    
    def _ler_tab(nome):
        p = pasta_tabelas / f"{nome}_{cnpj}.parquet"
        if not p.exists(): 
            return None
        return pl.scan_parquet(p)

    df_c100 = _ler_tab("c100")
    df_c170 = _ler_tab("c170")
    df_nfe = _ler_tab("nfe")
    df_bloco_h = _ler_tab("bloco_h")
    
    if df_c170 is None and df_nfe is None:
        rprint("[red]❌ Dados brutos (C170/NFe) não encontrados em " + str(pasta_tabelas) + ". Execute a extração primeiro.[/red]")
        return False

    rprint("[cyan]Consolidando movimentação de itens...[/cyan]")
    df_itens_base = processar_tabela_itens(cnpj, df_c170=df_c170, df_nfe_itens=df_nfe, df_bloco_h=df_bloco_h)
    
    rprint("[cyan]Executando enriquecimento (Paramétricas + Cruzamentos)...[/cyan]")
    p_sefin = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_produto_sefin.parquet"
    p_fatores = DIR_REFERENCIAS / "fatores_conversao_unidades.parquet"
    
    df_sefin = pl.scan_parquet(p_sefin) if p_sefin.exists() else pl.LazyFrame()
    df_fatores = pl.scan_parquet(p_fatores) if p_fatores.exists() else pl.LazyFrame()
    
    if df_sefin is None or not df_sefin.columns:
        rprint("[yellow]⚠️  Tabela SEFIN não encontrada. Enriquecimento limitado.[/yellow]")

    df_itens_enriquecido = enriquecer_itens_com_referencias(df_itens_base, df_sefin, df_fatores)
    
    rprint("[cyan]Agrupando produtos consolidados...[/cyan]")
    df_produtos = processar_tabela_produtos(df_itens_enriquecido)
    
    df_docs = processar_tabela_documentos(df_nfe) if df_nfe is not None else None

    rprint("[yellow]Salvando resultados finais no analises/produtos...[/yellow]")
    salvar_para_parquet(df_itens_enriquecido, pasta_analises / f"tabela_itens_caracteristicas_{cnpj}.parquet")
    salvar_para_parquet(df_produtos, pasta_analises / f"tabela_descricoes_{cnpj}.parquet")
    if df_docs is not None:
         salvar_para_parquet(df_docs, pasta_analises / f"tabela_documentos_{cnpj}.parquet")
    
    return True

def executar_pipeline_completo(cnpj: str, data_limite: str = None, pasta_sql_path: Path = None, pasta_dados_path: Path = None, apenas_extrair: bool = False, apenas_processar: bool = False):
    """
    Orquestrador que gerencia a execução parcial ou total do pipeline.
    """
    rprint(f"[bold green]🚀 Iniciando pipeline para CNPJ: {cnpj}[/bold green]")
    
    caminho_sql = pasta_sql_path or SQL_DIR
    caminho_dados_raiz = pasta_dados_path or DIR_DADOS
    
    executar_tudo = not (apenas_extrair or apenas_processar)

    if apenas_extrair or executar_tudo:
        sucesso = executar_extrair(cnpj, data_limite, caminho_sql, caminho_dados_raiz)
        if not sucesso:
            rprint("[red]❌ Falha na etapa de extração.[/red]")
            return False

    if apenas_processar or executar_tudo:
        sucesso = executar_processar(cnpj, caminho_dados_raiz)
        if not sucesso:
            rprint("[red]❌ Falha na etapa de processamento.[/red]")
            return False

    rprint(f"[bold green]✅ Pipeline concluído com sucesso para o CNPJ: {cnpj}[/bold green]")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador do Pipeline de Auditoria")
    parser.add_argument("--cnpj", required=True, help="CNPJ para processar (14 dígitos)")
    parser.add_argument("--data-limite", help="Data limite EFD (DD/MM/YYYY)")
    parser.add_argument("--sql-dir", help="Pasta com arquivos .sql")
    parser.add_argument("--saida", help="Pasta base para saída de dados")
    parser.add_argument("--apenas-extrair", action="store_true", help="Executa apenas a extração do banco")
    parser.add_argument("--apenas-processar", action="store_true", help="Executa apenas o processamento dos parquets locais")
    
    args = parser.parse_args()
    
    p_sql = Path(args.sql_dir) if args.sql_dir else None
    p_saida = Path(args.saida) if args.saida else None
    
    executar_pipeline_completo(
        cnpj=args.cnpj,
        data_limite=args.data_limite,
        pasta_sql_path=p_sql,
        pasta_dados_path=p_saida,
        apenas_extrair=args.apenas_extrair,
        apenas_processar=args.apenas_processar
    )
