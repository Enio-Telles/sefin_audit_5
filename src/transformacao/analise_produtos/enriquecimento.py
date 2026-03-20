import polars as pl
from rich import print as rprint

def enriquecer_itens_com_referencias(df_itens: pl.DataFrame, df_sefin: pl.DataFrame, df_fatores: pl.DataFrame) -> pl.DataFrame:
    """
    Faz o JOIN da tabela de itens com tabelas de referência para preencher co_sefin e aplicar fatores de conversão.
    """
    rprint("[cyan]Enriquecendo itens com referências SEFIN e Fatores de Conversão...[/cyan]")
    
    df_enriquecido = df_itens
    
    # Join com SEFIN (baseado em NCM)
    if not df_sefin.is_empty() and "ncm" in df_sefin.columns:
        df_enriquecido = df_enriquecido.join(df_sefin, on="ncm", how="left")
    else:
        rprint("[yellow]⚠️  Tabela SEFIN ignorada por estar vazia ou não possuir coluna 'ncm'.[/yellow]")
    
    # Aplicação de Fatores de Conversão
    if not df_fatores.is_empty() and "chave_item_id" in df_fatores.columns and "unidade" in df_fatores.columns:
        df_enriquecido = df_enriquecido.join(df_fatores, on=["chave_item_id", "unidade"], how="left")
    else:
        rprint("[yellow]⚠️  Tabela de Fatores ignorada por estar vazia ou faltar chaves.[/yellow]")
    
    return df_enriquecido
