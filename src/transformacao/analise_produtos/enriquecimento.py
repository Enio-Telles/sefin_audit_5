import polars as pl
from rich import print as rprint
from src.utilitarios.cache_decorator import cached_transform

@cached_transform(cache_dir="cache/enriquecimento")
def enriquecer_itens_com_referencias(df_itens: pl.LazyFrame, df_sefin: pl.LazyFrame, df_fatores: pl.LazyFrame) -> pl.LazyFrame:
    """
    Faz o JOIN da tabela de itens com tabelas de referência para preencher co_sefin e aplicar fatores de conversão.
    """
    rprint("[cyan]Enriquecendo itens com referências SEFIN e Fatores de Conversão...[/cyan]")
    
    df_enriquecido = df_itens
    
    # Join com SEFIN (baseado em NCM)
    if df_sefin is not None and "ncm" in df_sefin.columns:
        # Predicate pushdown manual: pre-filter SEFIN to only have valid NCMs (not null) to reduce join size
        df_sefin_filtered = df_sefin.filter(pl.col("ncm").is_not_null())
        pl.enable_string_cache()
        df_enriquecido = df_enriquecido.with_columns(pl.col("ncm").cast(pl.Categorical))
        df_sefin_filtered = df_sefin_filtered.with_columns(pl.col("ncm").cast(pl.Categorical))
        df_enriquecido = df_enriquecido.join(df_sefin_filtered, on="ncm", how="left")
    else:
        rprint("[yellow]⚠️  Tabela SEFIN ignorada por estar vazia ou não possuir coluna 'ncm'.[/yellow]")
    
    # Aplicação de Fatores de Conversão
    if df_fatores is not None and "chave_item_id" in df_fatores.columns and "unidade" in df_fatores.columns:
        pl.enable_string_cache()
        df_enriquecido = df_enriquecido.with_columns(pl.col("chave_item_id").cast(pl.Categorical))
        df_fatores = df_fatores.with_columns(pl.col("chave_item_id").cast(pl.Categorical))
        df_enriquecido = df_enriquecido.join(df_fatores, on=["chave_item_id", "unidade"], how="left")
    else:
        rprint("[yellow]⚠️  Tabela de Fatores ignorada por estar vazia ou faltar chaves.[/yellow]")
    
    return df_enriquecido
