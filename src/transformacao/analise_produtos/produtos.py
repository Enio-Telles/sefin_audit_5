import polars as pl
from rich import print as rprint
from src.utilitarios.cache_decorator import cached_transform

@cached_transform(cache_dir="cache/produtos")
def processar_tabela_produtos(df_itens: pl.LazyFrame) -> pl.LazyFrame:
    """
    Agrupa itens individuais em produtos baseados na descrição normalizada.
    """
    rprint("[cyan]Agrupando itens em produtos consolidados...[/cyan]")
    
    # Normalização de descrição para agrupamento
    df_itens = df_itens.with_columns(
        pl.col("descricao")
        .str.to_uppercase()
        .str.strip_chars()
        .str.replace_all(r"[ÁÀÂÃ]", "A")
        .str.replace_all(r"[ÉÈÊ]", "E")
        .str.replace_all(r"[ÍÌÎ]", "I")
        .str.replace_all(r"[ÓÒÔÕ]", "O")
        .str.replace_all(r"[ÚÙÛ]", "U")
        .str.replace_all(r"[Ç]", "C")
        .alias("descricao_normalizada")
    )
    
    df_produtos = (
        df_itens.group_by("descricao_normalizada")
        .agg([
            pl.col("descricao").first().alias("descricao"),
            pl.col("chave_item_id").unique().sort().alias("lista_itens_id"),
            pl.col("codigo").unique().sort().alias("lista_codigos"),
            pl.col("ncm").unique().sort().alias("lista_ncm"),
            pl.col("cest").unique().sort().alias("lista_cest"),
            pl.col("gtin").unique().sort().alias("lista_gtin"),
            pl.col("lista_unidades").flatten().unique().sort().alias("todas_unidades")
        ])
        .sort("descricao_normalizada")
        .with_columns([
            (pl.lit("prod_") + pl.int_range(1, pl.len() + 1).cast(pl.String)).alias("chave_produto"),
            pl.lit(False).alias("verificado"),
            pl.lit(None).cast(pl.String).alias("chave_id") # Para compatibilidade com modelos de agregação
        ])
    )
    
    return df_produtos
