import polars as pl
from rich import print as rprint
from src.utilitarios.cache_decorator import cached_transform


CAMPOS_CHAVE = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]

MAP_NFE = {
    "prod_cprod": "codigo", 
    "prod_xprod": "descricao", 
    "prod_ncm": "ncm", 
    "prod_cest": "cest", 
    "prod_ceantrib": "gtin", 
    "prod_ucom": "unidade",
    "prod_vprod": "valor_total", 
    "prod_qcom": "quantidade"
}

MAP_C170 = {
    "cod_item": "codigo", 
    "descr_item": "descricao", 
    "cod_ncm": "ncm",
    "cod_barra": "gtin", 
    "unid": "unidade", 
    "vl_item": "valor_total", 
    "qtd": "quantidade"
}

MAP_BLOCO_H = {
    "codigo_produto": "codigo", 
    "descricao_produto": "descricao", 
    "cod_ncm": "ncm", 
    "cod_barra": "gtin", 
    "unidade_medida": "unidade", 
    "valor_item": "valor_total", 
    "quantidade": "quantidade"
}

@cached_transform(cache_dir="cache/itens")
def processar_tabela_itens(cnpj: str, df_c170: pl.LazyFrame = None, df_nfe_itens: pl.LazyFrame = None, df_bloco_h: pl.LazyFrame = None) -> pl.LazyFrame:
    """
    Consolida a movimentação linha a linha, agrupando CFOP, CST, Quantidades e Valores.
    """
    rprint(f"[cyan]Processando tabela de itens consolidada para CNPJ: {cnpj}[/cyan]")
    
    fragmentos = []
    if df_c170 is not None:
        df_c170 = _normalizar_schema(df_c170, MAP_C170)
        fragmentos.append(df_c170.with_columns(pl.lit("C170").alias("fonte")))
    
    if df_nfe_itens is not None:
        df_nfe_itens = _normalizar_schema(df_nfe_itens, MAP_NFE)
        fragmentos.append(df_nfe_itens.with_columns(pl.lit("NFe").alias("fonte")))

    if df_bloco_h is not None:
        df_bloco_h = _normalizar_schema(df_bloco_h, MAP_BLOCO_H)
        fragmentos.append(df_bloco_h.with_columns(pl.lit("Bloco H").alias("fonte")))
        
    if not fragmentos:
        # Se não houver dados, retorna DF vazio com as colunas corretas
        return pl.LazyFrame({c: [] for c in CAMPOS_CHAVE + ["unidade", "valor_total", "quantidade", "fonte", "chave_item_id"]})

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    
    # Normalização e Geração de Chave Única
    df_total = _gerar_chave_item(df_total)
    
    # Agregação por chave de item individualizado
    df_resultado = (
        df_total
        .group_by("chave_item_individualizado")
        .agg([
            *[pl.col(c).drop_nulls().first().alias(c) for c in CAMPOS_CHAVE],
            pl.col("valor_total").cast(pl.Float64).fill_null(0.0).sum().alias("valor_total"),
            pl.col("quantidade").cast(pl.Float64).fill_null(0.0).sum().alias("quantidade"),
            pl.col("unidade").drop_nulls().unique().sort().alias("lista_unidades"),
            pl.col("fonte").unique().sort().alias("fontes")
        ])
        .sort(["descricao", "codigo"])
        .with_columns(
            (pl.lit("item_") + pl.int_range(1, pl.len() + 1).cast(pl.String)).alias("chave_item_id")
        )
    )
    
    return df_resultado

def _normalizar_schema(df: pl.LazyFrame, mapeamento: dict[str, str]) -> pl.LazyFrame:
    """Prepara o DF da fonte para o padrão interno."""
    # Renomeia se a coluna existir
    disponiveis = {old: new for old, new in mapeamento.items() if old in df.columns}
    df = df.rename(disponiveis)
    
    # Garante que colunas críticas existam
    for col in CAMPOS_CHAVE + ["unidade", "valor_total", "quantidade"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
            
    return df

def _gerar_chave_item(df: pl.LazyFrame) -> pl.LazyFrame:
    """Gera hash para identificação única do item baseado em suas características."""
    exprs_norm = [
        pl.col(c).cast(pl.String).fill_null("").str.strip_chars().str.to_uppercase().alias(f"_key_{c}")
        for c in CAMPOS_CHAVE
    ]
    
    return (
        df.with_columns(exprs_norm)
        .with_columns(
            pl.concat_str([f"_key_{c}" for c in CAMPOS_CHAVE], separator="|")
            .hash(seed=42)
            .cast(pl.String)
            .str.encode("hex")
            .alias("chave_item_individualizado")
        )
        .drop([f"_key_{c}" for c in CAMPOS_CHAVE])
    )
