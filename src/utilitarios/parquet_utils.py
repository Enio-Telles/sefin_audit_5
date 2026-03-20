import polars as pl
from pathlib import Path

def salvar_para_parquet(df: pl.DataFrame | pl.LazyFrame, caminho_arquivo: Path) -> bool:
    """
    Exporta um DataFrame ou LazyFrame para Parquet.
    """
    try:
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
            
        caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(caminho_arquivo, compression="snappy")
        print(f"   Parquet salvo em: {caminho_arquivo}")
        return True
    except Exception as e:
        print(f"   Erro ao salvar Parquet {caminho_arquivo}: {e}")
        return False
