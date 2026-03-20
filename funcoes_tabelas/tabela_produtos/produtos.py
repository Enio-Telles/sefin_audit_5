"""
Módulo: produtos.py
Objetivo: Gerar a tabela de produtos normalizados e únicos.
"""
import sys
from pathlib import Path
import unicodedata
import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes") if Path(r"c:\funcoes").exists() else Path(__file__).parent.parent.parent.parent
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
except ImportError:
    def salvar_para_parquet(df, pasta, nome):
        pasta.mkdir(parents=True, exist_ok=True)
        df.write_parquet(pasta / nome)
        return True

def _normalizar_texto(v: str | None) -> str | None:
    if not v: return None
    v = unicodedata.normalize("NFD", str(v))
    v = "".join(c for c in v if unicodedata.category(c) != "Mn")
    return " ".join(v.upper().strip().split())

def gerar_tabela_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> pl.DataFrame | None:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_unidades = pasta_produtos / f"produtos_unidades_{cnpj}.parquet"

    if not arq_unidades.exists():
        rprint(f"[red]Erro: Tabela base {arq_unidades} não encontrada.[/red]")
        return None

    df = pl.read_parquet(arq_unidades)

    if df.is_empty():
        return None

    df = df.with_columns([
        pl.col("descricao").map_elements(_normalizar_texto, return_dtype=pl.String).alias("descricao_normalizada")
    ])

    df_agrupado = (
        df.group_by("descricao_normalizada")
        .agg(
            pl.col("descricao").drop_nulls().first().alias("descricao"),
            pl.col("descr_compl").drop_nulls().unique().sort().alias("lista_desc_compl"),
            pl.col("codigo").drop_nulls().unique().sort().alias("lista_codigos"),
            pl.col("tipo_item").drop_nulls().unique().sort().alias("lista_tipo_item"),
            pl.col("ncm").drop_nulls().unique().sort().alias("lista_ncm"),
            pl.col("cest").drop_nulls().unique().sort().alias("lista_cest"),
            pl.col("gtin").drop_nulls().unique().sort().alias("lista_gtin"),
            pl.col("unid").drop_nulls().unique().sort().alias("lista_unid")
        )
        .sort("descricao_normalizada")
        .with_columns([
            (pl.lit("id_produto_") + pl.int_range(1, pl.len() + 1).cast(pl.String)).alias("chave_produto")
        ])
    )

    cols_finais = [
        "chave_produto", "descricao_normalizada", "descricao", "lista_desc_compl",
        "lista_codigos", "lista_tipo_item", "lista_ncm", "lista_cest", "lista_gtin", "lista_unid"
    ]
    df_agrupado = df_agrupado.select(cols_finais)

    nome_arquivo = f"produtos_{cnpj}.parquet"
    salvar_para_parquet(df_agrupado, pasta_produtos, nome_arquivo)

    rprint(f"[green]produtos gerado com {len(df_agrupado)} registros.[/green]")
    return df_agrupado

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_tabela_produtos(sys.argv[1])
    else:
        print("Uso: python produtos.py <cnpj>")
