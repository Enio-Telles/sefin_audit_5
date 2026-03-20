"""
Módulo: fatores_conversao.py
Objetivo: Calcular a relação entre diferentes unidades de medida do mesmo produto.
"""
import sys
from pathlib import Path
import unicodedata
import polars as pl
import logging
from rich import print as rprint

logging.basicConfig(level=logging.INFO, format="%(message)s")

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

def _determinar_unid_ref(df_unidades: pl.DataFrame, desc_norm: str) -> str | None:
    df_filtrado = df_unidades.filter(pl.col("descricao_normalizada") == desc_norm)
    if df_filtrado.is_empty():
        return None
    # Conta o número total de movimentos para cada unidade daquele produto
    contagens = df_filtrado.group_by("unid").agg(pl.count()).sort("count", descending=True)
    if not contagens.is_empty():
        return contagens.get_column("unid")[0]
    return None

def precos_medios_produtos_final(df_unidades: pl.DataFrame) -> pl.DataFrame:
    df_compras = (
        df_unidades.filter(pl.col("compras") > 0)
        .group_by(["descricao_normalizada", "unid"])
        .agg(
            pl.col("compras").sum().alias("total_compras"),
            pl.count().alias("qtd_compras")
        )
        .with_columns((pl.col("total_compras") / pl.col("qtd_compras")).alias("preco_medio_compra"))
    )

    df_vendas = (
        df_unidades.filter(pl.col("vendas") > 0)
        .group_by(["descricao_normalizada", "unid"])
        .agg(
            pl.col("vendas").sum().alias("total_vendas"),
            pl.count().alias("qtd_vendas")
        )
        .with_columns((pl.col("total_vendas") / pl.col("qtd_vendas")).alias("preco_medio_venda"))
    )

    # Lista de todas as combinacoes desc_norm + unid para outer join
    df_chaves = df_unidades.select(["descricao_normalizada", "unid"]).unique()

    df_agrupado = (
        df_chaves
        .join(df_compras, on=["descricao_normalizada", "unid"], how="left")
        .join(df_vendas, on=["descricao_normalizada", "unid"], how="left")
        .with_columns([
            pl.col("preco_medio_compra").fill_null(0.0),
            pl.col("preco_medio_venda").fill_null(0.0)
        ])
    )

    return df_agrupado

def gerar_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> pl.DataFrame | None:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"

    arq_unidades = pasta_produtos / f"produtos_unidades_{cnpj}.parquet"
    if not arq_unidades.exists():
        rprint(f"[red]Erro: Tabela de unidades não encontrada em {arq_unidades}.[/red]")
        return None

    df_unidades = pl.read_parquet(arq_unidades)

    # Normaliza descrições para garantir cruzamento e determinação correta
    df_unidades = df_unidades.with_columns([
        pl.col("descricao").map_elements(_normalizar_texto, return_dtype=pl.String).alias("descricao_normalizada")
    ])

    arq_produtos = pasta_produtos / f"produtos_{cnpj}.parquet"
    if not arq_produtos.exists():
        rprint(f"[red]Erro: Tabela de produtos {arq_produtos} não encontrada.[/red]")
        return None

    df_produtos = pl.read_parquet(arq_produtos)
    df_precos = precos_medios_produtos_final(df_unidades)

    df_joined = df_produtos.explode("lista_unid").rename({"lista_unid": "unid"}).join(
        df_precos, on=["descricao_normalizada", "unid"], how="inner"
    )

    fatores = []

    for ch_prod, group_df in df_joined.group_by("chave_produto"):
        if len(group_df) <= 1:
            continue

        desc_norm = group_df.get_column("descricao_normalizada")[0]
        unid_ref = _determinar_unid_ref(df_unidades, desc_norm)

        if not unid_ref:
            continue

        df_ref = group_df.filter(pl.col("unid") == unid_ref)
        if df_ref.is_empty():
            continue

        preco_ref_compra = df_ref.get_column("preco_medio_compra")[0]
        preco_ref_venda = df_ref.get_column("preco_medio_venda")[0]

        for row in group_df.iter_rows(named=True):
            fator = 1.0
            unid_atual = row["unid"]

            if unid_atual == unid_ref:
                fator = 1.0
            else:
                pm_compra = row["preco_medio_compra"]
                pm_venda = row["preco_medio_venda"]

                if pm_compra > 0 and preco_ref_compra > 0:
                    fator = pm_compra / preco_ref_compra
                elif pm_venda > 0 and preco_ref_venda > 0:
                    fator = pm_venda / preco_ref_venda
                else:
                    logging.warning(f"Aviso: Produto {ch_prod} sem preço médio de compra/venda para calcular fator (unid {unid_atual}).")
                    continue

            fatores.append({
                "id_produtos": ch_prod,
                "descr_padrao": row["descricao"],
                "unid": unid_atual,
                "unid_ref": unid_ref,
                "fator": fator
            })

    if not fatores:
        rprint("[yellow]Nenhum produto precisava de cálculo de fatores de conversão.[/yellow]")
        return None

    df_fatores = pl.DataFrame(fatores)
    nome_arquivo = f"fatores_conversao_{cnpj}.parquet"
    salvar_para_parquet(df_fatores, pasta_produtos, nome_arquivo)

    rprint(f"[green]Fatores de conversão gerados com {len(df_fatores)} registros.[/green]")
    return df_fatores

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_fatores_conversao(sys.argv[1])
    else:
        print("Uso: python fatores_conversao.py <cnpj>")
