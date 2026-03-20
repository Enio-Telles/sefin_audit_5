"""
Módulo: produtos_unidades.py
Objetivo: Gerar a tabela base de movimentações por unidade.
"""
import sys
from pathlib import Path
import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes") if Path(r"c:\funcoes").exists() else Path(__file__).parent.parent.parent.parent
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from encontrar_arquivo_cnpj import encontrar_arquivo
except ImportError:
    def salvar_para_parquet(df, pasta, nome):
        pasta.mkdir(parents=True, exist_ok=True)
        df.write_parquet(pasta / nome)
        return True
    def encontrar_arquivo(diretorio, prefixo, cnpj):
        for f in diretorio.glob(f"{prefixo}*{cnpj}*.parquet"):
            return f
        return None

def ler_cfop_mercantil() -> pl.DataFrame | None:
    cfop_path = FUNCOES_DIR / "referencias" / "cfop" / "cfop_bi.parquet"
    if not cfop_path.exists():
        cfop_path = Path("referencias/cfop/cfop_bi.parquet")
    if not cfop_path.exists():
        return None

    return (
        pl.scan_parquet(cfop_path)
        .filter(pl.col("operacao_mercantil") == "X")
        .select(pl.col("co_cfop").cast(pl.String))
        .collect()
    )

def processar_nfe_nfce(path: Path | None, cnpj: str, df_cfop: pl.DataFrame | None) -> pl.DataFrame | None:
    if not path or not path.exists():
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

    col_tp = next((c for c in ["tipo_operacao", "co_tp_nf", "tp_nf"] if c in schema), None)

    colunas_necessarias = ["co_emitente", "prod_cprod", "prod_xprod", "prod_ncm", "prod_ucom", "co_cfop"]
    col_valores = ["prod_vprod", "prod_vfrete", "prod_vseg", "prod_voutro", "prod_vdesc"]

    selecionar = [c for c in colunas_necessarias + col_valores if c in schema]
    if col_tp: selecionar.append(col_tp)

    for opt in ["prod_cest", "prod_ceantrib", "prod_cean"]:
        if opt in schema: selecionar.append(opt)

    lf = pl.scan_parquet(path).filter(pl.col("co_emitente") == cnpj)

    if col_tp:
        lf = lf.filter(pl.col(col_tp).cast(pl.String) == "1") # '1 - saida'

    if df_cfop is not None and "co_cfop" in schema:
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(df_cfop.lazy(), on="co_cfop", how="inner")

    df = lf.select(selecionar).collect()

    if df.is_empty():
        return None

    def _val(col_name):
        if col_name in df.columns:
            return pl.col(col_name).fill_null(0).cast(pl.Float64)
        return pl.lit(0.0)

    # Cálculo de Preço de Venda: prod_vprod + prod_vfrete + prod_vseg + prod_voutro - prod_vdesc
    df = df.with_columns([
        (_val("prod_vprod") + _val("prod_vfrete") + _val("prod_vseg") + _val("prod_voutro") - _val("prod_vdesc")).alias("vendas"),
        pl.lit(0.0).alias("compras"),
        pl.lit(None, dtype=pl.String).alias("descr_compl"),
        pl.lit(None, dtype=pl.String).alias("tipo_item")
    ])

    if "prod_ceantrib" in df.columns and "prod_cean" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("prod_ceantrib").is_null() | (pl.col("prod_ceantrib") == ""))
              .then(pl.col("prod_cean"))
              .otherwise(pl.col("prod_ceantrib"))
              .alias("gtin")
        )
    elif "prod_ceantrib" in df.columns:
        df = df.rename({"prod_ceantrib": "gtin"})
    elif "prod_cean" in df.columns:
        df = df.rename({"prod_cean": "gtin"})
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("gtin"))

    mapping = {
        "prod_cprod": "codigo",
        "prod_xprod": "descricao",
        "prod_ncm": "ncm",
        "prod_ucom": "unid",
        "prod_cest": "cest"
    }

    renames = {k: v for k, v in mapping.items() if k in df.columns}
    df = df.rename(renames)

    for c in ["cest"]:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(c))

    cols_finais = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid", "compras", "vendas"]
    return df.select([c for c in cols_finais if c in df.columns])

def processar_c170(path: Path | None, df_cfop: pl.DataFrame | None) -> pl.DataFrame | None:
    if not path or not path.exists():
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

    # Lógica de Compras: Identificar no C170 quando ind_oper = 0, o cfop constar em referencias\cfop\cfop_bi.parquet com operacao_mercantil = 'X'.
    lf = pl.scan_parquet(path).filter(pl.col("ind_oper").cast(pl.String) == "0")

    if df_cfop is not None and "co_cfop" in schema:
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(df_cfop.lazy(), on="co_cfop", how="inner")

    df = lf.collect()

    if df.is_empty():
        return None

    col_map = {
        "cod_item": "codigo",
        "descr_item": "descricao",
        "descr_compl": "descr_compl",
        "tipo_item": "tipo_item",
        "cod_ncm": "ncm",
        "cest": "cest",
        "cod_barra": "gtin",
        "unid": "unid",
    }

    for old, new in col_map.items():
        if old in df.columns:
            df = df.rename({old: new})
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(new))

    # O preço é o valor_item
    val_col = "vl_item" if "vl_item" in df.columns else "valor_item"
    if val_col in df.columns:
        df = df.with_columns(pl.col(val_col).fill_null(0).cast(pl.Float64).alias("compras"))
    else:
        df = df.with_columns(pl.lit(0.0).alias("compras"))

    df = df.with_columns(pl.lit(0.0).alias("vendas"))

    cols_finais = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid", "compras", "vendas"]
    return df.select([c for c in cols_finais if c in df.columns])

def processar_bloco_h(path: Path | None) -> pl.DataFrame | None:
    if not path or not path.exists():
        return None

    df = pl.scan_parquet(path).collect()
    if df.is_empty():
        return None

    col_map = {
        "codigo_produto": "codigo",
        "descricao_produto": "descricao",
        "tipo_item": "tipo_item",
        "cod_ncm": "ncm",
        "cest": "cest",
        "cod_barra": "gtin",
        "unidade_medida": "unid"
    }

    for old, new in col_map.items():
        if old not in df.columns and old == "codigo_produto" and "codigo_produto_original" in df.columns:
            df = df.rename({"codigo_produto_original": new})
        elif old in df.columns:
            df = df.rename({old: new})
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(new))

    df = df.with_columns([
        pl.lit(None, dtype=pl.String).alias("descr_compl"),
        pl.lit(0.0).alias("compras"),
        pl.lit(0.0).alias("vendas")
    ])

    cols_finais = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid", "compras", "vendas"]
    return df.select([c for c in cols_finais if c in df.columns])

def gerar_produtos_unidades(cnpj: str, pasta_cnpj: Path | None = None) -> pl.DataFrame | None:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    df_cfop = ler_cfop_mercantil()

    dirs_validos = [d for d in (pasta_cnpj / "arquivos_parquet", pasta_cnpj) if d.exists()]

    def _resolver(prefixo: str) -> Path | None:
        for d in dirs_validos:
            arq = encontrar_arquivo(d, prefixo, cnpj)
            if arq: return arq
        return None

    fragmentos = []

    df_nfe = processar_nfe_nfce(_resolver("NFe"), cnpj, df_cfop)
    if df_nfe is not None: fragmentos.append(df_nfe)

    df_nfce = processar_nfe_nfce(_resolver("NFCe"), cnpj, df_cfop)
    if df_nfce is not None: fragmentos.append(df_nfce)

    df_c170 = processar_c170(_resolver("c170_simplificada") or _resolver("c170"), df_cfop)
    if df_c170 is not None: fragmentos.append(df_c170)

    df_h = processar_bloco_h(_resolver("bloco_h"))
    if df_h is not None: fragmentos.append(df_h)

    if not fragmentos:
        rprint("[red]Nenhuma fonte encontrada para gerar produtos_unidades.[/red]")
        return None

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")

    for col in ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]:
        if col in df_total.columns:
            df_total = df_total.with_columns(pl.col(col).cast(pl.String))
        else:
            df_total = df_total.with_columns(pl.lit(None, dtype=pl.String).alias(col))

    for col in ["compras", "vendas"]:
        if col in df_total.columns:
            df_total = df_total.with_columns(pl.col(col).fill_null(0).cast(pl.Float64))
        else:
            df_total = df_total.with_columns(pl.lit(0.0).alias(col))

    # Ensure standard order
    cols_finais = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid", "compras", "vendas"]
    df_total = df_total.select(cols_finais)

    pasta_saida = pasta_cnpj / "analises" / "produtos"

    nome_arquivo = f"produtos_unidades_{cnpj}.parquet"
    salvar_para_parquet(df_total, pasta_saida, nome_arquivo)

    rprint(f"[green]produtos_unidades gerado com {len(df_total)} registros.[/green]")
    return df_total

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos_unidades(sys.argv[1])
    else:
        print("Uso: python produtos_unidades.py <cnpj>")
