from pathlib import Path
import polars as pl
from rich import print as rprint

def ler_nfe_nfce(path: Path | None, cnpj: str, fonte: str, cfop_df: pl.DataFrame | None = None, print_status: bool = False) -> pl.DataFrame | None:
    """Lê NFe ou NFCe, filtra pelo CNPJ emitente e mapeia colunas."""
    if path is None or not path.exists():
        print(f"  [!] {fonte} nao encontrado.")
        return None

    colunas_necesssarias = ["co_emitente", "prod_cprod", "prod_xprod",
                            "prod_ncm", "prod_ucom", "co_cfop",
                            "prod_vprod", "prod_vfrete", "prod_vseg", "prod_voutro", "prod_vdesc",
                            "prod_qcom", "ide_dh_emi"]

    schema = pl.read_parquet(path, n_rows=0).schema

    # Identifica coluna de tipo de operação (0=Entrada, 1=Saída)
    col_tp = next((c for c in ["tipo_operacao", "co_tp_nf", "tp_nf"] if c in schema), None)
    if col_tp:
        colunas_necesssarias.append(col_tp)

    opcionales = {"prod_cest": "cest_raw", "prod_ceantrib": "ceantrib_raw", "prod_cean": "cean_raw"}
    presentes  = {k: v for k, v in opcionales.items() if k in schema}

    selecionar = [c for c in colunas_necesssarias if c in schema] + list(presentes.keys())

    lf = pl.scan_parquet(path).filter(pl.col("co_emitente") == cnpj)

    if col_tp:
        lf = lf.filter(pl.col(col_tp).cast(pl.String) == "1") # Saída

    # Filtro de CFOP Mercantile 'X' se fornecido
    if cfop_df is not None and "co_cfop" in schema:
        # Garante tipos iguais para o join
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(cfop_df.lazy(), on="co_cfop", how="inner")

    df = lf.select(selecionar).collect()

    if df.is_empty():
        return None

    # Cálculo do valor final do item (Saída)
    def _val(col):
        return pl.col(col).fill_null(0).cast(pl.Float64)

    df = df.with_columns([
        (_val("prod_vprod") + _val("prod_vfrete") + _val("prod_vseg") + _val("prod_voutro") - _val("prod_vdesc"))
        .alias("valor_saida"),
        _val("prod_qcom").alias("quantidade_saida"),
        pl.lit(0.0).alias("quantidade_entrada"),
        pl.col("ide_dh_emi").str.slice(0, 4).alias("ano")
    ])

    # GTIN
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
        df = df.with_columns(pl.lit(None, pl.String).alias("gtin"))

    mapping = {
        "prod_cprod": "codigo",
        "prod_xprod": "descricao",
        "prod_ncm":   "ncm",
        "prod_ucom":  "unidade",
    }
    df = df.rename({k: v for k, v in mapping.items() if k in df.columns})

    if "prod_cest" in df.columns:
        df = df.rename({"prod_cest": "cest"})

    # Campos inexistentes nesta fonte
    df = df.with_columns([
        pl.lit(None, pl.String).alias("descr_compl"),
        pl.lit(None, pl.String).alias("tipo_item"),
        pl.lit(0.0).alias("valor_entrada")
    ])

    if print_status:
        print(f"  {fonte}: {len(df):,} linhas (emitente, saidas X)")

    return df


def ler_c170(path: Path | None, cfop_df: pl.DataFrame | None = None, ano_padrao: str = "", print_status: bool = False) -> pl.DataFrame | None:
    """Lê c170_simplificada (ou c170) e mapeia colunas."""
    if path is None or not path.exists():
        print("  [!] C170 nao encontrado.")
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

    # Mapeamento flexível
    col_map = {
        "cod_item": "codigo",
        "descr_item": "descricao",
        "descr_compl": "descr_compl",
        "tipo_item": "tipo_item",
        "cod_ncm": "ncm",
        "cest": "cest",
        "cod_barra": "gtin",
        "unid": "unidade",
        "valor_item": "valor_entrada",
        "co_cfop": "co_cfop",
        "ind_oper": "ind_oper",
        "qtd": "quantidade_entrada"
    }

    selecionar = [c for c in col_map.keys() if c in schema]

    lf = pl.scan_parquet(path)
    if "ind_oper" in schema:
        lf = lf.filter(pl.col("ind_oper") == "0") # Entrada

    if cfop_df is not None and "co_cfop" in schema:
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(cfop_df.lazy(), on="co_cfop", how="inner")

    df = lf.select(selecionar).collect().rename({c: col_map[c] for c in selecionar})

    if df.is_empty():
        return None

    def _val(col):
        return pl.col(col).fill_null(0).cast(pl.Float64) if col in df.columns else pl.lit(0.0)

    df = df.with_columns([
        _val("valor_entrada").alias("valor_entrada"),
        _val("quantidade_entrada").alias("quantidade_entrada"),
        pl.lit(0.0).alias("valor_saida"),
        pl.lit(0.0).alias("quantidade_saida"),
        pl.lit(ano_padrao).alias("ano")
    ])

    if print_status:
        print(f"  C170: {len(df):,} linhas (entradas X)")

    return df
