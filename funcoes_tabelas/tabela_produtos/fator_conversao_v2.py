"""
fator_conversao_v2.py

Calcula fatores de conversão usando a tabela oficial de produtos consolidados
como base de referência. Também gera uma base intermediária de conversão e
uma fila de verificação para auditoria dos casos frágeis.
"""

import sys
from pathlib import Path
import polars as pl

from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
    from encontrar_arquivo_cnpj import encontrar_arquivo
    from aux_leitura_notas import ler_nfe_nfce, ler_c170
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


CAMPOS_CHAVE = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]


def _normalizar(df: pl.DataFrame) -> pl.DataFrame:
    for col in CAMPOS_CHAVE + ["unidade", "fonte"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(col))
    return df


def _aplicar_normalizacao(df: pl.DataFrame) -> pl.DataFrame:
    import unicodedata

    def _norm(v):
        if v is None:
            return None
        v = unicodedata.normalize("NFD", str(v))
        v = "".join(c for c in v if unicodedata.category(c) != "Mn")
        return v.upper().strip()

    def _norm_codigo(v):
        if v is None:
            return None
        v = _norm(v)
        return v.lstrip("0") or "0"

    def _remove_pontos(v):
        if v is None:
            return None
        return _norm(v).replace(".", "")

    exprs = []
    for col in ["descricao", "descr_compl", "tipo_item"]:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_norm, return_dtype=pl.String).alias(col))
    if "codigo" in df.columns:
        exprs.append(pl.col("codigo").map_elements(_norm_codigo, return_dtype=pl.String).alias("codigo"))
    for col in ["ncm", "cest", "gtin"]:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_remove_pontos, return_dtype=pl.String).alias(col))
    if exprs:
        df = df.with_columns(exprs)
    return df


def _gerar_chave(df: pl.DataFrame) -> pl.DataFrame:
    exprs_norm = [
        pl.when(pl.col(c).is_null())
          .then(pl.lit(""))
          .otherwise(pl.col(c).cast(pl.String).str.strip_chars().str.to_uppercase())
          .alias(f"_key_{c}")
        for c in CAMPOS_CHAVE
    ]
    df = df.with_columns(exprs_norm)
    key_cols = [f"_key_{c}" for c in CAMPOS_CHAVE]
    df = df.with_columns(
        pl.concat_str(key_cols, separator="|")
        .hash(seed=42)
        .cast(pl.String)
        .str.encode("hex")
        .alias("chave_item_individualizado")
    ).drop(key_cols)
    return df


def ler_fatores_manuais(arquivo_excel: Path) -> pl.DataFrame | None:
    if not arquivo_excel.exists():
        return None
    try:
        df_manual = pl.read_excel(arquivo_excel)
        mapping = {
            "codigo_produto_ajustado": "chave_produto",
            "unid": "unidade",
            "fator": "fator_conversao_manual",
        }
        cols_presentes = [c for c in mapping if c in df_manual.columns]
        if len(cols_presentes) < 3:
            return None
        return (
            df_manual.select(cols_presentes)
            .rename({c: mapping[c] for c in cols_presentes})
            .with_columns([
                pl.col("chave_produto").cast(pl.String),
                pl.col("unidade").cast(pl.String),
                pl.col("fator_conversao_manual").cast(pl.Float64),
            ])
            .drop_nulls(subset=["chave_produto", "unidade", "fator_conversao_manual"])
        )
    except Exception as e:
        rprint(f"[yellow]Aviso ao ler fatores manuais:[/yellow] {e}")
        return None


def _carregar_produtos_base(pasta_produtos: Path, cnpj: str) -> pl.DataFrame | None:
    candidatos = [
        pasta_produtos / f"tabela_produtos_consolidados_{cnpj}.parquet",
        pasta_produtos / f"tabela_produtos_editavel_{cnpj}.parquet",
        pasta_produtos / f"tabela_descricoes_v2_{cnpj}.parquet",
        pasta_produtos / f"tabela_descricoes_{cnpj}.parquet",
    ]
    for arq in candidatos:
        if arq.exists():
            return pl.read_parquet(arq)
    return None


def _carregar_transacoes(cnpj: str, pasta_cnpj: Path) -> pl.DataFrame:
    cfop_bi_path = Path(r"c:\funcoes\referencias\cfop\cfop_bi.parquet")
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.read_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )

    arq_dir = pasta_cnpj / "arquivos_parquet"
    dirs_validos = [d for d in (arq_dir, pasta_cnpj) if d.exists()]

    def resolver_local(prefixo):
        for d in dirs_validos:
            a = encontrar_arquivo(d, prefixo, cnpj)
            if a:
                return a
        return None

    ano_base = ""
    reg_0000_path = resolver_local("reg_0000")
    if reg_0000_path:
        try:
            df_0000 = pl.read_parquet(reg_0000_path, n_rows=1)
            if "dt_ini" in df_0000.columns:
                ano_base = str(df_0000["dt_ini"][0])[0:4]
        except Exception:
            pass

    fragmentos = []
    for _, df_src in [
        ("NFe", ler_nfe_nfce(resolver_local("NFe"), cnpj, "NFe", cfop_df)),
        ("NFCe", ler_nfe_nfce(resolver_local("NFCe"), cnpj, "NFCe", cfop_df)),
        ("C170", ler_c170(resolver_local("c170_simplificada") or resolver_local("c170"), cfop_df, ano_base)),
    ]:
        if df_src is not None and not df_src.is_empty():
            cols = CAMPOS_CHAVE + ["unidade", "valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida", "ano"]
            fragmentos.append(df_src.select([c for c in cols if c in df_src.columns]))

    if not fragmentos:
        return pl.DataFrame()

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    df_total = _normalizar(df_total)
    df_total = _aplicar_normalizacao(df_total)
    df_total = _gerar_chave(df_total)
    return df_total


def _criar_mapa_produto_item(df_prod: pl.DataFrame) -> pl.DataFrame:
    if "lista_chave_item_individualizado" not in df_prod.columns:
        return pl.DataFrame(schema={"chave_produto": pl.String, "chave_item_individualizado": pl.String, "unid_padrao": pl.String, "score_confianca_produto": pl.Float64})

    cols = ["chave_produto", "lista_chave_item_individualizado"]
    if "unid_padrao" in df_prod.columns:
        cols.append("unid_padrao")
    if "score_confianca" in df_prod.columns:
        cols.append("score_confianca")

    df_map = (
        df_prod.select(cols)
        .explode("lista_chave_item_individualizado")
        .rename({"lista_chave_item_individualizado": "chave_item_individualizado"})
    )

    if "score_confianca" in df_map.columns:
        df_map = df_map.rename({"score_confianca": "score_confianca_produto"})
    else:
        df_map = df_map.with_columns(pl.lit(0.0).alias("score_confianca_produto"))

    if "unid_padrao" not in df_map.columns:
        df_map = df_map.with_columns(pl.lit(None, dtype=pl.String).alias("unid_padrao"))

    return df_map


def _agrupar_base_conversao(df_vols: pl.DataFrame) -> pl.DataFrame:
    if df_vols.is_empty():
        return df_vols

    return (
        df_vols.group_by(["chave_produto", "unidade", "unid_padrao"])
        .agg([
            pl.col("valor_entrada").sum().alias("v_entr_total"),
            pl.col("quantidade_entrada").sum().alias("q_entr_total"),
            pl.col("valor_saida").sum().alias("v_saida_total"),
            pl.col("quantidade_saida").sum().alias("q_saida_total"),
            pl.col("score_confianca_produto").max().alias("score_confianca_produto"),
            pl.len().alias("ocorrencias"),
            pl.col("descricao").drop_nulls().first().alias("descricao"),
        ])
        .with_columns([
            pl.when(pl.col("q_entr_total") > 0)
              .then(pl.col("v_entr_total") / pl.col("q_entr_total"))
              .otherwise(0.0)
              .alias("preco_medio_entrada"),
            pl.when(pl.col("q_saida_total") > 0)
              .then(pl.col("v_saida_total") / pl.col("q_saida_total"))
              .otherwise(0.0)
              .alias("preco_medio_saida"),
            (pl.col("q_entr_total").abs() + pl.col("q_saida_total").abs()).alias("volume_total"),
        ])
    )


def _calcular_fator(df_base: pl.DataFrame) -> pl.DataFrame:
    if df_base.is_empty():
        return df_base

    df_unid_padrao_auto = (
        df_base.group_by("chave_produto")
        .agg(pl.col("unidade").sort_by("volume_total", descending=True).first().alias("unid_padrao_auto"))
    )

    df_fator = (
        df_base.join(df_unid_padrao_auto, on="chave_produto", how="left")
        .with_columns(pl.coalesce(["unid_padrao", "unid_padrao_auto"]).alias("unid_padrao_ref"))
    )

    df_precos_ref = (
        df_base.select(["chave_produto", "unidade", "preco_medio_entrada", "preco_medio_saida", "volume_total"])
        .rename({
            "unidade": "unid_padrao_ref",
            "preco_medio_entrada": "preco_ref_ent",
            "preco_medio_saida": "preco_ref_sai",
            "volume_total": "volume_ref",
        })
    )

    df_final = (
        df_fator.join(df_precos_ref, on=["chave_produto", "unid_padrao_ref"], how="left")
        .with_columns([
            pl.when(pl.col("unidade") == pl.col("unid_padrao_ref"))
              .then(1.0)
              .when((pl.col("preco_ref_ent") > 0) & (pl.col("preco_medio_entrada") > 0))
              .then(pl.col("preco_ref_ent") / pl.col("preco_medio_entrada"))
              .when((pl.col("preco_ref_sai") > 0) & (pl.col("preco_medio_saida") > 0))
              .then(pl.col("preco_ref_sai") / pl.col("preco_medio_saida"))
              .otherwise(0.0)
              .alias("fator_sugerido"),
            (
                (pl.col("volume_total") / (pl.col("volume_total") + 1.0)) * 0.6 +
                (pl.col("score_confianca_produto").fill_null(0.0)) * 0.4
            ).cast(pl.Float64).alias("score_confianca"),
        ])
        .with_columns([
            pl.when(pl.col("fator_sugerido") <= 0)
              .then(pl.lit("erro"))
              .when((pl.col("fator_sugerido") > 1000) | (pl.col("fator_sugerido") < 0.001))
              .then(pl.lit("extremo"))
              .when(pl.col("score_confianca") >= 0.85)
              .then(pl.lit("auto_aprovavel"))
              .when(pl.col("score_confianca") >= 0.65)
              .then(pl.lit("revisar"))
              .otherwise(pl.lit("baixa_confianca"))
              .alias("status_validacao"),
        ])
        .select([
            "chave_produto", "descricao", "unidade", "unid_padrao_ref",
            "v_entr_total", "q_entr_total", "v_saida_total", "q_saida_total",
            "preco_medio_entrada", "preco_medio_saida", "volume_total",
            "fator_sugerido", "score_confianca", "status_validacao"
        ])
        .rename({"unid_padrao_ref": "unid_padrao"})
        .sort(["chave_produto", "unidade"])
    )

    return df_final


def calcular_fator_conversao_v2(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    df_prod = _carregar_produtos_base(pasta_produtos, cnpj)
    if df_prod is None or df_prod.is_empty():
        rprint("[red]Erro: base de produtos não encontrada para cálculo de conversão.[/red]")
        return False

    df_map = _criar_mapa_produto_item(df_prod)
    df_trans = _carregar_transacoes(cnpj, pasta_cnpj)
    if df_trans.is_empty():
        rprint("[red]Erro: nenhuma transação encontrada para cálculo de conversão.[/red]")
        return False

    df_vols = df_trans.join(df_map, on="chave_item_individualizado", how="inner")
    if df_vols.is_empty():
        rprint("[red]Erro: nenhuma transação foi relacionada aos produtos consolidados.[/red]")
        return False

    df_base = _agrupar_base_conversao(df_vols)
    df_fatores = _calcular_fator(df_base)

    arquivo_manual = pasta_produtos / f"fatores_manuais_{cnpj}.xlsx"
    df_manual = ler_fatores_manuais(arquivo_manual)
    if df_manual is not None:
        df_fatores = (
            df_fatores.join(df_manual, on=["chave_produto", "unidade"], how="left")
            .with_columns([
                pl.when(pl.col("fator_conversao_manual").is_not_null())
                  .then(pl.col("fator_conversao_manual"))
                  .otherwise(pl.col("fator_sugerido"))
                  .alias("fator_de_conversao"),
                pl.when(pl.col("fator_conversao_manual").is_not_null())
                  .then(pl.lit("manual"))
                  .otherwise(pl.lit("automatico"))
                  .alias("fonte_fator"),
            ])
            .drop("fator_conversao_manual")
        )
    else:
        df_fatores = df_fatores.with_columns([
            pl.col("fator_sugerido").alias("fator_de_conversao"),
            pl.lit("automatico").alias("fonte_fator"),
        ])

    df_fila = (
        df_fatores.filter(
            (pl.col("status_validacao") != "auto_aprovavel") |
            (pl.col("fator_de_conversao") <= 0) |
            ((pl.col("fator_de_conversao") > 1000) | (pl.col("fator_de_conversao") < 0.001))
        )
        .sort(["status_validacao", "score_confianca", "chave_produto"])
    )

    ok1 = salvar_para_parquet(df_base, pasta_produtos, f"base_conversao_unidades_{cnpj}.parquet")
    ok2 = salvar_para_parquet(df_fatores, pasta_produtos, f"fator_conversao_v2_{cnpj}.parquet")
    ok3 = salvar_para_parquet(df_fila, pasta_produtos, f"fila_verificacao_produtos_{cnpj}.parquet")
    return ok1 and ok2 and ok3


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
    else:
        cnpj_arg = input("CNPJ: ").strip()

    if not validar_cnpj(cnpj_arg):
        rprint(f"[red]CNPJ inválido: {cnpj_arg}[/red]")
        sys.exit(1)

    ok = calcular_fator_conversao_v2(cnpj_arg)
    sys.exit(0 if ok else 1)
