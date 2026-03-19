"""
Gera tabela_itens_caracteristicas_<cnpj>.parquet a partir dos parquets:
  - NFe_<cnpj>.parquet
  - NFCe_<cnpj>.parquet
  - c170_simplificada_<cnpj>.parquet   (ou c170_<cnpj>.parquet)
  - bloco_h_<cnpj>.parquet

Saída: CNPJ/<cnpj>/analises/tabela_itens_caracteristicas_<cnpj>.parquet

Colunas de saída:
  chave_item_individualizado, item_seq_id, codigo, descricao, descr_compl,
  tipo_item, ncm, cest, gtin, lista_unidades, fonte
"""

import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

FUNCOES_DIR     = Path(r"c:\funcoes")
AUXILIARES_DIR  = FUNCOES_DIR / "funcoes_auxiliares"
REFS_DIR        = FUNCOES_DIR / "referencias" / "CO_SEFIN"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
    from encontrar_arquivo_cnpj import encontrar_arquivo
    from co_sefin import co_sefin
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

    def _norm_desc(v):
        if v is None:
            return None
        return " ".join(_norm(v).split())

    cols_texto = ["descricao", "descr_compl", "tipo_item"]
    cols_pontos = ["ncm", "cest", "gtin"]
    cols_lista = ["lista_unidades", "fonte"]

    exprs = []

    for col in cols_texto:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_norm, return_dtype=pl.String).alias(col))

    if "descricao" in df.columns:
        exprs.append(
            pl.col("descricao").map_elements(_norm_desc, return_dtype=pl.String).alias("descricao_normalizada")
        )

    if "codigo" in df.columns:
        exprs.append(pl.col("codigo").map_elements(_norm_codigo, return_dtype=pl.String).alias("cod_normalizado"))

    for col in cols_pontos:
        if col in df.columns:
            exprs.append(pl.col(col).map_elements(_remove_pontos, return_dtype=pl.String).alias(col))

    for col in cols_lista:
        if col in df.columns:
            exprs.append(
                pl.col(col).list.eval(
                    pl.element().map_elements(_norm, return_dtype=pl.String)
                ).alias(col)
            )

    if exprs:
        df = df.with_columns(exprs)

    if "cod_normalizado" in df.columns and "codigo" in df.columns:
        cols = df.columns
        cols.remove("cod_normalizado")
        idx = cols.index("codigo")
        cols.insert(idx + 1, "cod_normalizado")
        df = df.select(cols)

    return df


def _ler_bloco_h(path: Path | None) -> pl.DataFrame | None:
    if path is None or not path.exists():
        rprint("[yellow]  ⚠️  Bloco H não encontrado.[/yellow]")
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

    col_map = {}
    for destino, candidatos in {
        "codigo": ["codigo_produto", "codigo_produto_original"],
        "descricao": ["descricao_produto"],
        "tipo_item": ["tipo_item"],
        "ncm": ["cod_ncm"],
        "cest": ["cest"],
        "gtin": ["cod_barra"],
        "unidade": ["unidade_medida"],
    }.items():
        for c in candidatos:
            if c in schema:
                col_map[c] = destino
                break

    selecionar = list(col_map.keys())
    df = (
        pl.scan_parquet(path)
        .select(selecionar)
        .collect()
        .rename(col_map)
    )

    if df.is_empty():
        return None

    df = df.with_columns(pl.lit(None, pl.String).alias("descr_compl"))

    rprint(f"[green]  Bloco H: {len(df):,} linhas[/green]")
    return df


def gerar_tabela_itens_caracteristicas_v2(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    arq_dir = pasta_cnpj / "arquivos_parquet"

    rprint(f"\n[bold cyan]Gerando tabela_itens_caracteristicas_v2 para CNPJ: {cnpj}[/bold cyan]")

    diretorios_validos = [d for d in (arq_dir, pasta_cnpj) if d.exists()]

    def _resolver(prefixo: str) -> Path | None:
        for diretorio in diretorios_validos:
            arq = encontrar_arquivo(diretorio, prefixo, cnpj)
            if arq:
                return arq
        return None

    ano_base = ""
    reg_0000_path = _resolver("reg_0000")
    if reg_0000_path:
        try:
            df_0000 = pl.read_parquet(reg_0000_path, n_rows=1)
            if "dt_ini" in df_0000.columns:
                ano_base = str(df_0000["dt_ini"][0])[0:4]
            elif "dt_periodo" in df_0000.columns:
                ano_base = str(df_0000["dt_periodo"][0])[0:4]
        except Exception:
            pass

    cfop_bi_path = Path(r"c:\funcoes\referencias\cfop\cfop_bi.parquet")
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.scan_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .collect()
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )

    fragmentos: list[pl.DataFrame] = []

    for nome_fonte, df_src in [
        ("NFe",    ler_nfe_nfce(_resolver("NFe"),  cnpj, "NFe", cfop_df, print_status=True)),
        ("NFCe",   ler_nfe_nfce(_resolver("NFCe"), cnpj, "NFCe", cfop_df, print_status=True)),
        ("C170",   ler_c170(_resolver("c170_simplificada") or _resolver("c170"), cfop_df, ano_base, print_status=True)),
        ("bloco_h", _ler_bloco_h(_resolver("bloco_h"))),
    ]:
        if df_src is not None and not df_src.is_empty():
            df_src = df_src.with_columns(pl.lit(nome_fonte, pl.String).alias("fonte"))
            df_src = _normalizar(df_src)
            cols_finais = CAMPOS_CHAVE + ["unidade", "fonte", "valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida", "ano"]
            df_src = df_src.select([c for c in cols_finais if c in df_src.columns])
            df_src = _normalizar(df_src)
            fragmentos.append(df_src)

    if not fragmentos:
        rprint("[red]❌ Nenhuma fonte disponível. Abortando.[/red]")
        return False

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    for col in ["valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida"]:
        if col not in df_total.columns:
            df_total = df_total.with_columns(pl.lit(0.0).alias(col))
        else:
            df_total = df_total.with_columns(pl.col(col).fill_null(0).cast(pl.Float64))

    if "ano" not in df_total.columns:
        df_total = df_total.with_columns(pl.lit(ano_base).alias("ano"))
    else:
        df_total = df_total.with_columns(pl.col("ano").fill_null(ano_base).cast(pl.String))

    df_total = _gerar_chave(df_total)

    df_resultado = (
        df_total
        .group_by("chave_item_individualizado")
        .agg(
            *[
                pl.col(c).drop_nulls().first().alias(c)
                for c in CAMPOS_CHAVE
            ],
            pl.col("valor_entrada").sum().alias("total_entradas"),
            pl.col("valor_saida").sum().alias("total_saidas"),
            pl.col("quantidade_entrada").sum().alias("qtd_entradas"),
            pl.col("quantidade_saida").sum().alias("qtd_saidas"),
            pl.col("unidade").filter(pl.col("unidade").is_not_null() & (pl.col("unidade") != "")).unique().sort().alias("lista_unidades"),
            pl.col("fonte").drop_nulls().unique().sort().alias("fonte"),
            pl.col("ano").drop_nulls().first().alias("ano"),
        )
        .sort(["descricao", "codigo"], nulls_last=True)
        .with_row_index(name="item_seq_id", offset=1)
        .with_columns((pl.lit("item_") + pl.col("item_seq_id").cast(pl.String)).alias("item_seq_id"))
    )

    pasta_saida = pasta_cnpj / "analises" / "produtos"
    ok = salvar_para_parquet(df_resultado, pasta_saida, f"tabela_itens_caracteristicas_v2_{cnpj}.parquet")

    df_normalizado = _aplicar_normalizacao(df_resultado)
    ok_norm = salvar_para_parquet(df_normalizado, pasta_saida, f"tab_itens_caract_normalizada_v2_{cnpj}.parquet")

    ok_sefin = co_sefin(cnpj, pasta_cnpj)
    return ok and ok_norm and ok_sefin


if __name__ == "__main__":
    import re

    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
    else:
        try:
            cnpj_arg = input("Informe o CNPJ: ").strip()
        except (KeyboardInterrupt, EOFError):
            rprint("\n[yellow]Cancelado.[/yellow]")
            sys.exit(0)

    cnpj_arg = re.sub(r"[^0-9]", "", cnpj_arg)

    if not validar_cnpj(cnpj_arg):
        rprint(f"[red]CNPJ inválido: {cnpj_arg}[/red]")
        sys.exit(1)

    sucesso = gerar_tabela_itens_caracteristicas_v2(cnpj_arg)
    sys.exit(0 if sucesso else 1)
