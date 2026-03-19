"""
fator_conversao.py

Script para calcular o fator de conversão de unidades para cada produto anualmente,
baseado na média de preços das entradas (C170) e saídas (NFe/NFCe).
"""

import sys
from pathlib import Path
from collections import Counter

import polars as pl
from rich import print as rprint

FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"
TABELA_PRODUTOS_DIR = FUNCOES_DIR / "funcoes_tabelas" / "tabela_produtos"

if str(AUXILIARES_DIR) not in sys.path:
    sys.path.insert(0, str(AUXILIARES_DIR))

try:
    from salvar_para_parquet import salvar_para_parquet
    from validar_cnpj import validar_cnpj
    from encontrar_arquivo_cnpj import encontrar_arquivo
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
CAMPOS_CHAVE = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]


def _md5_row(values: list) -> str:
    """Gera um hash MD5 a partir de uma lista de valores (normalizados)."""
    import hashlib
    partes = [str(v).strip().upper() if v is not None else "" for v in values]
    return hashlib.md5("|".join(partes).encode("utf-8")).hexdigest()


def _normalizar(df: pl.DataFrame) -> pl.DataFrame:
    """Garante que todos os campos chave existam no DataFrame (preenche com null se ausentes)."""
    for col in CAMPOS_CHAVE + ["unidade", "fonte"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias(col))
    return df


def _gerar_chave(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona a coluna chave_item_individualizado (MD5 dos campos chave)."""
    # Normaliza cada campo chave para string uppercase sem espaços laterais
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
          .map_elements(_md5_row, return_dtype=pl.String)
          .alias("chave_item_individualizado")
    ).drop(key_cols)

    return df


def _ler_nfe_nfce(path: Path | None, cnpj: str, fonte: str, cfop_df: pl.DataFrame | None = None) -> pl.DataFrame | None:
    """Lê NFe ou NFCe, filtra pelo CNPJ emitente e mapeia colunas."""
    if path is None or not path.exists():
        rprint(f"[yellow]  ⚠️  {fonte} não encontrado.[/yellow]")
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
    return df


def _ler_c170(path: Path | None, cfop_df: pl.DataFrame | None = None, ano_padrao: str = "") -> pl.DataFrame | None:
    """Lê c170_simplificada (ou c170) e mapeia colunas."""
    if path is None or not path.exists():
        rprint("[yellow]  ⚠️  C170 não encontrado.[/yellow]")
        return None

    schema = pl.read_parquet(path, n_rows=0).schema

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
    return df


def _agrupar_por_produto_ano(df_vols: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Agrupa os volumes por produto, unidade e ano para calcular médias e definir unidade padrão."""
    # 5. Agrupar por (chave_produto, unidade, ano) para calcular médias
    df_aggr = (
        df_vols
        .group_by(["chave_produto", "unidade", "ano", "unid_padrao_escolhida"])
        .agg([
            pl.col("valor_entrada").sum().alias("v_ent"),
            pl.col("quantidade_entrada").sum().alias("q_ent"),
            pl.col("valor_saida").sum().alias("v_sai"),
            pl.col("quantidade_saida").sum().alias("q_sai"),
            pl.len().alias("ocorrencias")
        ])
        .with_columns([
            (pl.col("v_ent") / pl.col("q_ent")).fill_nan(0).alias("preco_med_ent"),
            (pl.col("v_sai") / pl.col("q_sai")).fill_nan(0).alias("preco_med_sai"),
        ])
    )

    # 6. Unidade Padrão: usar a escolhida na tabela de descrições, ou a moda se vazia
    df_unid_padrao_auto = (
        df_aggr
        .group_by(["chave_produto", "ano"])
        .agg(pl.col("unidade").sort_by("ocorrencias", descending=True).first().alias("unid_padrao_auto"))
    )

    df_fator_pre = df_aggr.join(df_unid_padrao_auto, on=["chave_produto", "ano"])

    df_fator = df_fator_pre.with_columns(
        pl.coalesce(["unid_padrao_escolhida", "unid_padrao_auto"]).alias("unid_padrao")
    )
    return df_aggr, df_fator


def _calcular_fator_final(df_fator: pl.DataFrame, df_aggr: pl.DataFrame) -> pl.DataFrame:
    """Calcula o fator de conversão em relação à unidade padrão escolhida/calculada."""
    # 7. Join para calcular fator em relação à unid_padrao
    df_precos_padrao = (
        df_aggr
        .select(["chave_produto", "ano", "unidade", "preco_med_ent", "preco_med_sai"])
        .rename({"unidade": "unid_padrao", "preco_med_ent": "preco_padrao_ent", "preco_med_sai": "preco_padrao_sai"})
    )

    df_final = (
        df_fator.join(df_precos_padrao, on=["chave_produto", "ano", "unid_padrao"], how="left")
        .with_columns([
            # Cálculo do fator
            pl.when(pl.col("preco_padrao_ent") > 0)
              .then(pl.col("preco_med_ent") / pl.col("preco_padrao_ent"))
              .when((pl.col("preco_padrao_ent").is_null() | (pl.col("preco_padrao_ent") == 0)) & (pl.col("preco_padrao_sai") > 0))
              .then(pl.col("preco_med_sai") / pl.col("preco_padrao_sai"))
              .otherwise(0.0)
              .alias("fator_conversao")
        ])
        .select([
            "chave_produto", "ano", "unidade", "unid_padrao",
            "v_ent", "q_ent", "preco_med_ent",
            "v_sai", "q_sai", "preco_med_sai",
            "fator_conversao"
        ])
        .sort(["chave_produto", "ano", "unidade"])
    )

    return df_final


def calcular_fator_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    # Precisamos da tabela_descricoes para saber a chave_produto de cada item
    arq_descricoes = pasta_produtos / f"tabela_descricoes_{cnpj}.parquet"
    # Precisamos da tabela_itens_caracteristicas (original antes da aggr final se possível, 
    # ou reconstruir a partir das fontes com a mesma lógica)
    
    if not arq_descricoes.exists():
        rprint(f"[red]Erro: Tabela descrições não encontrada: {arq_descricoes}[/red]")
        return False

    rprint(f"\n[bold cyan]Calculando fator_conversao para CNPJ: {cnpj}[/bold cyan]")

    # 1. Carregar mapeamento de itens para chave_produto e unidade padrão escolhida
    df_desc = pl.read_parquet(arq_descricoes)
    
    # Mapeamento do item para chave_produto e unid_padrao do produto
    df_item_prod = (
        df_desc.select(["chave_produto", "unid_padrao", "lista_chave_item_individualizado"])
        .explode("lista_chave_item_individualizado")
        .rename({"lista_chave_item_individualizado": "chave_item_individualizado", "unid_padrao": "unid_padrao_escolhida"})
    )

    # 2. Precisamos dos itens originais com (chave_item, unidade, valor, qtd, ano)
    # Re-ler fontes com foco em valores e quantidades para cálculo de média
    
    cfop_bi_path = Path(r"c:\funcoes\referencias\cfop\cfop_bi.parquet")
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.read_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )

    # Ano base (EFD)
    ano_base = ""
    arq_dir = pasta_cnpj / "arquivos_parquet"
    
    dirs_validos = [d for d in (arq_dir, pasta_cnpj) if d.exists()]
    def resolver_local(prefixo):
        for d in dirs_validos:
            a = encontrar_arquivo(d, prefixo, cnpj)
            if a: return a
        return None
        
    reg_0000_path = resolver_local("reg_0000")
    if reg_0000_path:
        try:
            df_0000 = pl.read_parquet(reg_0000_path, n_rows=1)
            if "dt_ini" in df_0000.columns: ano_base = str(df_0000["dt_ini"][0])[0:4]
        except: pass

    fragmentos = []
    for nome, df_src in [
        ("NFe",  _ler_nfe_nfce(resolver_local("NFe"), cnpj, "NFe", cfop_df)),
        ("NFCe", _ler_nfe_nfce(resolver_local("NFCe"), cnpj, "NFCe", cfop_df)),
        ("C170", _ler_c170(resolver_local("c170_simplificada") or resolver_local("c170"), cfop_df, ano_base))
    ]:
        if df_src is not None:
            cols = CAMPOS_CHAVE + ["unidade", "valor_entrada", "valor_saida", "quantidade_entrada", "quantidade_saida", "ano"]
            df_src = df_src.select([c for c in cols if c in df_src.columns])
            fragmentos.append(df_src)

    if not fragmentos:
        rprint("[red]❌ Nenhuma fonte de valores encontrada p/ fator conversão.[/red]")
        return False

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    df_total = _gerar_chave(df_total) # chave_item_individualizado
    
    # 4. Join com chave_produto e unid_padrao_escolhida
    df_vols = df_total.join(df_item_prod, on="chave_item_individualizado", how="inner")

    # 5 e 6. Agrupar por produto/ano e definir unidade padrão
    df_aggr, df_fator = _agrupar_por_produto_ano(df_vols)

    # 7. Join para calcular fator em relação à unid_padrao
    df_final = _calcular_fator_final(df_fator, df_aggr)
    # 8. Salvar
    nome_saida = f"fator_conversao_{cnpj}.parquet"
    ok = salvar_para_parquet(df_final, pasta_produtos, nome_saida)
    
    return ok


if __name__ == "__main__":
    if len(sys.argv) > 1:
        calcular_fator_conversao(sys.argv[1])
    else:
        cnpj_input = input("CNPJ: ").strip()
        if cnpj_input:
            calcular_fator_conversao(cnpj_input)
