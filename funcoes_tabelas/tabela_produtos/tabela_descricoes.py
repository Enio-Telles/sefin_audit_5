"""
tabela_descricoes.py

Script para agrupar produtos pela descrição normalizada e consolidar os demais campos em listas.
"""

import hashlib
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
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos auxiliares:[/red] {e}")
    sys.exit(1)


def gerar_tabela_descricoes(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_entrada = pasta_produtos / f"tab_itens_caract_normalizada_{cnpj}.parquet"

    if not arq_entrada.exists():
        rprint(f"[red]Erro: Arquivo normalizado não encontrado: {arq_entrada}[/red]")
        return False

    rprint(f"\n[bold cyan]Gerando tabela_descricoes para CNPJ: {cnpj}[/bold cyan]")

    # Lê o parquet
    df = pl.read_parquet(arq_entrada)

    # Identifica colunas presentes
    cols = df.columns
    
    # Agrupamento por descrição
    agg_exprs = [
        pl.col("chave_item_individualizado").unique().sort().alias("lista_chave_item_individualizado"),
        pl.col("cod_normalizado").unique().sort().alias("lista_cod_normalizado"),
        pl.col("total_entradas").sum().alias("total_entradas"),
        pl.col("total_saidas").sum().alias("total_saidas"),
    ]

    # Campos opcionais/adicionais
    mapeamento_listas = {
        "descr_compl": "lista_descr_compl",
        "tipo_item": "lista_tipo_item",
        "ncm": "lista_ncm",
        "cest": "lista_cest",
        "gtin": "lista_gtin",
        "co_sefin_inferido": "lista_co_sefin_inferido"
    }

    for orig, dest in mapeamento_listas.items():
        if orig in cols:
            agg_exprs.append(pl.col(orig).unique().sort().alias(dest))

    # Campos que já são listas (precisam de flatten)
    if "lista_unidades" in cols:
        agg_exprs.append(pl.col("lista_unidades").flatten().unique().sort().alias("lista_unids"))
    if "fonte" in cols:
        agg_exprs.append(pl.col("fonte").flatten().unique().sort().alias("lista_fonte"))

    def calcular_moda(lista):
        if lista is None or len(lista) == 0:
            return None
        from collections import Counter
        # Filtra nulos e vazios
        limpos = [str(x).strip() for x in lista if x not in (None, "", [])]
        if not limpos:
            return None
        counts = Counter(limpos)
        max_freq = max(counts.values())
        candidatos = [val for val, count in counts.items() if count == max_freq]
        # Desempate: primeiro em ordem alfabética (natural_sort_key seria ideal, mas sorted básico resolve)
        return sorted(candidatos)[0]

    def gerar_chave_produto(lista_chaves):
        if lista_chaves is None or (hasattr(lista_chaves, "len") and lista_chaves.len() == 0) or len(lista_chaves) == 0:
            return ""
        # Ordena para garantir consistência e gera MD5
        texto_chaves = "".join(sorted([str(c) for c in lista_chaves]))
        return hashlib.md5(texto_chaves.encode()).hexdigest()

    # Monta expressões condicionais para with_columns
    with_exprs = [
        pl.col("lista_ncm").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("ncm_padrao"),
        pl.col("lista_cest").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("cest_padrao"),
        pl.col("lista_gtin").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("gtin_padrao"),
        pl.col("lista_tipo_item").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("tipo_item_padrao"),
        pl.col("lista_unids").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("unid_padrao"),
        pl.col("lista_chave_item_individualizado").map_elements(gerar_chave_produto, return_dtype=pl.Utf8).alias("chave_produto"),
        pl.lit(False).alias("verificado")
    ]

    # Adiciona colunas co_sefin apenas se existirem nos dados de origem
    if "co_sefin_inferido" in cols:
        with_exprs.extend([
            pl.col("lista_co_sefin_inferido").map_elements(calcular_moda, return_dtype=pl.Utf8).alias("co_sefin_agr"),
            pl.col("lista_co_sefin_inferido").list.unique().list.len().gt(1).alias("co_sefin_agr_divergente"),
        ])

    df_resultado = (
        df.group_by("descricao")
        .agg(agg_exprs)
        .with_columns(with_exprs)
        .sort("descricao")
    )

    rprint(f"[green]Total de descrições únicas: {len(df_resultado):,}[/green]")

    nome_saida = f"tabela_descricoes_{cnpj}.parquet"
    ok = salvar_para_parquet(df_resultado, pasta_produtos, nome_saida)

    return ok


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

    sucesso = gerar_tabela_descricoes(cnpj_arg)
    sys.exit(0 if sucesso else 1)
