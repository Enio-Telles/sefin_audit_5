"""
Módulo: produtos_agrupados.py
Objetivo: Permitir a união manual de linhas da tabela produtos em uma nova tabela produtos_agrupados.
"""
import sys
from pathlib import Path
from collections import Counter
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

def _contar_campos_preenchidos(d: dict) -> int:
    return sum(1 for campo in ["lista_ncm", "lista_cest", "lista_gtin"] if d.get(campo) and len(d.get(campo)) > 0)

def _escolher_melhor_descricao(descricoes: list[str], dict_grupo: list[dict]) -> str | None:
    if not descricoes:
        return None

    # 1. Moda
    limpos = [str(x).strip() for x in descricoes if x not in (None, "", []) and str(x).strip()]
    if not limpos:
        return None

    cont = Counter(limpos)
    maior = max(cont.values())
    candidatos = [k for k, v in cont.items() if v == maior]

    if len(candidatos) == 1:
        return candidatos[0]

    # 2. Desempate: Maior quantidade de campos preenchidos (NCM, CEST, GTIN) do registro de origem da descrição
    # Associar cada descrição candidata à quantidade de campos preenchidos de sua origem
    candidatos_com_peso = []
    for cand in candidatos:
        # Encontra o primeiro registro que tem essa descrição
        reg = next((d for d in dict_grupo if d.get("descricao") == cand), None)
        if reg:
            peso = _contar_campos_preenchidos(reg)
            candidatos_com_peso.append((cand, peso))
        else:
            candidatos_com_peso.append((cand, 0))

    candidatos_com_peso.sort(key=lambda x: x[1], reverse=True)

    # Se ainda houver empate, vamos para o desempate 2
    melhor_peso = candidatos_com_peso[0][1]
    empatados = [c for c, p in candidatos_com_peso if p == melhor_peso]

    # 3. Desempate: Tamanho da string
    empatados.sort(key=lambda x: len(x), reverse=True)
    return empatados[0]

def _moda_simples(lista: list[str] | None) -> str | None:
    if not lista:
        return None
    limpos = [str(x).strip() for x in lista if x not in (None, "", []) and str(x).strip()]
    if not limpos:
        return None

    cont = Counter(limpos)
    maior = max(cont.values())
    candidatos = [k for k, v in cont.items() if v == maior]
    candidatos.sort(key=lambda x: len(x), reverse=True)
    return candidatos[0]

def gerar_produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None, agrupamentos_manuais: dict[str, list[str]] | None = None) -> pl.DataFrame | None:
    import re
    cnpj = re.sub(r"[^0-9]", "", cnpj)

    if pasta_cnpj is None:
        pasta_cnpj = FUNCOES_DIR / "CNPJ" / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arq_produtos = pasta_produtos / f"produtos_{cnpj}.parquet"

    if not arq_produtos.exists():
        rprint(f"[red]Erro: Tabela base {arq_produtos} não encontrada.[/red]")
        return None

    df_produtos = pl.read_parquet(arq_produtos)

    if df_produtos.is_empty():
        return None

    if not agrupamentos_manuais:
        # Se não houver agrupamentos, a tabela inicia apenas como um "pass-through" 1-para-1
        df_agrupados = (
            df_produtos.with_columns([
                (pl.lit("id_agrupado_") + pl.int_range(1, pl.len() + 1).cast(pl.String)).alias("id_agrupado"),
                pl.col("chave_produto").map_elements(lambda x: [x], return_dtype=pl.List(pl.Utf8)).alias("lista_chave_produto"),
                pl.col("descricao").alias("descr_padrao"),
                pl.col("lista_ncm").map_elements(_moda_simples, return_dtype=pl.Utf8).alias("ncm_padrao"),
                pl.col("lista_cest").map_elements(_moda_simples, return_dtype=pl.Utf8).alias("cest_padrao"),
                pl.col("lista_gtin").map_elements(_moda_simples, return_dtype=pl.Utf8).alias("gtin_padrao"),
                pl.col("lista_unid").alias("lista_unidades")
            ])
            .select(["id_agrupado", "lista_chave_produto", "descr_padrao", "ncm_padrao", "cest_padrao", "gtin_padrao", "lista_unidades"])
        )
    else:
        # Lógica de agrupamento manual a partir do dicionário {id_agrupado: [chave_produto_1, chave_produto_2]}
        registros = []
        for id_agr, chaves in agrupamentos_manuais.items():
            df_grupo = df_produtos.filter(pl.col("chave_produto").is_in(chaves))
            if df_grupo.is_empty():
                continue

            dict_grupo = df_grupo.to_dicts()
            todas_descricoes = [d["descricao"] for d in dict_grupo if d.get("descricao")]
            todos_ncms = [item for d in dict_grupo if d.get("lista_ncm") for item in d["lista_ncm"]]
            todos_cests = [item for d in dict_grupo if d.get("lista_cest") for item in d["lista_cest"]]
            todos_gtins = [item for d in dict_grupo if d.get("lista_gtin") for item in d["lista_gtin"]]
            todas_unids = list(set([item for d in dict_grupo if d.get("lista_unid") for item in d["lista_unid"]]))
            todas_unids.sort()

            registros.append({
                "id_agrupado": id_agr,
                "lista_chave_produto": chaves,
                "descr_padrao": _escolher_melhor_descricao(todas_descricoes, dict_grupo),
                "ncm_padrao": _moda_simples(todos_ncms),
                "cest_padrao": _moda_simples(todos_cests),
                "gtin_padrao": _moda_simples(todos_gtins),
                "lista_unidades": todas_unids
            })

        # Adicionar os que não foram agrupados manualmente
        chaves_agrupadas = [ch for chaves in agrupamentos_manuais.values() for ch in chaves]
        df_restante = df_produtos.filter(~pl.col("chave_produto").is_in(chaves_agrupadas))

        idx = len(agrupamentos_manuais) + 1
        for d in df_restante.to_dicts():
            registros.append({
                "id_agrupado": f"id_agrupado_{idx}",
                "lista_chave_produto": [d["chave_produto"]],
                "descr_padrao": d["descricao"],
                "ncm_padrao": _moda_simples(d.get("lista_ncm", [])),
                "cest_padrao": _moda_simples(d.get("lista_cest", [])),
                "gtin_padrao": _moda_simples(d.get("lista_gtin", [])),
                "lista_unidades": d.get("lista_unid", [])
            })
            idx += 1

        df_agrupados = pl.DataFrame(registros)

    # Integração Final
    df_final = df_agrupados.explode("lista_chave_produto").join(
        df_produtos, left_on="lista_chave_produto", right_on="chave_produto", how="left"
    ).rename({"lista_chave_produto": "chave_produto"})

    # Recalcula e agrupa no formato final, mantendo id_agrupado como chave primaria no produtos_agrupados
    df_agrupados = df_agrupados.select([
        "id_agrupado", "lista_chave_produto", "descr_padrao", "ncm_padrao", "cest_padrao", "gtin_padrao", "lista_unidades"
    ])

    salvar_para_parquet(df_agrupados, pasta_produtos, f"produtos_agrupados_{cnpj}.parquet")
    salvar_para_parquet(df_final, pasta_produtos, f"produtos_final_{cnpj}.parquet")

    rprint(f"[green]produtos_agrupados gerado com {len(df_agrupados)} agrupamentos.[/green]")
    rprint(f"[green]produtos_final gerado com {len(df_final)} produtos vinculados.[/green]")
    return df_agrupados

if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos_agrupados(sys.argv[1])
    else:
        print("Uso: python produtos_agrupados.py <cnpj>")
