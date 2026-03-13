"""
Módulo de Consolidação e Unificação de Produtos (Master Data Management).
Otimizado para processamento em larga escala (Big Data) utilizando Polars.

Este script é responsável por:
1. Ingerir dados de múltiplas fontes (NFe, NFCe, EFD C170, EFD 0200).
2. Normalizar e converter tipos para economia de memória (pl.Categorical).
3. Gerar Tabela 1: Variações de produtos (nível código + atributos fiscais).
4. Gerar Tabela 2: Agrupamento por descrição com auditoria de códigos divergentes.
"""

import os
import logging
from typing import Dict, Tuple
import polars as pl

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PRODUTOS MDM] %(message)s'
)

# ---------------------------------------------------------------------------
# 1. DEFINIÇÃO DE SCHEMA OTIMIZADO (Redução de Footprint de Memória)
# ---------------------------------------------------------------------------
SCHEMA_FISCAL_PRODUTOS = {
    "codigo": pl.String,
    "descricao": pl.String,
    "descricao_ori": pl.String,
    "descr_compl": pl.String,
    "ncm": pl.Categorical,
    "cest": pl.Categorical,
    "gtin": pl.String,
    "unid": pl.Categorical,
    "tipo_item": pl.Categorical,
    "fonte": pl.Categorical
}

# ---------------------------------------------------------------------------
# 2. FUNÇÕES DE LIMPEZA E TRANSFORMAÇÃO
# ---------------------------------------------------------------------------

def limpar_caracteres_especiais(expr: pl.Expr, manter_espacos: bool = False) -> pl.Expr:
    """Limpa strings de forma performática evitando regex custosas."""
    expr_limpa = expr.str.to_uppercase().str.strip_chars()
    expr_limpa = expr_limpa.str.replace_all(r"[^A-Z0-9\s/.-]", "")
    
    if not manter_espacos:
        expr_limpa = expr_limpa.str.replace_all(r"\s+", "")
    
    return expr_limpa

def aplicar_mapeamento_e_schema(
    lf: pl.LazyFrame, 
    mapping: Dict[str, str], 
    fonte_nome: str
) -> pl.LazyFrame:
    """Padroniza o LazyFrame aplicando o SCHEMA_FISCAL_PRODUTOS."""
    cols_presentes = lf.collect_schema().names()
    exprs = []

    for col_destino, dtype in SCHEMA_FISCAL_PRODUTOS.items():
        if col_destino == "fonte":
            exprs.append(pl.lit(fonte_nome).cast(pl.Categorical).alias("fonte"))
            continue
            
        if col_destino == "descricao_ori":
            col_origem_desc = mapping.get("descricao")
            if col_origem_desc and col_origem_desc in cols_presentes:
                exprs.append(pl.col(col_origem_desc).cast(pl.String).alias("descricao_ori"))
            else:
                exprs.append(pl.lit(None).cast(pl.String).alias("descricao_ori"))
            continue

        col_origem = mapping.get(col_destino)

        if col_origem and col_origem in cols_presentes:
            expr = pl.col(col_origem)
            
            if col_destino in ["codigo", "ncm", "cest", "unid"]:
                expr = limpar_caracteres_especiais(expr.cast(pl.String), manter_espacos=False)
                if col_destino == "codigo":
                    expr = expr.str.strip_chars_start("0")
            
            elif col_destino in ["descricao", "descr_compl"]:
                expr = limpar_caracteres_especiais(expr.cast(pl.String), manter_espacos=True)
            
            elif col_destino == "gtin":
                gtin_limpo = expr.cast(pl.String).str.replace_all(r"[^0-9]", "")
                expr = pl.when(gtin_limpo.str.len_chars().is_in([8, 12, 13, 14])) \
                         .then(gtin_limpo).otherwise(pl.lit(None))
            
            elif col_destino == "tipo_item":
                expr = expr.fill_null("(Vazio)").cast(pl.String).str.strip_chars()
                expr = pl.when(expr == "").then(pl.lit("(Vazio)")).otherwise(expr)

            exprs.append(expr.cast(dtype).alias(col_destino))
        else:
            exprs.append(pl.lit(None).cast(dtype).alias(col_destino))

    return lf.select(exprs).drop_nulls(subset=["codigo", "descricao"])

def cruzar_c170_0200(lf_c170: pl.LazyFrame, lf_0200: pl.LazyFrame) -> pl.LazyFrame:
    """Enriquece o C170 com dados do 0200 prevenindo explosão de joins."""
    lf_0200_unique = lf_0200.select(
        ["codigo", "ncm", "cest", "gtin", "tipo_item"]
    ).unique(subset=["codigo"], keep="first")

    lf_enriched = lf_c170.join(
        lf_0200_unique, on="codigo", how="left", suffix="_0200"
    )

    return lf_enriched.with_columns([
        pl.coalesce(["ncm", "ncm_0200"]).alias("ncm"),
        pl.coalesce(["cest", "cest_0200"]).alias("cest"),
        pl.coalesce(["gtin", "gtin_0200"]).alias("gtin"),
        pl.coalesce(["tipo_item", "tipo_item_0200"]).alias("tipo_item")
    ]).drop(["ncm_0200", "cest_0200", "gtin_0200", "tipo_item_0200"])

# ---------------------------------------------------------------------------
# 3. CONSTRUÇÃO DAS TABELAS ANALÍTICAS (MÉTODO REESCRITO)
# ---------------------------------------------------------------------------

def construir_tabelas_analiticas(lf_base_detalhes: pl.LazyFrame) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Gera as duas tabelas solicitadas:
    1. Variações únicas dos produtos.
    2. Agrupamento macro por Descrição com deteção de anomalias por código.
    """
    logging.info("Construindo Tabelas Analíticas de Variação e Agrupamento...")

    # =========================================================================
    # TABELA 1: Variações Possíveis de Produtos
    # =========================================================================
    lf_variacoes = (
        lf_base_detalhes
        .group_by(["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"])
        .agg([
            pl.col("unid").unique().alias("lista_unid"),
            pl.col("fonte").unique().alias("lista_fontes"),
            pl.len().alias("qtd_transacoes")
        ])
    )

    # =========================================================================
    # TABELA 2: Agrupado por Descrição + Alertas de Revisão
    # =========================================================================
    
    # Passo A: Calcular quantas variações existem para CADA CÓDIGO.
    lf_codigo_stats = (
        lf_base_detalhes
        .select(["codigo", "descricao", "descr_compl", "ncm", "cest", "gtin", "tipo_item"])
        .unique() 
        .group_by("codigo")
        .agg(pl.len().alias("qtd_variacoes"))
        .with_columns([
            (pl.col("qtd_variacoes") > 1).alias("tem_variacao_caracteristicas"),
            pl.format("[{};{}]", pl.col("codigo"), pl.col("qtd_variacoes")).alias("cod_var_str")
        ])
    )

    # Passo B: Enriquecer a base transacional com as métricas do código
    lf_base_enriched = lf_base_detalhes.join(
        lf_codigo_stats, on="codigo", how="left"
    )

    # Passo C: Agrupar tudo pela DESCRIÇÃO consolidando as listas
    lf_agrupado_descricao = (
        lf_base_enriched
        .group_by("descricao")
        .agg([
            # 1. LISTAS (Mantém nulos para evidenciar a variação/ausência)
            pl.col("codigo").unique().alias("lista_codigo"),
            pl.col("descr_compl").unique().alias("lista_descr_compl"),
            pl.col("tipo_item").unique().alias("lista_tipo_item"),
            pl.col("ncm").unique().alias("lista_ncm"),
            pl.col("cest").unique().alias("lista_cest"),
            pl.col("gtin").unique().alias("lista_gtin"),
            pl.col("unid").unique().alias("lista_unid_raw"),
            pl.col("fonte").unique().alias("lista_fontes"),
            
            # 2. COLUNAS DE CONSENSO (Descarta nulos na eleição para priorizar informação útil)
            pl.col("codigo").drop_nulls().mode().first().alias("codigo_consenso"),
            pl.col("tipo_item").drop_nulls().mode().first().alias("tipo_item_consenso"),
            pl.col("ncm").drop_nulls().mode().first().alias("ncm_consenso"),
            pl.col("cest").drop_nulls().mode().first().alias("cest_consenso"),
            pl.col("gtin").drop_nulls().mode().first().alias("gtin_consenso"),
            pl.col("unid").drop_nulls().mode().first().alias("unid_consenso"),
            
            # 3. ALERTAS E MÉTRICAS
            pl.col("tem_variacao_caracteristicas").any().alias("requer_revisao_manual"),
            pl.col("cod_var_str").unique().alias("lista_cod_var"),
            pl.len().alias("qtd_transacoes_total")
        ])
        .with_columns([
            pl.col("descricao").alias("lista_descricao"), # Compatibilidade com UI
            pl.col("lista_cod_var").list.join(" | ").alias("descricoes_conflitantes"), # Compatibilidade com UI
            pl.col("lista_unid_raw").list.join(", ").alias("lista_unid") # Compatibilidade com UI (exibição em badge)
        ])
    )

    return lf_variacoes, lf_agrupado_descricao

# ---------------------------------------------------------------------------
# 4. ORQUESTRADOR PRINCIPAL
# ----------# ---------------------------------------------------------------------------
# 4. ORQUESTRADOR E INTEGRAÇÃO
# ---------------------------------------------------------------------------

def unificar_produtos_unidades(cnpj: str) -> Dict[str, str]:
    """
    Ponto de entrada principal para a unificação de produtos.
    Orquestra a leitura de fontes, aplicação de mapas manuais e geração de tabelas analíticas.
    """
    import importlib.util
    from pathlib import Path

    # Localiza o config.py (assumindo que está na raiz do projeto)
    _PROJETO_DIR = Path(__file__).resolve().parent.parent.parent
    _config_path = _PROJETO_DIR / "config.py"
    
    spec = importlib.util.spec_from_file_location("sefin_config", str(_config_path))
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)

    dir_parquet, dir_analises, _ = config.obter_diretorios_cnpj(cnpj)

    # Mapeamento de arquivos de entrada (Parquet extraídos)
    caminhos_entrada = {
        "nfe": os.path.join(dir_parquet, f"NFE_ITENS_{cnpj}.parquet"),
        "nfce": os.path.join(dir_parquet, f"NFCE_ITENS_{cnpj}.parquet"),
        "c170": os.path.join(dir_parquet, f"EFD_C170_{cnpj}.parquet"),
        "c0200": os.path.join(dir_parquet, f"EFD_0200_{cnpj}.parquet")
    }

    return processar_produtos_cnpj(cnpj, caminhos_entrada, str(dir_analises))

def processar_produtos_cnpj(
    cnpj: str, 
    caminhos_entrada: Dict[str, str], 
    diretorio_saida: str
) -> Dict[str, str]:
    """Processa arquivos Parquet das fontes e gera as tabelas analíticas."""
    os.makedirs(diretorio_saida, exist_ok=True)
    
    path_mapa_manual = os.path.join(diretorio_saida, f"mapa_manual_unificacao_{cnpj}.parquet")
    
    with pl.StringCache():
        lazy_frames = []
        
        # NFe
        if "nfe" in caminhos_entrada and os.path.exists(caminhos_entrada["nfe"]):
            lf_nfe = pl.scan_parquet(caminhos_entrada["nfe"])
            map_nfe = {"codigo": "CPROD", "descricao": "XPROD", "ncm": "NCM", "cest": "CEST", "gtin": "CEAN", "unid": "UCOM"}
            lazy_frames.append(aplicar_mapeamento_e_schema(lf_nfe, map_nfe, "NFe"))

        # NFCe
        if "nfce" in caminhos_entrada and os.path.exists(caminhos_entrada["nfce"]):
            lf_nfce = pl.scan_parquet(caminhos_entrada["nfce"])
            map_nfce = {"codigo": "CPROD", "descricao": "XPROD", "ncm": "NCM", "cest": "CEST", "gtin": "CEAN", "unid": "UCOM"}
            lazy_frames.append(aplicar_mapeamento_e_schema(lf_nfce, map_nfce, "NFCe"))

        # EFD (C170 + 0200)
        lf_c170 = None
        lf_0200 = None
        
        if "c0200" in caminhos_entrada and os.path.exists(caminhos_entrada["c0200"]):
            lf_0200_raw = pl.scan_parquet(caminhos_entrada["c0200"])
            map_0200 = {"codigo": "COD_ITEM", "descricao": "DESCR_ITEM", "ncm": "COD_NCM", "unid": "UNID_INV", "tipo_item": "TIPO_ITEM", "gtin": "COD_BARRA"}
            lf_0200 = aplicar_mapeamento_e_schema(lf_0200_raw, map_0200, "EFD_0200")
            
        if "c170" in caminhos_entrada and os.path.exists(caminhos_entrada["c170"]):
            lf_c170_raw = pl.scan_parquet(caminhos_entrada["c170"])
            map_170 = {"codigo": "COD_ITEM", "descricao": "DESCR_COMPL", "unid": "UNID"}
            lf_c170 = aplicar_mapeamento_e_schema(lf_c170_raw, map_170, "EFD_C170")
            
            if lf_0200 is not None:
                lf_c170 = cruzar_c170_0200(lf_c170, lf_0200)
            lazy_frames.append(lf_c170)
        
        elif lf_0200 is not None:
             lazy_frames.append(lf_0200)

        if not lazy_frames:
            logging.warning(f"Nenhum arquivo de produto encontrado para o CNPJ {cnpj}")
            return {}

        # -------------------------------------------------------------------
        # APLICAÇÃO DE REVISÃO MANUAL
        # -------------------------------------------------------------------
        lf_base_detalhes = pl.concat(lazy_frames, how="diagonal_relaxed")
        
        if os.path.exists(path_mapa_manual):
            logging.info("Aplicando Mapa de Revisão Manual...")
            lf_manual = pl.scan_parquet(path_mapa_manual)
            
            # Join para substituir dados originais pelos revisados
            # O mapa tem: fonte, codigo_original, descricao_original, tipo_item_original, codigo_novo, descricao_nova, etc.
            lf_base_detalhes = lf_base_detalhes.join(
                lf_manual,
                left_on=["fonte", "codigo", "descricao_ori", "tipo_item"],
                right_on=["fonte", "codigo_original", "descricao_original", "tipo_item_original"],
                how="left"
            ).with_columns([
                pl.coalesce(["codigo_novo", "codigo"]).alias("codigo"),
                pl.coalesce(["descricao_nova", "descricao"]).alias("descricao"),
                pl.coalesce(["ncm_novo", "ncm"]).alias("ncm"),
                pl.coalesce(["cest_novo", "cest"]).alias("cest"),
                pl.coalesce(["gtin_novo", "gtin"]).alias("gtin"),
                pl.coalesce(["tipo_item_novo", "tipo_item"]).alias("tipo_item")
            ]).drop([
                "codigo_novo", "descricao_nova", "ncm_novo", "cest_novo", "gtin_novo", "tipo_item_novo"
            ])

        # -------------------------------------------------------------------
        # EXECUÇÃO E GRAVAÇÃO EM DISCO
        # -------------------------------------------------------------------
        logging.info("Consolidando base de detalhes...")
        path_detalhes = os.path.join(diretorio_saida, f"base_detalhes_produtos_{cnpj}.parquet")
        lf_base_detalhes.sink_parquet(path_detalhes)
        
        # Recarrega para calculos analíticos
        lf_detalhes_reloaded = pl.scan_parquet(path_detalhes)
        lf_variacoes, lf_agrupado_descricao = construir_tabelas_analiticas(lf_detalhes_reloaded)
        
        # Inserção da chave_produto baseada no índice (exigência do usuário)
        df_agrupado = lf_agrupado_descricao.collect(streaming=True)
        df_agrupado = df_agrupado.with_columns(
            pl.format("ID_{:04d}", pl.int_range(1, df_agrupado.height + 1)).alias("chave_produto")
        ).select(["chave_produto", "*"])

        # Salva Tabela 2 (Visão)
        path_agrupado = os.path.join(diretorio_saida, f"produtos_agregados_{cnpj}.parquet")
        df_agrupado.write_parquet(path_agrupado)
        logging.info(f"Tabela de Agrupamento salva: {path_agrupado}")

        # Salva Tabela 1 (Variações)
        path_variacoes = os.path.join(diretorio_saida, f"variacoes_produtos_{cnpj}.parquet")
        lf_variacoes.collect(streaming=True).write_parquet(path_variacoes)
        
        # Geração de Mapas de Auditoria (Para compatibilidade com a UI)
        if os.path.exists(path_mapa_manual):
            # Mapa de Agregados (Unificados)
            path_mapa_agregados = os.path.join(diretorio_saida, f"mapa_auditoria_agregados_{cnpj}.parquet")
            pl.read_parquet(path_mapa_manual).write_parquet(path_mapa_agregados)
            
            # Mapa de Desagregados (Mesmo arquivo, pois a UI gerencia o filtro visual)
            path_mapa_desagregados = os.path.join(diretorio_saida, f"mapa_auditoria_desagregados_{cnpj}.parquet")
            pl.read_parquet(path_mapa_manual).write_parquet(path_mapa_desagregados)

        return {
            "success": True,
            "cnpj": cnpj,
            "base_detalhes": path_detalhes,
            "produtos_agregados": path_agrupado,
            "variacoes_produtos": path_variacoes
        }

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        unificar_produtos_unidades(sys.argv[1])
    else:
        print("Módulo Produto_Unid pronto. Uso: python produto_unid.py <CNPJ>")