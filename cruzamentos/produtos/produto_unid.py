"""
Modulo de Consolidacao e Unificacao de Produtos (Master Data Management).
Fachada publica do pipeline modularizado de produtos.
"""

from __future__ import annotations

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [PRODUTOS MDM] %(message)s",
)

if __package__:
    from ._produto_unid_analytics import (
        _aplicar_auto_consenso_por_descricao,
        _construir_consensos_por_descricao,
        _consenso_por_descricao_field,
        construir_tabela_codigos_multidescricao,
        construir_tabela_produtos_indexados,
        construir_tabelas_analiticas,
    )
    from ._produto_unid_manual import (
        aplicar_mapa_descricoes_manual,
        aplicar_mapa_revisao_manual,
        gerar_mapa_auditoria_descricoes_manual,
        gerar_mapas_auditoria_manual,
        merge_mapa_descricoes_manual,
    )
    from ._produto_unid_pipeline import processar_produtos_cnpj, unificar_produtos_unidades
    from ._produto_unid_shared import (
        AUTO_CONSENSO_FIELDS,
        DESCRIPTION_MANUAL_MAP_COLUMNS,
        FONTE_PIPELINE_CONFIGS,
        MANUAL_MAP_COLUMNS,
        SCHEMA_FISCAL_PRODUTOS,
        SOURCE_PRIORITY,
        _canon_expr,
        _ensure_columns_lazy,
        _hash_key_expr_from_cols,
        _lazy_empty_produtos_schema,
        _lista_valor_auditavel_expr,
        _manual_value_or_null_expr,
        _normalized_text_expr,
        _resolver_coluna_origem,
        _source_priority_expr,
        aplicar_mapeamento_e_schema,
        limpar_caracteres_especiais,
    )
else:
    from _produto_unid_analytics import (
        _aplicar_auto_consenso_por_descricao,
        _construir_consensos_por_descricao,
        _consenso_por_descricao_field,
        construir_tabela_codigos_multidescricao,
        construir_tabela_produtos_indexados,
        construir_tabelas_analiticas,
    )
    from _produto_unid_manual import (
        aplicar_mapa_descricoes_manual,
        aplicar_mapa_revisao_manual,
        gerar_mapa_auditoria_descricoes_manual,
        gerar_mapas_auditoria_manual,
        merge_mapa_descricoes_manual,
    )
    from _produto_unid_pipeline import processar_produtos_cnpj, unificar_produtos_unidades
    from _produto_unid_shared import (
        AUTO_CONSENSO_FIELDS,
        DESCRIPTION_MANUAL_MAP_COLUMNS,
        FONTE_PIPELINE_CONFIGS,
        MANUAL_MAP_COLUMNS,
        SCHEMA_FISCAL_PRODUTOS,
        SOURCE_PRIORITY,
        _canon_expr,
        _ensure_columns_lazy,
        _hash_key_expr_from_cols,
        _lazy_empty_produtos_schema,
        _lista_valor_auditavel_expr,
        _manual_value_or_null_expr,
        _normalized_text_expr,
        _resolver_coluna_origem,
        _source_priority_expr,
        aplicar_mapeamento_e_schema,
        limpar_caracteres_especiais,
    )

__all__ = [
    "AUTO_CONSENSO_FIELDS",
    "DESCRIPTION_MANUAL_MAP_COLUMNS",
    "FONTE_PIPELINE_CONFIGS",
    "MANUAL_MAP_COLUMNS",
    "SCHEMA_FISCAL_PRODUTOS",
    "SOURCE_PRIORITY",
    "_aplicar_auto_consenso_por_descricao",
    "aplicar_mapa_descricoes_manual",
    "aplicar_mapa_revisao_manual",
    "gerar_mapa_auditoria_descricoes_manual",
    "gerar_mapas_auditoria_manual",
    "merge_mapa_descricoes_manual",
    "_canon_expr",
    "_construir_consensos_por_descricao",
    "_consenso_por_descricao_field",
    "construir_tabela_codigos_multidescricao",
    "construir_tabela_produtos_indexados",
    "_ensure_columns_lazy",
    "_hash_key_expr_from_cols",
    "_lazy_empty_produtos_schema",
    "_lista_valor_auditavel_expr",
    "_manual_value_or_null_expr",
    "_normalized_text_expr",
    "_resolver_coluna_origem",
    "_source_priority_expr",
    "aplicar_mapeamento_e_schema",
    "construir_tabelas_analiticas",
    "limpar_caracteres_especiais",
    "processar_produtos_cnpj",
    "unificar_produtos_unidades",
]


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        unificar_produtos_unidades(sys.argv[1])
    else:
        print("Modulo Produto_Unid pronto. Uso: python produto_unid.py <CNPJ>")
