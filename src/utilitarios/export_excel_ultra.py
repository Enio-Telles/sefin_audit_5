
"""
Módulo para exportar DataFrame para Excel com formatação automática
baseada no tipo de tabela.

Detecta automaticamente presets para:
- tabela_descricoes
- tabela_codigos
- tabela_itens_caracteristicas / tab_itens_caract_normalizada
- c170_sped
- bloco_h_inventario
- nfe_bi_detalhe
- nfce_bi_detalhe
- nfe_dados_st_xml
- nfe_evento
- reg_0200_sped
- c176_ressarcimento
- c176_mensal_resumo
- c176_v2_analitico
- dados_cadastrais
- e111_ajustes
- fronteira_resumida
- fronteira_completo
- genérico
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from rich import print as rprint
from xlsxwriter.utility import xl_col_to_name


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

def _is_empty_df(df: Any) -> bool:
    return df.height == 0 if hasattr(df, "height") else df.empty


def _to_pandas(df: Any) -> pd.DataFrame:
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    if isinstance(df, pd.DataFrame):
        return df.copy()
    return pd.DataFrame(df)


def _sanitize_sheet_name(nome: str) -> str:
    proibidos = set(r'[]:*?/\\')
    nome = "".join("_" if c in proibidos else c for c in str(nome))
    nome = nome.strip() or "Dados"
    return nome[:31]


def _serializar_valor(v: Any) -> Any:
    if isinstance(v, (list, tuple, dict)):
        return str(v)
    import numpy as np
    if isinstance(v, np.ndarray):
        return str(v)
    if pd.isna(v):
        return None
    return v


def _serializar_listas(df_pd: pd.DataFrame) -> pd.DataFrame:
    df_pd = df_pd.copy()
    for col in df_pd.columns:
        df_pd[col] = df_pd[col].map(_serializar_valor)
    return df_pd


def _normalizar_objetos(df_pd: pd.DataFrame) -> pd.DataFrame:
    df_pd = df_pd.copy()
    for col in df_pd.columns:
        if pd.api.types.is_object_dtype(df_pd[col]):
            df_pd[col] = df_pd[col].fillna("")
    return df_pd


def _colunas_lower(df_pd: pd.DataFrame) -> set[str]:
    return {str(c).strip().lower() for c in df_pd.columns}


# -----------------------------------------------------------------------------
# Detecção de preset
# -----------------------------------------------------------------------------

def _detectar_preset(nome_base: str, df_pd: pd.DataFrame) -> str:
    nome = str(nome_base).strip().lower()
    cols = _colunas_lower(df_pd)

    # Detecção por nome - regras mais específicas primeiro
    if "nfe_dados_st" in nome or "dados_st" in nome:
        return "nfe_dados_st_xml"
    if "nfe_evento" in nome or ("evento" in nome and "nfe" in nome):
        return "nfe_evento"
    if "reg_0200" in nome or nome == "0200":
        return "reg_0200_sped"
    if "c176_mensal" in nome:
        return "c176_mensal_resumo"
    if "c176_v2" in nome:
        return "c176_v2_analitico"
    if "c176" in nome:
        return "c176_ressarcimento"
    if "dados_cadastrais" in nome or "cadast" in nome:
        return "dados_cadastrais"
    if nome == "e111" or nome.startswith("e111_"):
        return "e111_ajustes"
    if "fronteira_completo" in nome:
        return "fronteira_completo"
    if "fronteira" in nome:
        return "fronteira_resumida"
    if "tabela_descricoes" in nome:
        return "tabela_descricoes"
    if "tabela_codigos" in nome:
        return "tabela_codigos"
    if "tabela_itens_caracteristicas" in nome or "tab_itens_caract" in nome:
        return "tabela_itens_caracteristicas"
    if "bloco_h" in nome or "inventario" in nome:
        return "bloco_h_inventario"
    if "nfce" in nome:
        return "nfce_bi_detalhe"
    if nome == "nfe" or "fato_nfe" in nome or nome.startswith("nfe_"):
        return "nfe_bi_detalhe"
    if "c170" in nome:
        return "c170_sped"

    # Detecção por assinatura de colunas
    if {"descricao", "lista_chave_item_individualizado", "lista_cod_normalizado"}.issubset(cols):
        return "tabela_descricoes"

    if {"cod_normalizado", "lista_descricao", "qtd_descr"}.issubset(cols):
        return "tabela_codigos"

    if {"chave_item_individualizado", "codigo", "descricao", "lista_unidades", "fonte"}.issubset(cols):
        return "tabela_itens_caracteristicas"

    if {"periodo_efd", "chv_nfe", "num_item", "cod_item", "descr_item"}.issubset(cols):
        return "c170_sped"

    if {"dt_inv", "codigo_produto", "descricao_produto", "valor_total_inventario_h005"}.issubset(cols):
        return "bloco_h_inventario"

    if {"tipo_operacao", "chave_acesso", "prod_nitem", "prod_cprod", "prod_xprod", "co_indpres"}.issubset(cols):
        return "nfce_bi_detalhe"

    if {"tipo_operacao", "chave_acesso", "prod_nitem", "prod_cprod", "prod_xprod"}.issubset(cols):
        return "nfe_bi_detalhe"

    if {"chave_acesso", "prod_nitem", "prod_cprod", "icms_vbcst", "icms_vicmsst"}.issubset(cols):
        return "nfe_dados_st_xml"

    if {"chave_acesso", "nsu_evento", "evento_dhevento", "evento_tpevento"}.issubset(cols):
        return "nfe_evento"

    if {"periodo_efd", "cod_item", "descr_item", "cod_ncm", "tipo_item"}.issubset(cols):
        return "reg_0200_sped"

    if {"periodo_efd", "chave_saida", "cod_mot_res", "vl_ressarc_st_retido"}.issubset(cols):
        return "c176_ressarcimento"

    if {"periodo_efd", "qtd_itens_analisados_c176", "diferenca_credito_proprio", "diferenca_st_retido"}.issubset(cols):
        return "c176_mensal_resumo"

    if {"periodo_efd", "chv_nfe", "descr_item", "descricao_cst_icms", "vl_icms_st"}.issubset(cols):
        return "c176_v2_analitico"

    if {"cnpj", "ie", "nome", "situação da ie"}.issubset(cols):
        return "dados_cadastrais"

    if {"periodo_efd", "codigo_ajuste", "valor_ajuste", "descricao_codigo_ajuste"}.issubset(cols):
        return "e111_ajustes"

    if {"tipo_operacao", "chave_acesso", "num_item", "cod_item", "valor_icms_fronteira"}.issubset(cols):
        return "fronteira_resumida"

    if {"chave", "nota", "cnpj_emit", "prod_nitem", "valor_devido", "valor_pago", "situação"}.issubset(cols):
        return "fronteira_completo"

    return "generico"


# -----------------------------------------------------------------------------
# Presets
# -----------------------------------------------------------------------------

def _base_config() -> dict[str, Any]:
    return {
        "zoom": 90,
        "default_row": 18,
        "freeze_panes": (1, 0),
        "hide_gridlines": False,
        "texto_forcado": {
            "cnpj", "cpf", "codigo", "cod_normalizado", "ncm", "cest",
            "gtin", "co_sefin_inferido", "chave_item_individualizado"
        },
        "wrap_cols": {"descricao", "descr_compl", "fonte"},
        "larguras_fixas": {},
        "boolean_cols": set(),
        "integer_cols": set(),
        "decimal_cols": set(),
        "date_cols": set(),
        "datetime_cols": set(),
        "url_cols": set(),
        "highlight_rules": [],
    }


def _obter_preset_config(preset: str) -> dict[str, Any]:
    base = _base_config()

    presets = {
        "tabela_itens_caracteristicas": {
            **base,
            "zoom": 85,
            "default_row": 20,
            "wrap_cols": base["wrap_cols"] | {"lista_unidades"},
            "larguras_fixas": {
                "chave_item_individualizado": 34,
                "codigo": 16,
                "cod_normalizado": 16,
                "descricao": 42,
                "descr_compl": 28,
                "tipo_item": 14,
                "ncm": 12,
                "cest": 12,
                "gtin": 18,
                "lista_unidades": 18,
                "fonte": 14,
                "co_sefin_inferido": 14,
            },
        },
        "tabela_descricoes": {
            **base,
            "zoom": 85,
            "default_row": 22,
            "wrap_cols": base["wrap_cols"] | {
                "lista_chave_item_individualizado", "lista_cod_normalizado",
                "lista_descr_compl", "lista_tipo_item", "lista_ncm", "lista_cest",
                "lista_gtin", "lista_unids", "lista_fonte", "lista_co_sefin_inferido"
            },
            "boolean_cols": {"co_sefin_divergentes"},
            "larguras_fixas": {
                "descricao": 42,
                "lista_chave_item_individualizado": 34,
                "lista_cod_normalizado": 20,
                "lista_descr_compl": 28,
                "lista_tipo_item": 16,
                "lista_ncm": 16,
                "lista_cest": 16,
                "lista_gtin": 20,
                "lista_unids": 18,
                "lista_fonte": 14,
                "lista_co_sefin_inferido": 18,
                "co_sefin_divergentes": 12,
            },
            "highlight_rules": [
                {"type": "boolean_true", "column": "co_sefin_divergentes"}
            ],
        },
        "tabela_codigos": {
            **base,
            "zoom": 85,
            "default_row": 22,
            "wrap_cols": base["wrap_cols"] | {
                "lista_chave_item_individualizado", "lista_descricao", "cods_desagregados"
            },
            "integer_cols": {"qtd_descr"},
            "larguras_fixas": {
                "cod_normalizado": 18,
                "lista_chave_item_individualizado": 34,
                "lista_descricao": 42,
                "cods_desagregados": 28,
                "qtd_descr": 10,
            },
            "highlight_rules": [
                {"type": "greater_than", "column": "qtd_descr", "value": 1}
            ],
        },
        "c170_sped": {
            **base,
            "zoom": 82,
            "texto_forcado": base["texto_forcado"] | {
                "periodo_efd", "chv_nfe", "cod_sit", "ind_emit", "ind_oper",
                "num_doc", "cod_item", "cod_barra", "cod_ncm", "cfop", "cst_icms",
                "unid", "aliq_icms", "aliq_st"
            },
            "wrap_cols": base["wrap_cols"] | {"descr_item"},
            "integer_cols": {"num_item"},
            "decimal_cols": {"qtd", "vl_item", "vl_icms", "vl_bc_icms", "vl_bc_icms_st", "vl_icms_st"},
            "date_cols": {"dt_doc"},
            "larguras_fixas": {
                "periodo_efd": 11, "chv_nfe": 48, "cod_sit": 10, "num_doc": 16,
                "dt_doc": 12, "num_item": 10, "cod_item": 18, "cod_barra": 18,
                "cod_ncm": 12, "cest": 12, "tipo_item": 12, "descr_item": 36,
                "descr_compl": 28, "cfop": 10, "cst_icms": 10, "qtd": 12,
                "unid": 10, "vl_item": 14, "vl_icms": 14, "vl_bc_icms": 14,
                "aliq_icms": 10, "vl_bc_icms_st": 16, "vl_icms_st": 14, "aliq_st": 10,
            },
        },
        "bloco_h_inventario": {
            **base,
            "zoom": 85,
            "texto_forcado": base["texto_forcado"] | {
                "cod_mot_inv", "codigo_produto", "cod_ncm", "cod_barra",
                "tipo_item", "unidade_medida", "indicador_propriedade",
                "participante_terceiro", "cst_icms"
            },
            "wrap_cols": base["wrap_cols"] | {"mot_inv_desc", "descricao_produto", "obs_complementar"},
            "decimal_cols": {
                "valor_total_inventario_h005", "quantidade", "valor_unitario",
                "valor_item", "bc_icms", "vl_icms"
            },
            "date_cols": {"dt_inv"},
            "larguras_fixas": {
                "cnpj": 18, "dt_inv": 12, "cod_mot_inv": 12, "mot_inv_desc": 28,
                "valor_total_inventario_h005": 18, "codigo_produto": 18, "descricao_produto": 38,
                "cod_ncm": 12, "cest": 12, "cod_barra": 18, "tipo_item": 12,
                "unidade_medida": 12, "quantidade": 12, "valor_unitario": 14,
                "valor_item": 14, "indicador_propriedade": 12, "participante_terceiro": 18,
                "obs_complementar": 28, "cst_icms": 10, "bc_icms": 14, "vl_icms": 14,
            },
            "highlight_rules": [
                {"type": "not_blank", "column": "participante_terceiro"}
            ],
        },
        "nfe_bi_detalhe": {
            **base,
            "zoom": 80,
            "texto_forcado": base["texto_forcado"] | {
                "tipo_operacao", "co_destinatario", "co_emitente", "cnpj_filtro",
                "nsu", "chave_acesso", "ide_co_cuf", "ide_co_indpag", "ide_co_mod",
                "ide_serie", "nnf", "co_tp_nf", "co_iddest", "co_cmun_fg", "co_tpemis",
                "co_finnfe", "co_indfinal", "co_indpres", "co_uf_emit", "co_cad_icms_emit",
                "co_cad_icms_st", "co_crt", "co_cmun_emit", "cep_emit", "cpais_emit",
                "co_uf_dest", "co_indiedest", "co_cad_icms_dest", "co_cmun_dest",
                "cep_dest", "cpais_dest", "prod_cprod", "prod_cean", "prod_ncm",
                "prod_cest", "prod_extipi", "co_cfop", "prod_ucom", "prod_ceantrib",
                "prod_utrib", "icms_csosn", "icms_cst", "icms_modbc", "icms_modbcst",
                "icms_motdesicms", "icms_orig", "icms_ufst", "ipi_clenq", "ipi_cnpjprod",
                "ipi_cselo", "ipi_cenq", "ipi_cst", "veic_prod_tpop", "veic_prod_chassi",
                "veic_prod_ccor", "veic_prod_nserie", "veic_prod_tpcomb", "veic_prod_nmotor",
                "veic_prod_tpveic", "veic_prod_espveic", "veic_prod_vin", "veic_prod_condveic",
                "veic_prod_cmod", "veic_prod_ccordenatran", "veic_prod_tprest",
                "comb_cprodanp", "comb_codif", "comb_ufcons", "infprot_cstat", "versao",
                "prod_indescala", "prod_cnpjfab", "prod_cbenef", "med_cprodanvisa",
                "icms_cst_a", "icms_csosn_a", "seq_nitem", "status_carga_campo_fcp",
                "status_carga_campo_rem_dest", "in_versao", "email_dest", "co_indiedest_",
                "fone_dest_a8", "ibscbs"
            },
            "wrap_cols": base["wrap_cols"] | {
                "xnome_emit", "xfant_emit", "xlgr_emit", "xcpl_emit", "xbairro_emit",
                "xmun_emit", "xpais_emit", "xnome_dest", "xlgr_dest", "xcpl_dest",
                "xbairro_dest", "xmun_dest", "xpais_dest", "prod_xprod"
            },
            "integer_cols": {"prod_nitem", "seq_nitem", "veic_prod_anomod", "veic_prod_anofab"},
            "decimal_cols": {
                "prod_qcom", "prod_vuncom", "prod_vprod", "prod_qtrib", "prod_vuntrib",
                "prod_vfrete", "prod_vseg", "prod_vdesc", "prod_voutro", "icms_pbcop",
                "icms_pcredsn", "icms_pdif", "icms_picms", "icms_picmsst", "icms_pmvast",
                "icms_predbc", "icms_predbcst", "icms_vbc", "icms_vbcst", "icms_vbcstdest",
                "icms_vbcstret", "icms_vcredicmssn", "icms_vicms", "icms_vicmsdeson",
                "icms_vicmsdif", "icms_vicmsop", "icms_vicmsst", "icms_vicmsstdest",
                "icms_vicmsstret", "ipi_qselo", "ipi_vbc", "ipi_pipi", "ipi_qunid",
                "ipi_vunid", "ipi_vipi", "ii_vbc", "ii_vdespadu", "ii_vii", "ii_viof",
                "veic_prod_pot", "veic_prod_cilin", "veic_prod_pesol", "veic_prod_pesob",
                "veic_prod_cmt", "veic_prod_dist", "veic_prod_lota", "comb_pmixgn",
                "comb_qtemp", "tot_vbc", "tot_vicms", "tot_vicmsdeson", "tot_vbcst",
                "tot_vst", "tot_vprod", "tot_vfrete", "tot_vseg", "tot_vdesc",
                "tot_vii", "tot_vipi", "tot_vpis", "tot_vcofins", "tot_voutro",
                "tot_vnf", "tot_vtottrib", "icms_vbcfcp", "icms_pfcp", "icms_vfcp",
                "icms_vbcfcpst", "icms_pfcpst", "icms_vfcpst", "icms_vbcufdest",
                "icms_vbcfcpufdest", "icms_pfcpufdest", "icms_picmsufdest",
                "icms_picmsinter", "icms_picmsinterpart", "icms_vfcpufdest",
                "icms_vicmsufdest", "icms_vicmsufremet", "icms_pst", "icms_vbcfcpstret",
                "icms_pfcpstret", "icms_vfcpstret", "icms_predbcefet", "icms_vbcefet",
                "icms_picmsefet", "icms_vicmsefet", "med_vpmc", "tot_vfcpufdest",
                "tot_vicmsufdest", "tot_vicmsufremet", "tot_vfcp", "tot_vfcpst",
                "tot_vfcpstret", "tot_vipidevol", "cofins_vcofins", "cofins_vbc",
                "cofins_pcofins", "pis_vpis", "pis_vbc", "pis_ppis"
            },
            "datetime_cols": {"dhemi", "dhsaient", "dt_gravacao"},
            "larguras_fixas": {
                "tipo_operacao": 14, "co_destinatario": 18, "co_emitente": 18,
                "cnpj_filtro": 18, "nsu": 16, "chave_acesso": 48, "prod_nitem": 10,
                "ide_co_mod": 10, "ide_serie": 10, "nnf": 14, "dhemi": 20,
                "dhsaient": 20, "xnome_emit": 28, "xnome_dest": 28, "prod_cprod": 18,
                "prod_cean": 18, "prod_xprod": 42, "prod_ncm": 12, "prod_cest": 12,
                "co_cfop": 10, "prod_ucom": 10, "prod_qcom": 12, "prod_vuncom": 14,
                "prod_vprod": 14, "icms_csosn": 10, "icms_cst": 10, "icms_vbc": 14,
                "icms_vbcst": 14, "icms_vicms": 14, "icms_vicmsst": 14, "ipi_vbc": 14,
                "ipi_vipi": 14, "tot_vbc": 14, "tot_vbcst": 14, "tot_vprod": 14,
                "tot_vnf": 14, "infprot_cstat": 12, "versao": 10, "dt_gravacao": 20,
                "email_dest": 24,
            },
            "highlight_rules": [
                {"type": "greater_than", "column": "prod_nitem", "value": 1}
            ],
        },
        "nfce_bi_detalhe": {
            **base,
            "zoom": 80,
            "texto_forcado": base["texto_forcado"] | {
                "tipo_operacao", "co_destinatario", "co_emitente", "cnpj_filtro",
                "nsu", "chave_acesso", "ide_co_cuf", "ide_co_indpag", "ide_co_mod",
                "ide_serie", "nnf", "co_tp_nf", "co_iddest", "co_cmun_fg", "co_tpemis",
                "co_finnfe", "co_indpres", "co_indfinal", "co_uf_emit", "co_cad_icms_emit",
                "co_crt", "co_cmun_emit", "cep_emit", "cpais_emit", "co_uf_dest",
                "co_indiedest", "co_cmun_dest", "cep_dest", "cpais_dest", "prod_cprod",
                "prod_cean", "prod_ncm", "prod_cest", "co_cfop", "prod_ucom",
                "prod_ceantrib", "prod_utrib", "icms_csosn", "icms_cst", "icms_modbc",
                "icms_modbcst", "icms_motdesicms", "icms_orig", "icms_ufst",
                "infprot_cstat", "icms_cst_a", "icms_csosn_a", "seq_nitem",
                "status_carga_campo_fcp"
            },
            "wrap_cols": base["wrap_cols"] | {
                "xnome_emit", "xfant_emit", "xlgr_emit", "xcpl_emit", "xbairro_emit",
                "xmun_emit", "xpais_emit", "xnome_dest", "xlgr_dest", "xcpl_dest",
                "xbairro_dest", "xmun_dest", "xpais_dest", "prod_xprod"
            },
            "integer_cols": {"prod_nitem", "seq_nitem"},
            "decimal_cols": {
                "prod_qcom", "prod_vuncom", "prod_vprod", "prod_qtrib", "prod_vuntrib",
                "prod_vfrete", "prod_vseg", "prod_vdesc", "prod_voutro", "icms_pbcop",
                "icms_pcredsn", "icms_pdif", "icms_picms", "icms_picmsst", "icms_pmvast",
                "icms_predbc", "icms_predbcst", "icms_vbc", "icms_vbcst", "icms_vbcstdest",
                "icms_vbcstret", "icms_vcredicmssn", "icms_vicms", "icms_vicmsdeson",
                "icms_vicmsdif", "icms_vicmsop", "icms_vicmsst", "icms_vicmsstdest",
                "icms_vicmsstret", "icms_vbcfcp", "icms_pfcp", "icms_vfcp",
                "icms_vbcfcpst", "icms_pfcpst", "icms_vfcpst", "icms_vbcufdest",
                "icms_vbcfcpufdest", "icms_pfcpufdest", "icms_picmsufdest",
                "icms_picmsinter", "icms_picmsinterpart", "icms_vfcpufdest",
                "icms_vicmsufdest", "icms_vicmsufremet", "icms_pst", "icms_vbcfcpstret",
                "icms_pfcpstret", "icms_vfcpstret", "icms_predbcefet", "icms_vbcefet",
                "icms_picmsefet", "icms_vicmsefet", "tot_vbc", "tot_vicms",
                "tot_vicmsdeson", "tot_vbcst", "tot_vst", "tot_vprod", "tot_vfrete",
                "tot_vseg", "tot_vdesc", "tot_vii", "tot_vipi", "tot_vpis",
                "tot_vcofins", "tot_voutro", "tot_vnf", "tot_vtottrib", "tot_vfcpufdest",
                "tot_vicmsufdest", "tot_vicmsufremet", "tot_vfcp", "tot_vfcpst",
                "tot_vfcpstret", "tot_vipidevol"
            },
            "datetime_cols": {"dhemi", "dt_gravacao"},
            "larguras_fixas": {
                "tipo_operacao": 14, "co_destinatario": 18, "co_emitente": 18,
                "cnpj_filtro": 18, "nsu": 16, "chave_acesso": 48, "prod_nitem": 10,
                "ide_co_mod": 10, "ide_serie": 10, "nnf": 14, "dhemi": 20,
                "xnome_emit": 28, "xnome_dest": 28, "prod_cprod": 18, "prod_cean": 18,
                "prod_xprod": 42, "prod_ncm": 12, "prod_cest": 12, "co_cfop": 10,
                "prod_ucom": 10, "prod_qcom": 12, "prod_vuncom": 14, "prod_vprod": 14,
                "icms_csosn": 10, "icms_cst": 10, "icms_vbc": 14, "icms_vbcst": 14,
                "icms_vicms": 14, "icms_vicmsst": 14, "tot_vbc": 14, "tot_vbcst": 14,
                "tot_vprod": 14, "tot_vnf": 14, "infprot_cstat": 12, "dt_gravacao": 20,
            },
            "highlight_rules": [
                {"type": "equals", "column": "ide_co_mod", "value": "65"},
                {"type": "greater_than", "column": "prod_nitem", "value": 1},
            ],
        },
        "nfe_dados_st_xml": {
            **base,
            "zoom": 88,
            "texto_forcado": base["texto_forcado"] | {"chave_acesso", "prod_cprod"},
            "integer_cols": {"prod_nitem"},
            "decimal_cols": {
                "icms_vbcst", "icms_vicmsst", "icms_vicmssubstituto", "icms_vicmsstret",
                "icms_vbcfcpst", "icms_pfcpst", "icms_vfcpst"
            },
            "larguras_fixas": {
                "chave_acesso": 48, "prod_nitem": 10, "prod_cprod": 18,
                "icms_vbcst": 14, "icms_vicmsst": 14, "icms_vicmssubstituto": 18,
                "icms_vicmsstret": 16, "icms_vbcfcpst": 14, "icms_pfcpst": 12,
                "icms_vfcpst": 14,
            },
            "highlight_rules": [
                {"type": "greater_than", "column": "icms_vfcpst", "value": 0},
                {"type": "greater_than", "column": "icms_vicmsstret", "value": 0},
            ],
        },
        "nfe_evento": {
            **base,
            "zoom": 90,
            "texto_forcado": base["texto_forcado"] | {"chave_acesso", "nsu_evento", "evento_tpevento"},
            "wrap_cols": base["wrap_cols"] | {"evento_descevento"},
            "datetime_cols": {"evento_dhevento"},
            "larguras_fixas": {
                "chave_acesso": 48, "nsu_evento": 16, "evento_dhevento": 20,
                "evento_tpevento": 14, "evento_descevento": 34,
            },
        },
        "reg_0200_sped": {
            **base,
            "zoom": 85,
            "default_row": 20,
            "texto_forcado": base["texto_forcado"] | {
                "periodo_efd", "cod_item", "cod_ant_item", "r0205_cod_ant_item",
                "unid_inv", "unid_conv", "cod_barra", "cod_ncm", "cest",
                "tipo_item", "cod_gen", "cod_fin_efd"
            },
            "wrap_cols": base["wrap_cols"] | {
                "descr_item", "descr_ant_item", "desc_tipo_item", "descricao_cod_gen"
            },
            "decimal_cols": {"aliq_icms", "fat_conv"},
            "date_cols": {"dt_ini_ant_item", "dt_fim_ant_item", "data_entrega_efd_periodo"},
            "larguras_fixas": {
                "periodo_efd": 10, "cod_item": 18, "cod_ant_item": 18, "r0205_cod_ant_item": 18,
                "descr_item": 40, "aliq_icms": 10, "unid_inv": 10, "descr_ant_item": 30,
                "dt_ini_ant_item": 12, "dt_fim_ant_item": 12, "unid_conv": 10,
                "fat_conv": 12, "cod_barra": 18, "cod_ncm": 12, "cest": 12,
                "tipo_item": 10, "desc_tipo_item": 26, "cod_gen": 10, "descricao_cod_gen": 40,
                "cod_fin_efd": 10, "data_entrega_efd_periodo": 12,
            },
            "highlight_rules": [
                {"type": "not_blank", "column": "descr_ant_item"},
                {"type": "not_blank", "column": "unid_conv"},
            ],
        },
        "c176_ressarcimento": {
            **base,
            "zoom": 84,
            "texto_forcado": base["texto_forcado"] | {
                "periodo_efd", "cod_fin_efd", "finalidade_efd", "chave_saida",
                "num_nf_saida", "cod_item", "num_item_saida", "cfop_saida", "unid_saida",
                "cod_mot_res", "chave_nfe_ultima_entrada", "c176_num_item_ult_e_declarado"
            },
            "wrap_cols": base["wrap_cols"] | {"descricao_item", "descricao_motivo_ressarcimento"},
            "decimal_cols": {
                "qtd_item_saida", "vl_total_item", "vl_unit_bc_st_entrada",
                "vl_unit_icms_proprio_entrada", "vl_unit_ressarcimento_st",
                "vl_ressarc_credito_proprio", "vl_ressarc_st_retido", "vr_total_ressarcimento"
            },
            "date_cols": {"data_entrega_efd_periodo", "dt_doc_saida", "dt_e_s_saida", "dt_ultima_entrada"},
            "larguras_fixas": {
                "periodo_efd": 10, "data_entrega_efd_periodo": 12, "cod_fin_efd": 10,
                "finalidade_efd": 18, "chave_saida": 48, "num_nf_saida": 14,
                "dt_doc_saida": 12, "dt_e_s_saida": 12, "cod_item": 18, "descricao_item": 34,
                "num_item_saida": 10, "cfop_saida": 10, "unid_saida": 10, "qtd_item_saida": 12,
                "vl_total_item": 14, "cod_mot_res": 10, "descricao_motivo_ressarcimento": 28,
                "chave_nfe_ultima_entrada": 48, "c176_num_item_ult_e_declarado": 12,
                "dt_ultima_entrada": 12, "vl_unit_bc_st_entrada": 14,
                "vl_unit_icms_proprio_entrada": 16, "vl_unit_ressarcimento_st": 16,
                "vl_ressarc_credito_proprio": 18, "vl_ressarc_st_retido": 16,
                "vr_total_ressarcimento": 18,
            },
        },
        "c176_mensal_resumo": {
            **base,
            "zoom": 88,
            "texto_forcado": base["texto_forcado"] | {"cnpj", "periodo_efd"},
            "integer_cols": {"qtd_itens_analisados_c176"},
            "decimal_cols": {
                "total_ressarc_credito_proprio", "total_ajuste_credito_proprio_e111",
                "diferenca_credito_proprio", "total_ressarc_st_retido",
                "total_ajuste_st_retido_e111", "diferenca_st_retido",
                "total_ajuste_ro020050_e111", "total_ajuste_ro020048_e111"
            },
            "larguras_fixas": {
                "cnpj": 18, "periodo_efd": 10, "qtd_itens_analisados_c176": 12,
                "total_ressarc_credito_proprio": 18, "total_ajuste_credito_proprio_e111": 18,
                "diferenca_credito_proprio": 16, "total_ressarc_st_retido": 18,
                "total_ajuste_st_retido_e111": 18, "diferenca_st_retido": 16,
                "total_ajuste_ro020050_e111": 18, "total_ajuste_ro020048_e111": 18,
            },
            "highlight_rules": [
                {"type": "not_equal_zero", "column": "diferenca_credito_proprio"},
                {"type": "not_equal_zero", "column": "diferenca_st_retido"},
            ],
        },
        "c176_v2_analitico": {
            **base,
            "zoom": 82,
            "texto_forcado": base["texto_forcado"] | {
                "cod_fin", "descricao_fin", "periodo_efd", "c100_reg", "cod_sit",
                "ind_oper", "oper", "ind_emit", "descricao_ind_emit", "chv_nfe",
                "num_doc", "cod_part", "c170_reg", "num_item", "cod_item", "cod",
                "cod_barra", "tipo_item", "cod_gen", "cod_ncm", "cest", "segmento_cest",
                "cfop", "cod_nat", "cst_icms", "ind_mov", "unid_inv", "unid",
                "cst_ipi", "cod_enq", "cod_cta"
            },
            "wrap_cols": base["wrap_cols"] | {
                "descricao_fin", "descricao_cod_sit", "descr_item", "descricao_tipo_item",
                "descricao_cod_gen", "no_segmento", "descricao_cfop", "descricao_cod_nat",
                "descricao_cst_icms", "descricao_ind_mov"
            },
            "integer_cols": {"ano_efd", "num_item"},
            "decimal_cols": {
                "aliq_icms", "qtd", "vl_item", "vl_desc", "vl_bc_icms", "vl_icms",
                "vl_bc_icms_st", "aliq_st", "vl_icms_st", "vl_bc_ipi", "aliq_ipi",
                "vl_ipi", "vl_abat_nt"
            },
            "date_cols": {"dt_ultima_entrega", "dt_doc", "dt_e_s"},
            "larguras_fixas": {
                "dt_ultima_entrega": 12, "cod_fin": 10, "descricao_fin": 24, "ano_efd": 10,
                "periodo_efd": 10, "c100_reg": 10, "cod_sit": 10, "descricao_cod_sit": 30,
                "ind_oper": 10, "oper": 12, "ind_emit": 10, "descricao_ind_emit": 18,
                "chv_nfe": 48, "num_doc": 16, "cod_part": 16, "dt_doc": 12, "dt_e_s": 12,
                "c170_reg": 10, "num_item": 10, "cod_item": 18, "cod": 18, "cod_barra": 18,
                "descr_item": 40, "tipo_item": 10, "descricao_tipo_item": 24, "cod_gen": 10,
                "descricao_cod_gen": 24, "cod_ncm": 12, "cest": 12, "segmento_cest": 12,
                "no_segmento": 20, "cfop": 10, "descricao_cfop": 24, "cod_nat": 12,
                "descricao_cod_nat": 24, "cst_icms": 10, "descricao_cst_icms": 20,
                "aliq_icms": 10, "ind_mov": 10, "descricao_ind_mov": 18, "unid_inv": 10,
                "unid": 10, "qtd": 12, "vl_item": 14, "vl_desc": 14, "vl_bc_icms": 14,
                "vl_icms": 14, "vl_bc_icms_st": 14, "aliq_st": 10, "vl_icms_st": 14,
                "cst_ipi": 10, "cod_enq": 12, "vl_bc_ipi": 14, "aliq_ipi": 10,
                "vl_ipi": 14, "cod_cta": 14, "vl_abat_nt": 14,
            },
        },
        "dados_cadastrais": {
            **base,
            "zoom": 90,
            "texto_forcado": base["texto_forcado"] | {
                "cnpj", "ie", "uf", "regime de pagamento", "situação da ie"
            },
            "wrap_cols": base["wrap_cols"] | {
                "nome", "nome fantasia", "endereço", "município", "regime de pagamento", "situação da ie"
            },
            "date_cols": {"data de início da atividade", "data da última situação"},
            "url_cols": {"redesim"},
            "larguras_fixas": {
                "cnpj": 18, "ie": 16, "nome": 30, "nome fantasia": 24, "endereço": 36,
                "município": 18, "uf": 8, "regime de pagamento": 24, "situação da ie": 24,
                "data de início da atividade": 14, "data da última situação": 14,
                "período em atividade": 18, "redesim": 40,
            },
        },
        "e111_ajustes": {
            **base,
            "zoom": 88,
            "texto_forcado": base["texto_forcado"] | {"periodo_efd", "codigo_ajuste", "cod_fin_efd"},
            "wrap_cols": base["wrap_cols"] | {"descricao_codigo_ajuste", "descr_compl"},
            "decimal_cols": {"valor_ajuste"},
            "date_cols": {"data_entrega_efd_periodo"},
            "larguras_fixas": {
                "periodo_efd": 10, "codigo_ajuste": 14, "descricao_codigo_ajuste": 30,
                "descr_compl": 40, "valor_ajuste": 14, "data_entrega_efd_periodo": 12,
                "cod_fin_efd": 10,
            },
        },
        "fronteira_resumida": {
            **base,
            "zoom": 86,
            "texto_forcado": base["texto_forcado"] | {
                "tipo_operacao", "chave_acesso", "cod_item", "ncm", "cest",
                "co_sefin", "cod_rotina_calculo"
            },
            "wrap_cols": base["wrap_cols"] | {"desc_item"},
            "integer_cols": {"num_item"},
            "decimal_cols": {
                "qtd_comercial", "valor_produto", "bc_icms_st_destacado",
                "icms_st_destacado", "valor_icms_fronteira"
            },
            "larguras_fixas": {
                "tipo_operacao": 14, "chave_acesso": 48, "num_item": 10, "cod_item": 18,
                "desc_item": 34, "ncm": 12, "cest": 12, "qtd_comercial": 12, "valor_produto": 14,
                "bc_icms_st_destacado": 16, "icms_st_destacado": 16, "co_sefin": 12,
                "cod_rotina_calculo": 14, "valor_icms_fronteira": 16,
            },
            "highlight_rules": [
                {"type": "greater_than", "column": "valor_icms_fronteira", "value": 0}
            ],
        },
        "fronteira_completo": {
            **base,
            "zoom": 80,
            "texto_forcado": base["texto_forcado"] | {
                "chave", "nota", "cnpj_emit", "uf_emitente", "comando", "prod_nitem",
                "co_cfop", "ncm", "prod_ucom", "receita", "guia", "situação", "co_sefin"
            },
            "wrap_cols": base["wrap_cols"] | {"nome_emit", "prod_xprod", "nome_co_sefin", "situação"},
            "integer_cols": {"prod_nitem"},
            "decimal_cols": {
                "prod_qcom", "prod_vuncom", "prod_vprod", "prod_vfrete", "prod_vdesc",
                "prod_voutro", "prod_vseg", "total_produto", "icms_vbc", "icms_picms",
                "icms_vicms", "icms_vbcst", "icms_vicmsst", "valor_devido", "valor_pago",
                "vl_merc", "vl_bc_merc", "aliq", "vl_tot_deb", "vl_tot_cred", "vl_icms",
                "it_pc_aliquota_interna", "it_pc_aliquota_origem", "it_pc_agregacao_interna",
                "it_pc_interna"
            },
            "date_cols": {"emissao", "entrada"},
            "larguras_fixas": {
                "chave": 48, "nota": 14, "cnpj_emit": 18, "nome_emit": 28, "uf_emitente": 10,
                "emissao": 12, "entrada": 12, "comando": 14, "prod_nitem": 10, "prod_xprod": 34,
                "co_cfop": 10, "ncm": 12, "prod_ucom": 10, "prod_qcom": 12, "prod_vuncom": 14,
                "prod_vprod": 14, "prod_vfrete": 14, "prod_vdesc": 14, "prod_voutro": 14,
                "prod_vseg": 14, "total_produto": 14, "icms_vbc": 14, "icms_picms": 10,
                "icms_vicms": 14, "icms_vbcst": 14, "icms_vicmsst": 14, "receita": 12,
                "guia": 14, "valor_devido": 14, "valor_pago": 14, "situação": 28, "co_sefin": 12,
                "nome_co_sefin": 28, "vl_merc": 14, "vl_bc_merc": 14, "aliq": 10, "vl_tot_deb": 14,
                "vl_tot_cred": 14, "vl_icms": 14,
            },
            "highlight_rules": [
                {"type": "equals", "column": "situação", "value": "VERIFICAR"},
                {"type": "compare_columns", "left": "valor_pago", "op": "<", "right": "valor_devido"},
            ],
        },
        "generico": base,
    }
    return presets.get(preset, base)


# -----------------------------------------------------------------------------
# Formatação
# -----------------------------------------------------------------------------

def _criar_formatos(workbook):
    return {
        "padrao": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top"
        }),
        "cabecalho": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "bold": True,
            "text_wrap": True, "align": "center", "valign": "vcenter", "border": 1
        }),
        "texto": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "num_format": "@"
        }),
        "wrap": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "text_wrap": True
        }),
        "inteiro": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "align": "right", "num_format": "0"
        }),
        "decimal": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "align": "right", "num_format": "#,##0.00"
        }),
        "data": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "align": "center", "num_format": "dd/mm/yyyy"
        }),
        "data_hora": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "align": "center", "num_format": "dd/mm/yyyy hh:mm:ss"
        }),
        "booleano": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top", "align": "center"
        }),
        "url": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "valign": "top",
            "font_color": "blue", "underline": 1
        }),
        "destaque": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "bg_color": "#FFF2CC"
        }),
        "destaque_forte": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "bg_color": "#FCE4D6"
        }),
        "destaque_suave": workbook.add_format({
            "font_name": "Arial", "font_size": 8, "bg_color": "#E2F0D9"
        }),
    }


def _largura_auto(serie: pd.Series, header: str, minimo: int = 10, maximo: int = 60) -> int:
    try:
        max_len_dados = int(serie.astype(str).map(len).max()) if len(serie) else 0
    except Exception:
        max_len_dados = 0
    largura = max(max_len_dados, len(str(header))) + 2
    largura = min(largura, maximo)
    largura = max(largura, minimo)
    return largura


def _escolher_formato(col_lower: str, dtype: str, cfg: dict[str, Any], formatos: dict[str, Any]):
    if col_lower in cfg["datetime_cols"] or "datetime64" in dtype:
        return formatos["data_hora"]
    if col_lower in cfg["date_cols"]:
        return formatos["data"]
    if col_lower in cfg["wrap_cols"] or col_lower.startswith("lista_"):
        return formatos["wrap"]
    if col_lower in cfg["url_cols"]:
        return formatos["url"]
    if (
        col_lower in cfg["texto_forcado"]
        or any(chave in col_lower for chave in ["codigo", "ncm", "cest", "gtin", "chave", "sefin"])
    ):
        return formatos["texto"]
    if col_lower in cfg["boolean_cols"]:
        return formatos["booleano"]
    if col_lower in cfg["integer_cols"]:
        return formatos["inteiro"]
    if col_lower in cfg["decimal_cols"]:
        return formatos["decimal"]
    if "float" in dtype:
        return formatos["decimal"]
    if "int" in dtype:
        return formatos["inteiro"]
    return formatos["padrao"]


def _aplicar_condicional(worksheet, df_pd: pd.DataFrame, cfg: dict[str, Any], formatos: dict[str, Any]):
    if df_pd.empty:
        return

    mapa_cols = {str(c).strip().lower(): i for i, c in enumerate(df_pd.columns)}
    ultima_linha = len(df_pd)
    ultima_coluna = len(df_pd.columns) - 1

    for regra in cfg.get("highlight_rules", []):
        regra_tipo = regra.get("type")
        if regra_tipo == "compare_columns":
            left = str(regra.get("left", "")).lower()
            right = str(regra.get("right", "")).lower()
            if left not in mapa_cols or right not in mapa_cols:
                continue
            left_col = xl_col_to_name(mapa_cols[left])
            right_col = xl_col_to_name(mapa_cols[right])
            op = regra.get("op", "<")
            formula = f'=${left_col}2{op}${right_col}2'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_forte"],
            })
            continue

        col_lower = str(regra.get("column", "")).lower()
        if col_lower not in mapa_cols:
            continue

        idx = mapa_cols[col_lower]
        letra = xl_col_to_name(idx)

        if regra_tipo == "boolean_true":
            formula = f'=${letra}2=TRUE'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque"],
            })
        elif regra_tipo == "greater_than":
            valor = regra.get("value", 0)
            formula = f'=${letra}2>{valor}'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_forte"],
            })
        elif regra_tipo == "not_blank":
            formula = f'=${letra}2<>""'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_suave"],
            })
        elif regra_tipo == "equals":
            valor = str(regra.get("value", "")).replace('"', '""')
            formula = f'=${letra}2="{valor}"'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_suave"],
            })
        elif regra_tipo == "not_equal_zero":
            formula = f'=AND(${letra}2<>"",${letra}2<>0)'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_forte"],
            })


def _aplicar_links_url(worksheet, df_pd: pd.DataFrame, cfg: dict[str, Any], formatos: dict[str, Any]):
    if df_pd.empty or not cfg.get("url_cols"):
        return

    mapa_cols = {str(c).strip().lower(): i for i, c in enumerate(df_pd.columns)}
    for col_lower in cfg["url_cols"]:
        if col_lower not in mapa_cols:
            continue
        col_idx = mapa_cols[col_lower]
        for row_idx, valor in enumerate(df_pd.iloc[:, col_idx], start=1):
            if isinstance(valor, str) and valor.strip().lower().startswith(("http://", "https://")):
                worksheet.write_url(row_idx, col_idx, valor, formatos["url"], string=valor)


# -----------------------------------------------------------------------------
# Função principal
# -----------------------------------------------------------------------------

def exportar_excel(
    df: Any,
    nome_base: str,
    diretorio_saida: Path,
    nome_aba: str | None = None,
    preset: str | None = None,
) -> Path | None:
    """
    Exporta DataFrame para Excel com preset automático de formatação.

    Args:
        df: DataFrame Pandas ou Polars
        nome_base: nome base do arquivo (sem extensão)
        diretorio_saida: pasta de saída
        nome_aba: nome da aba; se omitido, usa o preset detectado
        preset: força um preset específico; se omitido, detecta automaticamente

    Returns:
        Path do arquivo gerado, ou None se o DataFrame estiver vazio.
    """
    if _is_empty_df(df):
        rprint("[yellow]=> Sem resultados. Arquivo Excel não gerado.[/yellow]")
        return None

    diretorio_saida.mkdir(parents=True, exist_ok=True)
    arquivo_excel = diretorio_saida / f"{nome_base}.xlsx"

    df_pd = _to_pandas(df)
    df_pd = _serializar_listas(df_pd)
    df_pd = _normalizar_objetos(df_pd)

    preset_detectado = preset or _detectar_preset(nome_base, df_pd)
    cfg = _obter_preset_config(preset_detectado)
    nome_aba = _sanitize_sheet_name(nome_aba or preset_detectado or "Dados")

    with pd.ExcelWriter(arquivo_excel, engine="xlsxwriter") as writer:
        df_pd.to_excel(writer, sheet_name=nome_aba, index=False)

        workbook = writer.book
        worksheet = writer.sheets[nome_aba]
        formatos = _criar_formatos(workbook)

        if cfg.get("freeze_panes"):
            worksheet.freeze_panes(*cfg["freeze_panes"])

        worksheet.autofilter(0, 0, len(df_pd), len(df_pd.columns) - 1)
        worksheet.set_default_row(cfg.get("default_row", 18))
        worksheet.set_zoom(cfg.get("zoom", 90))

        if cfg.get("hide_gridlines"):
            worksheet.hide_gridlines(2)

        for col_idx, col_name in enumerate(df_pd.columns):
            worksheet.write(0, col_idx, col_name, formatos["cabecalho"])

        for col_idx, col_name in enumerate(df_pd.columns):
            col_lower = str(col_name).strip().lower()
            dtype_str = str(df_pd[col_name].dtype).lower()

            largura = cfg["larguras_fixas"].get(col_lower)
            if largura is None:
                if col_lower in cfg["wrap_cols"] or col_lower.startswith("lista_"):
                    largura = _largura_auto(df_pd[col_name], col_name, minimo=16, maximo=42)
                elif col_lower in cfg["texto_forcado"] or col_lower in cfg["url_cols"]:
                    largura = _largura_auto(df_pd[col_name], col_name, minimo=12, maximo=42)
                elif col_lower in cfg["date_cols"] or col_lower in cfg["datetime_cols"]:
                    largura = _largura_auto(df_pd[col_name], col_name, minimo=12, maximo=20)
                else:
                    largura = _largura_auto(df_pd[col_name], col_name, minimo=10, maximo=30)

            fmt = _escolher_formato(col_lower, dtype_str, cfg, formatos)
            worksheet.set_column(col_idx, col_idx, largura, fmt)

        _aplicar_condicional(worksheet, df_pd, cfg, formatos)
        _aplicar_links_url(worksheet, df_pd, cfg, formatos)

    rprint(
        f"[green]   => Relatório Excel exportado:[/green] {arquivo_excel.name} "
        f"[cyan](preset: {preset_detectado})[/cyan]"
    )
    return arquivo_excel


__all__ = ["exportar_excel"]
