import re
from pathlib import Path

file_path = Path("server/python/core/produto_runtime.py")
content = file_path.read_text()

# 1. _prepare_group_rows
old_1 = """    rows: list[dict[str, Any]] = []
    for row in df_agregados.to_dicts():
        raw_codes = str(row.get(lista_codigos_col) or "").strip() if lista_codigos_col else ""
        codes = [item.strip() for item in raw_codes.split(",") if item.strip()]
        rows.append(
            {
                "chave_produto": str(row.get(chave_col) or "").strip(),
                "descricao": str(row.get(descricao_col) or "").strip(),
                "descricao_normalizada": str(row.get(descricao_norm_col) or doc_normalize_description_key(row.get(descricao_col))).strip(),
                "ncm": str(row.get(ncm_col) or "").strip() if ncm_col else "",
                "cest": str(row.get(cest_col) or "").strip() if cest_col else "",
                "gtin": str(row.get(gtin_col) or "").strip() if gtin_col else "",
                "lista_descr_compl": str(row.get(lista_descr_compl_col) or "").strip() if lista_descr_compl_col else "",
                "codigos": codes,
                "qtd_codigos": len(codes),
                "conflitos": str(row.get(conflitos_col) or "").strip() if conflitos_col else "",
            }
        )
    return [row for row in rows if row["chave_produto"] and row["descricao"]]"""

new_1 = """    # ⚡ Bolt Optimization: Avoid to_dicts() overhead by using zip() for significantly faster row iteration
    len_df = len(df_agregados)
    chave_vals = df_agregados[chave_col].to_list() if chave_col else [None] * len_df
    descricao_vals = df_agregados[descricao_col].to_list() if descricao_col else [None] * len_df
    ncm_vals = df_agregados[ncm_col].to_list() if ncm_col else [None] * len_df
    cest_vals = df_agregados[cest_col].to_list() if cest_col else [None] * len_df
    gtin_vals = df_agregados[gtin_col].to_list() if gtin_col else [None] * len_df
    conflitos_vals = df_agregados[conflitos_col].to_list() if conflitos_col else [None] * len_df
    descricao_norm_vals = df_agregados[descricao_norm_col].to_list() if descricao_norm_col else [None] * len_df
    lista_codigos_vals = df_agregados[lista_codigos_col].to_list() if lista_codigos_col else [None] * len_df
    lista_descr_compl_vals = df_agregados[lista_descr_compl_col].to_list() if lista_descr_compl_col else [None] * len_df

    rows: list[dict[str, Any]] = []
    for chave, descricao, ncm, cest, gtin, conflitos, descricao_norm, lista_codigos, lista_descr_compl in zip(
        chave_vals, descricao_vals, ncm_vals, cest_vals, gtin_vals, conflitos_vals, descricao_norm_vals, lista_codigos_vals, lista_descr_compl_vals
    ):
        raw_codes = str(lista_codigos or "").strip() if lista_codigos_col else ""
        codes = [item.strip() for item in raw_codes.split(",") if item.strip()]
        rows.append(
            {
                "chave_produto": str(chave or "").strip(),
                "descricao": str(descricao or "").strip(),
                "descricao_normalizada": str(descricao_norm or doc_normalize_description_key(descricao)).strip(),
                "ncm": str(ncm or "").strip() if ncm_col else "",
                "cest": str(cest or "").strip() if cest_col else "",
                "gtin": str(gtin or "").strip() if gtin_col else "",
                "lista_descr_compl": str(lista_descr_compl or "").strip() if lista_descr_compl_col else "",
                "codigos": codes,
                "qtd_codigos": len(codes),
                "conflitos": str(conflitos or "").strip() if conflitos_col else "",
            }
        )
    return [row for row in rows if row["chave_produto"] and row["descricao"]]"""

content = content.replace(old_1, new_1)

# 2. _aplicar_desagregacao_codigos
old_2 = """    replacements: dict[str, str] = {}
    for row in code_groups.to_dicts():
        codigo = str(row.get("codigo") or "").strip()
        groups = [str(item or "").strip() for item in (row.get("__descricoes_norm") or []) if str(item or "").strip()]
        for index, descricao_norm in enumerate(groups, start=1):
            replacements[f"{codigo}|{descricao_norm}"] = f"{codigo}_SEPARADO_{index:02d}"]"""

new_2 = """    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() to prevent memory overhead
    replacements: dict[str, str] = {}
    for codigo_val, descricoes_norm_val in zip(code_groups["codigo"].to_list(), code_groups["__descricoes_norm"].to_list()):
        codigo = str(codigo_val or "").strip()
        groups = [str(item or "").strip() for item in (descricoes_norm_val or []) if str(item or "").strip()]
        for index, descricao_norm in enumerate(groups, start=1):
            replacements[f"{codigo}|{descricao_norm}"] = f"{codigo}_SEPARADO_{index:02d}"]"""

content = content.replace(old_2.replace(']', ''), new_2.replace(']', ''))


# 3. _normalize_mapa_descricoes_manual
old_3 = """    rows: list[dict[str, str]] = []
    for row in df.select(DESCRIPTION_MANUAL_MAP_COLUMNS).to_dicts():
        tipo_regra = _canon_text(row.get("tipo_regra"), "")
        origem = _canon_text(row.get("descricao_origem"), "")
        destino = _canon_text(row.get("descricao_destino"), "")
        descricao_par = _canon_text(row.get("descricao_par"), "")
        rows.append(
            {
                "tipo_regra": tipo_regra,
                "descricao_origem": origem,
                "descricao_destino": destino,
                "descricao_par": descricao_par,
                "hash_descricoes_key": str(
                    row.get("hash_descricoes_key")
                    or _build_description_hash(origem, destino, descricao_par, tipo_regra)
                ),
                "chave_grupo_a": _canon_text(row.get("chave_grupo_a"), ""),
                "chave_grupo_b": _canon_text(row.get("chave_grupo_b"), ""),
                "score_origem": str(row.get("score_origem") or "").strip(),
                "acao_manual": _canon_text(row.get("acao_manual"), default_acao),
            }
        )"""

new_3 = """    rows: list[dict[str, str]] = []
    # ⚡ Bolt Optimization: Extract columns and use zip() to iterate without to_dicts() memory overhead
    df_sel = df.select(DESCRIPTION_MANUAL_MAP_COLUMNS)
    for tipo_regra, origem, destino, desc_par, hash_key, grupo_a, grupo_b, score, acao in zip(
        df_sel["tipo_regra"].to_list(),
        df_sel["descricao_origem"].to_list(),
        df_sel["descricao_destino"].to_list(),
        df_sel["descricao_par"].to_list(),
        df_sel["hash_descricoes_key"].to_list(),
        df_sel["chave_grupo_a"].to_list(),
        df_sel["chave_grupo_b"].to_list(),
        df_sel["score_origem"].to_list(),
        df_sel["acao_manual"].to_list(),
    ):
        tipo_regra_str = _canon_text(tipo_regra, "")
        origem_str = _canon_text(origem, "")
        destino_str = _canon_text(destino, "")
        descricao_par_str = _canon_text(desc_par, "")
        rows.append(
            {
                "tipo_regra": tipo_regra_str,
                "descricao_origem": origem_str,
                "descricao_destino": destino_str,
                "descricao_par": descricao_par_str,
                "hash_descricoes_key": str(
                    hash_key
                    or _build_description_hash(origem_str, destino_str, descricao_par_str, tipo_regra_str)
                ),
                "chave_grupo_a": _canon_text(grupo_a, ""),
                "chave_grupo_b": _canon_text(grupo_b, ""),
                "score_origem": str(score or "").strip(),
                "acao_manual": _canon_text(acao, default_acao),
            }
        )"""
content = content.replace(old_3, new_3)

# 4. _resolve_description_unions
old_4 = """    parent: dict[str, str] = {}
    for row in df.to_dicts():
        if row.get("tipo_regra") != "UNIR_GRUPOS":
            continue
        origem = _canon_text(row.get("descricao_origem"), "")
        destino = _canon_text(row.get("descricao_destino"), "")
        if origem and destino:
            parent[origem] = destino"""
new_4 = """    parent: dict[str, str] = {}
    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() for row-wise processing
    for tipo_regra, origem_val, destino_val in zip(df["tipo_regra"].to_list(), df["descricao_origem"].to_list(), df["descricao_destino"].to_list()):
        if tipo_regra != "UNIR_GRUPOS":
            continue
        origem = _canon_text(origem_val, "")
        destino = _canon_text(destino_val, "")
        if origem and destino:
            parent[origem] = destino"""
content = content.replace(old_4, new_4)


# 5. _source_frame_rows
old_5 = """    rows: list[dict[str, Any]] = []
    for row in df.to_dicts():
        codigo = _clean_value(row.get(mappings.get("codigo", "")))
        descricao = _clean_value(row.get(mappings.get("descricao", "")))
        if not codigo or not descricao:
            continue
        tipo_item = _clean_value(row.get(mappings.get("tipo_item", "")))
        rows.append(
            {
                "fonte": fonte,
                "codigo": codigo,
                "descricao": descricao,
                "descr_compl": _clean_value(row.get(mappings.get("descr_compl", ""))),
                "tipo_item": tipo_item,
                "ncm": doc_clean_ncm(row.get(mappings.get("ncm", ""))),
                "cest": doc_clean_cest(row.get(mappings.get("cest", ""))),
                "gtin": _clean_gtin(row.get(mappings.get("gtin", ""))),
                "unid": doc_normalize_unit(row.get(mappings.get("unid", ""))),
                "codigo_original": codigo,
                "descricao_original": descricao,
                "tipo_item_original": tipo_item,
                "hash_manual_key": "",
            }
        )"""

new_5 = """    rows: list[dict[str, Any]] = []

    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() for significantly faster row iteration
    cols = []
    keys = ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]
    for key in keys:
        col_name = mappings.get(key, "")
        if col_name in df.columns:
            cols.append(df[col_name].to_list())
        else:
            cols.append([None] * len(df))

    for codigo_val, descricao_val, descr_compl_val, tipo_item_val, ncm_val, cest_val, gtin_val, unid_val in zip(*cols):
        codigo = _clean_value(codigo_val)
        descricao = _clean_value(descricao_val)
        if not codigo or not descricao:
            continue
        tipo_item = _clean_value(tipo_item_val)
        rows.append(
            {
                "fonte": fonte,
                "codigo": codigo,
                "descricao": descricao,
                "descr_compl": _clean_value(descr_compl_val),
                "tipo_item": tipo_item,
                "ncm": doc_clean_ncm(ncm_val),
                "cest": doc_clean_cest(cest_val),
                "gtin": _clean_gtin(gtin_val),
                "unid": doc_normalize_unit(unid_val),
                "codigo_original": codigo,
                "descricao_original": descricao,
                "tipo_item_original": tipo_item,
                "hash_manual_key": "",
            }
        )"""

content = content.replace(old_5, new_5)

# 6. _build_produtos_agregados
old_6 = """    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in df_base.to_dicts():
        desc_norm = doc_normalize_description_key(row.get("descricao"))
        if not desc_norm:
            continue
        buckets.setdefault(desc_norm, []).append(row)"""

new_6 = """    buckets: dict[str, list[dict[str, Any]]] = {}

    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() to prevent memory overhead
    keys = ["descricao", "descr_compl", "codigo", "ncm", "cest", "gtin", "tipo_item", "unid"]
    cols = [df_base[k].to_list() if k in df_base.columns else [None]*len(df_base) for k in keys]
    for descricao, descr_compl, codigo, ncm, cest, gtin, tipo_item, unid in zip(*cols):
        desc_norm = doc_normalize_description_key(descricao)
        if not desc_norm:
            continue
        row = {
            "descricao": descricao,
            "descr_compl": descr_compl,
            "codigo": codigo,
            "ncm": ncm,
            "cest": cest,
            "gtin": gtin,
            "tipo_item": tipo_item,
            "unid": unid,
        }
        buckets.setdefault(desc_norm, []).append(row)"""

content = content.replace(old_6, new_6)

file_path.write_text(content)
