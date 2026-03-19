from pathlib import Path

file_path = Path("server/python/core/produto_runtime.py")
content = file_path.read_text()

old_1 = """    buckets: dict[str, list[dict[str, Any]]] = {}

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

new_1 = """    buckets: dict[str, list[dict[str, Any]]] = {}

    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() to prevent memory overhead
    # dynamically extract all columns to avoid dropping any original data
    keys = df_base.columns
    cols = [df_base[k].to_list() for k in keys]

    for row_vals in zip(*cols):
        row = dict(zip(keys, row_vals))
        desc_norm = doc_normalize_description_key(row.get("descricao"))
        if not desc_norm:
            continue
        buckets.setdefault(desc_norm, []).append(row)"""

content = content.replace(old_1, new_1)
file_path.write_text(content)
