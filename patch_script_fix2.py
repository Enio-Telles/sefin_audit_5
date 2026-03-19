from pathlib import Path

file_path = Path("server/python/core/produto_runtime.py")
content = file_path.read_text()

old_2 = """    replacements: dict[str, str] = {}
    for row in code_groups.to_dicts():
        codigo = str(row.get("codigo") or "").strip()
        groups = [str(item or "").strip() for item in (row.get("__descricoes_norm") or []) if str(item or "").strip()]
        for index, descricao_norm in enumerate(groups, start=1):
            replacements[f"{codigo}|{descricao_norm}"] = f"{codigo}_SEPARADO_{index:02d}" """

new_2 = """    # ⚡ Bolt Optimization: Use zip() instead of to_dicts() to prevent memory overhead
    replacements: dict[str, str] = {}
    for codigo_val, descricoes_norm_val in zip(code_groups["codigo"].to_list(), code_groups["__descricoes_norm"].to_list()):
        codigo = str(codigo_val or "").strip()
        groups = [str(item or "").strip() for item in (descricoes_norm_val or []) if str(item or "").strip()]
        for index, descricao_norm in enumerate(groups, start=1):
            replacements[f"{codigo}|{descricao_norm}"] = f"{codigo}_SEPARADO_{index:02d}" """

content = content.replace(old_2.strip(), new_2.strip())
file_path.write_text(content)
