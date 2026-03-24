import re

with open("server/python/routers/filesystem.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Add import QueryCatalogService
import_replacement = """from core.audit_artifacts_service import obter_arquivos_auditoria
from services.query_catalog import QueryCatalogService"""
content = content.replace("from core.audit_artifacts_service import obter_arquivos_auditoria", import_replacement)

# 2. Update list_sql_queries
sql_queries_pattern = r"""        queries = \[\]
        files = \(
            \[target_path\]
            if target_path\.is_file\(\) and target_path\.suffix\.lower\(\) == "\.sql"
            else list\(target_path\.glob\("\*\.sql"\)\)
            if target_path\.is_dir\(\)
            else \[\]
        \)
        for file in files:
            try:
                sql_content = ler_sql\(file\)
                params_set = extrair_parametros_sql\(sql_content\)
                params_list = \[
                    p for p in params_set if p\.lower\(\) not in \("cnpj", "cnpj_raiz"\)
                \]
            except Exception:
                params_list = \[\]
            queries\.append\(
                \{
                    "id": str\(file\.resolve\(\)\),
                    "name": file\.stem,
                    "description": f"Arquivo SQL: \{file\.name\}",
                    "parameters": params_list,
                \}
            \)
        return \{"queries": sorted\(queries, key=lambda x: x\["name"\]\)\}"""

sql_queries_replacement = """        if target_path.is_file() and target_path.suffix.lower() == ".sql":
            # If it's a single file, QueryCatalogService processes its directory and we filter
            catalog = QueryCatalogService(target_path.parent)
            all_queries = catalog.list_queries()
            queries = [q for q in all_queries if q["caminho"] == str(target_path.resolve())]
        else:
            catalog = QueryCatalogService(target_path)
            queries = catalog.list_queries()

        # Map back to old response format to not break compatibility
        mapped_queries = []
        for q in queries:
            mapped_queries.append({
                "id": q["caminho"],
                "name": q["nome"],
                "description": q["descricao"],
                "parameters": q["parametros"],
            })

        return {"queries": sorted(mapped_queries, key=lambda x: x["name"])}"""

content = re.sub(sql_queries_pattern, sql_queries_replacement, content)


# 3. Update list_auxiliary_queries
aux_queries_pattern = r"""        queries = \[\]
        for file in target_path\.glob\("\*\.sql"\):
            try:
                sql_content = ler_sql\(file\)
                params_set = extrair_parametros_sql\(sql_content\)
                params_list = \[
                    p for p in params_set if p\.lower\(\) not in \("cnpj", "cnpj_raiz"\)
                \]
            except Exception:
                params_list = \[\]
            queries\.append\(
                \{
                    "id": str\(file\.resolve\(\)\),
                    "name": file\.stem,
                    "description": f"Tabela auxiliar: \{file\.name\}",
                    "parameters": params_list,
                \}
            \)
        sorted_queries = sorted\(queries, key=lambda x: x\["name"\]\)
        return \{"queries": sorted_queries, "count": len\(sorted_queries\)\}"""

aux_queries_replacement = """        catalog = QueryCatalogService(target_path)
        queries = catalog.list_auxiliary_queries(target_path)

        mapped_queries = []
        for q in queries:
            mapped_queries.append({
                "id": q["caminho"],
                "name": q["nome"],
                "description": q["descricao"],
                "parameters": q["parametros"],
            })

        sorted_queries = sorted(mapped_queries, key=lambda x: x["name"])
        return {"queries": sorted_queries, "count": len(sorted_queries)}"""

content = re.sub(aux_queries_pattern, aux_queries_replacement, content)


# 4. Update listar_consultas_disponiveis
consultas_pattern = r"""        if not DIR_SQL\.exists\(\):
            return \{"success": True, "consultas": \[\]\}
        sql_files = sorted\(DIR_SQL\.glob\("\*\.sql"\)\)
        return \{
            "success": True,
            "consultas": \[\{"id": f\.name, "nome": f\.stem\} for f in sql_files\],
        \}"""

consultas_replacement = """        if not DIR_SQL.exists():
            return {"success": True, "consultas": []}

        catalog = QueryCatalogService(DIR_SQL)
        queries = catalog.list_queries()

        return {
            "success": True,
            "consultas": [{"id": Path(q["caminho"]).name, "nome": q["nome"]} for q in queries],
        }"""

content = re.sub(consultas_pattern, consultas_replacement, content)

with open("server/python/routers/filesystem.py", "w", encoding="utf-8") as f:
    f.write(content)
