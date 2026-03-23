import polars as pl
from pathlib import Path
from datetime import datetime

def obter_arquivos_auditoria(cnpj_limpo: str, dir_parquet: Path, dir_analises: Path, dir_relatorios: Path) -> dict:
    def _list_files(d: Path, pattern: str) -> list[dict]:
        if not d.exists() or not d.is_dir():
            return []
        out = []
        for f in d.glob(pattern):
            if f.is_file():
                st = f.stat()
                item = {
                    "name": f.name,
                    "path": str(f.resolve()),
                    "size": st.st_size,
                    "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
                }
                if f.suffix == ".parquet":
                    try:
                        # Tenta ler schema e row_count do parquet de forma eficiente
                        schema = pl.scan_parquet(str(f)).collect_schema()
                        row_count = pl.scan_parquet(str(f)).select(pl.len()).collect().item()
                        item["columns"] = len(schema.names())
                        item["rows"] = row_count
                    except Exception:
                        pass # Fallback: envia sem rows/columns

                # Para arquivos de relatório, tenta inferir tipo
                if d == dir_relatorios:
                    if f.suffix == ".docx":
                        item["tipo"] = "Word (DOCX)"
                    elif f.suffix in (".txt", ".html"):
                        item["tipo"] = "Texto/HTML"
                    else:
                        item["tipo"] = "Outro"

                # add query derived from name (e.g. name_00000000000191.parquet -> name)
                if f.stem.endswith(f"_{cnpj_limpo}"):
                    item["query"] = f.stem.replace(f"_{cnpj_limpo}", "")
                out.append(item)
        return sorted(out, key=lambda x: x["name"])

    # Separar análises gerais de arquivos de produtos
    all_analises = _list_files(dir_analises, "*.parquet")
    arquivos_analises = []
    arquivos_produtos = []

    for f in all_analises:
        name = f["name"]
        if name.startswith("produtos_agregados_"):
            f["analise"] = "Tabela Final"
            arquivos_produtos.append(f)
        elif name.startswith("status_analise_produtos_"):
            f["analise"] = "Status de Analise"
            arquivos_produtos.append(f)
        elif name.startswith("base_detalhes_produtos_"):
            f["analise"] = "Base Detalhes"
            arquivos_produtos.append(f)
        elif name.startswith("mapa_auditoria_"):
            if "desagregados" in name:
                f["analise"] = "Mapa de Desagregados"
                arquivos_produtos.append(f)
            elif "agregados" in name:
                f["analise"] = "Mapa de Agregados"
                arquivos_produtos.append(f)
            else:
                arquivos_analises.append(f)
        elif "ressarcimento" in name:
            f["analise"] = "Ressarcimento C176"
            arquivos_analises.append(f)
        elif "resumo_mensal" in name:
            f["analise"] = "Resumo Mensal C176"
            arquivos_analises.append(f)
        elif "omissao" in name:
            f["analise"] = "Omissão de Saída"
            arquivos_analises.append(f)
        else:
            arquivos_analises.append(f)

    return {
        "arquivos_extraidos": _list_files(dir_parquet, "*.parquet"),
        "arquivos_analises": arquivos_analises,
        "arquivos_produtos": arquivos_produtos,
        "arquivos_relatorios": _list_files(dir_relatorios, "*.*"),
    }
