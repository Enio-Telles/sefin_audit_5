import polars as pl
import json

path = r'c:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\sefin_audit_2\referencias\NCM\tabela_ncm.parquet'
df = pl.read_parquet(path)

# 1. Look for exact matches for 2208 or 22.08
exact = df.filter(pl.col("Codigo_NCM").str.replace_all(r"[^0-9]", "").is_in(["2208", "22.08"]))

# 2. Look for ANY row with Posicao = 2208 and non-null Descr_Posicao
with_pos_desc = df.filter(
    (pl.col("Posicao").str.replace_all(r"[^0-9]", "") == "2208") & 
    (~pl.col("Descr_Posicao").is_null())
).head(5)

# 3. Look for the Capitulo 22 row
cap22 = df.filter(pl.col("Codigo_NCM").str.replace_all(r"[^0-9]", "") == "22").head(1)

output = {
    "exact_2208": exact.to_dicts(),
    "with_pos_desc": with_pos_desc.to_dicts(),
    "cap22": cap22.to_dicts()
}

with open('ncm_debug_2208.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print("Diagnostic Done")
