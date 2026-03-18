import time
import polars as pl
from core.produto_runtime import doc_normalize_description_key

# Create a sample DataFrame with 100,000 rows, containing some repetitive text
n = 1000000
df = pl.DataFrame({
    "descricao": [f"PRODUTO DE TESTE {i % 100}" for i in range(n)]
})

# Test 1: Current approach (map_elements over all rows)
t0 = time.time()
res1 = df.with_columns(
    pl.col("descricao").map_elements(doc_normalize_description_key, return_dtype=pl.Utf8).alias("descricao_normalizada")
)
t1 = time.time()
print(f"map_elements all rows: {t1 - t0:.4f} seconds")

# Test 2: Optimized approach (unique -> map_elements -> join)
t0 = time.time()
unique_desc = df.select("descricao").unique().with_columns(
    pl.col("descricao").map_elements(doc_normalize_description_key, return_dtype=pl.Utf8).alias("descricao_normalizada")
)
res2 = df.join(unique_desc, on="descricao", how="left")
t1 = time.time()
print(f"unique -> map_elements -> join: {t1 - t0:.4f} seconds")

# Verify correctness
assert res1["descricao_normalizada"].to_list() == res2["descricao_normalizada"].to_list()
print("Results match.")
