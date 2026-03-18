import polars as pl
import time
from core.produto_classification import normalize_description_key as doc_normalize_description_key

# Generate dummy data with many duplicate descriptions
n_rows = 1_000_000
unique_descriptions = [f"PRODUTO TESTE {i} UNIDADE" for i in range(1_000)]
data = {
    "codigo": [str(i % 100) for i in range(n_rows)],
    "descricao": [unique_descriptions[i % 1000] for i in range(n_rows)]
}
df = pl.DataFrame(data)

# Method 1: direct map_elements
start1 = time.time()
res1 = df.with_columns(
    pl.col("descricao")
    .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
    .alias("descricao_normalizada")
)
end1 = time.time()

# Method 2: unique + map_elements + join
start2 = time.time()
unique_desc = df.select("descricao").unique().with_columns(
    pl.col("descricao")
    .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
    .alias("descricao_normalizada")
)
res2 = df.join(unique_desc, on="descricao", how="left")
end2 = time.time()

print(f"Direct map_elements: {end1 - start1:.4f} seconds")
print(f"Unique + join:       {end2 - start2:.4f} seconds")

# Assert correctness
assert res1["descricao_normalizada"].to_list() == res2["descricao_normalizada"].to_list()
print("Results match!")
