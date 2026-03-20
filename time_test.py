import time
import polars as pl
import os

from core.produto_runtime import doc_normalize_description_key

# Let's generate a large dataframe of repeated descriptions
N = 1000000
unique_descriptions = ["Maçã", "Banana", "Laranja", "Uva", "Melancia"] * (N // 5)
df = pl.DataFrame({"descricao": unique_descriptions})

start_time = time.time()
df.with_columns(
    pl.col("descricao")
    .map_elements(doc_normalize_description_key, return_dtype=pl.Utf8)
    .alias("descricao_normalizada")
)
end_time = time.time()
print(f"map_elements time: {end_time - start_time:.4f} seconds")

start_time = time.time()
# The Bolt trick: unique + map_elements + join
unique_df = df.select("descricao").unique().with_columns(
    pl.col("descricao").map_elements(
        doc_normalize_description_key,
        return_dtype=pl.Utf8,
    ).alias("descricao_normalizada")
)
df.join(unique_df, on="descricao", how="left")
end_time = time.time()
print(f"bolt_trick time: {end_time - start_time:.4f} seconds")

