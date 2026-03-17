## 2024-03-24 - [Polars Python FFI Bottlenecks]
**Learning:** `map_elements` with Python lambdas (like `hashlib.sha1`) triggers massive FFI overhead per row. Native hashing isn't always viable if exact SHA-1 is expected for backwards compatibility.
**Action:** When a Python lambda MUST be used in Polars, map the elements ONLY over `.unique()` values, then `join(..., how='left')` back into the main DataFrame. This provides near-native performance (~3x to 5x faster) on datasets with repeated string permutations.

## 2024-05-15 - [Polars Python FFI Bottleneck Mitigation on Map_Elements and Replace]
**Learning:** Polars `map_elements` and even `replace_strict` with custom lambdas/dictionaries carry substantial FFI crossing overhead for repetitive strings. Specifically, calculating `descricao_normalizada` per row directly triggers significant delays.
**Action:** Extend the pattern of `select(keys).unique().with_columns(map_elements(...)).join(..., how="left")` not only for hashing but for all expensive string normalization or dictionary lookup methods over large DataFrames.
