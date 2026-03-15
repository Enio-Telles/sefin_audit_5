## 2024-03-24 - [Polars Python FFI Bottlenecks]
**Learning:** `map_elements` with Python lambdas (like `hashlib.sha1`) triggers massive FFI overhead per row. Native hashing isn't always viable if exact SHA-1 is expected for backwards compatibility.
**Action:** When a Python lambda MUST be used in Polars, map the elements ONLY over `.unique()` values, then `join(..., how='left')` back into the main DataFrame. This provides near-native performance (~3x to 5x faster) on datasets with repeated string permutations.
