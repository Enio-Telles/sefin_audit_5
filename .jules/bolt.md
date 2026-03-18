## 2024-03-24 - [Polars Python FFI Bottlenecks]
**Learning:** `map_elements` with Python lambdas (like `hashlib.sha1`) triggers massive FFI overhead per row. Native hashing isn't always viable if exact SHA-1 is expected for backwards compatibility.
**Action:** When a Python lambda MUST be used in Polars, map the elements ONLY over `.unique()` values, then `join(..., how='left')` back into the main DataFrame. This provides near-native performance (~3x to 5x faster) on datasets with repeated string permutations.

## $(date +%Y-%m-%d) - Optimize Polars string processing by memoizing function calls via joins
**Learning:** Polars `map_elements` is slow when applying a Python function over large columns with repetitive string data due to FFI overhead.
**Action:** Extract unique string values first, apply the Python function via `map_elements` on that small unique set, and use `join` to bring the results back to the original DataFrame.
