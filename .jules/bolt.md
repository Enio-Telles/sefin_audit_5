## 2024-03-24 - [Polars Python FFI Bottlenecks]
**Learning:** `map_elements` with Python lambdas (like `hashlib.sha1`) triggers massive FFI overhead per row. Native hashing isn't always viable if exact SHA-1 is expected for backwards compatibility.
**Action:** When a Python lambda MUST be used in Polars, map the elements ONLY over `.unique()` values, then `join(..., how='left')` back into the main DataFrame. This provides near-native performance (~3x to 5x faster) on datasets with repeated string permutations.

## 2024-03-24 - [Polars iter_rows vs to_dicts overhead]
**Learning:** `to_dicts()` eagerly materializes the entire Polars DataFrame into Python dictionary objects, incurring a heavy FFI overhead. Using `iter_rows(named=True)` acts as a lazy iterator and provides a 15-30% speedup for iterating over DataFrames when converting rows to dictionaries sequentially. Positional `iter_rows()` without `named=True` is even faster but sacrifices readability slightly.
**Action:** Replace `for row in df.to_dicts():` loops with `for row in df.iter_rows(named=True):` whenever possible in pure Python iteration bottlenecks.

## 2024-03-24 - [Python set conversion overhead in hot loops]
**Learning:** Performing `set(a)` inside heavily executed $O(N^2)$ loops (like pairwise `_jaccard` comparisons) causes significant dynamic allocation overhead.
**Action:** When creating tokenized tuples for downstream set operations (like intersections), memoize the result as a `frozenset` at generation time instead of `tuple` to skip the runtime casting step entirely, providing ~20-30% speedup per comparison.
