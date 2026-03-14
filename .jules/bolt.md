## 2024-05-18 - Replacing map_elements with Native Polars String Expressions
**Learning:** Polars' `map_elements` applying a Python lambda function crosses the Rust-Python FFI boundary per row, causing a severe performance bottleneck.
**Action:** Always favor native Polars string expressions (like `str.strip_chars`, `str.to_uppercase`, `replace_strict`, etc.) over `map_elements` or `apply` loops to keep computation inside the optimized Rust engine.
