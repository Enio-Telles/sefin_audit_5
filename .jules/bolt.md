## 2024-05-19 - Fast Lookups in Polars Nested Loops
**Learning:** Polars Series lookups `df["col"][i]` are very slow when done inside nested loops due to the FFI boundary (crossing between Rust and Python). This can severely bottleneck operations over millions of rows, such as in text similarity score processing.
**Action:** When row-by-row iteration is necessary in Python, extract the required columns into native Python lists before the loop (`lista = df["col"].to_list()`) and index the lists directly `lista[int(i)]`. This changes lookups to O(1) in pure Python and results in nearly a 2x speedup on processing similarity scores.

## 2024-05-20 - Efficient Count of Truthy Values in Sort Keys
**Learning:** Using a generator expression with `sum()` (e.g., `sum(1 for x in [a, b, c] if x)`) inside a sorting key function introduces significant overhead due to the creation of temporary list and generator objects for every comparison.
**Action:** Replace generator-based counts with direct boolean summation (e.g., `bool(a) + bool(b) + bool(c)`). Since booleans are integers in Python, this is logically equivalent and provides a measurable performance improvement (~25-30%) for large-scale sorting operations.

## 2024-05-19 - Fast Sparse Matrix Thresholding in Polars/Scipy
**Learning:** Extracting coordinate values using `zip(*matrix.nonzero())` and row-wise indexing into a SciPy `csr_matrix` is exceptionally slow for large matrices since it incurs O(N) lookup costs per matched coordinate and object overhead for creating tuples.
**Action:** When filtering a sparse matrix by value, convert it to a `coo_matrix` to directly access the 1D `.row`, `.col`, and `.data` numpy arrays. Create boolean masks (`np.where(mask)`) directly on these arrays for filtering. This is orders of magnitude faster (e.g., 280s -> 3.5s).

## 2026-03-25 - Eliminating Generator Overhead in Counting Operations
**Learning:** Python generator expressions inside functions like `sum(1 for x in items if condition)` introduce notable overhead (due to object creation and function calls) when evaluating simple counts over many iterations in tight loops (e.g., product pair classifications).
**Action:** Replace generators with native, C-implemented methods when possible. Use `tuple.count(value)` or `list.count(value)` for exact matches. For evaluating boolean truthiness, use direct addition `bool(a) + bool(b) + bool(c)`. This optimization can reduce execution time of the specific counting logic by ~50-70%.
