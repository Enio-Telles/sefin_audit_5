## 2024-04-16 - Path Traversal in File Export
**Vulnerability:** The Python export router (`server/python/routers/export.py`) accepted user-provided paths for reading parquet files and writing excel files without resolving them or checking if they were within allowed project directories.
**Learning:** Any endpoint taking a file path as input is susceptible to path traversal (e.g. `../../etc/passwd`). Validating just the filename is not enough.
**Prevention:** Always resolve paths using `Path(path).resolve()` and validate them against a whitelist of allowed directories using a centralized utility (like `_is_path_allowed`) before performing filesystem operations.
