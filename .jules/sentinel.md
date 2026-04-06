## 2025-04-06 - Path Traversal in Export Endpoints
**Vulnerability:** The `/excel` and `/excel-download` endpoints accepted user-controlled paths (`output_dir`, `source_files`, `file_path`) without proper validation, leading to path traversal vulnerabilities.
**Learning:** Always validate any user-provided path before using it for file I/O operations (reads and writes).
**Prevention:** Use the centralized validation function `_is_path_allowed` after resolving the path (`Path().resolve()`).
