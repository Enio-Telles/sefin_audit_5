
## 2026-04-09 - Path Traversal in FastAPI UploadFile
**Vulnerability:** The client-provided `UploadFile.filename` in `upload_parquet` was concatenated directly with a target directory (`dir_path / file.filename`), allowing path traversal if the filename contained `../`.
**Learning:** Even if the base directory (`dir_path`) is validated and secured, concatenating unsanitized client input (like `file.filename`) creates a vulnerability, as an attacker can escape the safe directory.
**Prevention:** Always sanitize `UploadFile.filename` by extracting the basename (e.g., `Path(file.filename).name`) before using it in any filesystem operations.
