## 2024-04-10 - Path Traversal via UploadFile.filename
**Vulnerability:** `file_path = dir_path / file.filename` exposes the system to path traversal if `file.filename` contains `../` or starts with `/`. This could allow writing files outside the intended directory.
**Learning:** `UploadFile.filename` is client-controlled and cannot be trusted. Even if `dir_path` is validated to be within allowed directories, concatenating it with an absolute path from the client replaces the base directory entirely (`Path("/safe") / "/etc/passwd" == Path("/etc/passwd")`).
**Prevention:** Always extract the basename using `Path(file.filename).name` before joining it with the target directory.
