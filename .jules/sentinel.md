## 2024-04-13 - Path Traversal in File Uploads
**Vulnerability:** The application used `file.filename` directly from FastAPI's `UploadFile` to construct the target file path, which allows path traversal attacks (e.g., uploading `../../../etc/passwd` to overwrite arbitrary files).
**Learning:** Even if the target directory is validated, the client-provided filename can contain directory traversal characters.
**Prevention:** Always extract the basename using `Path(file.filename).name` before concatenating it with a target directory to sanitize the filename.
