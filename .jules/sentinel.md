## 2026-04-15 - Pathlib Concatenation Overrides Base Directory
**Vulnerability:** Arbitrary File Write / Path Traversal
**Learning:** In Python's `pathlib`, appending an absolute path to a base path (e.g., `Path('/safe/dir') / '/etc/passwd'`) entirely overrides the base path, resolving to the absolute path instead of a subpath. Even with directory validation, directly concatenating `UploadFile.filename` is unsafe as it allows arbitrary file paths to be specified.
**Prevention:** Always sanitize client-provided filenames by extracting only the base name (e.g., using `Path(file.filename).name`) before concatenating them to the validated target directory.
