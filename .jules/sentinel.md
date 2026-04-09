## 2024-05-24 - Missing path validation in file export endpoint
**Vulnerability:** Path Traversal
**Learning:** File paths passed from user input (`request.output_dir`, `request.source_files`, `file_path`) were used directly in `Path(user_input)` without verifying if the user has access to these paths or if they escape the restricted directories. This allows an attacker to write/read arbitrary files.
**Prevention:** Always validate file paths by resolving them (`.resolve()`) and ensuring they fall within permitted bounds using validation functions (e.g., `_is_path_allowed`).
