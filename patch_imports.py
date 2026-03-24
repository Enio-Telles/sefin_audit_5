with open("server/python/routers/filesystem.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

import_lines = []
other_lines = []

for line in lines:
    if line.startswith("import ") or line.startswith("from "):
        import_lines.append(line)
    elif "logger = logging.getLogger" in line:
        pass
    else:
        other_lines.append(line)

new_content = "".join(import_lines) + "\nlogger = logging.getLogger('sefin_audit_python')\n" + "".join(other_lines)

with open("server/python/routers/filesystem.py", "w", encoding="utf-8") as f:
    f.write(new_content)
