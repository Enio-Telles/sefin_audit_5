files = [
    "server/python/core/produto_runtime.py",
    "server/python/routers/produto_unid.py"
]

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        content = f.read()

    # Remove the ones we added at the top
    content = content.replace("import difflib\nfrom functools import lru_cache\n", "", 1)

    # We will just add them below __future__ imports if they exist
    lines = content.split('\n')
    insert_idx = 0
    for i, line in enumerate(lines):
        if line.startswith("from __future__"):
            insert_idx = i + 1

    lines.insert(insert_idx, "import difflib\nfrom functools import lru_cache")

    with open(file, "w", encoding="utf-8") as f:
        f.write('\n'.join(lines))
    print(f"Fixed {file}")
