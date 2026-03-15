file = "server/python/routers/produto_unid.py"
with open(file, "r", encoding="utf-8-sig") as f:
    content = f.read()

with open(file, "w", encoding="utf-8") as f:
    f.write(content)
print("Removed BOM")
