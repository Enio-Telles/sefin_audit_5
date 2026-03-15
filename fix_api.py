import re

with open("server/python/api.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Remover tudo o que for duplicado do Exception Handler
# Vamos pegar do handler original até o primeiro # CORS e só manter a primeira ocorrência do handler.
regex = r"@app\.exception_handler\(Exception\)(.*?)(?=# CORS)"
matches = list(re.finditer(regex, content, re.DOTALL))

if len(matches) > 1:
    # Substituir a segunda ocorrência por vazio
    second_match = matches[1]
    content = content[:second_match.start()] + content[second_match.end():]

# Salvar o arquivo consertado
with open("server/python/api.py", "w", encoding="utf-8") as f:
    f.write(content)
