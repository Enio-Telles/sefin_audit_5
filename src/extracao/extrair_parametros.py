"""
Módulo auxiliar para identificar bind variables em comandos SQL.
"""
import re

def extrair_parametros_sql(sql: str) -> set[str]:
    """
    Identifica variáveis de ligação (bind variables) no formato :nome_variavel dentro de uma string SQL.
    
    Args:
        sql (str): O conteúdo do SQL.
        
    Returns:
        set[str]: Um conjunto contendo os nomes das variáveis encontradas (sem o dois-pontos).
    """
    # Expressão regular para encontrar :variavel
    # (?<!\[) garante que no pegue :alnum:, :digit:, etc (POSIX classes do Oracle)
    # A regex r":(\w+)" captura palavras que começam com :
    bind_regex = r"(?<!\[):([a-zA-Z0-9_]+)"
    binds = re.findall(bind_regex, sql)
    return set(binds)

def extract_sql_parameters(sql: str) -> list[dict]:
    """
    Identifica variáveis de ligação (bind variables) no formato :nome_variavel dentro de uma string SQL.
    Retorna uma lista de dicionários com metadados inferidos a partir do nome do parâmetro.

    Args:
        sql (str): O conteúdo do SQL.

    Returns:
        list[dict]: Uma lista contendo dicionários com os metadados das variáveis encontradas.
        Exemplo:
        [
          {"name": "CNPJ", "type": "text", "required": True},
          {"name": "data_inicial", "type": "date", "required": False}
        ]
    """
    binds = extrair_parametros_sql(sql)
    parametros = []

    for bind in binds:
        bind_lower = bind.lower()

        # Inferência de tipo
        if "data" in bind_lower:
            tipo = "date"
        elif "cnpj" in bind_lower:
            tipo = "text" # Ou mask
        elif "valor" in bind_lower or "numero" in bind_lower or "qtd" in bind_lower or "quantidade" in bind_lower:
             tipo = "number"
        else:
            tipo = "text"

        # Obrigatório (exemplo: CNPJ costuma ser obrigatório)
        required = "cnpj" in bind_lower

        parametros.append({
            "name": bind,
            "type": tipo,
            "required": required
        })

    return parametros
