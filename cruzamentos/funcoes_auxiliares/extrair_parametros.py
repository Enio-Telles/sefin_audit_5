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
    # O lookbehind (?<![:\w]) garante que o dois-pontos não seja precedido por outro dois-pontos (ex: ::cast)
    # ou por um caractere de palavra (ex: HH24:MI:SS).
    # A regex r"(?<![:\w]):(\w+)" captura palavras que começam com : e são precedidas por um espaço,
    # início de linha ou caractere não alfanumérico.
    binds = re.findall(r"(?<![:\w]):(\w+)", sql)
    return set(binds)
