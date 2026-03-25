from pathlib import Path

def ler_sql(caminho_arquivo: str | Path) -> str:
    """
    Lê arquivo SQL com tratamento de encoding e limpeza básica.
    """
    caminho = Path(caminho_arquivo)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {caminho}")

    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for enc in encodings:
        try:
            conteudo = caminho.read_text(encoding=enc)
            return conteudo.strip().rstrip(';')
        except UnicodeDecodeError:
            continue    
        except Exception as e:
            print(f"Erro ao ler {caminho.name}: {e}")
            continue 
            
    raise Exception(f"Não foi possível ler o arquivo '{caminho.name}' com os encodings testados.")
