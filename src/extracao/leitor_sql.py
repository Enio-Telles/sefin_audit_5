from pathlib import Path

def ler_sql(caminho_arquivo: str | Path) -> str:
    """
    Lê arquivo SQL com tratamento de encoding e limpeza básica.
    """
    if isinstance(caminho_arquivo, str):
        caminho_arquivo = Path(caminho_arquivo)
        
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for enc in encodings:
        try:
            conteudo = caminho_arquivo.read_text(encoding=enc)            
            return conteudo.strip().rstrip(';')
        except UnicodeDecodeError:
            continue    
        except Exception as e:
            print(f"Erro ao ler {caminho_arquivo.name}: {e}")
            continue 
            
    raise Exception(f"Não foi possível ler o arquivo '{caminho_arquivo.name}' com os encodings testados.")
