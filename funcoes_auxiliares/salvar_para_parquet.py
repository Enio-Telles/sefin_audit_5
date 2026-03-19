import polars as pl
from pathlib import Path
# from rich import print as rprint

def salvar_para_parquet(df, caminho_saida: Path, nome_arquivo: str = None, metadata: dict = None) -> bool:
    """
    Exporta um DataFrame ou LazyFrame do Polars para um arquivo Parquet.
    
    Args:
        df: polars.DataFrame ou polars.LazyFrame a ser salvo.
        caminho_saida: Diretório (Path) ou caminho completo do arquivo.
        nome_arquivo: Nome do arquivo (opcional se caminho_saida for o arquivo completo).
        metadata: Dicionário com metadados para as colunas {col_name: "description"}.
        
    Returns:
        bool: True se salvo com sucesso, False em caso de erro.
    """
    try:
        # Se nome_arquivo for fornecido, trata caminho_saida como diretório
        if nome_arquivo:
            # Garante que termine com .parquet
            if not str(nome_arquivo).lower().endswith(".parquet"):
                nome_arquivo = f"{nome_arquivo}.parquet"
            arquivo = caminho_saida / nome_arquivo
        else:
            arquivo = caminho_saida
            
        print(f"   [debug] Salvando em: {arquivo}")
            
        # Garante que o diretório pai existe
        arquivo.parent.mkdir(parents=True, exist_ok=True)
        
        # Se for LazyFrame, faz o collect primeiro
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
            
        # Se estiver vazio, avisa mas salva mesmo assim (dependendo da necessidade do sistema)
        if df.is_empty():
            print(f"   [!] Aviso: O DataFrame a ser salvo em {arquivo.name} esta vazio.")
            
        if metadata:
            # Converte para pyarrow para aplicar metadados de campo (field-level metadata)
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            table = df.to_arrow()
            
            # Atualiza o schema com metadados para cada campo
            new_fields = []
            for field in table.schema:
                if field.name in metadata:
                    # Adiciona metadados ao campo
                    existing_meta = field.metadata if field.metadata else {}
                    desc_value = metadata[field.name].encode('utf-8')
                    # Adiciona em múltiplas chaves para garantir visibilidade
                    new_meta = {
                        **existing_meta, 
                        b"metadata": desc_value,
                        b"description": desc_value,
                        b"comment": desc_value
                    }
                    new_fields.append(field.with_metadata(new_meta))
                else:
                    new_fields.append(field)
            
            new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
            # Reconstroi a tabela com o novo schema
            table = pa.Table.from_batches(table.to_batches(), new_schema)
            
            pq.write_table(table, arquivo)
        else:
            df.write_parquet(arquivo)
            
        print(f"   Parquet salvo com sucesso em: {arquivo}")
        return True
        
    except Exception as e:
        print(f"   Erro ao salvar arquivo Parquet {arquivo}: {e}")
        return False
