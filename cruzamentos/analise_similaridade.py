import polars as pl
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import csr_matrix
import time
import gc
import os

def encontrar_similares_16gb_ram(
    df: pl.DataFrame, 
    threshold: float = 0.85, 
    batch_size: int = 5000, 
    output_dir: str = "temp_similaridades"
) -> pl.LazyFrame:
    """
    Compara 600k+ strings limitando estritamente o uso de RAM (Safe para 16GB).
    Gera arquivos Parquet temporários e retorna um LazyFrame unificado.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print("Iniciando vetorização (TF-IDF) em float32...")
    t0 = time.time()
    
    # max_features opcional: se a RAM estourar já na vetorização, adicione max_features=100000
    vectorizer = TfidfVectorizer(
        analyzer='char_wb', 
        ngram_range=(2, 4), 
        min_df=2, 
        dtype=np.float32 # Essencial para cortar uso de RAM pela metade
    )
    
    descricoes = df["descricao_limpa"].to_list()
    tfidf_matrix = vectorizer.fit_transform(descricoes)
    
    # Liberar a lista de descrições da memória (já temos no df)
    del descricoes
    gc.collect()
    
    print(f"Vetorização concluída em {time.time() - t0:.2f}s. Shape: {tfidf_matrix.shape}")

    num_rows = tfidf_matrix.shape[0]
    arquivos_gerados = []

    print("Iniciando multiplicação em lotes com flushing para disco...")
    
    # Extract to Python lists for O(1) loop lookups to avoid slow Polars Series indexing
    lista_codigos = df["codigo"].to_list()
    lista_descricoes = df["descricao_limpa"].to_list()

    for start_row in range(0, num_rows, batch_size):
        end_row = min(start_row + batch_size, num_rows)
        resultados_chunk = []
        
        # Produto escalar (apenas um pedaço da matriz contra ela toda)
        similaridade_lote = tfidf_matrix[start_row:end_row].dot(tfidf_matrix.T)
        similaridade_lote.setdiag(0) # Não comparar o item com ele mesmo
        
        # Extrai coordenadas com score acima do threshold
        tuplas_acima_threshold = zip(*similaridade_lote.nonzero())
        
        for i_lote, j_global in tuplas_acima_threshold:
            i_global = start_row + i_lote
            
            # i_global < j_global evita pares duplicados (A->B e B->A)
            if i_global < j_global:
                sim_score = similaridade_lote[i_lote, j_global]
                if sim_score >= threshold:
                    resultados_chunk.append({
                        "id_A": lista_codigos[int(i_global)],
                        "desc_A": lista_descricoes[int(i_global)],
                        "id_B": lista_codigos[int(j_global)],
                        "desc_B": lista_descricoes[int(j_global)],
                        "score": round(float(sim_score), 4)
                    })
        
        # FLUSHING PARA DISCO (A Mágica para 16GB de RAM)
        if resultados_chunk:
            df_chunk = pl.DataFrame(resultados_chunk)
            chunk_path = f"{output_dir}/chunk_{start_row}_{end_row}.parquet"
            df_chunk.write_parquet(chunk_path)
            arquivos_gerados.append(chunk_path)
            
        # Liberar memória do chunk atual explicitamente
        del similaridade_lote
        del resultados_chunk
        gc.collect()
        
        print(f"Processado: {end_row}/{num_rows} linhas. Memória limpa.")

    print(f"Processamento concluído. {len(arquivos_gerados)} lotes salvos.")
    
    # Retorna um LazyFrame lendo todos os arquivos gerados, sem carregar tudo na RAM
    if arquivos_gerados:
        return pl.scan_parquet(f"{output_dir}/*.parquet").sort("score", descending=True)
    else:
        return pl.LazyFrame()

# COMO USAR:
# df_limpo = pl.read_parquet("produtos_limpos.parquet") # Certifique-se de ter as colunas 'codigo' e 'descricao_limpa'
# lf_resultados = encontrar_similares_16gb_ram(df_limpo, threshold=0.90, batch_size=5000)
# 
# # Ao invés de carregar na RAM com .collect(), salve direto no disco processando via streaming
# lf_resultados.sink_parquet("similaridades_finais.parquet")