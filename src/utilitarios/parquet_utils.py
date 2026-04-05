import polars as pl
from pathlib import Path
import os
import shutil
import json
from datetime import datetime

def salvar_para_parquet(df: pl.DataFrame | pl.LazyFrame, caminho_arquivo: Path) -> bool:
    """
    Exporta um DataFrame ou LazyFrame para Parquet de forma idempotente.
    Usa padrão DELETE-INSERT escrevendo num arquivo temporário e renomeando.
    Salva metadados de execução.
    """
    try:
        caminho_arquivo = Path(caminho_arquivo)
        caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = caminho_arquivo.with_suffix('.tmp')
        meta_path = caminho_arquivo.with_suffix('.meta.json')

        # Coleta os dados em streaming se for LazyFrame
        if isinstance(df, pl.LazyFrame):
            import os
            if os.getenv("DEBUG_POLARS") == "1":
                print("\n--- PLANO DE EXECUCAO POLARS ---")
                print(df.explain())
                print("--------------------------------\n")
            df = df.collect(streaming=True)
            
        # Escreve no temporário para atomicidade
        df.write_parquet(tmp_path, compression="snappy")

        # Operação atômica de substituição (Idempotência)
        if hasattr(os, 'replace'):
            os.replace(tmp_path, caminho_arquivo)
        else:
            shutil.move(tmp_path, caminho_arquivo)

        # Salva metadados de execução
        # Generate basic hash input representing data
        import hashlib
        h = hashlib.sha256()
        h.update(str(df.schema).encode())
        h.update(str(len(df)).encode())
        h.update(str(datetime.now().isoformat()).encode())

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "rows": len(df),
            "schema": {k: str(v) for k, v in df.schema.items()},
            "version": "1.0",
            "hash_input": h.hexdigest()
        }
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"   Parquet salvo atômicamente em: {caminho_arquivo}")
        return True
    except Exception as e:
        print(f"   Erro ao salvar Parquet {caminho_arquivo}: {e}")
        # Cleanup do arquivo temporário se algo deu errado
        if 'tmp_path' in locals() and tmp_path.exists():
            tmp_path.unlink()
        return False
