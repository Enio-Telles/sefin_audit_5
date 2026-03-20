from pathlib import Path
from funcoes_auxiliares.encontrar_arquivo_cnpj import encontrar_arquivo

def test_encontrar_arquivo_agrupado(tmp_path: Path):
    prefixo = "NFE"
    cnpj = "12345678901234"

    # Criar arquivo AGRUPADO
    arquivo_agrupado = tmp_path / f"{prefixo}_AGRUPADO_{cnpj}.parquet"
    arquivo_agrupado.touch()

    # Criar arquivo normal (deve ser ignorado)
    arquivo_normal = tmp_path / f"{prefixo}_{cnpj}.parquet"
    arquivo_normal.touch()

    resultado = encontrar_arquivo(tmp_path, prefixo, cnpj)

    assert resultado == arquivo_agrupado

def test_encontrar_arquivo_normal(tmp_path: Path):
    prefixo = "NFE"
    cnpj = "12345678901234"

    # Criar arquivo normal
    arquivo_normal = tmp_path / f"{prefixo}_{cnpj}.parquet"
    arquivo_normal.touch()

    # Criar outro arquivo irrelevante
    arquivo_outro = tmp_path / f"OUTRO_{cnpj}.parquet"
    arquivo_outro.touch()

    resultado = encontrar_arquivo(tmp_path, prefixo, cnpj)

    assert resultado == arquivo_normal

def test_encontrar_arquivo_fallback(tmp_path: Path):
    prefixo = "NFE"
    cnpj = "12345678901234"

    # Criar arquivo que cai no fallback (prefixo.lower() in arq.stem.lower() e cnpj in arq.stem)
    # Por exemplo: nfe_alguma_coisa_12345678901234.parquet
    arquivo_fallback = tmp_path / f"nfe_alguma_coisa_{cnpj}.parquet"
    arquivo_fallback.touch()

    resultado = encontrar_arquivo(tmp_path, prefixo, cnpj)

    assert resultado == arquivo_fallback

def test_encontrar_arquivo_nao_encontrado(tmp_path: Path):
    prefixo = "NFE"
    cnpj = "12345678901234"

    # Diretório vazio
    resultado = encontrar_arquivo(tmp_path, prefixo, cnpj)
    assert resultado is None

    # Arquivo com CNPJ diferente
    arquivo_diferente = tmp_path / f"{prefixo}_99999999999999.parquet"
    arquivo_diferente.touch()

    resultado = encontrar_arquivo(tmp_path, prefixo, cnpj)
    assert resultado is None
