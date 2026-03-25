import polars as pl
from src.transformacao.analise_produtos.itens import _gerar_chave_item

def test_gerar_chave_item_normaliza_maiusculas_e_espacos():
    df = pl.DataFrame({
        "codigo": [" 001 ", "001"],
        "descricao": [" Produto a ", "PRODUTO A"],
        "descr_compl": [None, None],
        "tipo_item": ["00", "00"],
        "ncm": ["12345678", "12345678"],
        "cest": [None, None],
        "gtin": [None, None],
    })

    df_res = _gerar_chave_item(df)
    chaves = df_res.get_column("chave_item_individualizado").to_list()

    # As duas linhas devem gerar a mesma chave devido ao strip() e to_uppercase()
    assert chaves[0] == chaves[1]

def test_gerar_chave_item_valores_nulos_tratados_como_vazio():
    df = pl.DataFrame({
        "codigo": ["001", "001"],
        "descricao": ["Prod", "Prod"],
        "descr_compl": [None, ""],
        "tipo_item": ["00", "00"],
        "ncm": ["12345678", "12345678"],
        "cest": [None, ""],
        "gtin": [None, ""],
    })

    df_res = _gerar_chave_item(df)
    chaves = df_res.get_column("chave_item_individualizado").to_list()

    # None e string vazia ("") geram a mesma chave
    assert chaves[0] == chaves[1]

def test_gerar_chave_item_diferentes_geram_chaves_distintas():
    df = pl.DataFrame({
        "codigo": ["001", "002"],
        "descricao": ["Prod A", "Prod A"],
        "descr_compl": [None, None],
        "tipo_item": ["00", "00"],
        "ncm": ["12345678", "12345678"],
        "cest": [None, None],
        "gtin": [None, None],
    })

    df_res = _gerar_chave_item(df)
    chaves = df_res.get_column("chave_item_individualizado").to_list()

    assert chaves[0] != chaves[1]

def test_gerar_chave_item_mantem_outras_colunas():
    df = pl.DataFrame({
        "codigo": ["001"],
        "descricao": ["Prod A"],
        "descr_compl": [None],
        "tipo_item": ["00"],
        "ncm": ["12345678"],
        "cest": [None],
        "gtin": [None],
        "outra_coluna": ["valor"],
        "valor_total": [10.5]
    })

    df_res = _gerar_chave_item(df)

    assert "outra_coluna" in df_res.columns
    assert "valor_total" in df_res.columns
    assert "chave_item_individualizado" in df_res.columns
