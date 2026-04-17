import pytest
import re
from src.utilitarios.export_excel_ultra import _sanitize_sheet_name

def test_sanitize_sheet_name_valid():
    """Testa um nome válido de aba, sem caracteres especiais e tamanho <= 31."""
    assert _sanitize_sheet_name("Aba Valida 123") == "Aba Valida 123"

def test_sanitize_sheet_name_max_length():
    """Testa se o nome é truncado corretamente em 31 caracteres."""
    long_name = "Este nome de aba e muito longo para o excel"
    # Len: 43
    # Esperado os primeiros 31 caracteres
    assert _sanitize_sheet_name(long_name) == "Este nome de aba e muito longo "
    assert len(_sanitize_sheet_name(long_name)) == 31

def test_sanitize_sheet_name_forbidden_chars():
    """Testa se caracteres proibidos (\\, /, *, ?, :, [, ]) são substituídos por _."""
    assert _sanitize_sheet_name(r"Aba\Com/Barras") == "Aba_Com_Barras"
    assert _sanitize_sheet_name("Aba*Com?Asterisco") == "Aba_Com_Asterisco"
    assert _sanitize_sheet_name("Aba:Com[Colchetes]") == "Aba_Com_Colchetes_"
    assert _sanitize_sheet_name(r"Todos: []\*?/") == "Todos_ ______"

def test_sanitize_sheet_name_long_and_forbidden():
    """Testa a combinação de caracteres proibidos e truncamento."""
    name = "Relatorio: Vendas de [Janeiro/2023] e *[Fevereiro/2023]*"
    # Expected: "Relatorio_ Vendas de _Janeiro_2" (len=31)
    # Len original: 54
    assert _sanitize_sheet_name(name) == "Relatorio_ Vendas de _Janeiro_2"

import numpy as np
import pandas as pd
from src.utilitarios.export_excel_ultra import _serializar_valor

def test_serializar_valor_list_tuple_dict():
    assert _serializar_valor([1, 2, 3]) == str([1, 2, 3])
    assert _serializar_valor(("a", "b", "c")) == str(("a", "b", "c"))
    d = {"x": 1, "y": 2}
    assert _serializar_valor(d) == str(d)

def test_serializar_valor_none_and_na():
    assert _serializar_valor(None) is None
    assert _serializar_valor(pd.NA) is None
    assert _serializar_valor(pd.NaT) is None
    assert _serializar_valor(np.nan) is None

def test_serializar_valor_numpy_array():
    arr = np.array([1, 2, 3])
    res = _serializar_valor(arr)
    assert res == str(arr)

def test_serializar_valor_primitive():
    assert _serializar_valor("test") == "test"
    assert _serializar_valor(123) == 123
    assert _serializar_valor(45.6) == 45.6
