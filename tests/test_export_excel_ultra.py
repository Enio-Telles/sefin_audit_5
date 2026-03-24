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
