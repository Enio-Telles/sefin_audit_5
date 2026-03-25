import pytest
from unittest.mock import patch
from src.utilitarios.text import remove_accents, normalize_text

def test_remove_accents_normal():
    assert remove_accents("Ação") == "Acao"
    assert remove_accents("João") == "Joao"
    assert remove_accents("Coração") == "Coracao"
    assert remove_accents("Texto normal") == "Texto normal"
    assert remove_accents(None) is None

def test_remove_accents_exception_fallback():
    # Mock unicodedata.normalize to raise an Exception
    with patch("src.utilitarios.text.unicodedata.normalize", side_effect=Exception("mocked error")):
        original_text = "texto com acentuação"
        assert remove_accents(original_text) == original_text

def test_normalize_text_basic():
    assert normalize_text("Texto Simples") == "TEXTO SIMPLES"
    assert normalize_text("Ação") == "ACAO"

def test_normalize_text_whitespace():
    assert normalize_text("  espacos   extras  ") == "ESPACOS EXTRAS"
    assert normalize_text("\n\t multilinhas \n") == "MULTILINHAS"

def test_normalize_text_edge_cases():
    assert normalize_text(None) == ""
    assert normalize_text("") == ""
    assert normalize_text("   ") == ""
