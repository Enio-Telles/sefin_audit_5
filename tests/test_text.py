import pytest
from unittest.mock import patch
from src.utilitarios.text import remove_accents, normalize_text, natural_sort_key

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

def test_natural_sort_key_basic():
    assert natural_sort_key("item 10") == ["item ", 10, ""]
    assert natural_sort_key("item 2") == ["item ", 2, ""]
    assert natural_sort_key("apple") == ["apple"]

def test_natural_sort_key_sorting():
    items = ["item 10", "item 2", "item 1", "item 20", "item 11"]
    sorted_items = sorted(items, key=natural_sort_key)
    assert sorted_items == ["item 1", "item 2", "item 10", "item 11", "item 20"]

    items2 = ["A10", "a2", "A1"]
    sorted_items2 = sorted(items2, key=natural_sort_key)
    assert sorted_items2 == ["A1", "a2", "A10"]

def test_natural_sort_key_complex():
    assert natural_sort_key("v1.2.10") == ["v", 1, ".", 2, ".", 10, ""]
    assert natural_sort_key("v1.2.2") == ["v", 1, ".", 2, ".", 2, ""]
    versions = ["v1.2.10", "v1.2.2", "v1.10.1", "v2.0.0"]
    sorted_versions = sorted(versions, key=natural_sort_key)
    assert sorted_versions == ["v1.2.2", "v1.2.10", "v1.10.1", "v2.0.0"]

def test_natural_sort_key_none_and_empty():
    assert natural_sort_key(None) == []
    assert natural_sort_key("") == []
    assert natural_sort_key("   ") == ["   "]
