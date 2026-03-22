import pytest
from fastapi.testclient import TestClient
from api import app
from unittest.mock import patch
import polars as pl
from pathlib import Path

client = TestClient(app)

def test_produtos_revisao_final_contract():
    res = client.get("/api/python/produtos/revisao-final?cnpj=11222333000181")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "available" in data
    assert "file_path" in data
    assert "summary" in data

def test_produtos_status_analise_contract():
    res = client.get("/api/python/produtos/status-analise?cnpj=11222333000181")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "file_path" in data
    assert "data" in data
    assert "resumo" in data

def test_produtos_runtime_status_contract():
    res = client.get("/api/python/produtos/runtime-status?cnpj=11222333000181")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "cnpj" in data
    assert "runtime" in data
    assert "files" in data["runtime"]

def test_produtos_vectorizacao_status_contract():
    # Requires CNPJ
    res = client.get("/api/python/produtos/vectorizacao-status?cnpj=11222333000181")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "status" in data
    assert "caches" in data

def test_produtos_codigos_multidescricao_contract():
    res = client.get("/api/python/produtos/codigos-multidescricao?cnpj=11222333000181")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "file_path" in data
    assert "data" in data
    assert "page" in data
    assert "page_size" in data
    assert "total" in data
    assert "total_pages" in data

def test_produtos_codigo_multidescricao_resumo_contract():
    res = client.get("/api/python/produtos/codigo-multidescricao-resumo?cnpj=11222333000181&codigo=123")
    assert res.status_code == 200
    data = res.json()
    assert "success" in data
    assert "codigo" in data
    assert "resumo" in data
    assert "grupos_descricao" in data
    assert "opcoes_consenso" in data
