import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from api import app
import polars as pl

@pytest.fixture
def client():
    return TestClient(app)

VALID_CNPJ = "00000000000191"

def mock_get_config_var(var, default=None):
    if var == "obter_diretorios_cnpj":
        return lambda cnpj: (Path(f"/tmp/parquet/{cnpj}"), Path(f"/tmp/analises/{cnpj}"), Path(f"/tmp/relatorios/{cnpj}"))
    return default

# 1. /produtos/revisao-final
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("routers.produtos.revisao.unificar_produtos_unidades")
@patch("routers.produtos.revisao._load_cnpj_dirs", return_value=(Path("/tmp"), Path("/tmp"), Path("/tmp")), create=True)
def test_produtos_revisao_final_contract(mock_load, mock_unificar, mock_config, client):
    mock_df = pl.DataFrame({
        "codigo": ["123", "456"],
        "descricao": ["produto teste", "produto b"],
        "unidade": ["UN", "KG"],
        "quantidade": [1.0, 2.0],
        "valor_total": [10.0, 50.0],
        "qtd_ocorrencias": [1, 2],
        "valor_unitario": [10.0, 25.0]
    })
    mock_unificar.return_value = mock_df

    response = client.get(f"/api/python/produtos/revisao-final?cnpj={VALID_CNPJ}")
    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "available" in data
    assert "file_path" in data
    assert "summary" in data

# 2. /produtos/status-analise
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("routers.produtos.status._gravar_status_analise", return_value=Path("/tmp/status.parquet"), create=True)
@patch("routers.produtos.status._resumir_status_analise", return_value={"job_status": "concluido", "progresso": 100}, create=True)
@patch("routers.produtos.status._STATUS_ANALISE_COLUMNS", ["codigo", "status", "revisado_por", "divergente", "sugestao_descricao", "confianca_sugestao", "revisao_manual"], create=True)
@patch("polars.read_parquet")
def test_produtos_status_analise_contract(mock_read, mock_resumir, mock_gravar, mock_config, client):
    mock_status_df = pl.DataFrame({
        "codigo": ["123"],
        "status": ["CONCLUIDO"],
        "revisado_por": ["AI"],
        "divergente": [False],
        "sugestao_descricao": [""],
        "confianca_sugestao": [1.0],
        "revisao_manual": [""]
    })
    mock_read.return_value = mock_status_df

    response = client.get(f"/api/python/produtos/status-analise?cnpj={VALID_CNPJ}")
    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "file_path" in data
    assert "data" in data
    assert "resumo" in data

# 3. /produtos/runtime-status
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("pathlib.Path.exists", return_value=True)
@patch("pathlib.Path.stat")
def test_produtos_runtime_status_contract(mock_stat, mock_exists, mock_config, client):
    mock_stat.return_value = MagicMock(st_size=1024)
    response = client.get(f"/api/python/produtos/runtime-status?cnpj={VALID_CNPJ}")

    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "cnpj" in data
    assert "runtime" in data
    assert "files" in data["runtime"]

# 4. /produtos/vectorizacao-status
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("routers.produtos.vectorizacao.verificar_status_cache", create=True)
def test_produtos_vectorizacao_status_contract(mock_verificar, mock_config, client):
    mock_verificar.return_value = {
        "success": True,
        "current_base_hash": "abc",
        "status": {
            "available": True,
            "message": "Modelos prontos"
        },
        "caches": {
            "faiss": {"items": 100, "stale": False},
            "light": {"items": 100, "stale": False}
        }
    }

    response = client.get(f"/api/python/produtos/vectorizacao-status?cnpj={VALID_CNPJ}")

    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "status" in data
    assert "caches" in data

# 5. /produtos/codigos-multidescricao
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("routers.produtos.multidescricao._paginate_frame", create=True)
@patch("routers.produtos.multidescricao._normalize_page", return_value=1, create=True)
@patch("routers.produtos.multidescricao._normalize_page_size", return_value=50, create=True)
@patch("routers.produtos.multidescricao._gravar_status_analise", return_value=Path("/tmp"), create=True)
@patch("routers.produtos.multidescricao._load_cnpj_dirs", return_value=(Path("/tmp"), Path("/tmp"), Path("/tmp")), create=True)
@patch("pathlib.Path.exists", return_value=True)
@patch("polars.read_parquet")
def test_produtos_codigos_multidescricao_contract(mock_read, mock_exists, mock_load, mock_gravar, mock_nps, mock_np, mock_paginate, mock_config, client):
    df = pl.DataFrame({
        "codigo": ["123", "123", "456"],
        "descricao": ["prod 1", "prod 2", "prod 3"],
        "hash_descricao": ["hash1", "hash2", "hash3"],
        "tipo_ref": ["POR_CODIGO", "POR_CODIGO", "POR_CODIGO"],
        "divergente": [False, False, False],
        "confianca_sugestao": [1.0, 1.0, 1.0],
        "status": ["CONCLUIDO", "CONCLUIDO", "CONCLUIDO"],
        "status_analise": ["OK", "OK", "OK"],
        "ref_id": ["", "", ""]
    })
    mock_read.return_value = df
    mock_paginate.return_value = (df, 1, 1)

    response = client.get(f"/api/python/produtos/codigos-multidescricao?cnpj={VALID_CNPJ}")

    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "file_path" in data
    assert "data" in data
    assert "page" in data
    assert "page_size" in data
    assert "total" in data
    assert "total_pages" in data

# 6. /produtos/codigo-multidescricao-resumo
@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
@patch("routers.produtos.multidescricao._load_cnpj_dirs", return_value=(Path("/tmp"), Path("/tmp"), Path("/tmp")), create=True)
@patch("pathlib.Path.exists", return_value=True)
@patch("polars.read_parquet")
def test_produtos_codigo_multidescricao_resumo_contract(mock_read, mock_exists, mock_load, mock_config, client):
    df = pl.DataFrame({
        "codigo": ["123", "123"],
        "descricao": ["prod 1", "prod 2"],
        "descr_compl": ["", ""],
        "unidade": ["UN", "CX"],
        "quantidade": [1.0, 2.0],
        "valor_total": [10.0, 20.0],
        "qtd_ocorrencias": [1, 1],
        "qtd_linhas": [1, 1],
        "valor_unitario": [10.0, 10.0],
        "hash_descricao": ["hash1", "hash2"],
        "tipo_item": ["Mercadoria", "Mercadoria"],
        "cst_icms": ["00", "00"],
        "cfop": ["5102", "5102"],
        "aliq_icms": [18.0, 18.0],
        "ano_mes": ["2023-01", "2023-01"],
        "operacao": ["SAIDA", "SAIDA"],
        "ncm": ["12345678", "12345678"],
        "cest": ["", ""],
        "gtin": ["", ""],
        "lista_unidades": ["", ""],
        "lista_fontes": ["", ""],
        "chave_produto": ["123", "123"]
    })
    mock_read.return_value = df

    response = client.get(f"/api/python/produtos/codigo-multidescricao-resumo?cnpj={VALID_CNPJ}&codigo=123")

    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert "codigo" in data
    assert "resumo" in data
    assert "grupos_descricao" in data
    assert "opcoes_consenso" in data
