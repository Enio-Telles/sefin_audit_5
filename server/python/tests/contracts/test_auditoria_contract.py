from fastapi.testclient import TestClient
from api import app

client = TestClient(app)

def test_auditoria_pipeline_contract():
    # Invalid CNPJ
    res_invalid = client.post("/api/python/auditoria/pipeline", json={"cnpj": "00000000000000"})
    assert res_invalid.status_code == 400

    # Valid CNPJ - Mocking background tasks to not actually run
    from unittest.mock import patch
    with patch("fastapi.background.BackgroundTasks.add_task", create=True):
        res_valid = client.post("/api/python/auditoria/pipeline", json={"cnpj": "11222333000181"})
        assert res_valid.status_code in (200, 202)
        data = res_valid.json()

        # Check required fields
        assert "success" in data
        assert "cnpj" in data
        assert "job_status" in data
        assert "message" in data
        assert "dir_parquet" in data
        assert "dir_analises" in data
        assert "dir_relatorios" in data

        # Check specific values
        assert data["job_status"] == "agendada"
        assert data["cnpj"] == "11222333000181"

def test_auditoria_status_contract():
    # Valid CNPJ
    res = client.get("/api/python/auditoria/status/11222333000181")
    assert res.status_code == 200
    data = res.json()

    # Check required fields
    assert "success" in data
    assert "cnpj" in data
    assert "job_status" in data
    assert "message" in data
    assert "etapas" in data
    assert "erros" in data
    assert "arquivos_extraidos" in data
    assert "arquivos_analises" in data
    assert "arquivos_produtos" in data
    assert "arquivos_relatorios" in data
    assert "dir_parquet" in data
    assert "dir_analises" in data
    assert "dir_relatorios" in data

    # Check specific values
    assert data["job_status"] in ["agendada", "executando", "concluida", "erro"]
    assert isinstance(data["etapas"], list)
    assert isinstance(data["erros"], list)
    assert data["cnpj"] == "11222333000181"

def test_auditoria_historico_contract():
    res = client.get("/api/python/auditoria/historico/11222333000181")
    assert res.status_code == 200
    data = res.json()

    assert "success" in data
    assert "cnpj" in data
    assert "arquivos_extraidos" in data
    assert "arquivos_analises" in data
    assert "arquivos_produtos" in data
    assert "arquivos_relatorios" in data
    assert "dir_parquet" in data
    assert "dir_analises" in data
    assert "dir_relatorios" in data

    assert isinstance(data["arquivos_extraidos"], list)
    assert isinstance(data["arquivos_analises"], list)
    assert isinstance(data["arquivos_produtos"], list)
    assert isinstance(data["arquivos_relatorios"], list)
