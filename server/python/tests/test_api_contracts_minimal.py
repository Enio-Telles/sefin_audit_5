import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from api import app

# Minimal mock configurations
@pytest.fixture
def client():
    return TestClient(app)

def test_health_check(client):
    """Teste básico para verificar se a API está online."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["ok", "healthy"]

@patch("core.config_loader.get_config_var")
@patch("routers.analysis.iniciar_status_agendado")
def test_auditoria_pipeline_contract(mock_status, mock_config, client):
    """Garante que a rota /auditoria/pipeline não quebre."""

    # Mocking config variables
    mock_config.side_effect = lambda var: {
        "obter_diretorios_cnpj": lambda cnpj: (Path("/tmp"), Path("/tmp"), Path("/tmp")),
        "DIR_SQL": Path("/tmp")
    }.get(var)

    payload = {
        "cnpj": "00000000000191",
        "username": "tester",
        "mes_ano_inicio": "012023",
        "mes_ano_fim": "122023"
    }

    # Simulate add_task
    mock_background = MagicMock()
    with patch("fastapi.BackgroundTasks.add_task", mock_background):
        response = client.post("/api/python/auditoria/pipeline", json=payload)

    # Verify response schema
    assert response.status_code == 200
    data = response.json()
    assert data.get("success") is True
    assert data.get("cnpj") == "00000000000191"

@patch("core.config_loader.get_config_var")
def test_auditoria_historico_cnpj_contract(mock_config, client):
    """Garante o contrato de /auditoria/historico/{cnpj} (chamando a lógica centralizada)."""

    # Simulate an empty but existing CNPJ directory
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        cnpj_dir = Path(temp_dir) / "00000000000191"
        cnpj_dir.mkdir(parents=True)
        (cnpj_dir / "arquivos_parquet").mkdir()
        (cnpj_dir / "analises").mkdir()
        (cnpj_dir / "relatorios").mkdir()

        mock_config.side_effect = lambda var, default=None: {
            "DIR_CNPJS": Path(temp_dir)
        }.get(var, default)

        response = client.get("/api/python/auditoria/historico/00000000000191")

    assert response.status_code == 200
    data = response.json()

    # Validate critical keys required by the frontend
    assert data.get("success") is True
    assert "cnpj" in data
    assert "etapas" in data
    assert "erros" in data
    assert "arquivos_extraidos" in data
    assert "arquivos_analises" in data
    assert "arquivos_produtos" in data
    assert "arquivos_relatorios" in data

@patch("core.config_loader.get_config_var")
def test_auditoria_status_cnpj_contract(mock_config, client):
    """Garante o contrato de /auditoria/status/{cnpj}."""

    mock_config.side_effect = lambda var: {
        "obter_diretorios_cnpj": lambda cnpj: (Path("/tmp"), Path("/tmp"), Path("/tmp"))
    }.get(var)

    response = client.get("/api/python/auditoria/status/00000000000191")

    assert response.status_code == 200
    data = response.json()

    assert data.get("success") is True
    assert "cnpj" in data
    assert "job_status" in data
    assert "message" in data
    assert "etapas" in data
