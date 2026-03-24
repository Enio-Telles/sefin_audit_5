import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from api import app

@pytest.fixture
def client():
    return TestClient(app)

VALID_CNPJ = "00000000000191"
INVALID_CNPJ = "000"

def mock_get_config_var(var, default=None):
    if var == "obter_diretorios_cnpj":
        return lambda cnpj: (Path(f"/tmp/parquet/{cnpj}"), Path(f"/tmp/analises/{cnpj}"), Path(f"/tmp/relatorios/{cnpj}"))
    if var == "DIR_SQL":
        return Path("/tmp/sql")
    if var == "DIR_CNPJS":
        return Path("/tmp")
    return default

@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
def test_auditoria_pipeline_invalid_cnpj(mock_config, client):
    payload = {"cnpj": INVALID_CNPJ}
    response = client.post("/api/python/auditoria/pipeline", json=payload)

    # Validar: rejeita CNPJ inválido com erro coerente
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data

@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
def test_auditoria_pipeline_valid_cnpj(mock_config, client):
    payload = {"cnpj": VALID_CNPJ}
    mock_background = MagicMock()

    with patch("fastapi.BackgroundTasks.add_task", mock_background, create=True):
        with patch("routers.analysis.iniciar_status_agendado", MagicMock()):
            response = client.post("/api/python/auditoria/pipeline", json=payload)

    # Validar: aceita CNPJ válido
    assert response.status_code == 200
    data = response.json()

    # Validar: body contém shape mínimo
    assert "success" in data
    assert "cnpj" in data
    assert "job_status" in data
    assert "message" in data
    assert "dir_parquet" in data
    assert "dir_analises" in data
    assert "dir_relatorios" in data

    # Validar: job_status deve ser "agendada"
    assert data["job_status"] == "agendada"


@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
def test_auditoria_status_valid_cnpj(mock_config, client):
    mock_status_response = {
        "success": True,
        "cnpj": VALID_CNPJ,
        "job_status": "executando",
        "message": "Executando...",
        "etapas": [],
        "erros": [],
        "arquivos_extraidos": [],
        "arquivos_analises": [],
        "arquivos_produtos": [],
        "arquivos_relatorios": [],
        "dir_parquet": "/tmp/parquet/00000000000191",
        "dir_analises": "/tmp/analises/00000000000191",
        "dir_relatorios": "/tmp/relatorios/00000000000191"
    }

    with patch("routers.analysis.construir_resposta_status", return_value=mock_status_response):
        response = client.get(f"/api/python/auditoria/status/{VALID_CNPJ}")

    # Validar: resposta 200 para CNPJ válido
    assert response.status_code == 200
    data = response.json()

    # Validar: body contém todos os campos obrigatórios
    required_keys = [
        "success", "cnpj", "job_status", "message", "etapas", "erros",
        "arquivos_extraidos", "arquivos_analises", "arquivos_produtos",
        "arquivos_relatorios", "dir_parquet", "dir_analises", "dir_relatorios"
    ]
    for key in required_keys:
        assert key in data

    # Validar: job_status no conjunto permitido
    assert data["job_status"] in ["agendada", "executando", "concluida", "erro"]

    # Validar: etapas e erros são arrays (lists in Python)
    assert isinstance(data["etapas"], list)
    assert isinstance(data["erros"], list)


@patch("core.config_loader.get_config_var", side_effect=mock_get_config_var)
def test_auditoria_historico_valid_cnpj(mock_config, client):
    import tempfile

    # Use real temporary directory for filesystem operations to avoid complex mocking of pathlib
    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        cnpj_dir = base_path / VALID_CNPJ
        cnpj_dir.mkdir(parents=True)
        (cnpj_dir / "arquivos_parquet").mkdir()
        (cnpj_dir / "analises").mkdir()
        (cnpj_dir / "relatorios").mkdir()

        # Override the mock to point to the real temp directory
        def temp_mock_get_config_var(var, default=None):
            if var == "DIR_CNPJS":
                return base_path
            return mock_get_config_var(var, default)

        with patch("core.config_loader.get_config_var", side_effect=temp_mock_get_config_var):
            response = client.get(f"/api/python/auditoria/historico/{VALID_CNPJ}")

    # Validar: resposta 200 para fixture válida
    assert response.status_code == 200
    data = response.json()

    # Validar: body contém chaves esperadas
    required_keys = [
        "success", "cnpj", "arquivos_extraidos", "arquivos_analises",
        "arquivos_produtos", "arquivos_relatorios",
        "dir_parquet", "dir_analises", "dir_relatorios"
    ]
    for key in required_keys:
        assert key in data

    # Validar: arrays vêm como arrays
    assert isinstance(data["arquivos_extraidos"], list)
    assert isinstance(data["arquivos_analises"], list)
    assert isinstance(data["arquivos_produtos"], list)
    assert isinstance(data["arquivos_relatorios"], list)
