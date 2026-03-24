import pytest
from fastapi.testclient import TestClient
from api import app

@pytest.fixture
def client():
    return TestClient(app)

def test_health_contract(client):
    response = client.get("/health")

    # Validar: resposta 200
    assert response.status_code == 200
    data = response.json()

    # Validar: body contém status, version, engine
    assert "status" in data
    assert "version" in data
    assert "engine" in data

    # Validar: os três são strings
    assert isinstance(data["status"], str)
    assert isinstance(data["version"], str)
    assert isinstance(data["engine"], str)

    # Validar opcionalmente (conforme spec)
    assert data["status"] in ["ok", "healthy"]
    assert data["engine"] == "python"
