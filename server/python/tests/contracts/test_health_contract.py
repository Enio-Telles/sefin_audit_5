from fastapi.testclient import TestClient
from api import app

client = TestClient(app)

def test_health_contract():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()

    assert "status" in data
    assert "version" in data
    assert "engine" in data

    assert isinstance(data["status"], str)
    assert isinstance(data["version"], str)
    assert isinstance(data["engine"], str)

    assert data["status"] == "ok"
    assert data["engine"] == "python"
