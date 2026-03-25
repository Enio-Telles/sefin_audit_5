import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from core.models import ExtractionRequest, OracleConnectionConfig
from api import app

client = TestClient(app)

@pytest.fixture
def mock_job_manager():
    with patch("routers.oracle.job_manager") as mock_mgr, \
         patch("routers.jobs.job_manager", mock_mgr):
        yield mock_mgr

def test_extract_creates_job(mock_job_manager):
    # Setup mock
    mock_job = MagicMock()
    mock_job.job_id = "test-job-id"
    mock_job.status = "queued"
    mock_job.model_dump.return_value = {"job_id": "test-job-id", "status": "queued"}
    mock_job_manager.submit_job.return_value = mock_job

    # Mock precheck
    with patch("routers.oracle._oracle_network_precheck"):
        response = client.post("/api/python/oracle/extract", json={
            "connection": {
                "user": "test",
                "password": "pwd"
            },
            "output_dir": "/tmp/test",
            "queries": ["query1.sql"]
        })

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["job_id"], str) and len(data["job_id"]) > 0
    assert data["status"] == "queued"
    assert mock_job_manager.submit_job.called

def test_get_job_status(mock_job_manager):
    mock_job = MagicMock()
    mock_job.model_dump.return_value = {"job_id": "test-job-id", "status": "running", "progress": 50.0}
    mock_job_manager.get_job.return_value = mock_job

    response = client.get("/api/python/jobs/test-job-id")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["job"]["status"] == "running"
    assert data["job"]["progress"] == 50.0

def test_cancel_job(mock_job_manager):
    mock_job_manager.cancel_job.return_value = True

    response = client.post("/api/python/jobs/test-job-id/cancel")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "Cancelamento solicitado" in data["message"]
