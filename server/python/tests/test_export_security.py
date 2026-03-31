import pytest
from fastapi.testclient import TestClient
from pathlib import Path
from unittest.mock import patch, MagicMock

# Assuming we can run this by importing from api or similar
import sys
import os

# Adjust path to import correctly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from routers.export import router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)
client = TestClient(app)

def test_export_excel_download_path_traversal():
    # Attempt to read a file outside allowed paths (e.g., /etc/passwd or something outside)
    # Using a fake path that _is_path_allowed should reject
    response = client.get("/api/python/export/excel-download?file_path=/etc/passwd")

    assert response.status_code in (403, 400, 404)
    if response.status_code == 403:
        assert response.json()["detail"] == "Acesso ao caminho não permitido"

def test_export_to_excel_path_traversal():
    # Mock _is_path_allowed to fail on source but pass on output_dir
    with patch("routers.export._is_path_allowed", side_effect=lambda x: str(x) == "/valid/output"):
        payload = {
            "source_files": ["/etc/passwd", "../../../../../etc/passwd"],
            "output_dir": "/valid/output"
        }
        # Also need to mock output_dir creation since it's a fake path
        with patch("routers.export.Path.mkdir"):
            response = client.post("/api/python/export/excel", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["results"]) == 2
        for res in data["results"]:
            assert res["status"] == "error"
            assert res["message"] in ("Acesso ao caminho não permitido", "Caminho inválido")

def test_export_to_excel_output_dir_path_traversal():
    # Attempt to use path traversal in output_dir
    payload = {
        "source_files": ["/tmp/safe.parquet"],
        "output_dir": "/etc/cron.d"
    }
    response = client.post("/api/python/export/excel", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert len(data["results"]) == 1
    assert data["results"][0]["status"] == "error"
    assert data["results"][0]["message"] in ("Acesso ao diretório de saída não permitido", "Caminho de saída inválido")
