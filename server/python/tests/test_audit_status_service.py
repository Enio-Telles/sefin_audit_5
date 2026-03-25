import json
from pathlib import Path
from unittest.mock import patch, mock_open
import pytest

from core.audit_status_service import atualizar_status_pipeline, obter_status_pipeline

@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path

def test_atualizar_status_pipeline_new_file(temp_dir):
    atualizar_status_pipeline(
        dir_analises=temp_dir,
        status="em_andamento",
        message="Processando dados"
    )

    status_file = temp_dir / "status_pipeline.json"
    assert status_file.exists()

    with open(status_file, "r") as f:
        data = json.load(f)

    assert data["status"] == "em_andamento"
    assert data["message"] == "Processando dados"
    assert "updated_at" in data
    assert len(data["etapas"]) == 4
    assert data["erros"] == []

def test_atualizar_status_pipeline_existing_file(temp_dir):
    # Setup initial state
    initial_data = {
        "status": "agendada",
        "message": "Inicial",
        "etapas": [
            {"etapa": "Extração de Dados", "status": "pendente"},
            {"etapa": "Cruzamentos e Análises", "status": "pendente"},
            {"etapa": "Análise de Produtos", "status": "pendente"},
            {"etapa": "Geração de Relatórios", "status": "pendente"},
        ],
        "erros": [],
        "extra_field": "kept"
    }

    status_file = temp_dir / "status_pipeline.json"
    with open(status_file, "w") as f:
        json.dump(initial_data, f)

    atualizar_status_pipeline(
        dir_analises=temp_dir,
        status="em_andamento",
        message="Atualizado",
        etapas=[{"etapa": "Extração de Dados", "status": "concluido"}],
        erros=["Erro 1"]
    )

    with open(status_file, "r") as f:
        data = json.load(f)

    assert data["status"] == "em_andamento"
    assert data["message"] == "Atualizado"
    assert data["extra_field"] == "kept"
    assert data["erros"] == ["Erro 1"]
    assert data["etapas"][0]["status"] == "concluido"
    assert data["etapas"][1]["status"] == "pendente"

def test_atualizar_status_pipeline_invalid_json(temp_dir):
    status_file = temp_dir / "status_pipeline.json"
    with open(status_file, "w") as f:
        f.write("{invalid_json}")

    # Should not crash, just overwrite/update with defaults
    atualizar_status_pipeline(
        dir_analises=temp_dir,
        status="novo_status",
        message="nova_mensagem"
    )

    with open(status_file, "r") as f:
        data = json.load(f)

    assert data["status"] == "novo_status"

def test_atualizar_status_pipeline_write_error(temp_dir, caplog):
    # Mock open to raise an exception on write
    with patch("builtins.open", mock_open()) as m_open:
        m_open.side_effect = Exception("Write failed")

        atualizar_status_pipeline(
            dir_analises=temp_dir,
            status="status",
            message="message"
        )

    assert "Erro ao atualizar status: Write failed" in caplog.text

def test_obter_status_pipeline_not_exists(temp_dir):
    result = obter_status_pipeline(temp_dir)
    assert result["job_status"] == "agendada"
    assert result["message"] == "Auditoria agendada em segundo plano."
    assert result["etapas"] == []
    assert result["erros"] == []

def test_obter_status_pipeline_exists(temp_dir):
    data = {
        "status": "concluido",
        "message": "Finalizado",
        "etapas": [{"etapa": "1"}],
        "erros": ["Erro"]
    }
    status_file = temp_dir / "status_pipeline.json"
    with open(status_file, "w") as f:
        json.dump(data, f)

    result = obter_status_pipeline(temp_dir)
    assert result["job_status"] == "concluido"
    assert result["message"] == "Finalizado"
    assert result["etapas"] == [{"etapa": "1"}]
    assert result["erros"] == ["Erro"]

def test_obter_status_pipeline_fallback_message(temp_dir):
    data = {
        "motivo": "Falhou por X"
    }
    status_file = temp_dir / "status_pipeline.json"
    with open(status_file, "w") as f:
        json.dump(data, f)

    result = obter_status_pipeline(temp_dir)
    assert result["message"] == "Falhou por X"

def test_obter_status_pipeline_invalid_json(temp_dir):
    status_file = temp_dir / "status_pipeline.json"
    with open(status_file, "w") as f:
        f.write("not a json")

    result = obter_status_pipeline(temp_dir)
    assert result["job_status"] == "agendada"
    assert result["message"] == "Auditoria agendada em segundo plano."
