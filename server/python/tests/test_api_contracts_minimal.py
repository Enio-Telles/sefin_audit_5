import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from api import app

@pytest.fixture
def client():
    return TestClient(app)

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["ok", "healthy"]

@patch("core.config_loader.get_config_var")
def test_auditoria_pipeline_contract(mock_config, client):
    mock_config.side_effect = lambda var, default=None: {
        "obter_diretorios_cnpj": lambda cnpj: (Path("/tmp"), Path("/tmp"), Path("/tmp")),
        "DIR_SQL": Path("/tmp")
    }.get(var, default)

    payload = {
        "cnpj": "00000000000191",
        "username": "tester",
        "mes_ano_inicio": "012023",
        "mes_ano_fim": "122023"
    }

    mock_background = MagicMock()
    with patch("fastapi.BackgroundTasks.add_task", mock_background):
        response = client.post("/api/python/auditoria/pipeline", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data.get("success") is True
    assert data.get("cnpj") == "00000000000191"

@patch("core.config_loader.get_config_var")
def test_auditoria_historico_cnpj_contract(mock_config, client):
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
    mock_config.side_effect = lambda var, default=None: {
        "obter_diretorios_cnpj": lambda cnpj: (Path("/tmp"), Path("/tmp"), Path("/tmp"))
    }.get(var, default)

    response = client.get("/api/python/auditoria/status/00000000000191")

    assert response.status_code == 200
    data = response.json()
    assert data.get("success") is True
    assert "cnpj" in data
    assert "job_status" in data
    assert "message" in data
    assert "etapas" in data

@patch("core.config_loader.get_config_var")
@patch("routers.produtos.status._PROJETO_DIR", new_callable=MagicMock, create=True)
@patch("routers.produtos.status._resumir_status_analise", create=True)
@patch("routers.produtos.status._gravar_status_analise", create=True)
@patch("routers.produtos.status._STATUS_ANALISE_COLUMNS", ["col1"], create=True)
@patch("routers.produtos.status.pl.read_parquet")
def test_produtos_status_analise_contract(mock_read, mock_gravar, mock_resumir, mock_projeto, mock_config, client):
    mock_config.side_effect = lambda var, default=None: {
        "obter_diretorios_cnpj": lambda cnpj: (Path("/tmp"), Path("/tmp"), Path("/tmp"))
    }.get(var, default)

    mock_gravar.return_value = Path('/tmp/mock.parquet')
    import polars as pl
    mock_read.return_value = pl.DataFrame({"status": ["concluido"]})
    mock_resumir.return_value = {
        "cnpj": "00000000000191",
        "has_tabela_final": True,
        "rows_total": 100,
        "rows_analisados": 100,
        "progresso": 100.0,
        "job_status": "concluido",
        "etapa_atual": "Finalizado"
    }

    response = client.get("/api/python/produtos/status-analise?cnpj=00000000000191")
    assert response.status_code == 200
    data = response.json()
    assert "has_tabela_final" in data["resumo"]
    assert "progresso" in data["resumo"]
    assert data["resumo"]["cnpj"] == "00000000000191"
