import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Setup path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.append(str(_PROJECT_ROOT))

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


@patch("importlib.util.spec_from_file_location")
def test_auditoria_pipeline_contract_invalid_cnpj(mock_spec):
    response = client.post("/api/python/auditoria/pipeline", json={"cnpj": "123"})
    assert response.status_code == 400
    assert response.json() == {"detail": "CNPJ inválido"}

@patch("importlib.util.spec_from_file_location")
@patch("routers.analysis.BackgroundTasks.add_task")
def test_auditoria_pipeline_contract_valid_cnpj(mock_add_task, mock_spec):
    # Mock config loader
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))
    mock_module.DIR_SQL = Path("/tmp/sql")

    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance

    with patch("importlib.util.module_from_spec", return_value=mock_module):
        with patch("builtins.open", MagicMock()):
            response = client.post("/api/python/auditoria/pipeline", json={"cnpj": "00000000000191"})

            assert response.status_code == 200
            data = response.json()

            # Check mandatory shape
            assert "success" in data
            assert data["success"] is True
            assert "cnpj" in data
            assert data["cnpj"] == "00000000000191"
            assert "job_status" in data
            assert data["job_status"] == "agendada"
            assert "message" in data
            assert "dir_parquet" in data
            assert "dir_analises" in data
            assert "dir_relatorios" in data

            # Verify background task was called
            mock_add_task.assert_called_once()

@patch("routers.filesystem.detalhes_historico_cnpj")
@patch("importlib.util.spec_from_file_location")
def test_auditoria_status_contract(mock_spec, mock_detalhes):
    # Mock config
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance

    # Mock file system reader function to return dummy file list
    mock_detalhes.return_value = {
        "etapas": [{"nome": "Extracao", "status": "concluido"}],
        "erros": [],
        "arquivos_extraidos": [],
        "arquivos_analises": [],
        "arquivos_produtos": [],
        "arquivos_relatorios": []
    }

    with patch("importlib.util.module_from_spec", return_value=mock_module):
        with patch("builtins.open", MagicMock()):
            # Test valid
            response = client.get("/api/python/auditoria/status/00000000000191")

            assert response.status_code == 200
            data = response.json()

            # Check mandatory shape
            assert "success" in data
            assert data["success"] is True
            assert "cnpj" in data
            assert "job_status" in data
            assert data["job_status"] in ["agendada", "executando", "concluida", "erro"]
            assert "message" in data
            assert "etapas" in data
            assert isinstance(data["etapas"], list)
            assert "erros" in data
            assert isinstance(data["erros"], list)
            assert "arquivos_extraidos" in data
            assert isinstance(data["arquivos_extraidos"], list)


@patch("routers.produtos.revisao._load_cnpj_dirs", create=True)
@patch("pathlib.Path.exists", return_value=False)
def test_produtos_revisao_final_contract(mock_exists, mock_load):
    mock_load.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    response = client.get("/api/python/produtos/revisao-final?cnpj=00000000000191")
    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert data["success"] is True
    assert "available" in data
    assert "file_path" in data
    assert "summary" in data
    assert isinstance(data["summary"], dict)

@patch("routers.produtos.status._gravar_status_analise", create=True)
@patch("routers.produtos.status._resumir_status_analise", create=True)
@patch("importlib.util.spec_from_file_location")
@patch("routers.produtos.status._PROJETO_DIR", new_callable=MagicMock, create=True)
@patch("routers.produtos.status._STATUS_ANALISE_COLUMNS", ["col1"], create=True)
def test_produtos_status_analise_contract(mock_projeto, mock_spec, mock_resumir, mock_gravar):
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance

    # Path of generated file
    mock_gravar.return_value = Path("/tmp/analises/status.parquet")
    mock_resumir.return_value = {"total_processado": 10}

    with patch("importlib.util.module_from_spec", return_value=mock_module):
        with patch("pathlib.Path.exists", return_value=False):
            response = client.get("/api/python/produtos/status-analise?cnpj=00000000000191&include_data=true")
            assert response.status_code == 200
            data = response.json()

            assert "success" in data
            assert data["success"] is True
            assert "file_path" in data
            assert "data" in data
            assert isinstance(data["data"], list)
            assert "resumo" in data
            assert isinstance(data["resumo"], dict)

@patch("routers.produtos.status.obter_runtime_produtos_status", create=True)
@patch("importlib.util.spec_from_file_location")
@patch("routers.produtos.status._PROJETO_DIR", new_callable=MagicMock, create=True)
def test_produtos_runtime_status_contract(mock_projeto, mock_spec, mock_runtime):
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance

    mock_runtime.return_value = {"files": [{"name": "teste", "size": 1}]}

    with patch("importlib.util.module_from_spec", return_value=mock_module):
        response = client.get("/api/python/produtos/runtime-status?cnpj=00000000000191")
        assert response.status_code == 200
        data = response.json()

        assert "success" in data
        assert data["success"] is True
        assert "cnpj" in data
        assert data["cnpj"] == "00000000000191"
        assert "runtime" in data
        assert "files" in data["runtime"]


@patch("routers.produtos.vectorizacao.read_vector_cache_metadata", create=True)
@patch("routers.produtos.vectorizacao.compute_file_sha1", create=True)
@patch("importlib.util.spec_from_file_location")
@patch("routers.produtos.vectorizacao._PROJETO_DIR", new_callable=MagicMock, create=True)
def test_produtos_vectorizacao_status_contract(mock_projeto, mock_spec, mock_sha1, mock_metadata):
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance

    mock_sha1.return_value = "abcdef123"
    mock_metadata.return_value = {"base_hash": "abcdef123"}

    with patch("importlib.util.module_from_spec", return_value=mock_module):
        with patch("pathlib.Path.exists", return_value=True):
            response = client.get("/api/python/produtos/vectorizacao-status?cnpj=00000000000191")
            assert response.status_code == 200
            data = response.json()

            assert "success" in data
            assert data["success"] is True
            assert "status" in data
            assert "caches" in data

@patch("routers.produtos.multidescricao._load_cnpj_dirs", create=True)
@patch("routers.produtos.multidescricao._normalize_page", return_value=1, create=True)
@patch("routers.produtos.multidescricao._normalize_page_size", return_value=50, create=True)
@patch("pathlib.Path.exists", return_value=False)
def test_produtos_codigos_multidescricao_contract(mock_exists, mock_page_size, mock_page, mock_load):
    mock_load.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))

    response = client.get("/api/python/produtos/codigos-multidescricao?cnpj=00000000000191")
    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert data["success"] is True
    assert "file_path" in data
    assert "data" in data
    assert "page" in data
    assert "page_size" in data
    assert "total" in data
    assert "total_pages" in data

@patch("routers.produtos.multidescricao._load_cnpj_dirs", create=True)
@patch("routers.produtos.multidescricao._PROJETO_DIR", new_callable=MagicMock, create=True)
@patch("importlib.util.spec_from_file_location")
@patch("pathlib.Path.exists", return_value=False)
def test_produtos_codigo_multidescricao_resumo_contract(mock_exists, mock_spec, mock_projeto, mock_load):
    mock_load.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))
    mock_module = MagicMock()
    mock_module.obter_diretorios_cnpj.return_value = (Path("/tmp/parquet"), Path("/tmp/analises"), Path("/tmp/relatorios"))
    mock_spec_instance = MagicMock()
    mock_spec_instance.loader.exec_module = lambda m: None
    mock_spec.return_value = mock_spec_instance
    with patch("importlib.util.module_from_spec", return_value=mock_module):
        response = client.get("/api/python/produtos/codigo-multidescricao-resumo?cnpj=00000000000191&codigo=TESTE")
    assert response.status_code == 200
    data = response.json()

    assert "success" in data
    assert data["success"] is True
    assert "codigo" in data
    assert "grupos_descricao" in data
    assert "opcoes_consenso" in data
    assert "resumo" in data
