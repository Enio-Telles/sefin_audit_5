import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from server.python.core.config_loader import get_config_module, get_config_var

def test_get_config_module_success(tmp_path):
    config_file = tmp_path / "config.py"
    config_file.write_text("MY_VAR = 'test_value'\n")

    with patch("server.python.core.config_loader.Path") as mock_path_class:
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.resolve.return_value.parent.parent.parent.parent = tmp_path

        cfg = get_config_module()
        assert cfg is not None
        assert cfg.MY_VAR == "test_value"

def test_get_config_module_file_not_found(tmp_path):
    with patch("server.python.core.config_loader.Path") as mock_path_class:
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.resolve.return_value.parent.parent.parent.parent = tmp_path

        cfg = get_config_module()
        assert cfg is None

def test_get_config_module_invalid_spec(tmp_path):
    config_file = tmp_path / "config.py"
    config_file.write_text("MY_VAR = 'test_value'\n")

    with patch("server.python.core.config_loader.Path") as mock_path_class, \
         patch("importlib.util.spec_from_file_location", return_value=None):
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.resolve.return_value.parent.parent.parent.parent = tmp_path

        cfg = get_config_module()
        assert cfg is None

def test_get_config_module_invalid_spec_loader(tmp_path):
    config_file = tmp_path / "config.py"
    config_file.write_text("MY_VAR = 'test_value'\n")

    mock_spec = MagicMock()
    mock_spec.loader = None

    with patch("server.python.core.config_loader.Path") as mock_path_class, \
         patch("importlib.util.spec_from_file_location", return_value=mock_spec):
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.resolve.return_value.parent.parent.parent.parent = tmp_path

        cfg = get_config_module()
        assert cfg is None

def test_get_config_module_exception(tmp_path):
    config_file = tmp_path / "config.py"
    config_file.write_text("MY_VAR = 'test_value'\n")

    with patch("server.python.core.config_loader.Path") as mock_path_class, \
         patch("importlib.util.module_from_spec", side_effect=Exception("Test Error")):
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.resolve.return_value.parent.parent.parent.parent = tmp_path

        cfg = get_config_module()
        assert cfg is None

def test_get_config_var_success():
    mock_cfg = MagicMock()
    mock_cfg.MY_VAR = "my_value"

    with patch("server.python.core.config_loader.get_config_module", return_value=mock_cfg):
        result = get_config_var("MY_VAR")
        assert result == "my_value"

def test_get_config_var_not_found_with_default():
    mock_cfg = object()

    with patch("server.python.core.config_loader.get_config_module", return_value=mock_cfg):
        result = get_config_var("MISSING_VAR", default="default_value")
        assert result == "default_value"

def test_get_config_var_no_module():
    with patch("server.python.core.config_loader.get_config_module", return_value=None):
        result = get_config_var("ANY_VAR", default="fallback")
        assert result == "fallback"
