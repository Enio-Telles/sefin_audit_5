import pytest
from pathlib import Path
from unittest.mock import Mock, call
from extracao.leitor_sql import ler_sql

def test_ler_sql_utf8(monkeypatch):
    """Test successful reading with the default utf-8 encoding."""
    mock_exists = Mock(return_value=True)
    monkeypatch.setattr(Path, "exists", mock_exists)
    mock_read_text = Mock()
    mock_read_text.return_value = "SELECT * FROM table; "
    monkeypatch.setattr(Path, "read_text", mock_read_text)

    result = ler_sql("dummy_file.sql")

    assert result == "SELECT * FROM table"
    mock_read_text.assert_called_once_with(encoding='utf-8')

def test_ler_sql_fallback_unicode_error(monkeypatch):
    """Test fallback when utf-8 raises UnicodeDecodeError and latin-1 succeeds."""
    mock_exists = Mock(return_value=True)
    monkeypatch.setattr(Path, "exists", mock_exists)
    mock_read_text = Mock()

    def side_effect(encoding):
        if encoding == 'utf-8':
            raise UnicodeDecodeError('utf-8', b'', 0, 1, 'mock reason')
        return "SELECT * FROM table;"

    mock_read_text.side_effect = side_effect
    monkeypatch.setattr(Path, "read_text", mock_read_text)

    result = ler_sql("dummy_file.sql")

    assert result == "SELECT * FROM table"
    assert mock_read_text.call_count == 2
    assert mock_read_text.call_args_list == [call(encoding='utf-8'), call(encoding='latin-1')]

def test_ler_sql_file_not_found():
    """Test reading a file that doesn't exist."""
    fake_path = "caminho/fake/que/nao/existe.sql"
    with pytest.raises(FileNotFoundError, match=f"Arquivo SQL não encontrado: {fake_path}"):
        ler_sql(fake_path)
