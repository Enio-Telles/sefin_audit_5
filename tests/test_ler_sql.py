import pytest
from pathlib import Path
from funcoes_auxiliares.ler_sql import ler_sql

def test_ler_sql_utf8(mocker):
    """Test successful reading with the default utf-8 encoding."""
    mock_read_text = mocker.patch('pathlib.Path.read_text')
    mock_read_text.return_value = "SELECT * FROM table; "

    result = ler_sql("dummy_file.sql")

    assert result == "SELECT * FROM table"
    mock_read_text.assert_called_once_with(encoding='utf-8')

def test_ler_sql_fallback_unicode_error(mocker):
    """Test fallback when utf-8 raises UnicodeDecodeError and latin-1 succeeds."""
    mock_read_text = mocker.patch('pathlib.Path.read_text')

    def side_effect(encoding):
        if encoding == 'utf-8':
            raise UnicodeDecodeError('utf-8', b'', 0, 1, 'mock reason')
        return "SELECT * FROM table;"

    mock_read_text.side_effect = side_effect

    result = ler_sql("dummy_file.sql")

    assert result == "SELECT * FROM table"
    assert mock_read_text.call_count == 2
    mock_read_text.assert_any_call(encoding='utf-8')
    mock_read_text.assert_any_call(encoding='latin-1')
