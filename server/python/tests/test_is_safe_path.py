import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import importlib.util
spec = importlib.util.spec_from_file_location("api_Enio", str(Path(__file__).parent.parent / "api-Enio.py"))
api_Enio = importlib.util.module_from_spec(spec)
sys.modules["api_Enio"] = api_Enio
spec.loader.exec_module(api_Enio)

is_safe_path = api_Enio.is_safe_path
_PROJETO_DIR = api_Enio._PROJETO_DIR
_CRUZAMENTOS_DIR = api_Enio._CRUZAMENTOS_DIR

def test_is_safe_path_resolves_and_validates():
    safe_target = _PROJETO_DIR / "some_safe_file.txt"
    result = is_safe_path(str(safe_target))
    assert isinstance(result, Path)
    assert result.resolve() == safe_target.resolve()

    unsafe_target = _CRUZAMENTOS_DIR / ".." / ".." / "etc" / "passwd"
    result_unsafe = is_safe_path(str(unsafe_target))
    assert result_unsafe is None

    result_absolute_unsafe = is_safe_path("/etc/passwd")
    assert result_absolute_unsafe is None
