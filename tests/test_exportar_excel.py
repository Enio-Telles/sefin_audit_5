import pytest
import pandas as pd
import polars as pl
from pathlib import Path
from funcoes_auxiliares.exportar_excel import exportar_excel

def test_exportar_excel_pandas_empty(tmp_path):
    df = pd.DataFrame()
    result = exportar_excel(df, "teste_pandas_empty", tmp_path)
    assert result is None
    assert not (tmp_path / "teste_pandas_empty.xlsx").exists()

def test_exportar_excel_polars_empty(tmp_path):
    df = pl.DataFrame()
    result = exportar_excel(df, "teste_polars_empty", tmp_path)
    assert result is None
    assert not (tmp_path / "teste_polars_empty.xlsx").exists()

def test_exportar_excel_pandas_valid(tmp_path):
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    result = exportar_excel(df, "teste_pandas_valid", tmp_path)
    assert result == tmp_path / "teste_pandas_valid.xlsx"
    assert result.exists()
    assert result.is_file()

def test_exportar_excel_polars_valid(tmp_path):
    df = pl.DataFrame({"A": [1, 2], "B": [3, 4]})
    result = exportar_excel(df, "teste_polars_valid", tmp_path)
    assert result == tmp_path / "teste_polars_valid.xlsx"
    assert result.exists()
    assert result.is_file()

def test_exportar_excel_creates_directory(tmp_path):
    df = pd.DataFrame({"A": [1, 2]})
    new_dir = tmp_path / "new_folder"
    result = exportar_excel(df, "teste_new_dir", new_dir)
    assert result == new_dir / "teste_new_dir.xlsx"
    assert result.exists()
    assert result.is_file()
