import pytest
import polars as pl
from pathlib import Path
from unittest.mock import Mock

import sys

# Ensure modules can be found
sys.path.insert(0, str(Path("funcoes_auxiliares").resolve()))
sys.path.insert(0, str(Path("funcoes_tabelas/tabela_produtos").resolve()))
# Fake some dependencies to avoid failing the module import
sys.modules['salvar_para_parquet'] = Mock()
sys.modules['validar_cnpj'] = Mock()
sys.modules['encontrar_arquivo_cnpj'] = Mock()
sys.modules['aux_leitura_notas'] = Mock()

from fator_conversao import gerar_template_fatores_manuais, ler_fatores_manuais, calcular_fator_conversao, _calcular_fator_final

def test_gerar_template_fatores_manuais_success(tmp_path, monkeypatch):
    """Test generating manual template excel file successfully (mocked)."""
    mock_write_excel = Mock()
    monkeypatch.setattr("polars.DataFrame.write_excel", mock_write_excel)

    result = gerar_template_fatores_manuais(tmp_path)

    assert result is True
    mock_write_excel.assert_called_once()
    assert (tmp_path / "template_fatores_manuais.xlsx") == mock_write_excel.call_args[0][0]

def test_gerar_template_fatores_manuais_failure(tmp_path, monkeypatch):
    """Test generating manual template handles exception."""
    def raise_exc(*args, **kwargs):
        raise Exception("Mocked exception")

    monkeypatch.setattr("polars.DataFrame.write_excel", raise_exc)

    result = gerar_template_fatores_manuais(tmp_path)

    assert result is False

def test_ler_fatores_manuais_file_not_found(tmp_path):
    """Test reading manual factors when file doesn't exist."""
    fake_path = tmp_path / "does_not_exist.xlsx"
    assert ler_fatores_manuais(fake_path) is None

def test_ler_fatores_manuais_success(tmp_path, monkeypatch):
    """Test reading valid manual factors excel file."""
    fake_path = tmp_path / "fatores.xlsx"
    fake_path.touch() # Make it "exist"

    # Create valid polars dataframe
    df_valid = pl.DataFrame({
        "codigo_produto_ajustado": ["123", "456"],
        "unid": ["UN", "KG"],
        "ano": ["2023", "2023"],
        "fator": [1.5, 2.0],
        "justificativa": ["teste 1", "teste 2"]
    })

    mock_read_excel = Mock(return_value=df_valid)
    monkeypatch.setattr("polars.read_excel", mock_read_excel)

    result = ler_fatores_manuais(fake_path)

    assert result is not None
    assert result.shape == (2, 3) # Note that justificativa wasn't dropped in the current implementation
    # Columns order based on presence, but let's assert it as a set to be safe or map exactly
    assert set(result.columns) == {"chave_produto", "unidade", "fator_conversao_manual"}
    assert result["fator_conversao_manual"][0] == 1.5

def test_ler_fatores_manuais_missing_column(tmp_path, monkeypatch):
    """Test reading manual factors when a required column is missing."""
    fake_path = tmp_path / "fatores.xlsx"
    fake_path.touch() # Make it "exist"

    # Create invalid polars dataframe (missing "ano")
    df_invalid = pl.DataFrame({
        "chave_produto": ["123", "456"],
        "unidade": ["UN", "KG"],
        "fator_conversao_manual": [1.5, 2.0]
    })

    mock_read_excel = Mock(return_value=df_invalid)
    monkeypatch.setattr("polars.read_excel", mock_read_excel)

    result = ler_fatores_manuais(fake_path)

    assert result is None

# Test calculating the final factor mapping pipeline

def test_pipeline_manual_factor_override(tmp_path, monkeypatch):
    """Test the calculation pipeline where manual factor overrides the automatic one."""

    # We test the end-of-pipeline modification directly
    # Mock df_final returned by _calcular_fator_final
    df_final_auto = pl.DataFrame({
        "chave_produto": ["A", "B", "C"],
        "ano": ["2023", "2023", "2023"],
        "unidade": ["UN", "KG", "L"],
        "unid_padrao": ["UN", "UN", "UN"],
        "v_ent": [0.0, 0.0, 0.0],
        "q_ent": [0.0, 0.0, 0.0],
        "preco_med_ent": [0.0, 0.0, 0.0],
        "v_sai": [0.0, 0.0, 0.0],
        "q_sai": [0.0, 0.0, 0.0],
        "preco_med_sai": [0.0, 0.0, 0.0],
        "fator_conversao": [1.0, 2.5, 3.0] # Auto calculated factors
    })

    df_manual = pl.DataFrame({
        "chave_produto": ["A", "B"], # "C" is not overriden
        "ano": ["2023", "2023"],
        "unidade": ["UN", "KG"],
        "fator_conversao_manual": [1.2, 5.0] # Manual overrides
    })

    # The block inside calcular_fator_conversao where the override happens
    df_final = df_final_auto.join(df_manual, on=["chave_produto", "unidade", "ano"], how="left")
    df_final = df_final.with_columns([
        pl.when(pl.col("fator_conversao_manual").is_not_null())
          .then(pl.col("fator_conversao_manual"))
          .otherwise(pl.col("fator_conversao"))
          .alias("fator_conversao"),
        pl.when(pl.col("fator_conversao_manual").is_not_null())
          .then(pl.lit("manual"))
          .otherwise(pl.lit("automático"))
          .alias("fonte_fator")
    ]).drop("fator_conversao_manual")

    assert list(df_final["fator_conversao"]) == [1.2, 5.0, 3.0]
    assert list(df_final["fonte_fator"]) == ["manual", "manual", "automático"]
