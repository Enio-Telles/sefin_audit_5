import pytest
import polars as pl
from pathlib import Path
from funcoes_tabelas.tabela_produtos.co_sefin import co_sefin

def test_co_sefin_missing_reference_files(mocker):
    # Mock Path.exists to return False for reference files
    mocker.patch.object(Path, "exists", return_value=False)

    # Executar a função e verificar se retorna False
    resultado = co_sefin("12345678901234")
    assert resultado is False


def test_co_sefin_missing_target_files(mocker):
    # Mock Path.exists to return True for ref files, False for target files
    def mock_exists(self):
        # Reference files are in REFS_DIR which is mapped to 'sitafe_*.parquet'
        if "sitafe" in self.name:
            return True
        return False

    mocker.patch.object(Path, "exists", side_effect=mock_exists, autospec=True)

    # Executar a função e verificar se retorna False
    resultado = co_sefin("12345678901234")
    assert resultado is False


def test_co_sefin_happy_path(mocker):
    # Setup DataFrames para os mocks
    # ref_cest_ncm: it_nu_cest, it_nu_ncm, it_co_sefin
    df_ref_cn = pl.DataFrame({
        "it_nu_cest": ["123", "456"],
        "it_nu_ncm": ["111", "222"],
        "it_co_sefin": ["001", "002"]
    })

    # ref_cest: cest, co-sefin
    df_ref_c = pl.DataFrame({
        "cest": ["789"],
        "co-sefin": ["003"]
    })

    # ref_ncm: ncm, co-sefin
    df_ref_n = pl.DataFrame({
        "ncm": ["333"],
        "co-sefin": ["004"]
    })

    # target file
    df_target = pl.DataFrame({
        "ncm": ["111", "333"],
        "cest": ["123", "999"]
    })

    def mock_scan_parquet(source):
        source_str = str(source)
        if "sitafe_cest_ncm.parquet" in source_str:
            return df_ref_cn.lazy()
        elif "sitafe_cest.parquet" in source_str:
            return df_ref_c.lazy()
        elif "sitafe_ncm.parquet" in source_str:
            return df_ref_n.lazy()
        else:
            return df_target.lazy()

    mocker.patch("polars.scan_parquet", side_effect=mock_scan_parquet)

    def mock_exists(self):
        return True

    mocker.patch.object(Path, "exists", side_effect=mock_exists, autospec=True)
    mocker.patch("funcoes_tabelas.tabela_produtos.co_sefin.salvar_para_parquet", return_value=True)

    resultado = co_sefin("12345678901234")
    assert resultado is True
