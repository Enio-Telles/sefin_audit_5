import pytest
from unittest.mock import MagicMock
import polars as pl
from pathlib import Path

from funcoes_auxiliares.aux_classif_merc import aux_classif_merc

# Fixture to provide standard mock dataframes for read_parquet
@pytest.fixture
def mock_read_parquet(mocker):
    def _mock_read_parquet(path):
        p = str(path)
        if "sitafe_cest_ncm.parquet" in p:
            return pl.DataFrame({
                "it_nu_ncm": ["12345678", "87654321"],
                "it_nu_cest": ["1234567", "7654321"],
                "it_co_sefin": ["sefin_cest_ncm_1", "sefin_cest_ncm_2"]
            })
        elif "sitafe_cest.parquet" in p:
            return pl.DataFrame({
                "cest": ["1234567", "9999999"],
                "co-sefin": ["sefin_cest_1", "sefin_cest_2"]
            })
        elif "sitafe_ncm.parquet" in p:
            return pl.DataFrame({
                "ncm": ["12345678", "88888888"],
                "co-sefin": ["sefin_ncm_1", "sefin_ncm_2"]
            })
        elif "sitafe_produto_sefin.parquet" in p:
            return pl.DataFrame({
                "it_co_sefin": [
                    "sefin_cest_ncm_1", "sefin_cest_1", "sefin_ncm_1",
                    "sefin_cest_ncm_2", "sefin_cest_2", "sefin_ncm_2",
                    "sefin_adicional_1"
                ],
                "it_no_produto": [
                    "Produto CEST NCM 1", "Produto CEST 1", "Produto NCM 1",
                    "Produto CEST NCM 2", "Produto CEST 2", "Produto NCM 2",
                    "Produto Adicional 1"
                ]
            })
        elif "sitafe_produto_sefin_aux.parquet" in p:
            return pl.DataFrame({
                "it_co_sefin": ["sefin_cest_ncm_1", "sefin_cest_1", "sefin_adicional_1", "sefin_adicional_1"],
                "it_da_inicio": ["20230101", "20230101", "20220101", "20230601"],
                "it_da_final": ["20231231", "20231231", "20230531", ""],
                "it_pc_interna": [18.0, 17.0, 12.0, 18.0],
                "it_in_st": ["S", "N", "S", "S"],
                "it_pc_mva": [40.0, 0.0, 50.0, 60.0],
                "it_in_mva_ajustado": ["N", "N", "S", "S"],
                "it_in_convenio": ["S", "N", "S", "S"],
                "it_in_isento_icms": ["N", "N", "N", "N"],
                "it_in_reducao": ["N", "N", "N", "N"],
                "it_pc_reducao": [0.0, 0.0, 0.0, 0.0],
                "it_in_reducao_credito": ["N", "N", "N", "N"],
                "it_in_pmpf": ["N", "N", "N", "N"]
            })
        return pl.DataFrame()
    return mocker.patch("polars.read_parquet", side_effect=_mock_read_parquet)

@pytest.fixture
def mock_env(mocker):
    # Mock Config Loader
    mocker.patch("importlib.util.spec_from_file_location")
    mock_module = MagicMock()
    mock_module.DIR_REFERENCIAS = Path("/dummy/referencias")
    mocker.patch("importlib.util.module_from_spec", return_value=mock_module)

    # Mock Path exists (for all paths to simulate successful env validation)
    mocker.patch("pathlib.Path.exists", return_value=True)

def test_aux_classif_merc_fallback_order(mock_env, mock_read_parquet):
    # Test fallback:
    # row 1: Matches CEST + NCM -> should be sefin_cest_ncm_1
    # row 2: Matches CEST only (NCM wrong) -> should be sefin_cest_2
    # row 3: Matches NCM only (CEST wrong) -> should be sefin_ncm_2
    # row 4: No match -> null

    df_input = pl.DataFrame({
        "ncm": ["12345678", "00000000", "88888888", "00000000"],
        "cest": ["1234567", "9999999", "0000000", "0000000"]
    })

    df_result = aux_classif_merc(df_input)

    co_sefin_list = df_result["co_sefin_inferido"].to_list()
    assert co_sefin_list[0] == "sefin_cest_ncm_1"
    assert co_sefin_list[1] == "sefin_cest_2"
    assert co_sefin_list[2] == "sefin_ncm_2"
    assert co_sefin_list[3] is None

    desc_list = df_result["descr_co_sefin_inferido"].to_list()
    assert desc_list[0] == "Produto CEST NCM 1"
    assert desc_list[1] == "Produto CEST 2"
    assert desc_list[2] == "Produto NCM 2"
    assert desc_list[3] is None

def test_aux_classif_merc_adicional(mock_env, mock_read_parquet):
    # Test with col_sefin_adicional provided
    df_input = pl.DataFrame({
        "ncm": ["12345678"],
        "cest": ["1234567"],
        "sefin_fronteira": ["sefin_adicional_1"]
    })

    df_result = aux_classif_merc(df_input, col_sefin_adicional="sefin_fronteira")

    assert "descr_sefin_fronteira" in df_result.columns
    assert df_result["descr_sefin_fronteira"].to_list()[0] == "Produto Adicional 1"

def test_aux_classif_merc_tributario(mock_env, mock_read_parquet):
    from datetime import date

    # Test retrieving tax attributes
    df_input = pl.DataFrame({
        "ncm": ["12345678"],
        "cest": ["1234567"],
        "dhemi": [date(2023, 6, 15)],
        "dhsaient": [None]
    })

    df_result = aux_classif_merc(df_input, col_dhemi="dhemi", col_dhsaient="dhsaient")

    assert "it_pc_interna_inferido" in df_result.columns
    assert df_result["it_pc_interna_inferido"].to_list()[0] == 18.0
    assert df_result["it_in_st_inferido"].to_list()[0] == "S"

def test_aux_classif_merc_tributario_adicional(mock_env, mock_read_parquet):
    from datetime import date

    # Test tax attributes for sefin adicional with time ranges
    df_input = pl.DataFrame({
        "ncm": ["12345678", "12345678"],
        "cest": ["1234567", "1234567"],
        "sefin_fronteira": ["sefin_adicional_1", "sefin_adicional_1"],
        "dhemi": [date(2023, 2, 1), date(2023, 8, 1)],
        "dhsaient": [date(2023, 1, 1), date(2023, 7, 1)] # dhsaient < dhemi, so max is dhemi
    })

    df_result = aux_classif_merc(df_input, col_sefin_adicional="sefin_fronteira", col_dhemi="dhemi", col_dhsaient="dhsaient")

    assert "it_pc_mva_fronteira" in df_result.columns
    mva_list = df_result["it_pc_mva_fronteira"].to_list()
    # Row 0: max date is 2023-02-01 -> matches [20220101, 20230531] -> mva = 50.0
    assert mva_list[0] == 50.0
    # Row 1: max date is 2023-08-01 -> matches [20230601, ''] -> mva = 60.0
    assert mva_list[1] == 60.0

def test_aux_classif_merc_missing_files(mocker):
    # Test FileNotFoundError when files are missing
    mocker.patch("importlib.util.spec_from_file_location")
    mock_module = MagicMock()
    mock_module.DIR_REFERENCIAS = Path("/dummy/referencias")
    mocker.patch("importlib.util.module_from_spec", return_value=mock_module)

    # Mock Path.exists to return False
    mocker.patch("pathlib.Path.exists", return_value=False)

    df_input = pl.DataFrame({"ncm": ["123"], "cest": ["456"]})

    with pytest.raises(FileNotFoundError, match="Um ou mais arquivos de referência sitafe não foram encontrados"):
        aux_classif_merc(df_input)
