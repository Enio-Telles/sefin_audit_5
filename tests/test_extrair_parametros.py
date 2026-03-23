import pytest
from extracao.extrair_parametros import extrair_parametros_sql, extract_sql_parameters


def test_extrair_parametros_sql():
    sql = "SELECT * FROM tabela WHERE cnpj = :CNPJ AND data >= :data_inicial AND data <= :data_final"
    binds = extrair_parametros_sql(sql)
    assert binds == {"CNPJ", "data_inicial", "data_final"}


def test_extrair_parametros_sql_duplicates():
    sql = "SELECT * FROM tabela WHERE cnpj = :CNPJ AND (cnpj_destino = :CNPJ OR data = :data)"
    binds = extrair_parametros_sql(sql)
    assert binds == {"CNPJ", "data"}


def test_extract_sql_parameters():
    sql = "SELECT * FROM tabela WHERE cnpj = :CNPJ AND data >= :data_inicial AND valor > :valor_minimo"
    params = extract_sql_parameters(sql)

    # Sort the list for consistent testing
    params.sort(key=lambda x: x["name"])

    expected = [
        {"name": "CNPJ", "type": "text", "required": True},
        {"name": "data_inicial", "type": "date", "required": False},
        {"name": "valor_minimo", "type": "number", "required": False},
    ]
    expected.sort(key=lambda x: x["name"])

    assert params == expected


def test_extract_sql_parameters_no_params():
    sql = "SELECT * FROM tabela WHERE status = 'ativo'"
    params = extract_sql_parameters(sql)
    assert params == []


def test_extract_sql_parameters_inferred_types():
    sql = "SELECT :cnpj, :data_inicio, :valor_total, :nome_cliente FROM dual"
    params = extract_sql_parameters(sql)
    params_dict = {p["name"]: p for p in params}

    assert params_dict["cnpj"]["type"] == "text"
    assert params_dict["cnpj"]["required"] is True

    assert params_dict["data_inicio"]["type"] == "date"
    assert params_dict["data_inicio"]["required"] is False

    assert params_dict["valor_total"]["type"] == "number"
    assert params_dict["valor_total"]["required"] is False

    assert params_dict["nome_cliente"]["type"] == "text"
    assert params_dict["nome_cliente"]["required"] is False


def test_extrair_parametros_sql_unusual_sql():
    # Test gracefully failing and ignoring invalid parameters
    assert extrair_parametros_sql("") == set()
    assert extrair_parametros_sql(":") == set()
    assert extrair_parametros_sql(":123_invalid_but_captured") == {
        "123_invalid_but_captured"
    }
    assert extrair_parametros_sql("::") == set()
    assert extrair_parametros_sql("SELECT * FROM table WHERE col = :") == set()
    assert extrair_parametros_sql("SELECT * FROM table WHERE col = :!@#") == set()
    assert extrair_parametros_sql(
        "SELECT :valid, :_also_valid, :123_valid FROM DUAL"
    ) == {"valid", "_also_valid", "123_valid"}
    assert extrair_parametros_sql("Some random text with :param inside") == {"param"}
    assert extrair_parametros_sql("Emojis 🤔 :smile and :") == {"smile"}


def test_extrair_parametros_sql_type_error():
    # Test handling of non-string inputs using pytest.raises
    with pytest.raises(TypeError):
        extrair_parametros_sql(None)
    with pytest.raises(TypeError):
        extrair_parametros_sql(123)
    with pytest.raises(TypeError):
        extrair_parametros_sql(["SELECT :param"])
