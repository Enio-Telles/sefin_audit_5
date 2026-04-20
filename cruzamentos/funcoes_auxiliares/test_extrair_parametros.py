from cruzamentos.funcoes_auxiliares.extrair_parametros import extrair_parametros_sql

def test_extrair_parametros_simples():
    sql = "SELECT * FROM tabela WHERE id = :id_valor"
    assert extrair_parametros_sql(sql) == {"id_valor"}

def test_extrair_parametros_multiplos():
    sql = "SELECT * FROM tabela WHERE id = :id AND nome = :nome_usuario"
    assert extrair_parametros_sql(sql) == {"id", "nome_usuario"}

def test_extrair_parametros_repetidos():
    sql = "SELECT * FROM tabela WHERE col1 = :val OR col2 = :val"
    assert extrair_parametros_sql(sql) == {"val"}

def test_extrair_parametros_sem_parametros():
    sql = "SELECT * FROM tabela"
    assert extrair_parametros_sql(sql) == set()

def test_extrair_parametros_com_underscore():
    sql = "SELECT :var_nome_longa FROM dual"
    assert extrair_parametros_sql(sql) == {"var_nome_longa"}

def test_extrair_parametros_falso_positivo_data():
    sql = "SELECT TO_CHAR(sysdate, 'HH24:MI:SS') FROM dual"
    # Current implementation will likely return {'MI', 'SS'}.
    # This test is expected to fail if we want to be strict.
    assert extrair_parametros_sql(sql) == set()

def test_extrair_parametros_postgres_cast():
    sql = "SELECT '123'::integer"
    # Current implementation will likely return {'integer'}.
    assert extrair_parametros_sql(sql) == set()
