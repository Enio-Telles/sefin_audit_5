from src.utilitarios.validacao import validar_cnpj


def test_validar_cnpj_valido():
    """Testa CNPJs validos, com e sem pontuação."""
    assert validar_cnpj("11.222.333/0001-81") is True
    assert validar_cnpj("11222333000181") is True
    # Outro CNPJ válido para garantir
    assert validar_cnpj("00.000.000/0001-91") is True
    assert validar_cnpj("00000000000191") is True


def test_validar_cnpj_invalido_tamanho():
    """Testa CNPJs com tamanho invalido."""
    assert validar_cnpj("1122233300018") is False  # 13 digitos
    assert validar_cnpj("112223330001810") is False  # 15 digitos
    assert validar_cnpj("123") is False
    assert validar_cnpj("") is False


def test_validar_cnpj_invalido_repetido():
    """Testa CNPJs com todos os digitos iguais."""
    assert validar_cnpj("00000000000000") is False
    assert validar_cnpj("11111111111111") is False
    assert validar_cnpj("22222222222222") is False


def test_validar_cnpj_invalido_digitos_verificadores():
    """Testa CNPJs com digitos verificadores incorretos."""
    assert validar_cnpj("11.222.333/0001-80") is False  # Digito correto seria 81
    assert validar_cnpj("11222333000182") is False
    assert validar_cnpj("00.000.000/0001-90") is False  # Digito correto seria 91
    assert validar_cnpj("00000000000100") is False


def test_validar_cnpj_alfanumerico():
    """Testa string com letras (deve remover letras e verificar tamanho)."""
    assert validar_cnpj("A1.222.333/0001-81") is False
    assert validar_cnpj("11.222.333/0001-XX") is False
