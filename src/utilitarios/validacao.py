import re

def validar_cnpj(cnpj: str) -> bool:
    """
    Valida se um CNPJ é válido.
    """
    cnpj = re.sub(r'[^0-9]', '', cnpj)
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    def calcular_digito(fatia, pesos):
        soma = sum(int(n) * p for n, p in zip(fatia, pesos))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    if calcular_digito(cnpj[:12], pesos1) != int(cnpj[12]):
        return False

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    if calcular_digito(cnpj[:13], pesos2) != int(cnpj[13]):
        return False

    return True
