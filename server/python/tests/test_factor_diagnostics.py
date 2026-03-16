from __future__ import annotations

import sys
import unittest
from pathlib import Path

import polars as pl


SERVER_PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(SERVER_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_PYTHON_DIR))

from core.factor_diagnostics import diagnosticar_fatores_conversao  # noqa: E402


class FactorDiagnosticsTests(unittest.TestCase):
    def test_diagnosticar_fatores_identifica_extremos_variacao_e_multiplas_unidades(self) -> None:
        df = pl.DataFrame(
            [
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "UN", "fator": 1.0, "editado_manual": False},
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "CX", "fator": 12.0, "editado_manual": True},
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "FD", "fator": 48.0, "editado_manual": False},
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "PC", "fator": 96.0, "editado_manual": False},
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "LT", "fator": 192.0, "editado_manual": False},
                {"chave_produto": "100", "ano_referencia": 2024, "unidade_origem": "KG", "fator": 384.0, "editado_manual": False},
                {"chave_produto": "200", "ano_referencia": 2024, "unidade_origem": "UN", "fator": 0.0, "editado_manual": False},
                {"chave_produto": "300", "ano_referencia": 2024, "unidade_origem": "CX", "fator": 2001.0, "editado_manual": False},
                {"chave_produto": "400", "ano_referencia": 2024, "unidade_origem": "SC", "fator": 0.0005, "editado_manual": False},
                {"chave_produto": "500", "ano_referencia": 2024, "unidade_origem": "", "fator": 2.0, "editado_manual": False},
            ]
        )

        result = diagnosticar_fatores_conversao(df)

        self.assertEqual(result["stats"]["total_registros"], 10)
        self.assertEqual(result["stats"]["editados_manual"], 1)
        self.assertEqual(result["stats"]["fatores_invalidos"], 1)
        self.assertEqual(result["stats"]["fatores_extremos_altos"], 1)
        self.assertEqual(result["stats"]["fatores_extremos_baixos"], 1)
        self.assertEqual(result["stats"]["grupos_muitas_unidades"], 1)
        self.assertEqual(result["stats"]["grupos_alta_variacao"], 1)

        issue_types = {item["tipo"] for item in result["issues"]}
        self.assertIn("FATOR_INVALIDO", issue_types)
        self.assertIn("FATOR_EXTREMO_ALTO", issue_types)
        self.assertIn("FATOR_EXTREMO_BAIXO", issue_types)
        self.assertIn("MULTIPLAS_UNIDADES", issue_types)
        self.assertIn("ALTA_VARIACAO_FATORES", issue_types)
        self.assertIn("UNIDADE_ORIGEM_VAZIA", issue_types)


if __name__ == "__main__":
    unittest.main()
