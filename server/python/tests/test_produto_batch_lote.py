from __future__ import annotations

import sys
import unittest
from pathlib import Path

import polars as pl


SERVER_PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(SERVER_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_PYTHON_DIR))

from core.produto_batch_lote import (  # noqa: E402
    RULE_R1,
    RULE_R2,
    RULE_R3,
    RULE_R6,
    construir_preview_unificacao_lote,
    filtrar_tabela_final_para_lote,
)
from core.produto_classification import (  # noqa: E402
    compare_nullable_metric,
    filled_evidence_count_from_relations,
    is_equal_nullable_metric,
)


class ProdutoBatchLoteTests(unittest.TestCase):
    def test_compare_nullable_metric_states(self) -> None:
        self.assertEqual(compare_nullable_metric("22083020", "22083020"), "EQUAL_FILLED")
        self.assertEqual(compare_nullable_metric("", ""), "EQUAL_NULL")
        self.assertEqual(compare_nullable_metric("0201600", ""), "INCOMPLETE")
        self.assertEqual(compare_nullable_metric("789", "456"), "CONFLICT")
        self.assertTrue(is_equal_nullable_metric("EQUAL_FILLED"))
        self.assertTrue(is_equal_nullable_metric("EQUAL_NULL"))
        self.assertEqual(
            filled_evidence_count_from_relations("EQUAL_FILLED", "EQUAL_NULL", "EQUAL_FILLED"),
            2,
        )

    def test_filtrar_tabela_final_para_lote_uses_same_operational_fields(self) -> None:
        df = pl.DataFrame(
            [
                {"chave_produto": "ID_1", "lista_descricao": "WHISKY RED LABEL", "ncm_consenso": "22083020", "cest_consenso": "0201600"},
                {"chave_produto": "ID_2", "lista_descricao": "CAFE TORRADO", "ncm_consenso": "09012100", "cest_consenso": ""},
            ]
        )
        filtered = filtrar_tabela_final_para_lote(df, descricao_contains="whisky", ncm_contains="2208")
        self.assertEqual(filtered.height, 1)
        self.assertEqual(filtered.to_dicts()[0]["chave_produto"], "ID_1")

    def test_construir_preview_unificacao_lote_emits_r1_r2_r3_and_r6(self) -> None:
        df_agregados = pl.DataFrame(
            [
                {
                    "chave_produto": "ID_0001",
                    "descricao": "WHISKY JW RED LABEL 1L",
                    "descricao_normalizada": "WHISKY JW RED LABEL 1L",
                    "lista_descricao": "WHISKY JW RED LABEL 1L",
                    "lista_descr_compl": "GARRAFA 1L",
                    "lista_codigos": "001",
                    "qtd_codigos": 1,
                    "lista_ncm": "22083020",
                    "lista_cest": "0201600",
                    "lista_gtin": "5000267014277",
                    "ncm_consenso": "22083020",
                    "cest_consenso": "0201600",
                    "gtin_consenso": "5000267014277",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0002",
                    "descricao": "WHISKY JOHNNIE WALKER RED LABEL 1000ML",
                    "descricao_normalizada": "WHISKY JOHNNIE WALKER RED LABEL 1000ML",
                    "lista_descricao": "WHISKY JOHNNIE WALKER RED LABEL 1000ML",
                    "lista_descr_compl": "GARRAFA 1L",
                    "lista_codigos": "002",
                    "qtd_codigos": 1,
                    "lista_ncm": "22083020",
                    "lista_cest": "0201600",
                    "lista_gtin": "5000267014277",
                    "ncm_consenso": "22083020",
                    "cest_consenso": "0201600",
                    "gtin_consenso": "5000267014277",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0003",
                    "descricao": "CAFE TORRADO 250G",
                    "descricao_normalizada": "CAFE TORRADO 250G",
                    "lista_descricao": "CAFE TORRADO 250G",
                    "lista_descr_compl": "",
                    "lista_codigos": "003",
                    "qtd_codigos": 1,
                    "lista_ncm": "09012100",
                    "lista_cest": "",
                    "lista_gtin": "",
                    "ncm_consenso": "09012100",
                    "cest_consenso": "",
                    "gtin_consenso": "",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0004",
                    "descricao": "CAFE TORRADO 250 GR",
                    "descricao_normalizada": "CAFE TORRADO 250 GR",
                    "lista_descricao": "CAFE TORRADO 250 GR",
                    "lista_descr_compl": "",
                    "lista_codigos": "004",
                    "qtd_codigos": 1,
                    "lista_ncm": "09012100",
                    "lista_cest": "",
                    "lista_gtin": "",
                    "ncm_consenso": "09012100",
                    "cest_consenso": "",
                    "gtin_consenso": "",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0005",
                    "descricao": "SABAO EM PO 800G",
                    "descricao_normalizada": "SABAO EM PO 800G",
                    "lista_descricao": "SABAO EM PO 800G",
                    "lista_descr_compl": "",
                    "lista_codigos": "005",
                    "qtd_codigos": 1,
                    "lista_ncm": "34025000",
                    "lista_cest": "",
                    "lista_gtin": "7890000000101",
                    "ncm_consenso": "34025000",
                    "cest_consenso": "",
                    "gtin_consenso": "7890000000101",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0006",
                    "descricao": "SABAO EM PO 800 GR",
                    "descricao_normalizada": "SABAO EM PO 800 GR",
                    "lista_descricao": "SABAO EM PO 800 GR",
                    "lista_descr_compl": "",
                    "lista_codigos": "006",
                    "qtd_codigos": 1,
                    "lista_ncm": "19019090",
                    "lista_cest": "",
                    "lista_gtin": "7890000000202",
                    "ncm_consenso": "19019090",
                    "cest_consenso": "",
                    "gtin_consenso": "7890000000202",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0007",
                    "descricao": "PERFUME FEMININO 100ML",
                    "descricao_normalizada": "PERFUME FEMININO 100ML",
                    "lista_descricao": "PERFUME FEMININO 100ML",
                    "lista_descr_compl": "",
                    "lista_codigos": "007",
                    "qtd_codigos": 1,
                    "lista_ncm": "33030010",
                    "lista_cest": "2000100",
                    "lista_gtin": "7891234567890",
                    "ncm_consenso": "33030010",
                    "cest_consenso": "2000100",
                    "gtin_consenso": "7891234567890",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0008",
                    "descricao": "PERFUME FEMININO 100 ML",
                    "descricao_normalizada": "PERFUME FEMININO 100 ML",
                    "lista_descricao": "PERFUME FEMININO 100 ML",
                    "lista_descr_compl": "",
                    "lista_codigos": "008",
                    "qtd_codigos": 1,
                    "lista_ncm": "33030010",
                    "lista_cest": "",
                    "lista_gtin": "7891234567890",
                    "ncm_consenso": "33030010",
                    "cest_consenso": "",
                    "gtin_consenso": "7891234567890",
                    "requer_revisao_manual": False,
                    "descricoes_conflitantes": "",
                },
            ]
        )
        df_pairs = pl.DataFrame(
            [
                {
                    "chave_produto_a": "ID_0001",
                    "chave_produto_b": "ID_0002",
                    "score_descricao": 0.89,
                },
                {
                    "chave_produto_a": "ID_0003",
                    "chave_produto_b": "ID_0004",
                    "score_descricao": 0.87,
                },
                {
                    "chave_produto_a": "ID_0005",
                    "chave_produto_b": "ID_0006",
                    "score_descricao": 0.83,
                },
                {
                    "chave_produto_a": "ID_0007",
                    "chave_produto_b": "ID_0008",
                    "score_descricao": 0.81,
                },
            ]
        )

        preview = construir_preview_unificacao_lote(
            df_agregados,
            df_pairs,
            rule_ids=[RULE_R1, RULE_R2, RULE_R3, RULE_R6],
            source_method="DOCUMENTAL",
            require_all_pairs_compatible=True,
            max_component_size=12,
        )

        self.assertEqual(preview["resumo"]["total_rows_considered"], 8)
        self.assertEqual(preview["resumo"]["total_candidate_pairs"], 4)
        self.assertEqual(preview["resumo"]["total_proposals"], 4)

        proposals_by_rule = {item["rule_id"]: item for item in preview["proposals"]}
        self.assertIn(RULE_R1, proposals_by_rule)
        self.assertIn(RULE_R2, proposals_by_rule)
        self.assertIn(RULE_R3, proposals_by_rule)
        self.assertIn(RULE_R6, proposals_by_rule)
        self.assertEqual(proposals_by_rule[RULE_R1]["relation_summary"]["gtin"], "EQUAL_FILLED")
        self.assertEqual(proposals_by_rule[RULE_R2]["relation_summary"]["cest"], "EQUAL_NULL")
        self.assertEqual(proposals_by_rule[RULE_R3]["relation_summary"]["gtin"], "EQUAL_FILLED")
        self.assertEqual(proposals_by_rule[RULE_R3]["relation_summary"]["cest"], "INCOMPLETE")
        self.assertEqual(proposals_by_rule[RULE_R6]["relation_summary"]["ncm"], "CONFLICT")


if __name__ == "__main__":
    unittest.main()
