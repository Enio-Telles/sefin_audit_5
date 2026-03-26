from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl


SERVER_PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(SERVER_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_PYTHON_DIR))

from core.produto_runtime import (  # noqa: E402
    _DETAIL_COLUMNS,
    _aplicar_desagregacao_codigos,
    _aplicar_mapas_manuais,
    _build_codigos_multidescricao,
    _build_produtos_agregados,
    _build_produtos_indexados,
    _build_variacoes_produtos,
    _clean_value,
    _classificar_par,
    construir_tabela_pares_descricoes_faiss,
    construir_tabela_pares_descricoes_light,
)


def _base_df() -> pl.DataFrame:
    rows = [
        {
            "fonte": "C170",
            "codigo": "001",
            "descricao": "ARROZ TIPO 1",
            "descr_compl": "PACOTE 5KG",
            "tipo_item": "00",
            "ncm": "10063021",
            "cest": "",
            "gtin": "789000000001",
            "unid": "UN",
            "codigo_original": "001",
            "descricao_original": "ARROZ TIPO 1",
            "tipo_item_original": "00",
            "hash_manual_key": "hash-a",
        },
        {
            "fonte": "NFE",
            "codigo": "001",
            "descricao": "ARROZ TIPO 1",
            "descr_compl": "PACOTE 1KG",
            "tipo_item": "00",
            "ncm": "10063021",
            "cest": "",
            "gtin": "789000000001",
            "unid": "FD",
            "codigo_original": "001",
            "descricao_original": "ARROZ TIPO 1",
            "tipo_item_original": "00",
            "hash_manual_key": "hash-b",
        },
        {
            "fonte": "REG0200",
            "codigo": "002",
            "descricao": "FEIJAO CARIOCA",
            "descr_compl": "",
            "tipo_item": "00",
            "ncm": "07133319",
            "cest": "",
            "gtin": "789000000002",
            "unid": "UN",
            "codigo_original": "002",
            "descricao_original": "FEIJAO CARIOCA",
            "tipo_item_original": "00",
            "hash_manual_key": "hash-c",
        },
        {
            "fonte": "NFCe",
            "codigo": "002A",
            "descricao": "FEIJAO CARIOCA",
            "descr_compl": "PREMIUM",
            "tipo_item": "00",
            "ncm": "07133319",
            "cest": "",
            "gtin": "789000000099",
            "unid": "UN",
            "codigo_original": "002A",
            "descricao_original": "FEIJAO CARIOCA",
            "tipo_item_original": "00",
            "hash_manual_key": "hash-d",
        },
    ]
    return pl.DataFrame(rows).select(_DETAIL_COLUMNS)


class ProdutoRuntimeBuildersTests(unittest.TestCase):
    def test_build_produtos_agregados_group_by_descricao(self) -> None:
        df = _build_produtos_agregados(_base_df())

        self.assertEqual(df.height, 2)
        arroz = df.filter(pl.col("descricao") == "ARROZ TIPO 1").to_dicts()[0]
        feijao = df.filter(pl.col("descricao") == "FEIJAO CARIOCA").to_dicts()[0]

        self.assertEqual(arroz["qtd_codigos"], 1)
        self.assertFalse(arroz["requer_revisao_manual"])
        self.assertEqual(arroz["lista_unid"], "FD, UN")

        self.assertEqual(feijao["qtd_codigos"], 2)
        self.assertTrue(feijao["requer_revisao_manual"])
        self.assertIn("CODIGO", feijao["descricoes_conflitantes"])
        self.assertIn("GTIN", feijao["descricoes_conflitantes"])

    def test_build_produtos_indexados_and_codigos_multidescricao(self) -> None:
        df_base = _base_df()
        df_agregados = _build_produtos_agregados(df_base)
        df_indexados = _build_produtos_indexados(df_base, df_agregados)
        df_codigos = _build_codigos_multidescricao(df_indexados)

        self.assertEqual(df_indexados.height, 4)
        self.assertEqual(df_codigos.height, 0)

        duplicated = pl.concat(
            [
                df_indexados,
                pl.DataFrame(
                    [
                        {
                            "chave_produto": "ID_0003",
                            "codigo": "009",
                            "descricao": "CAFE TORRADO",
                            "descr_compl": "250G",
                            "tipo_item": "00",
                            "ncm": "09012100",
                            "cest": "",
                            "gtin": "789000000300",
                            "lista_unidades": "UN",
                            "lista_fontes": "C170",
                            "qtd_linhas": 1,
                        },
                        {
                            "chave_produto": "ID_0004",
                            "codigo": "009",
                            "descricao": "CAFE SUPERIOR",
                            "descr_compl": "250G",
                            "tipo_item": "00",
                            "ncm": "09012100",
                            "cest": "",
                            "gtin": "789000000301",
                            "lista_unidades": "UN",
                            "lista_fontes": "NFE",
                            "qtd_linhas": 1,
                        },
                    ]
                ),
            ],
            how="diagonal_relaxed",
        )
        df_codigos = _build_codigos_multidescricao(duplicated)
        row = df_codigos.filter(pl.col("codigo") == "009").to_dicts()[0]

        self.assertEqual(row["qtd_descricoes"], 2)
        self.assertEqual(row["qtd_grupos_descricao_afetados"], 2)
        self.assertIn("CAFE TORRADO", row["lista_descricoes"])
        self.assertIn("250G", row["lista_descr_compl"])

    def test_build_variacoes_produtos_flags_description_variations(self) -> None:
        df_variacoes = _build_variacoes_produtos(_base_df())
        row = df_variacoes.filter(pl.col("descricao") == "FEIJAO CARIOCA").to_dicts()[0]
        self.assertEqual(row["qtd_codigos"], 2)
        self.assertEqual(row["qtd_gtin"], 2)

    def test_aplicar_desagregacao_codigos_elimina_codigo_repetido_entre_descricoes(self) -> None:
        df_base = pl.DataFrame(
            [
                {
                    "fonte": "NFE",
                    "codigo": "777",
                    "descricao": "BISCOITO CHOCOLATE",
                    "descr_compl": "",
                    "tipo_item": "",
                    "ncm": "19053100",
                    "cest": "1704700",
                    "gtin": "7890000007771",
                    "unid": "UN",
                    "codigo_original": "777",
                    "descricao_original": "BISCOITO CHOCOLATE",
                    "tipo_item_original": "",
                    "hash_manual_key": "h1",
                },
                {
                    "fonte": "NFE",
                    "codigo": "777",
                    "descricao": "BISCOITO MORANGO",
                    "descr_compl": "",
                    "tipo_item": "",
                    "ncm": "19053100",
                    "cest": "1704700",
                    "gtin": "7890000007772",
                    "unid": "UN",
                    "codigo_original": "777",
                    "descricao_original": "BISCOITO MORANGO",
                    "tipo_item_original": "",
                    "hash_manual_key": "h2",
                },
            ]
        ).select(_DETAIL_COLUMNS)

        desagregado = _aplicar_desagregacao_codigos(df_base)
        produtos = _build_produtos_agregados(desagregado)

        self.assertEqual(produtos.height, 2)
        lista_codigos = produtos.get_column("lista_codigos").to_list()
        self.assertEqual(len(set(lista_codigos)), 2)
        self.assertTrue(all("777_SEPARADO_" in codigo for codigo in lista_codigos))
        self.assertEqual(
            set(desagregado.get_column("codigo").to_list()),
            {"777_SEPARADO_01", "777_SEPARADO_02"},
        )

    def test_aplicar_mapas_manuais_unifies_descriptions_and_overrides_item(self) -> None:
        df_base = _base_df()
        with tempfile.TemporaryDirectory() as tmp:
            dir_analises = Path(tmp)
            pl.DataFrame(
                [
                    {
                        "tipo_regra": "UNIR_GRUPOS",
                        "descricao_origem": "FEIJAO CARIOCA",
                        "descricao_destino": "FEIJAO CANONICO",
                        "descricao_par": "",
                        "hash_descricoes_key": "",
                        "chave_grupo_a": "",
                        "chave_grupo_b": "",
                        "score_origem": "",
                        "acao_manual": "AGREGAR",
                    }
                ]
            ).write_parquet(str(dir_analises / "mapa_manual_descricoes_123.parquet"))
            pl.DataFrame(
                [
                    {
                        "fonte": "NFCE",
                        "codigo_original": "002A",
                        "descricao_original": "FEIJAO CARIOCA",
                        "tipo_item_original": "00",
                        "hash_manual_key": "hash-d",
                        "codigo_novo": "002B",
                        "descricao_nova": "FEIJAO ESPECIAL",
                        "ncm_novo": "07133319",
                        "cest_novo": "",
                        "gtin_novo": "789000000555",
                        "tipo_item_novo": "01",
                        "acao_manual": "AGREGAR",
                    }
                ]
            ).write_parquet(str(dir_analises / "mapa_manual_unificacao_123.parquet"))

            applied = _aplicar_mapas_manuais(df_base, dir_analises, "123")

        manual_row = applied.filter(pl.col("hash_manual_key") == "hash-d").to_dicts()[0]
        other_row = applied.filter(pl.col("hash_manual_key") == "hash-c").to_dicts()[0]

        self.assertEqual(manual_row["codigo"], "002B")
        self.assertEqual(manual_row["descricao"], "FEIJAO ESPECIAL")
        self.assertEqual(manual_row["gtin"], "789000000555")
        self.assertEqual(manual_row["tipo_item"], "01")
        self.assertEqual(other_row["descricao"], "FEIJAO CANONICO")

    def test_classificar_par_uses_golden_key_when_gtin_ncm_cest_match(self) -> None:
        result = _classificar_par(
            score_descricao=0.05,
            score_ncm=1.0,
            score_cest=1.0,
            score_gtin=1.0,
            a={"gtin": "789000000555", "ncm": "22083020", "cest": "0201600"},
            b={"gtin": "789000000555", "ncm": "22083020", "cest": "0201600"},
        )

        self.assertEqual(result["recomendacao"], "UNIR_AUTOMATICO_ELEGIVEL")
        self.assertTrue(result["uniao_automatica_elegivel"])
        self.assertEqual(result["motivo_recomendacao"], "GTIN valido coincide com NCM/CEST compativeis.")
        self.assertGreaterEqual(result["score_final"], 0.99)

    def test_construir_tabela_pares_descricoes_light_prioritizes_close_descriptions(self) -> None:
        df_agregados = pl.DataFrame(
            [
                {
                    "chave_produto": "ID_0001",
                    "descricao": "ARROZ TIPO 1 5KG",
                    "descricao_normalizada": "ARROZ TIPO 1 5KG",
                    "lista_descricao": "ARROZ TIPO 1 5KG",
                    "lista_descr_compl": "PACOTE 5KG",
                    "lista_codigos": "001",
                    "ncm_consenso": "10063021",
                    "cest_consenso": "",
                    "gtin_consenso": "7890000000011",
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0002",
                    "descricao": "ARROZ TIPO 1 5 KG",
                    "descricao_normalizada": "ARROZ TIPO 1 5 KG",
                    "lista_descricao": "ARROZ TIPO 1 5 KG",
                    "lista_descr_compl": "PACOTE 5KG",
                    "lista_codigos": "002",
                    "ncm_consenso": "10063021",
                    "cest_consenso": "",
                    "gtin_consenso": "",
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0003",
                    "descricao": "REFRIGERANTE UVA 2L",
                    "descricao_normalizada": "REFRIGERANTE UVA 2L",
                    "lista_descricao": "REFRIGERANTE UVA 2L",
                    "lista_descr_compl": "",
                    "lista_codigos": "003",
                    "ncm_consenso": "22021000",
                    "cest_consenso": "",
                    "gtin_consenso": "",
                    "descricoes_conflitantes": "",
                },
            ]
        )

        df_pairs = construir_tabela_pares_descricoes_light(df_agregados, top_k=5, min_score=0.70)

        self.assertEqual(df_pairs.height, 1)
        row = df_pairs.to_dicts()[0]
        self.assertEqual({row["chave_produto_a"], row["chave_produto_b"]}, {"ID_0001", "ID_0002"})
        self.assertGreaterEqual(row["score_descricao"], 0.60)
        self.assertEqual(row["metodo_similaridade"], "LIGHT_VECTOR")
        self.assertEqual(row["modelo_vetorizacao"], "CHAR_NGRAM_TFIDF_V1")

    def test_construir_tabela_pares_descricoes_faiss_prioritizes_semantic_neighbors(self) -> None:
        df_agregados = pl.DataFrame(
            [
                {
                    "chave_produto": "ID_0001",
                    "descricao": "WHISKY RED LABEL 1L",
                    "descricao_normalizada": "WHISKY RED LABEL 1L",
                    "lista_descricao": "WHISKY RED LABEL 1L",
                    "lista_descr_compl": "GARRAFA 1L",
                    "lista_codigos": "001",
                    "ncm_consenso": "22083020",
                    "cest_consenso": "0201600",
                    "gtin_consenso": "",
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0002",
                    "descricao": "WHISKY JW RED LABEL 1000ML",
                    "descricao_normalizada": "WHISKY JW RED LABEL 1000ML",
                    "lista_descricao": "WHISKY JW RED LABEL 1000ML",
                    "lista_descr_compl": "GARRAFA 1L",
                    "lista_codigos": "002",
                    "ncm_consenso": "22083020",
                    "cest_consenso": "0201600",
                    "gtin_consenso": "",
                    "descricoes_conflitantes": "",
                },
                {
                    "chave_produto": "ID_0003",
                    "descricao": "SABAO EM PO 800G",
                    "descricao_normalizada": "SABAO EM PO 800G",
                    "lista_descricao": "SABAO EM PO 800G",
                    "lista_descr_compl": "",
                    "lista_codigos": "003",
                    "ncm_consenso": "34025000",
                    "cest_consenso": "",
                    "gtin_consenso": "",
                    "descricoes_conflitantes": "",
                },
            ]
        )

        mock_vectors = np.asarray(
            [
                [1.0, 0.0, 0.0],
                [0.96, 0.04, 0.0],
                [0.0, 1.0, 0.0],
            ],
            dtype="float32",
        )

        with patch("core.produto_runtime._semantic_runtime_available", return_value=True), patch(
            "core.produto_runtime._encode_faiss_rows", return_value=mock_vectors
        ), patch("core.produto_runtime._search_faiss_neighbors", return_value=(np.asarray([[1.0, 0.95, 0.0], [0.95, 1.0, 0.0], [0.0, 0.0, 1.0]], dtype="float32"), np.asarray([[0, 1, -1], [1, 0, -1], [2, -1, -1]], dtype="int64"))):
            df_pairs = construir_tabela_pares_descricoes_faiss(df_agregados, top_k=4, min_score=0.80)

        self.assertEqual(df_pairs.height, 1)
        row = df_pairs.to_dicts()[0]
        self.assertEqual({row["chave_produto_a"], row["chave_produto_b"]}, {"ID_0001", "ID_0002"})
        self.assertEqual(row["metodo_similaridade"], "FAISS_VECTOR")
        self.assertEqual(row["origem_par_hibrido"], "faiss_cosine")
        self.assertGreaterEqual(float(row["score_semantico"]), 0.90)


class TestProdutoRuntimeUtils(unittest.TestCase):
    def test_clean_value(self):
        # Empty and None values should return empty strings
        self.assertEqual(_clean_value(None), "")
        self.assertEqual(_clean_value(""), "")
        self.assertEqual(_clean_value("   "), "")

        # Falsy types evaluating to False/0 should return empty string since `(False or "")` gives `""`
        self.assertEqual(_clean_value(False), "")
        self.assertEqual(_clean_value(0), "")
        self.assertEqual(_clean_value(0.0), "")

        # Valid strings should be stripped
        self.assertEqual(_clean_value("  foo  "), "foo")
        self.assertEqual(_clean_value("bar"), "bar")
        self.assertEqual(_clean_value(" a b c "), "a b c")

        # Truthy non-string types should be converted to string
        self.assertEqual(_clean_value(True), "True")
        self.assertEqual(_clean_value(123), "123")
        self.assertEqual(_clean_value(3.14), "3.14")

        import math
        self.assertEqual(_clean_value(math.nan), "")


if __name__ == "__main__":
    unittest.main()
