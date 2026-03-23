import logging
from pathlib import Path
import polars as pl

from core.factor_diagnostics import diagnosticar_fatores_conversao

logger = logging.getLogger("sefin_audit_python")

def processar_fatores_excel(
    fatores_path: Path,
    content: bytes
) -> dict:
    import pandas as pd
    from io import BytesIO

    df_excel = pd.read_excel(BytesIO(content))
    df_excel.columns = [str(c).strip().lower() for c in df_excel.columns]

    required_cols = {"chave_produto", "ano_referencia", "unidade_origem", "fator"}
    if not required_cols.issubset(set(df_excel.columns)):
        raise ValueError("Colunas obrigatórias ausentes no Excel.")

    pl_excel = (
        pl.from_pandas(df_excel)
        .with_columns(
            [
                pl.col("chave_produto").cast(pl.Int64),
                pl.col("ano_referencia").cast(pl.Int64),
                pl.col("unidade_origem").cast(pl.Utf8).str.strip_chars(),
                pl.col("fator").cast(pl.Float64),
            ]
        )
        .unique(
            subset=["chave_produto", "ano_referencia", "unidade_origem"],
            keep="last",
        )
    )

    fatores = pl.read_parquet(fatores_path)
    fatores = fatores.rename({c: c.lower() for c in fatores.columns})
    join_keys = ["chave_produto", "ano_referencia", "unidade_origem"]

    fatores_atualizados = (
        fatores.join(
            pl_excel.select(join_keys + ["fator"]),
            on=join_keys,
            how="left",
            suffix="_novo",
        )
        .with_columns(
            [
                pl.when(pl.col("fator_novo").is_not_null())
                .then(pl.col("fator_novo"))
                .otherwise(pl.col("fator"))
                .alias("fator_atual"),
                pl.when(pl.col("fator_novo").is_not_null())
                .then(pl.lit(True))
                .otherwise(pl.col("editado_manual").fill_null(False))
                .alias("editado_manual_atual"),
            ]
        )
        .drop(["fator", "fator_novo", "editado_manual"])
        .rename({"fator_atual": "fator", "editado_manual_atual": "editado_manual"})
    )

    fatores_atualizados.write_parquet(fatores_path)
    return {
        "file": str(fatores_path),
        "registros": fatores_atualizados.height,
    }

def obter_diagnostico_fatores(
    fatores_path: Path,
    cnpj_limpo: str
) -> dict:
    if not fatores_path.exists():
        return {
            "success": True,
            "available": False,
            "cnpj": cnpj_limpo,
            "file": "",
            "stats": {
                "total_registros": 0,
                "produtos_unicos": 0,
                "anos_unicos": 0,
                "unidades_unicas": 0,
                "editados_manual": 0,
                "fatores_invalidos": 0,
                "fatores_extremos_altos": 0,
                "fatores_extremos_baixos": 0,
                "grupos_muitas_unidades": 0,
                "grupos_alta_variacao": 0,
            },
            "issues": [],
            "message": "Arquivo de fatores não encontrado.",
        }

    fatores = pl.read_parquet(fatores_path)
    diagnostico = diagnosticar_fatores_conversao(fatores)
    return {
        "success": True,
        "available": True,
        "cnpj": cnpj_limpo,
        "file": str(fatores_path),
        **diagnostico,
    }
