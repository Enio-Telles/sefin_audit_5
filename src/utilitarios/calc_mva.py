import polars as pl
from rich import print as rprint
from dataclasses import dataclass
import warnings

@dataclass
class CalcMvaAjustadoCols:
    col_mva_ajustado_flag: str
    col_mva_orig: str
    col_aliq_interna: str
    col_aliq_inter: str
    col_uf_emit: str
    col_uf_dest: str

def aux_calc_mva_ajustado(
    df: pl.DataFrame,
    col_mva_ajustado_flag: str | CalcMvaAjustadoCols,
    col_mva_orig: str | None = None,
    col_aliq_interna: str | None = None,
    col_aliq_inter: str | None = None,
    col_uf_emit: str | None = None,
    col_uf_dest: str | None = None,
    nome_coluna_saida: str = "mva_ajustado_calc"
) -> pl.DataFrame:
    """
    Calcula o MVA Ajustado conforme fórmula:

    MVA_ajustado = [(1 + MVA_orig) × (1 - ALQ_inter) / (1 - ALQ_interna)] - 1

    Regras:
    - Se it_in_mva_ajustado (strip) == "S", o MVA já é ajustado ⇒ retorna null
      (indica que o MVA original já contempla o ajuste)
    - A ALQ_inter só é aplicada se CO_UF_EMIT <> "RO" e CO_UF_DEST == "RO"
      (operação interestadual com destino a Rondônia)
    - Caso contrário (operação interna), o MVA ajustado não se aplica ⇒ null

    Os valores de MVA e alíquotas são esperados em percentual (ex: 17.0 = 17%).
    """
    rprint(f"[blue]Calculando {nome_coluna_saida}...[/blue]")

    if isinstance(col_mva_ajustado_flag, CalcMvaAjustadoCols):
        cols = col_mva_ajustado_flag
    else:
        # Compatibility with legacy calls
        warnings.warn("Passing individual string arguments is deprecated, pass a CalcMvaAjustadoCols instance instead", DeprecationWarning, stacklevel=2)
        if None in (col_mva_orig, col_aliq_interna, col_aliq_inter, col_uf_emit, col_uf_dest):
            raise ValueError("All column string names must be provided if not using CalcMvaAjustadoCols.")
        cols = CalcMvaAjustadoCols(
            col_mva_ajustado_flag=col_mva_ajustado_flag,
            col_mva_orig=col_mva_orig, # type: ignore
            col_aliq_interna=col_aliq_interna, # type: ignore
            col_aliq_inter=col_aliq_inter, # type: ignore
            col_uf_emit=col_uf_emit, # type: ignore
            col_uf_dest=col_uf_dest # type: ignore
        )

    # Converte percentuais em fração (17.0 → 0.17)
    mva_orig_frac = pl.col(cols.col_mva_orig).cast(pl.Float64) / 100.0
    aliq_inter_frac = pl.col(cols.col_aliq_inter).cast(pl.Float64) / 100.0
    aliq_interna_frac = pl.col(cols.col_aliq_interna).cast(pl.Float64) / 100.0

    # Fórmula: [(1 + MVA_orig) × (1 - ALQ_inter) / (1 - ALQ_interna)] - 1
    formula = (
        ((1.0 + mva_orig_frac) * (1.0 - aliq_inter_frac)) / (1.0 - aliq_interna_frac)
    ) - 1.0

    # Condições:
    # 1. it_in_mva_ajustado (strip) == "N" → null (não precisa ajustar)
    # 2. it_in_mva_ajustado (strip) == "S" e CO_UF_EMIT != "RO" e CO_UF_DEST == "RO" → aplica fórmula
    # 3. Caso contrário → null
    eh_nao_ajusta = (
        pl.col(cols.col_mva_ajustado_flag).cast(pl.String).str.strip_chars() == "N"
    )
    eh_ajusta = (
        pl.col(cols.col_mva_ajustado_flag).cast(pl.String).str.strip_chars() == "S"
    )
    eh_interestadual = (
        (pl.col(cols.col_uf_emit).cast(pl.String).str.strip_chars() != "RO")
        & (pl.col(cols.col_uf_dest).cast(pl.String).str.strip_chars() == "RO")
    )

    df = df.with_columns(
        pl.when(eh_nao_ajusta)
        .then(pl.lit(None).cast(pl.Float64))
        .when(eh_ajusta & eh_interestadual)
        .then(formula)
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias(nome_coluna_saida)
    )

    return df
