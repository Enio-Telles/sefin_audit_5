import polars as pl
from rich import print as rprint

def aux_calc_VBC_ST(
    df: pl.DataFrame,
    col_vprod: str,
    col_vfrete: str,
    col_vseg: str,
    col_voutro: str,
    col_vdesc: str,
    col_vipi: str,
    col_mva_flag: str,       # it_in_mva_ajustado
    col_mva_original: str,   # it_pc_mva (em %)
    col_mva_ajustado: str,   # resultado da aux_calc_mva_ajustado (decimal)
    nome_coluna_saida: str = "vbc_st_calc"
) -> pl.DataFrame:
    """
    Calcula a Base de Cálculo do ICMS ST (VBC_ST).
    Fórmula: VBC_ST = (VPROD + VFRETE + VSEG + VOUTRO - VDESC + VIPI) * (1 + MVA/100)
    
    Regras para MVA:
    1. Se it_in_mva_ajustado == 'S' e mva_ajustado não for nulo, usa mva_ajustado já calculado.
    2. Se it_in_mva_ajustado == 'N' ou mva_ajustado for nulo, usa it_pc_mva / 100.
    """
    rprint(f"[blue]Calculando {nome_coluna_saida}...[/blue]")
    
    # 1. Determina o MVA (em decimal) para cada linha
    # MVA_ajustado já vem em decimal (ex: 0.25 para 25%)
    # it_pc_mva vem em percentual (ex: 30.0 para 30%)
    
    mva_val = pl.when(pl.col(col_mva_ajustado).is_not_null()) \
               .then(pl.col(col_mva_ajustado)) \
               .otherwise(pl.col(col_mva_original).cast(pl.Float64).fill_null(0.0) / 100.0)
    
    # 2. Calcula a base (prod_vprod + prod_vfrete + prod_vseg + prod_voutro - prod_vdesc + ipi_vipi)
    # Garante que nulos sejam tratados como zero somando
    vbc_inicial = (
        pl.col(col_vprod).cast(pl.Float64).fill_null(0.0) +
        pl.col(col_vfrete).cast(pl.Float64).fill_null(0.0) +
        pl.col(col_vseg).cast(pl.Float64).fill_null(0.0) +
        pl.col(col_voutro).cast(pl.Float64).fill_null(0.0) -
        pl.col(col_vdesc).cast(pl.Float64).fill_null(0.0) +
        pl.col(col_vipi).cast(pl.Float64).fill_null(0.0)
    )
    
    # 3. Aplica o MVA: VBC_ST = Base * (1 + MVA)
    df = df.with_columns(
        (vbc_inicial * (1.0 + mva_val)).alias(nome_coluna_saida)
    )
    
    return df
