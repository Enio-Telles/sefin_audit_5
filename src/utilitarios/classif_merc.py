import polars as pl
from rich import print as rprint
from dataclasses import dataclass
from typing import Optional

@dataclass
class ClassifMercParams:
    col_ncm: str = "ncm"
    col_cest: str = "cest"
    col_sefin_adicional: Optional[str] = None
    col_dhemi: Optional[str] = None
    col_dhsaient: Optional[str] = None

def aux_classif_merc(
    df: pl.DataFrame,
    params: Optional[ClassifMercParams] = None
) -> pl.DataFrame:
    """
    Função auxiliar para identificar o co_sefin (co_sefin_inferido) cruzando
    NCM e CEST com bases de referência da SEFIN.
    
    Ordem de fallback:
    1. sitafe_cest_ncm.parquet (it_nu_ncm, it_nu_cest)
    2. sitafe_cest.parquet (cest)
    3. sitafe_ncm.parquet (ncm)
    
    Se col_dhemi e col_dhsaient forem informados, busca também os atributos
    tributários em sitafe_produto_sefin_aux.parquet usando a maior data
    não-nula entre as duas colunas como referência para o intervalo
    [it_da_inicio, it_da_final].
    """

    if params is None:
        params = ClassifMercParams()

    col_ncm = params.col_ncm
    col_cest = params.col_cest
    col_sefin_adicional = params.col_sefin_adicional
    col_dhemi = params.col_dhemi
    col_dhsaient = params.col_dhsaient

    import importlib.util
    from pathlib import Path
    
    # Resolve absolute path to config.py located at the root of sefin-audit-tool
    _current_dir = Path(__file__).resolve().parent
    _root_dir = _current_dir.parent.parent
    _config_path = _root_dir / "config.py"
    
    _spec = importlib.util.spec_from_file_location("sefin_config_local", str(_config_path))
    _sefin_config = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_sefin_config)
    
    DIR_REFERENCIAS = _sefin_config.DIR_REFERENCIAS
    
    # Caminhos para os arquivos de referência
    path_cest_ncm = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_cest_ncm.parquet"
    path_cest = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_cest.parquet"
    path_ncm = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_ncm.parquet"
    path_produto = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_produto_sefin.parquet"
    path_produto_aux = DIR_REFERENCIAS / "CO_SEFIN" / "sitafe_produto_sefin_aux.parquet"

    # Verifica se os arquivos de referência existem
    arquivos_obrigatorios = [path_cest_ncm, path_cest, path_ncm, path_produto]
    if col_dhemi and col_dhsaient:
        arquivos_obrigatorios.append(path_produto_aux)
    if not all(p.exists() for p in arquivos_obrigatorios):
        raise FileNotFoundError("Um ou mais arquivos de referência sitafe não foram encontrados na pasta referencias/CO_SEFIN/")

    rprint("[blue]Carregando bases de referência NCM/CEST SEFIN...[/blue]")
    df_cest_ncm = pl.read_parquet(path_cest_ncm).select(
        pl.col("it_nu_ncm").cast(pl.String),
        pl.col("it_nu_cest").cast(pl.String),
        pl.col("it_co_sefin").alias("sefin_cest_ncm")
    ).unique(subset=["it_nu_ncm", "it_nu_cest"])

    df_cest = pl.read_parquet(path_cest).select(
        pl.col("cest").cast(pl.String).alias("ref_cest"),
        pl.col("co-sefin").alias("sefin_cest")
    ).unique(subset=["ref_cest"])

    df_ncm = pl.read_parquet(path_ncm).select(
        pl.col("ncm").cast(pl.String).alias("ref_ncm"),
        pl.col("co-sefin").alias("sefin_ncm")
    ).unique(subset=["ref_ncm"])

    df_produto_sefin = pl.read_parquet(path_produto).select(
        pl.col("it_co_sefin").alias("ref_co_sefin"),
        pl.col("it_no_produto").alias("it_no_produto")
    ).unique(subset=["ref_co_sefin"])

    # Padroniza as colunas de entrada para string no temp df
    df = df.with_columns(
        pl.col(col_ncm).cast(pl.String).alias("_temp_ncm"),
        pl.col(col_cest).cast(pl.String).alias("_temp_cest")
    )

    rprint("[blue]Realizando cruzamentos de classificação de mercadoria...[/blue]")
    # 1. Cruzamento CEST + NCM
    df = df.join(
        df_cest_ncm,
        left_on=["_temp_ncm", "_temp_cest"],
        right_on=["it_nu_ncm", "it_nu_cest"],
        how="left"
    )

    # 2. Cruzamento apenas CEST (Fallback)
    df = df.join(
        df_cest,
        left_on=["_temp_cest"],
        right_on=["ref_cest"],
        how="left"
    )

    # 3. Cruzamento apenas NCM (Fallback)
    df = df.join(
        df_ncm,
        left_on=["_temp_ncm"],
        right_on=["ref_ncm"],
        how="left"
    )

    # 4. Resolve o fallback usando coalesce e garante tipo string
    df = df.with_columns(
        pl.coalesce(["sefin_cest_ncm", "sefin_cest", "sefin_ncm"]).cast(pl.String).alias("co_sefin_inferido")
    )

    # 5. Busca a descrição do co_sefin_inferido
    df = df.join(
        df_produto_sefin.select(
            pl.col("ref_co_sefin"),
            pl.col("it_no_produto").alias("descr_co_sefin_inferido")
        ),
        left_on=["co_sefin_inferido"],
        right_on=["ref_co_sefin"],
        how="left"
    )

    # 6. Busca a descrição para o co_sefin_adicional (ex: co_sefin_fronteira)
    if col_sefin_adicional and col_sefin_adicional in df.columns:
        df = df.with_columns(pl.col(col_sefin_adicional).cast(pl.String))
        df = df.join(
            df_produto_sefin.select(
                pl.col("ref_co_sefin"),
                pl.col("it_no_produto").alias(f"descr_{col_sefin_adicional}")
            ),
            left_on=[col_sefin_adicional],
            right_on=["ref_co_sefin"],
            how="left"
        )

    # 7. Busca atributos tributários por co_sefin + intervalo de data
    if col_dhemi and col_dhsaient:
        _colunas_aux = [
            "it_pc_interna", "it_in_st", "it_pc_mva", "it_in_mva_ajustado",
            "it_in_convenio", "it_in_isento_icms", "it_in_reducao",
            "it_pc_reducao", "it_in_reducao_credito", "it_in_pmpf"
        ]

        df_aux = pl.read_parquet(path_produto_aux).select(
            [pl.col("it_co_sefin").alias("_aux_co_sefin"),
             pl.col("it_da_inicio").cast(pl.String).alias("_aux_da_inicio"),
             pl.col("it_da_final").cast(pl.String).alias("_aux_da_final")]
            + [pl.col(c) for c in _colunas_aux]
        )

        # Calcula a data de referência: max(dhemi, dhsaient) não-nulo, formato YYYYMMDD
        df = df.with_columns(
            pl.coalesce([
                pl.max_horizontal(pl.col(col_dhemi), pl.col(col_dhsaient)),
                pl.col(col_dhemi),
                pl.col(col_dhsaient)
            ]).dt.strftime("%Y%m%d").alias("_data_ref")
        )

        # Adiciona índice de linha para manter cardinalidade 1:1 após join+filter
        df = df.with_row_index("_row_idx")

        # --- 7a. Lookup para co_sefin_inferido ---
        rprint("[blue]Buscando atributos tributários para co_sefin_inferido...[/blue]")
        df_joined_inf = (
            df.select(["_row_idx", "co_sefin_inferido", "_data_ref"])
            .join(
                df_aux,
                left_on=["co_sefin_inferido"],
                right_on=["_aux_co_sefin"],
                how="left"
            )
            .filter(
                pl.col("_aux_da_inicio").is_null()
                | (
                    (pl.col("_data_ref") >= pl.col("_aux_da_inicio"))
                    & (
                        pl.col("_aux_da_final").is_null()
                        | (pl.col("_aux_da_final").str.strip_chars() == "")  # Vigente até hoje (vazio ou espaço)
                        | (pl.col("_data_ref") <= pl.col("_aux_da_final"))
                    )
                )
            )
            .select(["_row_idx"] + _colunas_aux)
        )
        # Renomeia com sufixo _inferido
        rename_inf = {c: f"{c}_inferido" for c in _colunas_aux}
        df_joined_inf = df_joined_inf.rename(rename_inf)

        df = df.join(df_joined_inf, on="_row_idx", how="left")

        # --- 7b. Lookup para col_sefin_adicional (co_sefin_fronteira) ---
        if col_sefin_adicional and col_sefin_adicional in df.columns:
            rprint(f"[blue]Buscando atributos tributários para {col_sefin_adicional}...[/blue]")
            df_joined_add = (
                df.select(["_row_idx", col_sefin_adicional, "_data_ref"])
                .join(
                    df_aux,
                    left_on=[col_sefin_adicional],
                    right_on=["_aux_co_sefin"],
                    how="left"
                )
                .filter(
                    pl.col("_aux_da_inicio").is_null()
                    | (
                        (pl.col("_data_ref") >= pl.col("_aux_da_inicio"))
                        & (
                            pl.col("_aux_da_final").is_null()
                            | (pl.col("_aux_da_final").str.strip_chars() == "")  # Vigente até hoje (vazio ou espaço)
                            | (pl.col("_data_ref") <= pl.col("_aux_da_final"))
                        )
                    )
                )
                .select(["_row_idx"] + _colunas_aux)
            )
            # Renomeia com sufixo _fronteira
            rename_add = {c: f"{c}_fronteira" for c in _colunas_aux}
            df_joined_add = df_joined_add.rename(rename_add)

            df = df.join(df_joined_add, on="_row_idx", how="left")

        df = df.drop(["_row_idx", "_data_ref"])

    # Limpeza das colunas temporárias e intermediárias
    colunas_para_remover = [
        "_temp_ncm", "_temp_cest",
        "sefin_cest_ncm", "sefin_cest", "sefin_ncm"
    ]
    df = df.drop(colunas_para_remover)

    return df
