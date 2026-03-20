import polars as pl
from rich import print as rprint

def processar_tabela_documentos(df_notas: pl.DataFrame) -> pl.DataFrame:
    """
    Consolida cabeçalhos de documentos fiscais (NFe, NFCe, C100).
    """
    rprint("[cyan]Consolidando cabeçalhos de documentos fiscais...[/cyan]")
    
    # Seleção de campos comuns
    campos_doc = [
        "ch_dnfe", "numero", "serie", "modelo", "dt_emissao", "valor_total", 
        "cnpj_emitente", "cnpj_destinatario", "fonte"
    ]
    
    df_doc = (
        df_notas.select([c for c in campos_doc if c in df_notas.columns])
        .unique(subset=["ch_dnfe"])
        .sort("dt_emissao", descending=True)
    )
    
    return df_doc
