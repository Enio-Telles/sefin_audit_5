from pathlib import Path
from core.audit_status_service import obter_status_pipeline
from core.audit_artifacts_service import obter_arquivos_auditoria


def construir_resposta_status(
    cnpj_limpo: str, dir_parquet: Path, dir_analises: Path, dir_relatorios: Path
) -> dict:
    """Monta o payload de resposta final combinando o status do pipeline e os arquivos gerados."""
    status_dados = obter_status_pipeline(dir_analises)
    arquivos = obter_arquivos_auditoria(
        cnpj_limpo, dir_parquet, dir_analises, dir_relatorios
    )

    return {
        "success": True,
        "cnpj": cnpj_limpo,
        **status_dados,
        "arquivos_extraidos": arquivos.get("arquivos_extraidos", []),
        "arquivos_analises": arquivos.get("arquivos_analises", []),
        "arquivos_produtos": arquivos.get("arquivos_produtos", []),
        "arquivos_relatorios": arquivos.get("arquivos_relatorios", []),
        "dir_parquet": str(dir_parquet),
        "dir_analises": str(dir_analises),
        "dir_relatorios": str(dir_relatorios),
    }
