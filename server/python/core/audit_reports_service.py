import logging
from pathlib import Path

logger = logging.getLogger("sefin_audit_python")

def gerar_relatorios_finais(
    cnpj_limpo: str,
    dir_analises: Path,
    dir_relatorios: Path,
    projeto_dir: Path
) -> tuple[list[dict], list[str]]:
    """
    Gera relatórios em Word (via Jinja) e o arquivo de resumo em TXT.
    Retorna a lista de relatórios gerados e possíveis erros.
    """
    from gerar_relatorio import gerar_relatorio_jinja, gerar_resumo_txt

    arquivos_relatorios = []
    erros = []

    try:
        dir_modelos = projeto_dir / "modelos_word"
        for template in [
            "notificacao_monitoramento_v_2.docx",
            "Papel_TIMBRADO_SEFIN.docx",
        ]:
            if (dir_modelos / template).exists():
                res = gerar_relatorio_jinja(
                    cnpj_limpo, dir_analises, dir_relatorios, dir_modelos, template
                )
                if res:
                    arquivos_relatorios.append(res)

        res_txt = gerar_resumo_txt(cnpj_limpo, dir_analises, dir_relatorios)
        if res_txt:
            arquivos_relatorios.append(res_txt)
    except Exception as e:
        logger.error(f"[audit_reports_service] Erro gerando relatórios: {e}")
        erros.append(f"Relatórios: {str(e)}")

    return arquivos_relatorios, erros
