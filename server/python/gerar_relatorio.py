"""
Módulo de Geração de Relatórios Word para o SEFIN Audit Tool.
Usa docxtpl (Jinja2) para preencher templates com dados de análise.
Adaptado de Sistema_Auditoria_Fiscal/gerar_relatorio_word.py.
"""
from pathlib import Path
import datetime
import logging

logger = logging.getLogger("sefin_audit_python")

try:
    from docxtpl import DocxTemplate
except ImportError:
    DocxTemplate = None
    logger.warning("docxtpl não instalado. Relatórios Word não disponíveis.")


def extrair_itens_analises(dir_analises: Path, cnpj: str) -> list:
    """
    Lê os parquets de análise e compila itens para os templates Word.
    Retorna lista de dicts com num_item, tipo_item, tabela_linhas.
    """
    itens = []

    return itens


def gerar_relatorio_jinja(
    cnpj: str,
    dir_analises: Path,
    dir_relatorios: Path,
    dir_modelos: Path,
    nome_template: str,
    dados_cadastrais: dict | None = None,
    afte: str = "",
    matricula: str = "",
    num_dsf: str = "",
) -> dict | None:
    """
    Gera relatório Word usando docxtpl.
    Retorna dict {name, path, tipo} ou None se falhar.
    """
    if DocxTemplate is None:
        logger.error("docxtpl não disponível")
        return None

    arq_template = dir_modelos / nome_template
    if not arq_template.exists():
        logger.error("Template não encontrado: %s", arq_template)
        return None

    try:
        doc = DocxTemplate(str(arq_template))
    except Exception as e:
        logger.error("Erro ao abrir template: %s", e)
        return None

    # Contexto base
    contexto = dados_cadastrais or {}
    contexto["cnpj"] = cnpj

    # Data por extenso
    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
             "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    hoje = datetime.date.today()
    contexto["data_extenso"] = f"{hoje.day} de {meses[hoje.month - 1]} de {hoje.year}"

    contexto["afte"] = afte or ""
    contexto["matricula"] = matricula or ""
    contexto["num_dsf"] = num_dsf or ""

    # Itens das análises
    itens = extrair_itens_analises(dir_analises, cnpj)
    contexto["itens"] = itens

    # Renderizar
    try:
        doc.render(contexto)
    except Exception as e:
        logger.error("Erro ao renderizar template: %s", e)
        return None

    # Salvar
    dir_relatorios.mkdir(parents=True, exist_ok=True)
    stem = Path(nome_template).stem
    caminho_saida = dir_relatorios / f"{stem}_{cnpj}.docx"

    try:
        doc.save(str(caminho_saida))
        logger.info("Relatório gerado: %s", caminho_saida)
        return {
            "name": caminho_saida.name,
            "path": str(caminho_saida),
            "tipo": "Word (DOCX)",
            "template": nome_template,
        }
    except Exception as e:
        logger.error("Erro ao salvar relatório: %s", e)
        return None


def gerar_resumo_txt(
    cnpj: str,
    dir_analises: Path,
    dir_relatorios: Path,
) -> dict | None:
    """
    Gera um resumo TXT com dados das análises.
    """
    itens = extrair_itens_analises(dir_analises, cnpj)
    if not itens:
        return None

    dir_relatorios.mkdir(parents=True, exist_ok=True)
    caminho = dir_relatorios / f"resumo_auditoria_{cnpj}.txt"

    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
             "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    hoje = datetime.date.today()
    data_ext = f"{hoje.day} de {meses[hoje.month - 1]} de {hoje.year}"

    lines = [
        "=" * 60,
        "RESUMO DE AUDITORIA FISCAL",
        f"CNPJ: {cnpj}",
        f"Data: {data_ext}",
        "=" * 60,
        "",
    ]

    for item in itens:
        lines.append(f"Item {item['num_item']} — {item['tipo_item']}")
        lines.append("-" * 40)
        for linha in item["tabela_linhas"]:
            lines.append(f"  {linha['desc']}: {linha['valor']}")
        lines.append("")

    lines.append("=" * 60)

    try:
        caminho.write_text("\n".join(lines), encoding="utf-8")
        return {
            "name": caminho.name,
            "path": str(caminho),
            "tipo": "Texto (TXT)",
        }
    except Exception as e:
        logger.error("Erro ao salvar resumo TXT: %s", e)
        return None

import re

def gerar_relatorio_fisconforme_html(
    cnpj: str,
    dir_relatorios: Path,
    dir_modelos: Path,
    dados_cadastrais: dict | None = None,
    nome_auditor: str = "",
    matricula_auditor: str = "",
    email_auditor: str = "",
    orgao: str = "GERÊNCIA DE FISCALIZAÇÃO",
    df_pendencias=None,
    numero_DSF: str = "",
    dir_dsf: Path | None = None,
) -> dict | None:
    """
    Gera notificação Fisconforme a partir do template, preenchendo apenas as tags.
    Salva tanto em HTML quanto em TXT conforme solicitado.
    Se df_pendencias for fornecido (polars DataFrame), anexa a tabela 3 linhas abaixo.
    """
    arq_template = dir_modelos / "modelo_relatorio_fisconforme.txt"
    if not arq_template.exists():
        logger.error("Template Fisconforme não encontrado: %s", arq_template)
        return None

    try:
        conteudo = arq_template.read_text(encoding="utf-8")
        
        dados = dados_cadastrais or {}
        
        # Preenchimento dos dados estritamente limitados aos campos {{}}
        conteudo = conteudo.replace("{{razao_social}}", dados.get("RAZAO_SOCIAL", ""))
        conteudo = conteudo.replace("{{cnpj}}", cnpj)
        conteudo = conteudo.replace("{{ie}}", dados.get("IE", ""))
        conteudo = conteudo.replace("{{nome_auditor}}", nome_auditor)
        conteudo = conteudo.replace("{{numero_DSF}}", numero_DSF)
        
        # Lida com a variação devido a acentuação web
        conteudo = conteudo.replace("{{matrícula_auditor}}", matricula_auditor)
        conteudo = conteudo.replace("{{matr&iacute;cula_auditor}}", matricula_auditor)
        
        conteudo = conteudo.replace("{{&oacute;rg&atilde;o}}", orgao)
        conteudo = conteudo.replace("{{órgão}}", orgao)
        
        if email_auditor and email_auditor.strip():
            conteudo = conteudo.replace("{{email_auditor}}", email_auditor)
        else:
            conteudo = re.sub(r"<br\s*/>\s*Contato:\s*\{\{email_auditor\}\}", "", conteudo, flags=re.IGNORECASE)
            conteudo = re.sub(r"Contato:\s*\{\{email_auditor\}\}", "", conteudo, flags=re.IGNORECASE)

        # Anexar tabela de pendências se fornecida
        if df_pendencias is not None and not df_pendencias.is_empty():
            tabela_html = _gerar_tabela_html_pendencias(df_pendencias)
            conteudo += "\n<br/>\n<br/>\n<br/>\n" + tabela_html

        # Embutir imagem do DSF (PDF convertido) no conteúdo
        logger.info("[DSF DEBUG] numero_DSF=%r, dir_dsf=%r", numero_DSF, dir_dsf)
        if numero_DSF and numero_DSF.strip() and dir_dsf:
            dsf_filename = f"DSF_{numero_DSF.strip()}.pdf"
            dsf_path = dir_dsf / dsf_filename
            if dsf_path.exists():
                dsf_img_html = _converter_pdf_para_img_html(dsf_path)
                if dsf_img_html:
                    conteudo += "\n<br/>\n<br/>\n" + dsf_img_html
                    logger.info("Imagem DSF embutida para %s", dsf_filename)
            else:
                logger.warning("DSF não encontrado: %s", dsf_path)
                conteudo += f"\n<br/>\n<br/>\n<b>[AVISO: Arquivo PDF do DSF '{dsf_filename}' n&atilde;o encontrado na pasta CNPJ/DSF]</b>\n"

        dir_relatorios.mkdir(parents=True, exist_ok=True)
        
        caminho_saida_html = dir_relatorios / f"notificacao_fisconforme_{cnpj}.html"
        caminho_saida_html.write_text(conteudo, encoding="utf-8")
        
        caminho_saida_txt = dir_relatorios / f"notificacao_fisconforme_{cnpj}.txt"
        caminho_saida_txt.write_text(conteudo, encoding="utf-8")
        
        logger.info("Notificação Fisconforme gerada: %s e %s", caminho_saida_html, caminho_saida_txt)
        return {
            "name": caminho_saida_txt.name,
            "path": str(caminho_saida_txt),
            "tipo": "TXT/HTML",
            "template": "modelo_relatorio_fisconforme.txt",
        }
    except Exception as e:
        logger.error("Erro ao gerar notificação fisconforme: %s", e)
        return None


def _gerar_tabela_html_pendencias(df) -> str:
    """Gera uma tabela HTML formatada a partir de um polars DataFrame de pendências."""
    colunas = df.columns
    linhas_html = []
    linhas_html.append('<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:10px;">')
    
    # Cabeçalho
    linhas_html.append("<tr style='background-color:#2d6a4f;color:white;'>")
    for col in colunas:
        linhas_html.append(f"<th style='padding:6px 10px;text-align:left;'>{col}</th>")
    linhas_html.append("</tr>")
    
    # Dados
    for i, row in enumerate(df.iter_rows()):
        bg = "#f0f0f0" if i % 2 == 0 else "#ffffff"
        linhas_html.append(f"<tr style='background-color:{bg};'>")
        for val in row:
            texto = str(val) if val is not None else ""
            linhas_html.append(f"<td style='padding:4px 8px;'>{texto}</td>")
        linhas_html.append("</tr>")
    
    linhas_html.append("</table>")
    return "\n".join(linhas_html)


def _converter_pdf_para_img_html(pdf_path: Path) -> str | None:
    """Converte cada página de um PDF em imagem PNG (base64) e retorna tags HTML <img>."""
    import base64
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.warning("PyMuPDF (fitz) não instalado. Não é possível converter PDF para imagem.")
        return None

    try:
        doc = fitz.open(str(pdf_path))
        imgs_html = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Renderiza a página como PNG com resolução 2x (150 DPI)
            pix = page.get_pixmap(dpi=150)
            img_bytes = pix.tobytes("png")
            b64 = base64.b64encode(img_bytes).decode("utf-8")
            imgs_html.append(
                f'<p style="text-align:center;"><img src="data:image/png;base64,{b64}" '
                f'style="max-width:100%;border:1px solid #ccc;" alt="DSF página {page_num + 1}" /></p>'
            )
        doc.close()
        return "\n".join(imgs_html)
    except Exception as e:
        logger.error("Erro ao converter PDF para imagem: %s", e)
        return None
