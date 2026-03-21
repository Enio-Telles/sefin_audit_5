import re
import logging
from pathlib import Path
from typing import Optional
import polars as pl

logger = logging.getLogger("sefin_audit_python")

def validar_cnpj(cnpj: str) -> bool:
    """Valida se um CNPJ é válido (dígitos verificadores e formato)."""
    cnpj = re.sub(r"[^0-9]", "", cnpj)
    if len(cnpj) != 14:
        return False
    if len(set(cnpj)) == 1:
        return False
    tamanho = 12
    numeros = cnpj[:tamanho]
    digitos = cnpj[tamanho:]
    soma = 0
    pos = tamanho - 7
    for i in range(tamanho, 0, -1):
        soma += int(numeros[tamanho - i]) * pos
        pos -= 1
        if pos < 2:
            pos = 9
    resultado = soma % 11
    digito_1 = 0 if resultado < 2 else 11 - resultado
    if digito_1 != int(digitos[0]):
        return False
    tamanho = 13
    numeros = cnpj[:tamanho]
    soma = 0
    pos = tamanho - 7
    for i in range(tamanho, 0, -1):
        soma += int(numeros[tamanho - i]) * pos
        pos -= 1
        if pos < 2:
            pos = 9
    resultado = soma % 11
    digito_2 = 0 if resultado < 2 else 11 - resultado
    if digito_2 != int(digitos[1]):
        return False
    return True


def normalizar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Normaliza nomes de colunas para minúsculas."""
    if df is not None and not df.is_empty():
        return df.rename({c: c.lower() for c in df.columns})
    return df


def extrair_parametros_sql(sql: str) -> set[str]:
    """Identifica bind variables no formato :nome_variavel."""
    # Remove comentários de linha
    sql_no_comments = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    # Remove comentários de bloco
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_no_comments, flags=re.DOTALL)
    # Remove strings literal ('...')
    sql_no_strings = re.sub(r"'[^']*'", "", sql_no_comments)
    # Extrai os binds
    return set(match.upper() for match in re.findall(r":([a-zA-Z0-9_]+)", sql_no_strings))


def ler_sql(arquivo: Path) -> str:
    """Lê arquivo SQL com tratamento robusto de encoding."""
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "iso-8859-1", "cp1250"]
    last_error: Optional[Exception] = None
    for enc in encodings:
        try:
            sql_txt = arquivo.read_text(encoding=enc).strip()
            # remove ponto-e-vírgula final opcional
            sql_txt = re.sub(r";\s*$", "", sql_txt)
            logger.info("[ler_sql] arquivo='%s' lido com encoding '%s' (tamanho=%d)", arquivo.name, enc, len(sql_txt))
            return sql_txt
        except UnicodeDecodeError as e:
            last_error = e
            continue
        except Exception as e:
            last_error = e
            continue
    raise Exception(
        (
            f"Não foi possível ler o arquivo '{arquivo.name}'. "
            f"Tente salvar em UTF-8 (sem BOM) ou CP1252. Erro: {last_error}"
        )
    )


def _write_excel_with_format(pdf, writer, sheet_name: str = "Plan1"):
    """Escreve DataFrame em planilha com Arial 9, cabeçalho em negrito e autoajuste de colunas."""
    # Escreve conteúdo inicial
    pdf.to_excel(writer, index=False, sheet_name=sheet_name)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Formatos
    default_fmt = workbook.add_format({"font_name": "Arial", "font_size": 9})
    header_fmt  = workbook.add_format({"bold": True, "font_name": "Arial", "font_size": 9})
    date_fmt    = workbook.add_format({"font_name": "Arial", "font_size": 9, "num_format": "dd/mm/yyyy"})
    int_fmt     = workbook.add_format({"font_name": "Arial", "font_size": 9, "num_format": "#,##0"})
    float_fmt   = workbook.add_format({"font_name": "Arial", "font_size": 9, "num_format": "#,##0.00"})

    # Cabeçalho
    for col_num, value in enumerate(pdf.columns.values):
        worksheet.write(0, col_num, value, header_fmt)

    # Heurística de largura e formatos por coluna
    sample = pdf.head(1000)
    for idx, col in enumerate(pdf.columns):
        try:
            col_values = sample[col].astype(str).tolist()
        except Exception:
            col_values = [str(v) for v in sample[col].tolist()]
        max_len = max([len(str(col))] + [len(str(v)) for v in col_values]) if len(col_values) > 0 else len(str(col))
        width = min(max(10, max_len + 2), 60)

        dtype_str = str(pdf[col].dtype)
        col_lower = str(col).lower()
        fmt = default_fmt
        if "datetime64" in dtype_str or "date" in col_lower:
            fmt = date_fmt
        elif dtype_str.startswith(("int", "Int")):
            fmt = int_fmt
        elif dtype_str.startswith(("float", "Float")) or dtype_str in ("float64", "Float64"):
            fmt = float_fmt
        worksheet.set_column(idx, idx, width, fmt)


def encontrar_arquivo(diretorio: Path, prefixo: str, cnpj: str) -> Optional[Path]:
    """Busca arquivo Parquet por prefixo e CNPJ no diretório especificado."""
    padrao = f"{prefixo}_{cnpj}.parquet"
    arquivos = list(diretorio.glob(padrao))
    if arquivos:
        return arquivos[0]
    for arq in diretorio.glob("*.parquet"):
        if prefixo.lower() in arq.stem.lower() and cnpj in arq.stem:
            return arq
    return None


def _human_size(size_bytes: int) -> str:
    """Converte bytes para formato legível."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"
