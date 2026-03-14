from __future__ import annotations
from pydantic import BaseModel
from typing import Optional, Any

class OracleConnectionConfig(BaseModel):
    host: str = "exa01-scan.sefin.ro.gov.br"
    port: int = 1521
    service: str = "sefindw"
    user: str
    password: str


class ExtractionRequest(BaseModel):
    connection: OracleConnectionConfig
    cnpj: str = ""
    output_dir: str
    queries: list[str]  # list of SQL file paths or query names
    include_auxiliary: bool = True
    auxiliary_queries_dir: Optional[str] = None  # directory with auxiliary .sql files
    normalize_columns: bool = True
    parameters: Optional[dict[str, str]] = None


class ParquetReadRequest(BaseModel):
    file_path: str
    page: int = 1
    page_size: int = 50
    filters: Optional[dict[str, str]] = None
    sort_column: Optional[str] = None
    sort_direction: str = "asc"


class ParquetWriteRequest(BaseModel):
    file_path: str
    row_index: int
    column: str
    value: str


class ParquetAddRowRequest(BaseModel):
    file_path: str


class ParquetAddColumnRequest(BaseModel):
    file_path: str
    column_name: str
    default_value: str = ""


class ExcelExportRequest(BaseModel):
    source_files: list[str]
    output_dir: str


class TimbradoReportRequest(BaseModel):
    orgao: str = "GERÊNCIA DE FISCALIZAÇÃO"
    razao_social: str
    cnpj: str
    ie: str = ""
    situacao_ie: str = ""
    regime_pagamento: str = ""
    regime_especial: str = ""
    atividade_principal: str = ""
    endereco: str = ""
    num_dsf: str = ""
    objeto: str = "Vistoria"
    relato: str = ""
    itens: list[dict] = []
    conclusao: str = ""
    afte: str = ""
    matricula: str = ""
    data_extenso: str = ""
    endereco_orgao: str = "Av. Presidente Dutra, 4250 - Olaria, Porto Velho - RO"


class DETNotificationRequest(BaseModel):
    razao_social: str
    cnpj: str
    ie: str = ""
    endereco: str = ""
    dsf: str = ""
    assunto: str = ""
    corpo: str = ""
    afte: str = ""
    matricula: str = ""


class AnaliseFaturamentoRequest(BaseModel):
    input_dir: str
    cnpj: Optional[str] = None
    data_ini: Optional[str] = None
    data_fim: Optional[str] = None
    output_dir: str
    arquivo_base: Optional[str] = None  # default: nfe_saida.parquet





class FisconformeRequest(BaseModel):
    cnpj: str
    nome_auditor: str
    matricula_auditor: str
    email_auditor: str = ""
    orgao: str = "GERÊNCIA DE FISCALIZAÇÃO"


class LoteCNPJRequest(BaseModel):
    cnpjs: list[str]
    queries: list[str]
    gerar_excel: bool = True
    gerar_relatorio_fisconforme: bool = True
    nome_auditor: str = ""
    matricula_auditor: str = ""
    email_auditor: str = ""
    orgao: str = "GERÊNCIA DE FISCALIZAÇÃO"
    numero_DSF: str = ""
    data_limite_processamento: Optional[str] = None


class AuditPipelineRequest(BaseModel):
    cnpj: str
    data_limite_processamento: Optional[str] = None


class ProdutoUnidRequest(BaseModel):
    cnpj: str


class RevisaoManualItem(BaseModel):
    fonte: str
    codigo_original: str
    descricao_original: str
    tipo_item_original: Optional[str] = None
    codigo_novo: str
    descricao_nova: str
    ncm_novo: Optional[str] = None
    cest_novo: Optional[str] = None
    gtin_novo: Optional[str] = None
    tipo_item_novo: Optional[str] = None


class RevisaoManualSubmitRequest(BaseModel):
    cnpj: str
    decisoes: list[RevisaoManualItem]

class ResolverManualUnificarRequest(BaseModel):
    cnpj: str
    itens: list[dict]
    decisao: dict

class ResolverManualDesagregarRequest(BaseModel):
    cnpj: str
    itens_decididos: list[dict]

class ResolverManualMultiDetalhesRequest(BaseModel):
    cnpj: str
    codigos: list[str]


class DescricaoManualMapItem(BaseModel):
    tipo_regra: str = "UNIR_GRUPOS"
    descricao_origem: str
    descricao_destino: str
    descricao_par: Optional[str] = None
    chave_grupo_a: Optional[str] = None
    chave_grupo_b: Optional[str] = None
    score_origem: Optional[str] = None
    acao_manual: Optional[str] = "AGREGAR"


class ResolverManualDescricoesRequest(BaseModel):
    cnpj: str
    regras: list[DescricaoManualMapItem]


class DesfazerManualCodigoRequest(BaseModel):
    cnpj: str
    codigo: str


class DesfazerManualDescricoesRequest(BaseModel):
    cnpj: str
    descricoes: list[str]
