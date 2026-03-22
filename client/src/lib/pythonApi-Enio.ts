/**
 * Python API client for SEFIN Audit Tool
 * Communicates with the FastAPI backend through the Express proxy at /api/python
 */

const BASE = "/api/python";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(error.detail || `API Error: ${res.status}`);
  }

  return res.json();
}

async function downloadFile(
  path: string,
  options?: RequestInit
): Promise<Blob> {
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(error.detail || `API Error: ${res.status}`);
  }

  return res.blob();
}

function triggerDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ============================================================
// Health
// ============================================================

export async function checkHealth() {
  return request<{ status: string; version: string; engine: string }>(
    "/health"
  );
}

// ============================================================
// Project Paths
// ============================================================

export async function getProjectPaths() {
  return request<{
    projeto_dir: string;
    consultas_fonte: string | null;
    consultas_fonte_auxiliares: string | null;
    cruzamentos: string | null;
    referencias: string | null;
  }>("/project/paths");
}

// ============================================================
// Audit Pipeline
// ============================================================

export type AuditFileResult = {
  name: string;
  path: string;
  rows?: number;
  columns?: number;
  query?: string;
  analise?: string;
  size?: number;
  modified?: string;
};

export type AuditReportResult = {
  name: string;
  path: string;
  tipo: string;
  template?: string;
};

export type AuditEtapa = {
  etapa: string;
  status: string;
  consultas_executadas?: number;
  consultas_com_erro?: number;
  documentos_gerados?: number;
  analises?: { nome: string; status: string; motivo?: string }[];
};

export type AuditPipelineBaseResponse = {
  success: boolean;
  cnpj: string;
  message?: string;
  dir_parquet: string;
  dir_analises: string;
  dir_relatorios: string;
};

export type AuditPipelineStartResponse = AuditPipelineBaseResponse & {
  job_status: "agendada";
};

export type AuditPipelineStatusResponse = AuditPipelineBaseResponse & {
  job_status: "agendada" | "executando" | "concluida" | "erro";
  etapas: AuditEtapa[];
  erros: string[];
  arquivos_extraidos: AuditFileResult[];
  arquivos_analises: AuditFileResult[];
  arquivos_produtos: AuditFileResult[];
  arquivos_relatorios: AuditReportResult[];
};

export type AuditPipelineFinalResult = AuditPipelineBaseResponse & {
  etapas: AuditEtapa[];
  erros: string[];
  arquivos_extraidos: AuditFileResult[];
  arquivos_analises: AuditFileResult[];
  arquivos_produtos: AuditFileResult[];
  arquivos_relatorios: AuditReportResult[];
};

export async function runAuditPipeline(
  cnpj: string,
  data_limite_processamento?: string
) {
  return request<AuditPipelineStartResponse>("/auditoria/pipeline", {
    method: "POST",
    body: JSON.stringify({
      cnpj,
      data_limite_processamento: data_limite_processamento || "",
    }),
  });
}

export type AuditHistorySummary = {
  cnpj: string;
  razao_social?: string | null;
  qtd_parquets: number;
  qtd_analises: number;
  qtd_relatorios: number;
  ultima_modificacao: string | null;
};

export async function getAuditHistory() {
  return request<{ success: boolean; historico: AuditHistorySummary[] }>(
    "/auditoria/historico"
  );
}

export async function getAuditDetails(cnpj: string) {
  return request<AuditPipelineFinalResult>(
    `/auditoria/historico/${encodeURIComponent(cnpj)}`
  );
}

// ============================================================
// Lote de Auditorias (Batch Processing)
// ============================================================

export type LoteCNPJRequest = {
  cnpjs: string[];
  queries: string[];
  gerar_excel: boolean;
  gerar_relatorio_fisconforme: boolean;
  nome_auditor: string;
  matricula_auditor: string;
  email_auditor: string;
  orgao: string;
  numero_DSF?: string;
  data_limite_processamento?: string;
};

export type LoteCNPJResult = {
  cnpj: string;
  sucesso: boolean;
  arquivos: string[];
  erros: string[];
};

export type LoteCNPJResponse = {
  success: boolean;
  lote: LoteCNPJResult[];
  total_processados: number;
};

export async function runBatchAudit(data: LoteCNPJRequest) {
  return request<LoteCNPJResponse>("/auditoria/lote", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function getAvailableQueries() {
  return request<{
    success: boolean;
    consultas: { id: string; nome: string }[];
  }>("/auditoria/consultas");
}

// ============================================================
// Filesystem
// ============================================================

export type BrowseEntry = {
  name: string;
  path: string;
  has_children: boolean;
};

export type BrowseResponse = {
  current: string;
  parent: string | null;
  entries: BrowseEntry[];
};

export async function browseDirectory(path: string = "") {
  return request<BrowseResponse>(
    `/filesystem/browse?path=${encodeURIComponent(path)}`
  );
}

export type SqlQueryDefinition = {
  id: string; // Absolute path to the .sql file
  name: string; // File name without .sql extension
  description: string;
  parameters?: string[];
};

export async function listSqlQueries(path: string) {
  return request<{ queries: SqlQueryDefinition[] }>(
    `/filesystem/sql-queries?path=${encodeURIComponent(path)}`
  );
}

export async function listAuxiliaryQueries(path: string) {
  return request<{ queries: SqlQueryDefinition[]; count: number }>(
    `/filesystem/auxiliary-queries?path=${encodeURIComponent(path)}`
  );
}

// ============================================================
// Oracle
// ============================================================

export type OracleConnectionConfig = {
  host: string;
  port: number;
  service: string;
  user: string;
  password: string;
};

export async function testOracleConnection(config: OracleConnectionConfig) {
  return request<{ success: boolean; message: string }>(
    "/oracle/test-connection",
    {
      method: "POST",
      body: JSON.stringify(config),
    }
  );
}

export async function getOracleCredentials() {
  return request<{
    success: boolean;
    has_credentials: boolean;
    user?: string;
    password?: string;
    message?: string;
  }>("/oracle/credentials");
}

export async function saveOracleCredentials(config: OracleConnectionConfig) {
  return request<{ success: boolean; message: string }>(
    "/oracle/save-credentials",
    {
      method: "POST",
      body: JSON.stringify(config),
    }
  );
}

export async function clearOracleCredentials() {
  return request<{ success: boolean; message: string }>(
    "/oracle/clear-credentials",
    {
      method: "DELETE",
    }
  );
}

export type ExtractionRequest = {
  connection: OracleConnectionConfig;
  cnpj?: string;
  output_dir: string;
  queries: string[];
  include_auxiliary: boolean;
  auxiliary_queries_dir?: string;
  normalize_columns: boolean;
  parameters?: Record<string, string>;
};

export type ExtractionResult = {
  query: string;
  rows?: number;
  columns?: number;
  file?: string;
  status: string;
  message?: string;
};

export async function extractOracleData(req: ExtractionRequest) {
  return request<{
    success: boolean;
    results: ExtractionResult[];
    output_dir: string;
  }>("/oracle/extract", { method: "POST", body: JSON.stringify(req) });
}

// ============================================================
// Parquet
// ============================================================

export type ParquetFileInfo = {
  name: string;
  path: string;
  size: number;
  size_human: string;
  rows: number;
  columns: number;
  modified: string;
  relative_path: string;
  error?: boolean;
};

export async function listParquetFiles(directory: string) {
  return request<{
    directory: string;
    files: ParquetFileInfo[];
    count: number;
  }>(`/parquet/list?directory=${encodeURIComponent(directory)}`);
}

export type ParquetReadResponse = {
  columns: string[];
  dtypes: Record<string, string>;
  rows: Record<string, unknown>[];
  total_rows: number;
  filtered_rows: number;
  page: number;
  page_size: number;
  total_pages: number;
  file_name: string;
};

export async function readParquet(params: {
  file_path: string;
  page?: number;
  page_size?: number;
  filters?: Record<string, string>;
  sort_column?: string;
  sort_direction?: string;
}) {
  return request<ParquetReadResponse>("/parquet/read", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function writeParquetCell(params: {
  file_path: string;
  row_index: number;
  column: string;
  value: string;
}) {
  return request<{ success: boolean; message: string }>("/parquet/write-cell", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function addParquetRow(file_path: string) {
  return request<{ success: boolean; new_row_count: number }>(
    "/parquet/add-row",
    {
      method: "POST",
      body: JSON.stringify({ file_path }),
    }
  );
}

export async function addParquetColumn(
  file_path: string,
  column_name: string,
  default_value = ""
) {
  return request<{
    success: boolean;
    column_name: string;
    total_columns: number;
  }>("/parquet/add-column", {
    method: "POST",
    body: JSON.stringify({ file_path, column_name, default_value }),
  });
}

export async function getUniqueValues(file_path: string, column: string) {
  return request<{ column: string; values: string[] }>(
    `/parquet/unique-values?file_path=${encodeURIComponent(file_path)}&column=${encodeURIComponent(column)}`
  );
}

export type ParquetMergeRequest = {
  file_a: string;
  file_b: string;
  how?: "inner" | "left" | "outer";
  on: string[];
  output_name: string;
  output_dir?: string;
  columns_a?: string[];
  columns_b?: string[];
};

export type ParquetMergeResponse = {
  success: boolean;
  message: string;
  file_path: string;
  rows: number;
  columns: number;
};

export async function mergeParquetFiles(params: ParquetMergeRequest) {
  return request<ParquetMergeResponse>("/parquet/merge", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

// ============================================================
// CNPJ
// ============================================================

export async function validateCnpj(cnpj: string) {
  return request<{ cnpj: string; cnpj_limpo: string; valid: boolean }>(
    `/validate-cnpj?cnpj=${encodeURIComponent(cnpj)}`
  );
}

// ============================================================
// Export Excel
// ============================================================

export async function exportToExcel(
  source_files: string[],
  output_dir: string
) {
  return request<{
    success: boolean;
    results: {
      file: string;
      output?: string;
      rows?: number;
      status: string;
      message?: string;
    }[];
  }>("/export/excel", {
    method: "POST",
    body: JSON.stringify({ source_files, output_dir }),
  });
}

export async function downloadExcel(file_path: string) {
  const blob = await downloadFile(
    `/export/excel-download?file_path=${encodeURIComponent(file_path)}`
  );
  const filename =
    file_path.split("/").pop()?.replace(".parquet", ".xlsx") || "export.xlsx";
  triggerDownload(blob, filename);
}

// ============================================================
// Agrupamento de Produtos & Fatores de Conversão
// ============================================================

export type AgrupamentoProdutosResponse = {
  success: boolean;
  cnpj: string;
  status: string;
  dim_produto?: string;
  mapa_0200?: string;
  mapa_nfe?: string;
  discrepancias?: string;
  duplicidades?: string;
  coincidencias?: string;
  tabela_produtos_completa?: string;
  qtd_produtos?: number;
  qtd_codigos_0200?: number;
  qtd_codigos_nfe?: number;
  qtd_discrepancias?: number;
  qtd_duplicidades?: number;
  qtd_coincidencias?: number;
  qtd_produtos_total?: number;
};

export async function runAgrupamentoProdutos(cnpj: string) {
  return request<AgrupamentoProdutosResponse>("/produtos/agrupamento", {
    method: "POST",
    body: JSON.stringify({ cnpj }),
  });
}

export type FatoresConversaoResponse = {
  success: boolean;
  cnpj: string;
  status: string;
  arquivo_fatores: string;
  qtd_registros: number;
};

export async function calcularFatoresConversao(cnpj: string) {
  return request<FatoresConversaoResponse>("/produtos/fatores-conversao", {
    method: "POST",
    body: JSON.stringify({ cnpj }),
  });
}

export type AplicarAgrupamentoResult = {
  arquivo: string;
  linhas: number;
  path: string;
};

export type AplicarAgrupamentoResponse = {
  success: boolean;
  cnpj: string;
  resultados: AplicarAgrupamentoResult[];
};

export async function aplicarAgrupamento(cnpj: string) {
  return request<AplicarAgrupamentoResponse>("/produtos/aplicar-agrupamento", {
    method: "POST",
    body: JSON.stringify({ cnpj }),
  });
}

export type ResolverLoteResponse = {
  success: boolean;
  cnpj: string;
  status: string;
  mensagem: string;
  resolvidos: number;
};

export async function resolverEmLote(
  cnpj: string,
  tipo: "discrepancias" | "duplicidades",
  nivel_minimo: number
) {
  return request<ResolverLoteResponse>("/produtos/resolver-em-lote", {
    method: "POST",
    body: JSON.stringify({ cnpj, tipo, nivel_minimo }),
  });
}

export type ImportFatoresExcelResponse = {
  success: boolean;
  cnpj: string;
  file: string;
  registros: number;
};

/**
 * Importa fatores de conversão a partir de um arquivo Excel preenchido pelo usuário.
 * O Excel deve conter as colunas:
 * - chave_produto
 * - ano_referencia
 * - unidade_origem
 * - fator
 */
export async function importFatoresExcel(cnpj: string, file: File) {
  const formData = new FormData();
  formData.append("file", file);

  const res = await fetch(
    `${BASE}/fatores/import-excel?cnpj=${encodeURIComponent(cnpj)}`,
    {
      method: "POST",
      body: formData,
      // Não definir Content-Type aqui; o browser define o boundary para multipart/form-data.
    }
  );

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(error.detail || `API Error: ${res.status}`);
  }

  return (await res.json()) as ImportFatoresExcelResponse;
}

// ============================================================
// Reports
// ============================================================

export type TimbradoRequest = {
  orgao: string;
  razao_social: string;
  cnpj: string;
  ie?: string;
  situacao_ie?: string;
  regime_pagamento?: string;
  regime_especial?: string;
  atividade_principal?: string;
  endereco?: string;
  num_dsf?: string;
  objeto?: string;
  relato?: string;
  itens?: { tipo: string; descricao: string }[];
  conclusao?: string;
  afte?: string;
  matricula?: string;
  data_extenso?: string;
  endereco_orgao?: string;
};

export async function generateTimbradoReport(data: TimbradoRequest) {
  const blob = await downloadFile("/reports/timbrado", {
    method: "POST",
    body: JSON.stringify(data),
  });
  const cnpjClean = data.cnpj.replace(/\D/g, "");
  triggerDownload(blob, `relatorio_${cnpjClean}.docx`);
}

export type DETNotificationRequest = {
  razao_social: string;
  cnpj: string;
  ie?: string;
  endereco?: string;
  dsf?: string;
  assunto?: string;
  corpo?: string;
  afte?: string;
  matricula?: string;
};

export async function generateDETNotification(
  data: DETNotificationRequest,
  format: "html" | "txt" = "html"
) {
  const endpoint =
    format === "txt"
      ? "/reports/det-notification-txt"
      : "/reports/det-notification";
  const blob = await downloadFile(endpoint, {
    method: "POST",
    body: JSON.stringify(data),
  });
  const cnpjClean = data.cnpj.replace(/\D/g, "");
  triggerDownload(
    blob,
    `notificacao_det_${cnpjClean}.${format === "txt" ? "txt" : "html"}`
  );
}

// ============================================================
// Agrupamento de Produtos - Resoluções Manuais
// ============================================================

export interface DetalhesCodigoResponse {
  success: boolean;
  codigo: string;
  itens: any[];
}

export interface ResolverManualResponse {
  status: string;
  mensagem: string;
}

export async function getProdutoDetalhes(cnpj: string, codigo: string) {
  return request<DetalhesCodigoResponse>(
    `/produtos/detalhes-codigo?cnpj=${encodeURIComponent(cnpj)}&codigo=${encodeURIComponent(codigo)}`
  );
}

export async function resolverManualUnificar(
  cnpj: string,
  itens: any[],
  decisao: any
) {
  return request<ResolverManualResponse>("/produtos/resolver-manual-unificar", {
    method: "POST",
    body: JSON.stringify({ cnpj, itens, decisao }),
  });
}

export async function resolverManualDesagregar(
  cnpj: string,
  itensDecididos: any[]
) {
  return request<ResolverManualResponse>(
    "/produtos/resolver-manual-desagregar",
    {
      method: "POST",
      body: JSON.stringify({ cnpj, itens_decididos: itensDecididos }),
    }
  );
}
