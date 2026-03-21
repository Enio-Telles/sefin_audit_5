/**
 * Python API client for SEFIN Audit Tool
 * Communicates with the FastAPI backend through the Express proxy at /api/python
 */

const BASE = "/api/python";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${BASE}${path}`, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...options?.headers,
      },
    });
  } catch (error: any) {
    const err: any = error instanceof Error ? error : new Error(String(error));
    err.path = path;
    err.isNetworkError = true;
    throw err;
  }

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    const err: any = new Error(error.detail || `API Error: ${res.status}`);
    err.status = res.status;
    err.path = path;
    throw err;
  }

  return res.json();
}

async function downloadFile(path: string, options?: RequestInit): Promise<Blob> {
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    const err: any = new Error(error.detail || `API Error: ${res.status}`);
    err.status = res.status;
    throw err;
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
  return request<{ status: string; version: string }>("/health");
}

// ============================================================
// Export — Revisão Manual
// ============================================================

export async function downloadRevisaoManualExcel(cnpj: string) {
  const blob = await downloadFile(`/export/revisao-manual-excel?cnpj=${encodeURIComponent(cnpj)}`);
  triggerDownload(blob, `revisao_manual_produtos_${cnpj.replace(/[^0-9]/g, "")}.xlsx`);
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
  rows: number;
  columns: number;
  query?: string;
  analise?: string;
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

export type AuditPipelineResponse = {
  success: boolean;
  cnpj: string;
  etapas: AuditEtapa[];
  arquivos_extraidos: AuditFileResult[];
  arquivos_analises: AuditFileResult[];
  arquivos_produtos: AuditFileResult[];
  arquivos_relatorios: AuditReportResult[];
  erros: string[];
  dir_parquet: string;
  dir_analises: string;
  dir_relatorios: string;
};

export async function runAuditPipeline(cnpj: string, data_limite_processamento?: string) {
  return request<AuditPipelineResponse>("/auditoria/pipeline", {
    method: "POST",
    body: JSON.stringify({ cnpj, data_limite_processamento: data_limite_processamento || "" }),
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
  return request<{ success: boolean; historico: AuditHistorySummary[] }>("/auditoria/historico");
}

export async function getAuditDetails(cnpj: string) {
  return request<AuditPipelineResponse>(`/auditoria/historico/${encodeURIComponent(cnpj)}`);
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
  return request<{ success: boolean; consultas: { id: string; nome: string }[] }>("/auditoria/consultas");
}

// ============================================================
// Filesystem
// ============================================================

export type BrowseEntry = {
  name: string;
  path: string;
  type: "directory" | "file";
  size?: number;
  human_size?: string;
  modified?: number;
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
  return request<{ success: boolean; message: string }>("/oracle/test-connection", {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function getOracleCredentials() {
  return request<{ success: boolean; has_credentials: boolean; user?: string; message?: string }>("/oracle/credentials");
}

export async function saveOracleCredentials(config: OracleConnectionConfig) {
  return request<{ success: boolean; message: string }>("/oracle/save-credentials", {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function clearOracleCredentials() {
  return request<{ success: boolean; message: string }>("/oracle/clear-credentials", {
    method: "DELETE",
  });
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
  return request<{ success: boolean; results: ExtractionResult[]; output_dir: string }>(
    "/oracle/extract",
    { method: "POST", body: JSON.stringify(req) }
  );
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
  modified: number;
  relative_path: string;
  error?: boolean;
};

export async function listParquetFiles(directory: string) {
  return request<{ directory: string; files: ParquetFileInfo[]; count: number }>(
    `/parquet/list?directory=${encodeURIComponent(directory)}`
  );
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
  return request<{ success: boolean; new_index: number }>("/parquet/add-row", {
    method: "POST",
    body: JSON.stringify({ file_path }),
  });
}

export async function addParquetColumn(file_path: string, column_name: string, default_value = "") {
  return request<{ success: boolean }>("/parquet/add-column", {
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
  on: string[];
  how: "inner" | "left" | "outer" | "cross";
  columns_a?: string[];
  columns_b?: string[];
  output_dir?: string;
  output_name: string;
};

export async function mergeParquetFiles(data: ParquetMergeRequest) {
  return request<{ success: boolean; message: string; file_path: string; rows: number; columns: number }>(
    "/parquet/merge",
    {
      method: "POST",
      body: JSON.stringify(data),
    }
  );
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

export async function exportToExcel(source_files: string[], output_dir: string) {
  return request<{ success: boolean; results: { file: string; output?: string; rows?: number; status: string; message?: string }[] }>(
    "/export/excel",
    { method: "POST", body: JSON.stringify({ source_files, output_dir }) }
  );
}

export async function downloadExcel(file_path: string) {
  const blob = await downloadFile(`/export/excel-download?file_path=${encodeURIComponent(file_path)}`);
  const filename = file_path.split("/").pop()?.replace(".parquet", ".xlsx") || "export.xlsx";
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
  produtos_agregados?: string;
  tabela_produtos_completa?: string;
  qtd_produtos?: number;
  qtd_produtos_total?: number;
  qtd_codigos_0200?: number;
  qtd_codigos_nfe?: number;
  qtd_discrepancias?: number;
  qtd_duplicidades?: number;
  qtd_coincidencias?: number;
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

export type FatorDiagnosticoItem = {
  tipo: string;
  severidade: string;
  chave_produto: string;
  ano_referencia?: number | null;
  unidade_origem: string;
  fator?: number | null;
  detalhes: string;
  sugestao: string;
};

export type FatoresDiagnosticoResponse = {
  success: boolean;
  available: boolean;
  cnpj: string;
  file: string;
  message?: string;
  stats: {
    total_registros: number;
    produtos_unicos: number;
    anos_unicos: number;
    unidades_unicas: number;
    editados_manual: number;
    fatores_invalidos: number;
    fatores_extremos_altos: number;
    fatores_extremos_baixos: number;
    grupos_muitas_unidades: number;
    grupos_alta_variacao: number;
  };
  issues: FatorDiagnosticoItem[];
};

export async function diagnosticarFatoresConversao(cnpj: string) {
  return request<FatoresDiagnosticoResponse>(`/fatores/diagnostico?cnpj=${encodeURIComponent(cnpj)}`);
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

export async function resolverEmLote(cnpj: string, tipo: 'discrepancias' | 'duplicidades', nivel_minimo: number) {
  return request<ResolverLoteResponse>("/produtos/resolver-em-lote", {
    method: "POST",
    body: JSON.stringify({ cnpj, tipo, nivel_minimo }),
  });
}

export type ProdutoUnidResponse = {
  success: boolean;
  cnpj: string;
  file: string;
  rows: number;
  columns: number;
  status: string;
};

export async function unificarProdutosUnidades(cnpj: string) {
  return request<ProdutoUnidResponse>("/produtos/unificar-produtos-unidades", {
    method: "POST",
    body: JSON.stringify({ cnpj }),
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

export async function generateDETNotification(data: DETNotificationRequest, format: "html" | "txt" = "html") {
  const endpoint = format === "txt" ? "/reports/det-notification-txt" : "/reports/det-notification";
  const blob = await downloadFile(endpoint, {
    method: "POST",
    body: JSON.stringify(data),
  });
  const cnpjClean = data.cnpj.replace(/\D/g, "");
  triggerDownload(blob, `notificacao_det_${cnpjClean}.${format === "txt" ? "txt" : "html"}`);
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

export interface ProdutosRevisaoManualResponse {
  success: boolean;
  data: Record<string, unknown>[];
}

export interface ProdutosRevisaoFinalResponse {
  success: boolean;
  available: boolean;
  file_path: string;
  summary: {
    total_grupos: number;
    grupos_revisao_manual: number;
    grupos_com_gtin: number;
    grupos_com_cest: number;
  };
}

export type BatchRuleId =
  | "R1_HIGH_CONFIDENCE_FULL_FISCAL"
  | "R2_NCM_CEST"
  | "R3_GTIN_NCM"
  | "R6_MANTER_SEPARADO";

export type FiscalRelationState = "EQUAL_FILLED" | "EQUAL_NULL" | "CONFLICT" | "INCOMPLETE";

export interface UnificacaoLotePreviewRequest {
  cnpj: string;
  source_context?: string;
  filters?: {
    descricao_contains?: string;
    ncm_contains?: string;
    cest_contains?: string;
    show_verified?: boolean;
  };
  grouping_mode?: string;
  similarity_source?: {
    engine?: "DOCUMENTAL" | "LIGHT" | "FAISS";
    use_cache?: boolean;
    top_k?: number;
    min_score?: number;
  };
  rule_ids?: BatchRuleId[];
  options?: {
    only_visible?: boolean;
    require_all_pairs_compatible?: boolean;
    max_component_size?: number;
  };
}

export interface UnificacaoLoteProposalItem {
  proposal_id: string;
  rule_id: BatchRuleId;
  button_label: string;
  confidence_band: "HIGH" | "MEDIUM_HIGH" | "MEDIUM" | "LOW";
  status: "ELEGIVEL";
  source_method: string;
  component_size: number;
  chaves_produto: string[];
  descricao_canonica_sugerida: string;
  lista_descricoes: string[];
  fiscal_signature: {
    ncm_values: string[];
    cest_values: string[];
    gtin_values: string[];
  };
  relation_summary: {
    ncm: FiscalRelationState;
    cest: FiscalRelationState;
    gtin: FiscalRelationState;
  };
  metrics: {
    score_descricao_min: number;
    score_descricao_avg: number;
    score_descr_compl_avg: number;
    filled_evidence_count: number;
    score_final_regra: number;
  };
  blocked: boolean;
  blocked_reason: string | null;
}

export interface UnificacaoLotePreviewResponse {
  success: boolean;
  cnpj: string;
  source_context: string;
  similarity_source: {
    engine: string;
    use_cache: boolean;
    top_k: number;
    min_score: number;
  };
  rule_ids: BatchRuleId[];
  dataset_hash?: string | null;
  generated_at_utc: string;
  resumo: {
    total_rows_considered: number;
    total_candidate_pairs: number;
    total_components: number;
    total_proposals: number;
    by_rule: Array<{
      rule_id: BatchRuleId;
      button_label: string;
      proposal_count: number;
      group_count: number;
    }>;
  };
  proposals: UnificacaoLoteProposalItem[];
}

export interface UnificacaoLoteApplyRequest extends UnificacaoLotePreviewRequest {
  action: "UNIFICAR" | "MANTER_SEPARADO";
  rule_id: BatchRuleId;
  proposal_ids: string[];
}

export interface UnificacaoLoteApplyResponse {
  success: boolean;
  cnpj: string;
  action: "UNIFICAR" | "MANTER_SEPARADO";
  rule_id: BatchRuleId;
  applied_count: number;
  affected_groups_count: number;
  skipped_count: number;
  skipped: Array<{ proposal_id: string; reason: string }>;
  status_updates_count: number;
  mapa_manual_path?: string;
  status_path?: string;
}

export interface ParesGruposSimilaresItem {
  chave_produto_a: string;
  descricao_a: string;
  ncm_a?: string;
  cest_a?: string;
  gtin_a?: string;
  qtd_codigos_a?: number;
  conflitos_a?: string;
  chave_produto_b: string;
  descricao_b: string;
  ncm_b?: string;
  cest_b?: string;
  gtin_b?: string;
  qtd_codigos_b?: number;
  conflitos_b?: string;
  score_descricao: number;
  score_semantico?: number;
  score_ncm: number;
  score_cest: number;
  score_gtin: number;
  score_final: number;
  recomendacao: string;
  motivo_recomendacao?: string;
  uniao_automatica_elegivel?: boolean;
  bloquear_uniao?: boolean;
  metodo_similaridade?: string;
  modelo_vetorizacao?: string;
  origem_par_hibrido?: string;
}

export interface ParesGruposSimilaresResponse {
  success: boolean;
  available?: boolean;
  metodo?: "lexical" | "light" | "faiss";
  message?: string;
  cache_metadata?: {
    metodo?: string;
    engine?: string | null;
    input_base_hash?: string | null;
    generated_at_utc?: string | null;
    modelo_vetorizacao?: string | null;
    top_k?: number | null;
    min_semantic_score?: number | null;
    batch_size?: number | null;
  };
  file_path: string;
  data: ParesGruposSimilaresItem[];
  page: number;
  page_size: number;
  total_file?: number;
  total_filtered?: number;
  total: number;
  total_pages: number;
  quick_filter_counts?: {
    todos: number;
    unirAutomatico: number;
    bloqueios: number;
    revisar: number;
  };
}

export interface CodigosMultiDescricaoResponse {
  success: boolean;
  file_path: string;
  data: Record<string, unknown>[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  summary?: {
    total_codigos: number;
    total_descricoes: number;
    total_grupos: number;
  };
}

export interface CodigoMultiDescricaoOpcao {
  valor: string;
  qtd_linhas: number;
}

export interface CodigoMultiDescricaoGrupoResumo {
  descricao: string;
  qtd_linhas: number;
  qtd_combinacoes: number;
  lista_chave_produto: string;
  lista_descr_compl: string;
  lista_tipo_item: string;
  lista_ncm: string;
  lista_cest: string;
  lista_gtin: string;
  lista_unidades: string;
  lista_fontes: string;
}

export interface CodigoMultiDescricaoResumoResponse {
  success: boolean;
  codigo: string;
  resumo: Record<string, unknown>;
  grupos_descricao: CodigoMultiDescricaoGrupoResumo[];
  opcoes_consenso: {
    descricao: CodigoMultiDescricaoOpcao[];
    ncm: CodigoMultiDescricaoOpcao[];
    cest: CodigoMultiDescricaoOpcao[];
    gtin: CodigoMultiDescricaoOpcao[];
  };
}

export interface RevisaoManualDecisionItem {
  fonte: string;
  codigo_original: string;
  descricao_original: string;
  tipo_item_original?: string;
  codigo_novo: string;
  descricao_nova: string;
  ncm_novo?: string;
  cest_novo?: string;
  gtin_novo?: string;
  tipo_item_novo?: string;
}

export interface DescricaoManualMapItem {
  tipo_regra?: string;
  descricao_origem: string;
  descricao_destino: string;
  descricao_par?: string;
  chave_grupo_a?: string;
  chave_grupo_b?: string;
  score_origem?: string;
  acao_manual?: string;
}

export interface ProdutoAnaliseStatusItem {
  tipo_ref: string;
  ref_id: string;
  ref_id_aux?: string;
  descricao_ref?: string;
  contexto_tela?: string;
  status_analise?: string;
  dt_ultima_acao?: string;
}

export interface ProdutoAnaliseStatusResumo {
  pendentes: number;
  verificados: number;
  consolidados: number;
  separados: number;
  decididos_entre_grupos: number;
}

export type AutoSepararResidualMode = "NCM_CEST_GTIN" | "NCM_GTIN" | "NCM_ONLY" | "TEXT_ONLY";

export interface AutoSepararResidualResponse {
  status: string;
  preview: boolean;
  modo: AutoSepararResidualMode;
  qtd_codigos_avaliados: number;
  qtd_codigos_elegiveis: number;
  qtd_codigos_aplicados: number;
  qtd_codigos_ignorados: number;
  motivos_ignorados: { codigo: string; motivo: string }[];
  resumo_motivos_ignorados: { motivo: string; qtd_codigos: number; codigos_amostra: string[] }[];
}

export async function getProdutoDetalhes(cnpj: string, codigo: string) {
  return request<DetalhesCodigoResponse>(`/produtos/detalhes-codigo?cnpj=${encodeURIComponent(cnpj)}&codigo=${encodeURIComponent(codigo)}`);
}

export async function getProdutosRevisaoManual(cnpj: string) {
  return request<ProdutosRevisaoManualResponse>(`/produtos/revisao-manual?cnpj=${encodeURIComponent(cnpj)}`);
}

export async function getProdutosRevisaoFinal(cnpj: string) {
  return request<ProdutosRevisaoFinalResponse>(`/produtos/revisao-final?cnpj=${encodeURIComponent(cnpj)}`);
}

export async function previewUnificacaoLote(req: UnificacaoLotePreviewRequest) {
  return request<UnificacaoLotePreviewResponse>("/produtos/unificacao-lote/propostas", {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export async function applyUnificacaoLote(req: UnificacaoLoteApplyRequest) {
  return request<UnificacaoLoteApplyResponse>("/produtos/unificacao-lote/aplicar", {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export async function getParesGruposSimilares(
  cnpj: string,
  metodo: "lexical" | "light" | "faiss" = "lexical",
  forcarRecalculo = false,
  options?: {
    topK?: number;
    minScore?: number;
    minSemanticScore?: number;
    page?: number;
    pageSize?: number;
    search?: string;
    quickFilter?: "TODOS" | "UNIR_AUTOMATICO" | "BLOQUEIOS" | "REVISAR";
    sortKey?: "PRIORIDADE" | "SIMILARIDADE" | "RECOMENDACAO";
    showAnalyzed?: boolean;
  }
) {
  const topK = options?.topK ?? 8;
  const minScore = options?.minScore ?? options?.minSemanticScore ?? 0.72;
  const minSemanticScore = options?.minSemanticScore ?? 0.32;
  const page = options?.page ?? 1;
  const pageSize = options?.pageSize ?? 50;
  const search = options?.search ?? "";
  const quickFilter = options?.quickFilter ?? "TODOS";
  const sortKey = options?.sortKey ?? "PRIORIDADE";
  const showAnalyzed = options?.showAnalyzed ?? false;
  return request<ParesGruposSimilaresResponse>(
    `/produtos/pares-grupos-similares?cnpj=${encodeURIComponent(cnpj)}&metodo=${encodeURIComponent(metodo)}&forcar_recalculo=${forcarRecalculo ? "true" : "false"}&top_k=${encodeURIComponent(String(topK))}&min_score=${encodeURIComponent(String(minScore))}&min_semantic_score=${encodeURIComponent(String(minSemanticScore))}&page=${encodeURIComponent(String(page))}&page_size=${encodeURIComponent(String(pageSize))}&search=${encodeURIComponent(search)}&quick_filter=${encodeURIComponent(quickFilter)}&sort_key=${encodeURIComponent(sortKey)}&show_analyzed=${showAnalyzed ? "true" : "false"}`
  );
}

export interface VectorizacaoStatusResponse {
  success: boolean;
  current_base_hash?: string | null;
  status: {
    available: boolean;
    light_available?: boolean;
    message: string;
    model_name?: string;
    engine?: string | null;
    modes?: {
      faiss?: { available: boolean; message: string; model_name?: string; engine?: string | null };
      light?: { available: boolean; message: string; model_name?: string; engine?: string | null };

    };
  };
  caches: {
    faiss?: Record<string, unknown> & { stale?: boolean };
    light?: Record<string, unknown> & { stale?: boolean };

  };
}

export async function getVectorizacaoStatus(cnpj: string) {
  return request<VectorizacaoStatusResponse>(`/produtos/vectorizacao-status?cnpj=${encodeURIComponent(cnpj)}`);
}

export interface ProdutoRuntimeStatusResponse {
  success: boolean;
  cnpj: string;
  runtime: {

    files: Record<string, { path: string; exists: boolean; size_bytes?: number }>;
  };
}

export async function getRuntimeProdutosStatus(cnpj: string) {
  return request<ProdutoRuntimeStatusResponse>(`/produtos/runtime-status?cnpj=${encodeURIComponent(cnpj)}`);
}

export async function rebuildRuntimeProdutos(cnpj: string) {
  return request<{ success: boolean; message: string; rows: number; runtime: ProdutoRuntimeStatusResponse["runtime"] }>(
    "/produtos/rebuild-runtime",
    {
      method: "POST",
      body: JSON.stringify({ cnpj }),
    }
  );
}

export async function clearVectorizacaoCache(cnpj: string, metodo: "faiss" | "light" | "all" = "all") {
  return request<{ success: boolean; message: string; removed: string[] }>(
    `/produtos/vectorizacao-clear-cache?cnpj=${encodeURIComponent(cnpj)}&metodo=${encodeURIComponent(metodo)}`,
    { method: "POST" }
  );
}

export async function getCodigosMultiDescricao(
  cnpj: string,
  options?: {
    page?: number;
    pageSize?: number;
    sortColumn?: string;
    sortDirection?: "asc" | "desc";
    showVerified?: boolean;
  }
) {
  const page = options?.page ?? 1;
  const pageSize = options?.pageSize ?? 50;
  const sortColumn = options?.sortColumn ?? "";
  const sortDirection = options?.sortDirection ?? "desc";
  const showVerified = options?.showVerified ?? false;
  return request<CodigosMultiDescricaoResponse>(
    `/produtos/codigos-multidescricao?cnpj=${encodeURIComponent(cnpj)}&page=${encodeURIComponent(String(page))}&page_size=${encodeURIComponent(String(pageSize))}&sort_column=${encodeURIComponent(sortColumn)}&sort_direction=${encodeURIComponent(sortDirection)}&show_verified=${showVerified ? "true" : "false"}`
  );
}

export async function getCodigoMultiDescricaoResumo(cnpj: string, codigo: string) {
  return request<CodigoMultiDescricaoResumoResponse>(
    `/produtos/codigo-multidescricao-resumo?cnpj=${encodeURIComponent(cnpj)}&codigo=${encodeURIComponent(codigo)}`
  );
}

export async function getProdutosDetalhesMulti(cnpj: string, codigos: string[]) {
  return request<{ success: boolean; itens: any[] }>("/produtos/detalhes-multi-codigo", {
    method: "POST",
    body: JSON.stringify({ cnpj, codigos }),
  });
}

export async function resolverManualUnificar(cnpj: string, itens: any[], decisao: any) {
  return request<ResolverManualResponse>("/produtos/resolver-manual-unificar", {
    method: "POST",
    body: JSON.stringify({ cnpj, itens, decisao }),
  });
}

export async function resolverManualDesagregar(cnpj: string, itensDecididos: any[]) {
  return request<ResolverManualResponse>("/produtos/resolver-manual-desagregar", {
    method: "POST",
    body: JSON.stringify({ cnpj, itens_decididos: itensDecididos }),
  });
}

export async function autoSepararResidual(cnpj: string, modo: AutoSepararResidualMode, preview = false, codigos?: string[]) {
  return request<AutoSepararResidualResponse>("/produtos/auto-separar-residual", {
    method: "POST",
    body: JSON.stringify({ cnpj, modo, preview, codigos }),
  });
}

export async function submitProdutosRevisaoManual(cnpj: string, decisoes: RevisaoManualDecisionItem[]) {
  return request<{ success: boolean; message: string }>("/produtos/revisao-manual/submit", {
    method: "POST",
    body: JSON.stringify({ cnpj, decisoes }),
  });
}

export async function resolverManualDescricoes(cnpj: string, regras: DescricaoManualMapItem[]) {
  return request<{ status: string; mensagem: string; arquivo: string; qtd_regras: number }>(
    "/produtos/resolver-manual-descricoes",
    {
      method: "POST",
      body: JSON.stringify({ cnpj, regras }),
    }
  );
}

export async function desfazerDecisaoCodigo(cnpj: string, codigo: string) {
  return request<{ status: string; mensagem: string; qtd_regras_removidas: number }>(
    "/produtos/desfazer-decisao-codigo",
    {
      method: "POST",
      body: JSON.stringify({ cnpj, codigo }),
    }
  );
}

export async function desfazerManualDescricoes(cnpj: string, descricoes: string[]) {
  return request<{ status: string; mensagem: string; qtd_regras_removidas: number }>(
    "/produtos/desfazer-manual-descricoes",
    {
      method: "POST",
      body: JSON.stringify({ cnpj, descricoes }),
    }
  );
}

export async function getStatusAnaliseProdutos(cnpj: string, options?: { includeData?: boolean }) {
  const includeData = options?.includeData ?? true;
  return request<{ success: boolean; file_path: string; data: ProdutoAnaliseStatusItem[]; resumo: ProdutoAnaliseStatusResumo }>(
    `/produtos/status-analise?cnpj=${encodeURIComponent(cnpj)}&include_data=${includeData ? "true" : "false"}`
  );
}

export async function marcarProdutoVerificado(item: ProdutoAnaliseStatusItem & { cnpj: string }) {
  return request<{ success: boolean; mensagem: string; arquivo: string; status_file: string }>(
    "/produtos/marcar-verificado",
    {
      method: "POST",
      body: JSON.stringify(item),
    }
  );
}

export async function desfazerProdutoVerificado(item: ProdutoAnaliseStatusItem & { cnpj: string }) {
  return request<{ success: boolean; mensagem: string; qtd_removidos: number; status_file: string }>(
    "/produtos/desfazer-verificado",
    {
      method: "POST",
      body: JSON.stringify(item),
    }
  );
}

// ============================================================
// Referências Fiscais (NCM/CEST)
// ============================================================

export interface NcmDetailsResponse {
  success: boolean;
  data: {
    codigo: string;
    capitulo: string;
    descr_capitulo: string;
    posicao: string;
    descr_posicao: string;
    descricao: string;
  };
}

export interface CestDetailsResponse {
  success: boolean;
  data: {
    codigo: string;
    segmento: string;
    nome_segmento: string;
    descricoes: string[];
    ncms_associados: string[];
  };
}

export async function getNcmDetails(codigo: string) {
  return request<NcmDetailsResponse>(`/references/ncm/${encodeURIComponent(codigo)}`);
}

export async function getCestDetails(codigo: string) {
  return request<CestDetailsResponse>(`/references/cest/${encodeURIComponent(codigo)}`);
}
