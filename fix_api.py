import re

with open('client/src/lib/pythonApi.ts', 'r') as f:
    content = f.read()

# 1. Update `AuditPipelineResponse` type
target_type = """export type AuditPipelineResponse = {
  success: boolean;
  cnpj: string;
  etapas: AuditEtapa[];"""

replacement_type = """export type AuditPipelineResponse = {
  success: boolean;
  cnpj: string;
  job_status?: "agendada" | "executando" | "concluida" | "erro";
  message?: string;
  etapas: AuditEtapa[];"""

content = content.replace(target_type, replacement_type)

# 2. Add `getAuditStatus` function
target_function = """export async function runAuditPipeline(cnpj: string, data_limite_processamento?: string) {
  return request<AuditPipelineResponse>("/auditoria/pipeline", {
    method: "POST",
    body: JSON.stringify({ cnpj, data_limite_processamento: data_limite_processamento || "" }),
  });
}"""

replacement_function = """export async function runAuditPipeline(cnpj: string, data_limite_processamento?: string) {
  return request<AuditPipelineResponse>("/auditoria/pipeline", {
    method: "POST",
    body: JSON.stringify({ cnpj, data_limite_processamento: data_limite_processamento || "" }),
  });
}

export async function getAuditStatus(cnpj: string) {
  return request<AuditPipelineResponse>(`/auditoria/status/${encodeURIComponent(cnpj)}`);
}"""

content = content.replace(target_function, replacement_function)

with open('client/src/lib/pythonApi.ts', 'w') as f:
    f.write(content)
