import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { startTestServer, stopTestServer, API_BASE } from "./setup";

beforeAll(async () => {
  await startTestServer();
});

afterAll(() => {
  stopTestServer();
});

const VALID_CNPJ = "00000000000191";
const INVALID_CNPJ = "000";

describe("2. /auditoria/pipeline Contract", () => {
  it("Rejeita CNPJ invalido com erro coerente", async () => {
    const res = await fetch(`${API_BASE}/api/python/auditoria/pipeline`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cnpj: INVALID_CNPJ }),
    });

    // Validate: 400 Bad Request
    expect(res.status).toBe(400);
    const data = await res.json();
    expect(data).toHaveProperty("detail");
  });

  it("Aceita CNPJ valido e valida o shape minimo inicial", async () => {
    const res = await fetch(`${API_BASE}/api/python/auditoria/pipeline`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cnpj: VALID_CNPJ }),
    });

    expect(res.status).toBe(200);
    const data = await res.json();

    expect(data).toHaveProperty("success");
    expect(data).toHaveProperty("cnpj");
    expect(data).toHaveProperty("job_status");
    expect(data).toHaveProperty("message");
    expect(data).toHaveProperty("dir_parquet");
    expect(data).toHaveProperty("dir_analises");
    expect(data).toHaveProperty("dir_relatorios");

    expect(data.job_status).toBe("agendada");
  });
});

describe("3. /auditoria/status/{cnpj} Contract", () => {
  it("Valida shape de status de cnpj valido", async () => {
    const res = await fetch(
      `${API_BASE}/api/python/auditoria/status/${VALID_CNPJ}`
    );

    expect(res.status).toBe(200);
    const data = await res.json();

    expect(data).toHaveProperty("success");
    expect(data).toHaveProperty("cnpj");
    expect(data).toHaveProperty("job_status");
    expect(data).toHaveProperty("message");
    expect(data).toHaveProperty("etapas");
    expect(data).toHaveProperty("erros");
    expect(data).toHaveProperty("arquivos_extraidos");
    expect(data).toHaveProperty("arquivos_analises");
    expect(data).toHaveProperty("arquivos_produtos");
    expect(data).toHaveProperty("arquivos_relatorios");
    expect(data).toHaveProperty("dir_parquet");
    expect(data).toHaveProperty("dir_analises");
    expect(data).toHaveProperty("dir_relatorios");

    const allowedStatuses = ["agendada", "executando", "concluida", "erro"];
    expect(allowedStatuses).toContain(data.job_status);

    expect(Array.isArray(data.etapas)).toBe(true);
    expect(Array.isArray(data.erros)).toBe(true);
  });
});

describe("4. /auditoria/historico/{cnpj} Contract", () => {
  it("Valida shape de historico de cnpj valido", async () => {
    const res = await fetch(
      `${API_BASE}/api/python/auditoria/historico/${VALID_CNPJ}`
    );

    expect(res.status).toBe(200);
    const data = await res.json();

    expect(data).toHaveProperty("success");
    expect(data).toHaveProperty("cnpj");
    expect(data).toHaveProperty("arquivos_extraidos");
    expect(data).toHaveProperty("arquivos_analises");
    expect(data).toHaveProperty("arquivos_produtos");
    expect(data).toHaveProperty("arquivos_relatorios");
    expect(data).toHaveProperty("dir_parquet");
    expect(data).toHaveProperty("dir_analises");
    expect(data).toHaveProperty("dir_relatorios");

    expect(Array.isArray(data.arquivos_extraidos)).toBe(true);
    expect(Array.isArray(data.arquivos_analises)).toBe(true);
    expect(Array.isArray(data.arquivos_produtos)).toBe(true);
    expect(Array.isArray(data.arquivos_relatorios)).toBe(true);
  });
});
