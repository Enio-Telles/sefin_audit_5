import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { startTestServer, stopTestServer, API_BASE } from "./setup";

beforeAll(async () => {
  await startTestServer();
});

afterAll(() => {
  stopTestServer();
});

describe("1. /health Contract", () => {
  it("Validates /health shape and properties", async () => {
    const res = await fetch(`${API_BASE}/health`);

    // Validate: resposta 200
    expect(res.status).toBe(200);
    const data = await res.json();

    // Validate: body contains status, version, engine
    expect(data).toHaveProperty("status");
    expect(data).toHaveProperty("version");
    expect(data).toHaveProperty("engine");

    // Validate: all three are strings
    expect(typeof data.status).toBe("string");
    expect(typeof data.version).toBe("string");
    expect(typeof data.engine).toBe("string");

    // Validate (optional per spec but good practice)
    expect(data.status).toBe("ok");
    expect(data.engine).toBe("python");
  });
});
