import { describe, it, expect } from "vitest";
import { execSync } from "child_process";
import { join } from "path";

describe("Python Backend API Contracts", () => {
  it("runs the python pytest contract suite successfully", () => {
    try {
      // Execute the pytest test suite inside server/python/tests/api_contract
      const stdout = execSync(
        "python -m pytest server/python/tests/api_contract/",
        {
            cwd: process.cwd(),
            env: { ...process.env, PYTHONPATH: 'server/python' },
            encoding: "utf-8",
            stdio: "pipe"
        }
      );

      console.log(stdout);
      // If execSync doesn't throw, it means exit code is 0 (all tests passed)
      expect(true).toBe(true);
    } catch (error: any) {
      console.error(error.stdout);
      console.error(error.stderr);
      // Fail the test if pytest fails
      expect(error.status).toBe(0);
    }
  });
});
