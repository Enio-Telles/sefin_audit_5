import { spawn, ChildProcess } from "child_process";
import { join } from "path";

let pythonServer: ChildProcess;
// Dynamically set port per worker/process to avoid conflicts if vitest runs in parallel
export const TEST_PORT = 8254; // Use fixed port again. Maybe the python server isn't starting?
export const API_BASE = `http://127.0.0.1:${TEST_PORT}`;

export async function startTestServer() {
  if (pythonServer) return;

  return new Promise((resolve, reject) => {
    pythonServer = spawn(
      "python",
      [
        "-m",
        "uvicorn",
        "api:app",
        "--port",
        TEST_PORT.toString(),
        "--host",
        "127.0.0.1",
      ],
      {
        cwd: join(process.cwd(), "server", "python"),
        env: { ...process.env, ALLOWED_ORIGINS: "*" },
      }
    );

    pythonServer.stdout?.on("data", (data: any) => {
      const msg = data.toString();
      if (msg.includes("Application startup complete")) {
        resolve(undefined);
      }
    });

    pythonServer.stderr?.on("data", (data: any) => {
      const msg = data.toString();
      console.log("[Python Server]", msg.trim());
      if (
        msg.includes("Application startup complete") ||
        msg.includes("Uvicorn running on")
      ) {
        resolve(undefined);
      }
    });

    // Fallback
    setTimeout(resolve, 5000);
  });
}

export function stopTestServer() {
  if (pythonServer) {
    pythonServer.kill();
  }
}
