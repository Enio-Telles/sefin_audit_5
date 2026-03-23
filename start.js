import { spawn, execSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const ROOT_DIR = path.dirname(__filename);
const PYTHON_DIR = path.join(ROOT_DIR, "server", "python");

function info(msg) {
  console.log(`\x1b[36m[INFO]\x1b[0m ${msg}`);
}

function warn(msg) {
  console.warn(`\x1b[33m[WARN]\x1b[0m ${msg}`);
}

function err(msg) {
  console.error(`\x1b[31m[ERROR]\x1b[0m ${msg}`);
}

function runCommandSync(cmd, cwd = ROOT_DIR) {
  try {
    return execSync(cmd, { cwd, stdio: "pipe" }).toString().trim();
  } catch (error) {
    return null;
  }
}

function checkPrerequisites() {
  info("Checking prerequisites...");

  const nodeVersion = runCommandSync("node -v");
  if (!nodeVersion) {
    err("Node.js is not installed or not in PATH.");
    process.exit(1);
  }
  info(`Found Node.js: ${nodeVersion}`);

  let pythonCmd = "python";
  let pythonVersion = runCommandSync(`${pythonCmd} --version`);

  if (!pythonVersion) {
    pythonCmd = "python3";
    pythonVersion = runCommandSync(`${pythonCmd} --version`);
    if (!pythonVersion) {
      err("Python is not installed or not in PATH.");
      process.exit(1);
    }
  }
  info(`Found Python: ${pythonVersion}`);

  const pnpmVersion = runCommandSync("pnpm -v");
  if (!pnpmVersion) {
    err("pnpm is not installed. Please install it globally: npm install -g pnpm");
    process.exit(1);
  }
  info(`Found pnpm: ${pnpmVersion}`);

  return pythonCmd;
}

function ensureEnvFile() {
  const envPath = path.join(ROOT_DIR, ".env");
  if (fs.existsSync(envPath)) {
    info(".env file already exists.");
  } else {
    info("Creating default .env file...");
    const defaultEnv = `DATABASE_URL=file:./sefin_audit.db
PYTHON_API_PORT=8001
PORT=3000
OAUTH_SERVER_URL=http://localhost:3000/mock-oauth
VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth
VITE_APP_ID=sefin-audit-tool
VITE_ANALYTICS_ENDPOINT=mock-endpoint
VITE_ANALYTICS_WEBSITE_ID=mock-id
JWT_SECRET=local_dev_secret_12345678
`;
    fs.writeFileSync(envPath, defaultEnv, "utf-8");
    info(".env file created with default configurations.");
  }
}

function installDependencies(pythonCmd) {
  info("Installing Node dependencies...");
  try {
    execSync("pnpm install", { stdio: "inherit", cwd: ROOT_DIR });
  } catch (error) {
    err("Failed to install Node dependencies.");
    process.exit(1);
  }

  info("Installing Python dependencies...");
  try {
    execSync(`${pythonCmd} -m pip install -r requirements.txt`, { stdio: "inherit", cwd: ROOT_DIR });
  } catch (error) {
    err("Failed to install Python dependencies.");
    process.exit(1);
  }
}

function initializeDatabase() {
  info("Initializing database...");
  try {
    execSync("npx tsx init_db.ts", { stdio: "inherit", cwd: ROOT_DIR });
  } catch (error) {
    err("Failed to initialize database.");
    process.exit(1);
  }
}

function startServers(pythonCmd) {
  info("Starting servers...");

  const pythonServer = spawn(pythonCmd, ["-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8001"], {
    cwd: PYTHON_DIR,
    stdio: "pipe",
    env: process.env
  });

  pythonServer.stdout.on("data", data => process.stdout.write(`\x1b[32m[Python API]\x1b[0m ${data}`));
  pythonServer.stderr.on("data", data => process.stderr.write(`\x1b[31m[Python API ERR]\x1b[0m ${data}`));
  pythonServer.on("error", (e) => err(`Python Spawn Error: ${e.message}`));

  const nodeServer = spawn("pnpm", ["dev"], {
    cwd: ROOT_DIR,
    stdio: "pipe",
    env: process.env
  });

  nodeServer.stdout.on("data", data => process.stdout.write(`\x1b[34m[Node App]\x1b[0m ${data}`));
  nodeServer.stderr.on("data", data => process.stderr.write(`\x1b[31m[Node App ERR]\x1b[0m ${data}`));
  nodeServer.on("error", (e) => err(`Node Spawn Error: ${e.message}`));

  // Optionally ensure frontend build for non-dev setup
  // info("Building frontend...");
  // try {
  //   execSync("npx vite build", { stdio: "inherit", cwd: path.join(ROOT_DIR, "client") });
  //   // move dist/public to server/_core/public...
  // } catch(e) {}

  const cleanup = () => {
    info("Shutting down servers...");
    pythonServer.kill();
    nodeServer.kill();
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
}

function main() {
  const pythonCmd = checkPrerequisites();
  installDependencies(pythonCmd);
  ensureEnvFile();
  initializeDatabase();
  startServers(pythonCmd);
}

process.on('uncaughtException', (e) => err(`Global Err: ${e.message}`));
main();
