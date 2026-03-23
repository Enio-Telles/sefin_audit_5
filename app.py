import subprocess
import sys
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
PYTHON_DIR = ROOT_DIR / "server" / "python"

def info(msg: str):
    print(f"\033[36m[INFO]\033[0m {msg}", flush=True)

def warn(msg: str):
    print(f"\033[33m[WARN]\033[0m {msg}", flush=True)

def err(msg: str):
    print(f"\033[31m[ERROR]\033[0m {msg}", flush=True)

def run_command_sync(cmd: str, cwd=ROOT_DIR) -> str | None:
    try:
        result = subprocess.run(
            cmd, shell=True, cwd=cwd, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def check_prerequisites() -> str:
    info("Checking prerequisites...")

    node_version = run_command_sync("node -v")
    if not node_version:
        err("Node.js is not installed or not in PATH.")
        sys.exit(1)
    info(f"Found Node.js: {node_version}")

    python_cmd = "python"
    python_version = run_command_sync(f"{python_cmd} --version")

    if not python_version:
        python_cmd = "python3"
        python_version = run_command_sync(f"{python_cmd} --version")
        if not python_version:
            err("Python is not installed or not in PATH.")
            sys.exit(1)
    info(f"Found Python: {python_version}")

    pnpm_version = run_command_sync("pnpm -v")
    if not pnpm_version:
        err("pnpm is not installed. Please install it globally: npm install -g pnpm")
        sys.exit(1)
    info(f"Found pnpm: {pnpm_version}")

    return python_cmd

def ensure_env_file():
    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        info(".env file already exists.")
    else:
        info("Creating default .env file...")
        default_env = """DATABASE_URL=file:./sefin_audit.db
PYTHON_API_PORT=8001
PORT=3000
OAUTH_SERVER_URL=http://localhost:3000/mock-oauth
VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth
VITE_APP_ID=sefin-audit-tool
VITE_ANALYTICS_ENDPOINT=mock-endpoint
VITE_ANALYTICS_WEBSITE_ID=mock-id
"""
        env_path.write_text(default_env, encoding="utf-8")
        info(".env file created with default configurations.")

def install_dependencies(python_cmd: str):
    info("Installing Node dependencies...")
    try:
        subprocess.run("pnpm install", shell=True, cwd=ROOT_DIR, check=True)
    except subprocess.CalledProcessError:
        err("Failed to install Node dependencies.")
        sys.exit(1)

    info("Installing Python dependencies...")
    try:
        subprocess.run(f"{python_cmd} -m pip install -r requirements.txt", shell=True, cwd=ROOT_DIR, check=True)
    except subprocess.CalledProcessError:
        err("Failed to install Python dependencies.")
        sys.exit(1)

def initialize_database():
    info("Initializing database...")
    try:
        subprocess.run("npx tsx init_db.ts", shell=True, cwd=ROOT_DIR, check=True)
    except subprocess.CalledProcessError:
        err("Failed to initialize database.")
        sys.exit(1)

def start_servers(python_cmd: str):
    import threading
    import signal

    info("Starting servers...")

    env = os.environ.copy()

    python_server = subprocess.Popen(
        [python_cmd, "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8001"],
        cwd=PYTHON_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1
    )

    node_server = subprocess.Popen(
        ["pnpm", "dev"],
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        bufsize=1
    )

    def print_stream(stream, prefix):
        for line in iter(stream.readline, ''):
            if line:
                sys.stdout.write(f"{prefix} {line}")
                sys.stdout.flush()

    threads = [
        threading.Thread(target=print_stream, args=(python_server.stdout, "\033[32m[Python API]\033[0m"), daemon=True),
        threading.Thread(target=print_stream, args=(python_server.stderr, "\033[31m[Python API ERR]\033[0m"), daemon=True),
        threading.Thread(target=print_stream, args=(node_server.stdout, "\033[34m[Node App]\033[0m"), daemon=True),
        threading.Thread(target=print_stream, args=(node_server.stderr, "\033[31m[Node App ERR]\033[0m"), daemon=True)
    ]

    for t in threads:
        t.start()

    def cleanup(signum, frame):
        info("Shutting down servers...")
        python_server.terminate()
        node_server.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        python_server.wait()
        node_server.wait()
    except KeyboardInterrupt:
        cleanup(None, None)

def main():
    try:
        python_cmd = check_prerequisites()
        install_dependencies(python_cmd)
        ensure_env_file()
        initialize_database()
        start_servers(python_cmd)
    except Exception as e:
        err(f"Global Err: {e}")

if __name__ == "__main__":
    main()
