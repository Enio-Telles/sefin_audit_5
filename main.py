from __future__ import annotations

import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
SERVER_PYTHON_DIR = ROOT_DIR / "server" / "python"
DEFAULT_NODE_PORT = 3000
DEFAULT_PYTHON_PORT = 8001
DEFAULT_CONDA_ENV = "audit"


def info(message: str) -> None:
    print(f"[INFO] {message}")


def warn(message: str) -> None:
    print(f"[WARN] {message}")


def fail(message: str, code: int = 1) -> None:
    print(f"[ERRO] {message}")
    raise SystemExit(code)


def command_exists(command: str) -> bool:
    return shutil.which(command) is not None


def resolve_executable(command: str) -> str:
    return shutil.which(command) or command


def is_port_open(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


def wait_for_port(port: int, host: str = "127.0.0.1", timeout_s: float = 20.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        if is_port_open(port, host):
            return True
        time.sleep(0.5)
    return False


def wait_for_http(url: str, timeout_s: float = 30.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                status = getattr(response, "status", 200)
                if 200 <= status < 500:
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
            pass
        time.sleep(0.5)
    return False


def wait_for_port_closed(port: int, host: str = "127.0.0.1", timeout_s: float = 10.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        if not is_port_open(port, host):
            return True
        time.sleep(0.5)
    return False


def get_pids_on_port(port: int) -> list[int]:
    if os.name == "nt":
        result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, shell=False)
        if result.returncode != 0:
            return []
        pattern = re.compile(rf"^\s*TCP\s+\S+:{port}\s+\S+\s+LISTENING\s+(\d+)\s*$", re.IGNORECASE)
        pids: set[int] = set()
        for line in result.stdout.splitlines():
            match = pattern.match(line)
            if match:
                pids.add(int(match.group(1)))
        return sorted(pids)

    if command_exists("lsof"):
        result = subprocess.run(["lsof", "-ti", f":{port}"], capture_output=True, text=True, shell=False)
        if result.returncode not in {0, 1}:
            return []
        return sorted({int(line.strip()) for line in result.stdout.splitlines() if line.strip().isdigit()})

    return []


def terminate_pids(pids: list[int]) -> bool:
    if not pids:
        return True

    if os.name == "nt":
        args = ["taskkill", "/F"]
        for pid in pids:
            args.extend(["/PID", str(pid)])
        result = subprocess.run(args, capture_output=True, text=True, shell=False)
        return result.returncode == 0

    result = subprocess.run(["kill", "-9", *[str(pid) for pid in pids]], capture_output=True, text=True, shell=False)
    return result.returncode == 0


def ensure_env_file() -> None:
    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        info(".env já existe.")
        return

    env_path.write_text(
        "\n".join(
            [
                "DATABASE_URL=file:./sefin_audit.db",
                f"PYTHON_API_PORT={DEFAULT_PYTHON_PORT}",
                f"PORT={DEFAULT_NODE_PORT}",
                "OAUTH_SERVER_URL=http://localhost:3000/mock-oauth",
                "VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth",
                "VITE_APP_ID=sefin-audit-tool",
                "VITE_ANALYTICS_ENDPOINT=mock-endpoint",
                "VITE_ANALYTICS_WEBSITE_ID=mock-id",
                "",
            ]
        ),
        encoding="utf-8",
    )
    info(".env criado com configuração mínima.")


def check_prerequisites(conda_env: str) -> None:
    if not command_exists("node"):
        fail("Node.js não encontrado no PATH.")
    if not command_exists("pnpm"):
        fail("pnpm não encontrado no PATH.")
    if not command_exists("conda"):
        fail("conda não encontrado no PATH.")

    envs_cmd = ["conda", "env", "list"]
    result = subprocess.run(envs_cmd, capture_output=True, text=True, shell=(os.name == "nt"))
    if result.returncode != 0:
        fail("Não foi possível listar os ambientes Conda.")

    if conda_env not in result.stdout:
        fail(
            f"Ambiente Conda '{conda_env}' não encontrado. "
            f"Crie-o antes de iniciar o sistema."
        )


def build_python_command(conda_env: str, python_port: int) -> str:
    return (
        f"$Host.UI.RawUI.WindowTitle = 'SEFIN Python API'; "
        f"Set-Location -LiteralPath '{SERVER_PYTHON_DIR}'; "
        f"conda run -n {conda_env} --live-stream python -m uvicorn "
        f"api:app --host 0.0.0.0 --port {python_port} --reload --reload-dir . --reload-dir ..\\.."
    )


def build_node_command() -> str:
    return (
        "$Host.UI.RawUI.WindowTitle = 'SEFIN Frontend'; "
        f"Set-Location -LiteralPath '{ROOT_DIR}'; "
        "$env:NODE_ENV='development'; "
        "pnpm dev"
    )


def start_in_new_terminal(title: str, command: str) -> None:
    if os.name == "nt":
        subprocess.Popen(
            ["powershell", "-NoExit", "-Command", command],
            cwd=str(ROOT_DIR),
            shell=False,
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
        return

    subprocess.Popen(["bash", "-lc", command], cwd=str(ROOT_DIR))


def start_inline(command: list[str], cwd: Path) -> subprocess.Popen[str]:
    if os.name == "nt":
        executable = resolve_executable(command[0])
        suffix = Path(executable).suffix.lower()
        if suffix in {".cmd", ".bat"}:
            return subprocess.Popen(
                ["cmd", "/c", executable, *command[1:]],
                cwd=str(cwd),
                text=True,
            )
        return subprocess.Popen([executable, *command[1:]], cwd=str(cwd), text=True)

    return subprocess.Popen(command, cwd=str(cwd), text=True)


def launch_system(conda_env: str, python_port: int, node_port: int, open_browser: bool, inline: bool, restart: bool) -> None:
    ensure_env_file()
    check_prerequisites(conda_env)

    python_running = is_port_open(python_port)
    node_running = is_port_open(node_port)
    python_pids = get_pids_on_port(python_port)
    node_pids = get_pids_on_port(node_port)

    if python_running:
        warn(f"Backend Python j? parece ativo na porta {python_port}." + (f" PID(s): {', '.join(str(pid) for pid in python_pids)}." if python_pids else ""))
    if node_running:
        warn(f"Frontend/Node j? parece ativo na porta {node_port}." + (f" PID(s): {', '.join(str(pid) for pid in node_pids)}." if node_pids else ""))

    if restart and python_running:
        info(f"Reiniciando backend Python na porta {python_port}...")
        if not terminate_pids(python_pids):
            fail(f"N?o foi poss?vel encerrar o backend Python na porta {python_port}.")
        if not wait_for_port_closed(python_port):
            fail(f"A porta {python_port} continuou ocupada ap?s tentar encerrar o backend Python.")
        python_running = False

    if restart and node_running:
        info(f"Reiniciando frontend/Node na porta {node_port}...")
        if not terminate_pids(node_pids):
            fail(f"N?o foi poss?vel encerrar o frontend/Node na porta {node_port}.")
        if not wait_for_port_closed(node_port):
            fail(f"A porta {node_port} continuou ocupada ap?s tentar encerrar o frontend/Node.")
        node_running = False

    if not python_running:
        info("Iniciando backend Python...")
        if inline:
            start_inline(
                [
                    "conda",
                    "run",
                    "-n",
                    conda_env,
                    "--live-stream",
                    "python",
                    "-m",
                    "uvicorn",
                    "api:app",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    str(python_port),
                    "--reload",
                    "--reload-dir",
                    ".",
                    "--reload-dir",
                    "..\\..",
                ],
                SERVER_PYTHON_DIR,
            )
        else:
            start_in_new_terminal("SEFIN Python API", build_python_command(conda_env, python_port))

    if not node_running:
        info("Iniciando frontend + Node...")
        if inline:
            start_inline(["pnpm", "dev"], ROOT_DIR)
        else:
            start_in_new_terminal("SEFIN Frontend", build_node_command())

    if not python_running:
        if wait_for_port(python_port, timeout_s=20):
            info(f"Backend Python disponível em http://localhost:{python_port}")
        else:
            warn(f"Backend Python não respondeu na porta {python_port} dentro do tempo esperado.")

    if not node_running:
        if wait_for_port(node_port, timeout_s=30):
            info(f"Frontend disponível em http://localhost:{node_port}")
        else:
            warn(f"Frontend não respondeu na porta {node_port} dentro do tempo esperado.")

    print()
    print("Sistema iniciado.")
    print(f"- Frontend: http://localhost:{node_port}")
    print(f"- Python API: http://localhost:{python_port}")
    print(f"- Healthcheck Python: http://localhost:{python_port}/health")

    if open_browser:
        webbrowser.open(f"http://localhost:{node_port}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inicializa o sistema completo da SEFIN Audit Tool."
    )
    parser.add_argument("--conda-env", default=DEFAULT_CONDA_ENV, help="Ambiente Conda do backend Python.")
    parser.add_argument("--python-port", type=int, default=DEFAULT_PYTHON_PORT, help="Porta da API Python.")
    parser.add_argument("--node-port", type=int, default=DEFAULT_NODE_PORT, help="Porta do frontend/Node.")
    parser.add_argument("--no-browser", action="store_true", help="Não abrir o navegador automaticamente.")
    parser.add_argument(
        "--inline",
        action="store_true",
        help="Executa os processos no terminal atual em vez de abrir novas janelas.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Encerra processos existentes nas portas configuradas antes de iniciar novamente.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    launch_system(
        conda_env=args.conda_env,
        python_port=args.python_port,
        node_port=args.node_port,
        open_browser=not args.no_browser,
        inline=args.inline,
        restart=args.restart,
    )


if __name__ == "__main__":
    main()
