import os
import sys
import shutil
import socket
import subprocess
from typing import List, Optional


def check_command_exists(cmd: str) -> bool:
    """Return True if command exists and responds to --version, else False."""
    try:
        subprocess.run([cmd, "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, shell=(os.name == 'nt'))
        return True
    except Exception:
        return False


def is_port_free(port: int, host: str = "127.0.0.1") -> bool:
    """Check if TCP port is available on host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        result = s.connect_ex((host, port))
        return result != 0


def run(cmd: List[str], friendly: str, check: bool = True, use_shell: Optional[bool] = None) -> int:
    """Run a subprocess with friendly logs. Returns exit code. Raises only if check=True and non-zero.

    This wrapper prints clear start/end messages and avoids throwing opaque stack traces.
    """
    if use_shell is None:
        use_shell = os.name == 'nt'
    print(f"\n> {friendly}: {' '.join(cmd)}")
    try:
        res = subprocess.run(cmd, text=True, shell=use_shell)
        if res.returncode == 0:
            print(f"[v] {friendly} concluído.")
        if res.returncode != 0:
            if check:
                # Provide a hint rather than raising raw exception
                print("[i] Non-zero output. Check your environment/network.")
        return res.returncode
    except FileNotFoundError:
        print(f"[x] {friendly} failed: command not found ({cmd[0]}).")
        return 127
    except subprocess.CalledProcessError as e:
        print(f"[x] {friendly} failed: {e}")
        return e.returncode
    except Exception as e:
        print(f"[x] {friendly} failed (unexpected error): {e}")
        return 1


def ensure_env_file():
    """Create a minimal .env if it doesn't exist."""
    if not os.path.exists(".env"):
        try:
            with open(".env", "w", encoding="utf-8") as f:
                f.write("DATABASE_URL=file:./sefin_audit.db\n")
                f.write("PYTHON_API_PORT=8001\n")
                f.write("PORT=3000\n")
                f.write("OAUTH_SERVER_URL=http://localhost:3000/mock-oauth\n")
                f.write("VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth\n")
                f.write("VITE_APP_ID=sefin-audit-tool\n")
                f.write("VITE_ANALYTICS_ENDPOINT=mock-endpoint\n")
                f.write("VITE_ANALYTICS_WEBSITE_ID=mock-id\n")
            print("[v] .env created.")
        except PermissionError:
            print("[x] Permission denied creating .env. Run terminal as Administrator.")
        except Exception as e:
            print(f"[x] Failed to create .env: {e}")
    else:
        print("[v] .env already exists.")


def ensure_conda_env(env_name: str = "audit") -> bool:
    """Ensure conda env exists. Create if missing. Return True if ready, else False."""
    use_shell = os.name == 'nt'
    try:
        envs_out = subprocess.run(["conda", "env", "list"], capture_output=True, text=True, shell=use_shell, check=True)
        envs = [line.split()[0] for line in envs_out.stdout.splitlines() if line and not line.startswith('#')]
    except subprocess.CalledProcessError as e:
        print("[x] Failed to query Conda environments.")
        print(e.stdout or e.stderr)
        return False

    if env_name not in envs:
        code = run(["conda", "create", "-n", env_name, "python=3.11", "-y"], f"Criar ambiente conda '{env_name}'", check=False)
        if code != 0:
            print("[!] Check conda channels/mirrors. Try: conda clean -a")
            return False
    else:
        print(f"[v] Conda environment '{env_name}' found.")

    # Sanity check python inside env
    code = run(["conda", "run", "-n", env_name, "python", "-c", "import sys; print(sys.version)"],
               f"Sanity check Python no ambiente '{env_name}'", check=False)
    if code != 0:
        print(f"[x] Environment '{env_name}' seems corrupted. Try removing and recreating: conda remove -n {env_name} --all")
        return False

    return True


def install_python_libs(env_name: str = "audit") -> bool:
    """Install required Python libs inside the conda env. Return True on success."""
    use_shell = os.name == 'nt'
    # Upgrade pip
    code = run(["conda", "run", "-n", env_name, "python", "-m", "pip", "install", "--upgrade", "pip"], "Atualizar pip", check=False)
    if code != 0:
        return False

    libs = [
        "polars",
        "pandas",
        "oracledb",
        "fastapi",
        "uvicorn",
        "python-docx",
        "openpyxl",
        "xlsxwriter",
        "pyarrow",
        "python-multipart",
        "keyring",
        "python-dotenv",
    ]

    code = run(["conda", "run", "-n", env_name, "python", "-m", "pip", "install"] + libs, "Instalar bibliotecas Python", check=False)
    if code != 0:
        print("[!] If 'oracledb' fails, install Oracle Instant Client and adjust PATH.")
        # Don't return False immediately; other libs might be OK.
    else:
        print("[v] Python libraries installed.")

    return True


def ensure_node_pnpm() -> bool:
    """Ensure Node and PNPM available. Install PNPM globally via npm if missing. Return True when ready."""
    if not check_command_exists("node"):
        print("❌ Node.js não encontrado. Instale Node 18+ e adicione ao PATH.")
        return False

    if not check_command_exists("pnpm"):
        print("ℹ️  Instalando pnpm globalmente via npm...")
        code = run(["npm", "install", "-g", "pnpm"], "Instalar pnpm (global)", check=False)
        if code != 0:
            print("❌ Falha ao instalar pnpm globalmente. Instale manualmente ou verifique seu npm/PATH.")
            return False

    code = run(["pnpm", "install"], "Instalar dependências Node", check=False)
    if code != 0:
        print("⚠️  Dicas: 'pnpm install --force' ou 'pnpm store prune' e rode novamente.")
        return False

    print("[v] Node dependencies installed.")
    return True


def start_servers(env_name: str = "audit") -> None:
    """Start FastAPI and Node dev server, handling Windows/non-Windows separately."""
    # Basic port checks for friendly warnings
    python_port = int(os.getenv("PYTHON_API_PORT", "8001"))
    node_port = int(os.getenv("PORT", "3000"))

    if not is_port_free(python_port):
        print(f"[!] Port {python_port} already in use.")
    if not is_port_free(node_port):
        print(f"[!] Port {node_port} already in use.")

    if os.name == 'nt':
        print("Abrindo o servidor Python (FastAPI) em uma nova janela...")
        os.system(
            'start cmd /k "cd server\\python && conda run -n '
            + env_name
            + ' --live-stream python -m uvicorn api:app --host 0.0.0.0 --port '
            + str(python_port)
            + ' --reload --reload-dir . --reload-dir ..\\.."'
        )

        print("Abrindo o servidor Node (Vite + Express) em uma nova janela...")
        # Importante: sem espaços após o valor no set var
        os.system('start cmd /k "set NODE_ENV=development&& pnpm dev"')
    else:
        # Linux/Mac
        print("Abrindo o servidor Python (FastAPI)...")
        subprocess.Popen(
            [
                "bash",
                "-c",
                f"conda run -n {env_name} uvicorn api:app --host 0.0.0.0 --port {python_port} --reload --reload-dir . --reload-dir ../..",
            ],
            cwd="server/python",
        )

        print("Abrindo o servidor Node (Vite + Express)...")
        subprocess.Popen(["bash", "-c", "pnpm dev"])  # PORT é resolvido do .env pelo Node

    print("\n🚀 Aplicação iniciada! Acesse a interface gráfica em: http://localhost:3000")
    print("ℹ️  Healthcheck Python: http://localhost:8001/api/python/health")


def main():
    print("Starting audit environment configuration...")

    # Pré-checagens de ferramentas
    if not check_command_exists("conda"):
        print("[-] 'conda' not found. Install Miniconda/Anaconda and run 'conda init' in your shell.")
        sys.exit(1)

    # Detecta se já estamos no ambiente 'audit'
    current_env = os.environ.get("CONDA_DEFAULT_ENV")

    # Na primeira execução, garanta que o env existe e está funcional
    if not ensure_conda_env("audit"):
        sys.exit(1)

    # Se não estamos dentro do env, re-invoca dentro dele para executar o restante (instalações etc.)
    if current_env != "audit":
        print("\n[>] Re-executing this script inside the 'audit' Conda environment...")
        cmd = ["conda", "run", "--no-capture-output", "-n", "audit", "python", sys.argv[0]] + sys.argv[1:]
        sys.exit(subprocess.run(cmd, shell=(os.name == 'nt')).returncode)
    else:
        print("\n[v] Environment 'audit' is already active!")

    # Instala bibliotecas Python
    if not install_python_libs("audit"):
        print("[-] Failed to install Python libraries in the environment. Check the logs above.")
        # Prosseguir pode ser aceitável (ex.: se oracledb falhou). Não encerramos aqui.

    # Node/pnpm
    if not ensure_node_pnpm():
        print("[-] Failed to prepare Node/PNPM environment. Check Node, npm, and pnpm.")
        sys.exit(1)

    # .env
    ensure_env_file()

    # Subir servidores
    start_servers("audit")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
            print(f"[-] Command not found: {e}")
    except subprocess.CalledProcessError as e:
            print(f"[-] Subprocess error: {e}")
    except Exception as e:
            print(f"[-] Unexpected error: {e}")
