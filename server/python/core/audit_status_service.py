import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("sefin_audit_python")

def atualizar_status_pipeline(
    dir_analises: Path,
    status: str,
    message: str,
    etapas: list[dict] = None,
    erros: list[str] = None,
):
    status_file = dir_analises / "status_pipeline.json"

    current_data = {
        "status": "agendada",
        "message": "Auditoria agendada em segundo plano.",
        "etapas": [
            {"etapa": "Extração de Dados", "status": "pendente"},
            {"etapa": "Cruzamentos e Análises", "status": "pendente"},
            {"etapa": "Análise de Produtos", "status": "pendente"},
            {"etapa": "Geração de Relatórios", "status": "pendente"},
        ],
        "erros": [],
    }

    if status_file.exists():
        try:
            with open(status_file, "r") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    current_data.update(loaded)
        except Exception:
            pass

    current_data["status"] = status
    current_data["message"] = message
    if etapas is not None:
        for idx, current_etapa in enumerate(current_data["etapas"]):
            for new_etapa in etapas:
                if current_etapa["etapa"] == new_etapa["etapa"]:
                    current_data["etapas"][idx].update(new_etapa)

    if erros is not None:
        current_data["erros"] = erros

    current_data["updated_at"] = datetime.now().isoformat()

    try:
        with open(status_file, "w") as f:
            json.dump(current_data, f, indent=2)
    except Exception as e:
        logger.error(f"[pipeline] Erro ao atualizar status: {e}")

def obter_status_pipeline(dir_analises: Path) -> dict:
    """Read the current status from json."""
    status_file = dir_analises / "status_pipeline.json"

    job_status = "agendada"
    message = "Auditoria agendada em segundo plano."
    etapas = []
    erros = []

    if status_file.exists():
        try:
            with open(status_file, "r") as f:
                data = json.load(f)
                job_status = data.get("status", job_status)
                message = data.get(
                    "message", data.get("motivo", data.get("detalhes", message))
                )
                etapas = data.get("etapas", [])
                erros = data.get("erros", [])
        except Exception:
            pass

    return {
        "job_status": job_status,
        "message": message,
        "etapas": etapas,
        "erros": erros,
    }
