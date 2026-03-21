from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


@dataclass
class ArtefatoRessarcimento:
    processo: str
    subprocesso: str
    nome_tabela: str
    arquivo_saida: Optional[str]
    total_registros: int
    detalhes: Dict[str, Any]


def verificar_ressarcimento(
    *,
    cnpj: str,
    base_dir: str | Path,
    parametros: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    parametros = parametros or {}
    base_path = Path(base_dir)
    output_dir = base_path / "outputs" / "ressarcimento"
    output_dir.mkdir(parents=True, exist_ok=True)

    artefato = ArtefatoRessarcimento(
        processo="ressarcimento",
        subprocesso="verificacao",
        nome_tabela="verificacao_ressarcimento",
        arquivo_saida=None,
        total_registros=0,
        detalhes={
            "cnpj": cnpj,
            "objetivo": "verificar regras e indícios de ressarcimento em processo dedicado",
            "parametros_recebidos": parametros,
            "status": "estrutura pronta para receber regras do domínio",
        },
    )

    ts = int(time.time())
    destino = output_dir / f"manifesto_ressarcimento_{cnpj}_{ts}.json"
    with destino.open("w", encoding="utf-8") as f:
        json.dump(asdict(artefato), f, ensure_ascii=False, indent=2)

    return {
        "ok": True,
        "processo": "ressarcimento",
        "subprocesso": "verificacao",
        "cnpj": cnpj,
        "arquivo_manifesto": str(destino),
        "mensagem": "Estrutura de ressarcimento criada com separação explícita de domínio.",
    }
