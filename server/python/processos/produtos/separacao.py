from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


@dataclass
class ArtefatoSeparacao:
    processo: str
    subprocesso: str
    nome_tabela: str
    arquivo_saida: Optional[str]
    total_registros: int
    detalhes: Dict[str, Any]


def _safe_len(obj: Any) -> int:
    try:
        return len(obj)
    except Exception:
        return 0


def separar_produtos_base(
    *,
    cnpj: str,
    base_dir: Path,
    fontes: Optional[Dict[str, Any]] = None,
) -> ArtefatoSeparacao:
    fontes = fontes or {}

    detalhes = {
        "cnpj": cnpj,
        "fontes_recebidas": list(fontes.keys()),
        "qtd_nfe": _safe_len(fontes.get("nfe")),
        "qtd_nfce": _safe_len(fontes.get("nfce")),
        "qtd_c170": _safe_len(fontes.get("c170")),
        "qtd_bloco_h": _safe_len(fontes.get("bloco_h")),
        "objetivo": "padronizar e separar produtos base por fonte",
    }

    return ArtefatoSeparacao(
        processo="produtos",
        subprocesso="separacao",
        nome_tabela="produtos_base_separados",
        arquivo_saida=None,
        total_registros=sum([
            detalhes["qtd_nfe"],
            detalhes["qtd_nfce"],
            detalhes["qtd_c170"],
            detalhes["qtd_bloco_h"],
        ]),
        detalhes=detalhes,
    )


def salvar_manifesto_separacao(
    *,
    artefato: ArtefatoSeparacao,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    destino = output_dir / f"manifesto_separacao_produtos_{ts}.json"

    with destino.open("w", encoding="utf-8") as f:
        json.dump(asdict(artefato), f, ensure_ascii=False, indent=2)

    return destino
