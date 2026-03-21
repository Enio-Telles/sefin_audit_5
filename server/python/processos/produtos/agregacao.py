from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


@dataclass
class ArtefatoAgregacao:
    processo: str
    subprocesso: str
    nome_tabela: str
    arquivo_saida: Optional[str]
    total_registros: int
    detalhes: Dict[str, Any]


def agregar_produtos(
    *,
    cnpj: str,
    base_dir: Path,
    entrada: Optional[Dict[str, Any]] = None,
) -> ArtefatoAgregacao:
    entrada = entrada or {}

    total_entrada = int(entrada.get("total_registros", 0)) if isinstance(entrada, dict) else 0

    detalhes = {
        "cnpj": cnpj,
        "entrada_total_registros": total_entrada,
        "objetivo": "agregar produtos equivalentes e gerar chaves consolidadas",
        "campos_referencia": [
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unid",
        ],
    }

    return ArtefatoAgregacao(
        processo="produtos",
        subprocesso="agregacao",
        nome_tabela="produtos_agregados",
        arquivo_saida=None,
        total_registros=total_entrada,
        detalhes=detalhes,
    )


def salvar_manifesto_agregacao(
    *,
    artefato: ArtefatoAgregacao,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    destino = output_dir / f"manifesto_agregacao_produtos_{ts}.json"

    with destino.open("w", encoding="utf-8") as f:
        json.dump(asdict(artefato), f, ensure_ascii=False, indent=2)

    return destino
