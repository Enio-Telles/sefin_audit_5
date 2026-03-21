from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


@dataclass
class ArtefatoFatoresConversao:
    processo: str
    subprocesso: str
    nome_tabela: str
    arquivo_saida: Optional[str]
    total_registros: int
    detalhes: Dict[str, Any]


def calcular_fatores_conversao(
    *,
    cnpj: str,
    base_dir: Path,
    entrada: Optional[Dict[str, Any]] = None,
) -> ArtefatoFatoresConversao:
    entrada = entrada or {}
    total_entrada = int(entrada.get("total_registros", 0)) if isinstance(entrada, dict) else 0

    detalhes = {
        "cnpj": cnpj,
        "entrada_total_registros": total_entrada,
        "objetivo": "calcular e rastrear fatores de conversão por agrupamento de produto",
        "saidas_previstas": [
            "fatores_conversao",
            "inconsistencias_fatores",
            "itens_sem_fator",
        ],
    }

    return ArtefatoFatoresConversao(
        processo="produtos",
        subprocesso="fatores_conversao",
        nome_tabela="fatores_conversao_produtos",
        arquivo_saida=None,
        total_registros=total_entrada,
        detalhes=detalhes,
    )


def salvar_manifesto_fatores(
    *,
    artefato: ArtefatoFatoresConversao,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    destino = output_dir / f"manifesto_fatores_conversao_{ts}.json"

    with destino.open("w", encoding="utf-8") as f:
        json.dump(asdict(artefato), f, ensure_ascii=False, indent=2)

    return destino
