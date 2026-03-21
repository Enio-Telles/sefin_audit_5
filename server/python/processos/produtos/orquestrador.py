from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time

from .separacao import separar_produtos_base, salvar_manifesto_separacao
from .agregacao import agregar_produtos, salvar_manifesto_agregacao
from .fatores_conversao import calcular_fatores_conversao, salvar_manifesto_fatores


def executar_processo_produtos(
    *,
    cnpj: str,
    base_dir: str | Path,
    fontes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base_path = Path(base_dir)
    output_dir = base_path / "outputs" / "produtos"
    output_dir.mkdir(parents=True, exist_ok=True)

    artefato_sep = separar_produtos_base(
        cnpj=cnpj,
        base_dir=base_path,
        fontes=fontes,
    )
    manifesto_sep = salvar_manifesto_separacao(
        artefato=artefato_sep,
        output_dir=output_dir,
    )

    artefato_agg = agregar_produtos(
        cnpj=cnpj,
        base_dir=base_path,
        entrada=asdict(artefato_sep),
    )
    manifesto_agg = salvar_manifesto_agregacao(
        artefato=artefato_agg,
        output_dir=output_dir,
    )

    artefato_fat = calcular_fatores_conversao(
        cnpj=cnpj,
        base_dir=base_path,
        entrada=asdict(artefato_agg),
    )
    manifesto_fat = salvar_manifesto_fatores(
        artefato=artefato_fat,
        output_dir=output_dir,
    )

    manifesto_final = {
        "processo": "produtos",
        "cnpj": cnpj,
        "subprocessos": [
            {
                "nome": "separacao",
                "manifesto": str(manifesto_sep),
                "saida": asdict(artefato_sep),
            },
            {
                "nome": "agregacao",
                "manifesto": str(manifesto_agg),
                "saida": asdict(artefato_agg),
            },
            {
                "nome": "fatores_conversao",
                "manifesto": str(manifesto_fat),
                "saida": asdict(artefato_fat),
            },
        ],
    }

    ts = int(time.time())
    arquivo_final = output_dir / f"manifesto_modularidade_produtos_{cnpj}_{ts}.json"
    with arquivo_final.open("w", encoding="utf-8") as f:
        json.dump(manifesto_final, f, ensure_ascii=False, indent=2)

    return {
        "ok": True,
        "processo": "produtos",
        "cnpj": cnpj,
        "arquivo_manifesto_final": str(arquivo_final),
        "etapas": manifesto_final["subprocessos"],
    }
