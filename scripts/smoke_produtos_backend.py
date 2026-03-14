from __future__ import annotations

import json
import re
import sys
from pathlib import Path


def _fail(message: str) -> int:
    print(f"[FAIL] {message}")
    return 1


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        return _fail("Uso: python scripts/smoke_produtos_backend.py <cnpj>")

    cnpj = re.sub(r"[^0-9]", "", argv[1])
    if len(cnpj) != 14:
        return _fail("CNPJ invalido.")

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str((repo_root / "server" / "python").resolve()))

    from routers.produto_unid import (
        get_pares_grupos_similares,
        get_runtime_produtos_status,
        get_status_analise_produtos,
        get_vectorizacao_status,
    )

    import asyncio

    async def _run() -> int:
        runtime = await get_runtime_produtos_status(cnpj)
        if not runtime.get("success"):
            return _fail("runtime-status falhou.")
        print("[OK] runtime-status")
        print(json.dumps(runtime["runtime"], ensure_ascii=False, indent=2))

        status = await get_status_analise_produtos(cnpj)
        if not status.get("success"):
            return _fail("status-analise falhou.")
        print("[OK] status-analise")
        print(json.dumps(status.get("resumo", {}), ensure_ascii=False, indent=2))

        lexical = await get_pares_grupos_similares(cnpj, metodo="lexical", forcar_recalculo=False, top_k=8, min_semantic_score=0.32)
        if not lexical.get("success"):
            return _fail(f"pares lexicais falharam: {lexical.get('message') or lexical}")
        print(f"[OK] pares lexicais: {len(lexical.get('data', []))} linhas")

        vector = await get_vectorizacao_status(cnpj)
        if not vector.get("success"):
            return _fail("vectorizacao-status falhou.")
        print("[OK] vectorizacao-status")
        print(json.dumps(vector.get("status", {}), ensure_ascii=False, indent=2))
        return 0

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
