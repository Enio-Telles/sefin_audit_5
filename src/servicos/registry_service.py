from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.config import REGISTRY_FILE


@dataclass
class CNPJRecord:
    cnpj: str
    added_at: str
    last_run_at: str | None = None


class RegistryService:
    def __init__(self, registry_file: Path = REGISTRY_FILE) -> None:
        self.registry_file = registry_file
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)

    def _load_raw(self) -> list[dict]:
        if not self.registry_file.exists():
            return []
        return json.loads(self.registry_file.read_text(encoding="utf-8"))

    def _save_raw(self, data: list[dict]) -> None:
        self.registry_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def list_records(self) -> list[CNPJRecord]:
        rows = self._load_raw()
        rows.sort(key=lambda item: item["cnpj"])
        return [CNPJRecord(**row) for row in rows]

    def upsert(self, cnpj: str, ran_now: bool = False) -> CNPJRecord:
        rows = self._load_raw()
        now = datetime.now().isoformat(timespec="seconds")
        existing = next((item for item in rows if item["cnpj"] == cnpj), None)
        if existing is None:
            existing = {"cnpj": cnpj, "added_at": now, "last_run_at": now if ran_now else None}
            rows.append(existing)
        elif ran_now:
            existing["last_run_at"] = now
        self._save_raw(rows)
        return CNPJRecord(**existing)
