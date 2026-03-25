from __future__ import annotations

import re
import unicodedata
from typing import Any

STOPWORDS = {
    "A", "AS", "O", "OS", "DE", "DA", "DO", "DAS", "DOS", "COM", "PARA", "POR",
    "E", "EM", "NA", "NO", "NAS", "NOS", "UM", "UMA",
}


def remove_accents(text: str | None) -> str | None:
    if text is None:
        return None
    try:
        normalized = unicodedata.normalize("NFKD", str(text))
        return "".join(ch for ch in normalized if not unicodedata.combining(ch))
    except Exception:
        return text


def normalize_text(text: str | None) -> str:
    if text is None:
        return ""
    text = remove_accents(text) or ""
    text = text.upper()
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    tokens = [token for token in text.split() if token and token not in STOPWORDS]
    return " ".join(tokens)


def natural_sort_key(value: str | None) -> list[Any]:
    if not value:
        return []
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", str(value))]


def display_cell(value: Any) -> str:
    if value is None:
        return ""
    
    # Handle Polars Series or other objects with to_list()
    if hasattr(value, "to_list") and callable(getattr(value, "to_list")):
        try:
            value = value.to_list()
        except Exception:
            pass

    if isinstance(value, (list, tuple)):
        # Join elements, recursively calling display_cell for each
        return ", ".join(display_cell(v) for v in value if v is not None)
    
    if isinstance(value, bool):
        return "true" if value else "false"
    
    return str(value)
