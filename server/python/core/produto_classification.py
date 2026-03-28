from __future__ import annotations

from collections import Counter
from difflib import SequenceMatcher
import re
import unicodedata
from typing import Any


_RE_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_RE_NON_ALNUM_NO_SPACE = re.compile(r"[^A-Z0-9]+")
_RE_DIGITS = re.compile(r"[^0-9]")

DESCRIPTION_STOPWORDS = {
    "A",
    "AS",
    "O",
    "OS",
    "DE",
    "DA",
    "DO",
    "DAS",
    "DOS",
    "E",
    "EM",
    "COM",
    "SEM",
    "PARA",
    "POR",
}

UNIT_SYNONYMS = {
    "UND": "UN",
    "UNID": "UN",
    "UNIDADE": "UN",
    "UNIDADES": "UN",
    "PECA": "PC",
    "PECAS": "PC",
    "PCA": "PC",
    "PCT": "PCT",
    "PAC": "PCT",
    "PACOTE": "PCT",
    "PACOTES": "PCT",
    "QUILO": "KG",
    "QUILOGRAMA": "KG",
    "QUILOGRAMAS": "KG",
    "LITRO": "LT",
    "LITROS": "LT",
}

VALID_GTIN_LENGTHS = {8, 12, 13, 14}
NULLABLE_EQUAL_FILLED = "EQUAL_FILLED"
NULLABLE_EQUAL_NULL = "EQUAL_NULL"
NULLABLE_CONFLICT = "CONFLICT"
NULLABLE_INCOMPLETE = "INCOMPLETE"


def _ascii_upper(value: Any) -> str:
    text = str(value or "").strip().upper()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_description_key(value: Any) -> str:
    clean = _RE_NON_ALNUM.sub(" ", _ascii_upper(value))
    tokens = [
        token for token in clean.split() if token and token not in DESCRIPTION_STOPWORDS
    ]
    return " ".join(tokens)


def normalize_unit(value: Any) -> str:
    token = _RE_NON_ALNUM_NO_SPACE.sub("", _ascii_upper(value))
    if not token:
        return ""
    return UNIT_SYNONYMS.get(token, token)


def clean_ncm(value: Any) -> str:
    digits = _RE_DIGITS.sub("", str(value or ""))
    return digits if len(digits) == 8 else ""


def clean_cest(value: Any) -> str:
    digits = _RE_DIGITS.sub("", str(value or ""))
    return digits if len(digits) == 7 else ""


def clean_gtin(value: Any) -> str:
    digits = _RE_DIGITS.sub("", str(value or ""))
    return digits if len(digits) in VALID_GTIN_LENGTHS else ""


def metric_score(left: Any, right: Any) -> float:
    a = str(left or "").strip()
    b = str(right or "").strip()
    if a and b:
        return 1.0 if a == b else 0.0
    return 0.5


def compare_nullable_metric(left: Any, right: Any) -> str:
    a = str(left or "").strip()
    b = str(right or "").strip()
    if a and b:
        return NULLABLE_EQUAL_FILLED if a == b else NULLABLE_CONFLICT
    if not a and not b:
        return NULLABLE_EQUAL_NULL
    return NULLABLE_INCOMPLETE


def is_equal_nullable_metric(state: str) -> bool:
    return state in {NULLABLE_EQUAL_FILLED, NULLABLE_EQUAL_NULL}


def is_conflict_metric(state: str) -> bool:
    return state == NULLABLE_CONFLICT


def filled_evidence_count_from_relations(*states: str) -> int:
    # ⚡ Bolt Optimization: Replace slow generator expression with fast native tuple count.
    return states.count(NULLABLE_EQUAL_FILLED)


def description_similarity(left: Any, right: Any) -> float:
    a = normalize_description_key(left)
    b = normalize_description_key(right)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    set_a = set(a.split())
    set_b = set(b.split())
    jaccard = len(set_a & set_b) / max(len(set_a | set_b), 1)
    sequence = SequenceMatcher(None, a, b).ratio()
    return round((0.65 * jaccard) + (0.35 * sequence), 6)


def choose_consensus(values: list[str]) -> str:
    filled = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not filled:
        return ""
    counts = Counter(filled)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def choose_standard_code(rows: list[dict[str, Any]]) -> str:
    candidates: dict[str, dict[str, Any]] = {}
    for row in rows:
        codigo = str(row.get("codigo") or "").strip()
        if not codigo:
            continue
        current = candidates.setdefault(
            codigo,
            {
                "freq": 0,
                "integridade": 0,
                "ultima_data": "",
            },
        )
        current["freq"] += 1
        current["integridade"] += (
            (8 if str(row.get("tipo_item") or "").strip() else 0)
            + (4 if str(row.get("gtin") or "").strip() else 0)
            + (2 if str(row.get("ncm") or "").strip() else 0)
            + (1 if str(row.get("cest") or "").strip() else 0)
        )
        data_mov = str(
            row.get("data_movimento")
            or row.get("data_emissao")
            or row.get("data_doc")
            or ""
        ).strip()
        if data_mov and data_mov > str(current["ultima_data"]):
            current["ultima_data"] = data_mov

    if not candidates:
        return ""

    def sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, int, str, int, str]:
        codigo, meta = item
        digits = re.sub(r"[^0-9]", "", codigo)
        numeric = int(digits) if digits else 10**18
        return (
            -int(meta["freq"]),
            -int(meta["integridade"]),
            str(meta["ultima_data"]),
            numeric,
            codigo,
        )

    return sorted(candidates.items(), key=sort_key)[0][0]


def classify_group_pair(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    desc_score = description_similarity(
        left.get("descricao_normalizada"), right.get("descricao_normalizada")
    )
    ncm_score = metric_score(left.get("ncm"), right.get("ncm"))
    cest_score = metric_score(left.get("cest"), right.get("cest"))
    gtin_score = metric_score(left.get("gtin"), right.get("gtin"))

    shared_codes = sorted(
        set(left.get("codigos") or []) & set(right.get("codigos") or [])
    )
    ncm_equal = ncm_score == 1.0
    cest_equal = cest_score == 1.0
    gtin_equal = (
        gtin_score == 1.0 and bool(left.get("gtin")) and bool(right.get("gtin"))
    )
    # ⚡ Bolt Optimization: Replacing slow generator expression with fast direct boolean summation.
    ncm_l = str(left.get("ncm") or "").strip()
    ncm_r = str(right.get("ncm") or "").strip()
    cest_l = str(left.get("cest") or "").strip()
    cest_r = str(right.get("cest") or "").strip()
    gtin_l = str(left.get("gtin") or "").strip()
    gtin_r = str(right.get("gtin") or "").strip()

    fiscal_conflict = (
        bool(ncm_l and ncm_r and ncm_l != ncm_r)
        + bool(cest_l and cest_r and cest_l != cest_r)
        + bool(gtin_l and gtin_r and gtin_l != gtin_r)
    )

    recommendation = "REVISAR"
    reason = "Fluxo documental exige analise do usuario."
    auto_join = False
    block_join = False

    if shared_codes:
        recommendation = "SEPARAR_SUGERIDO"
        reason = (
            f"Codigo reutilizado em descricoes diferentes: {', '.join(shared_codes)}."
        )
    elif (
        gtin_equal
        and ncm_equal
        and (cest_equal or not left.get("cest") or not right.get("cest"))
    ):
        recommendation = "UNIR_AUTOMATICO_ELEGIVEL"
        reason = "GTIN valido coincide com NCM/CEST compativeis."
        auto_join = True
    elif ncm_equal and cest_equal and desc_score >= 0.55:
        recommendation = "UNIR_SUGERIDO"
        reason = "Descricao normalizada semelhante com NCM e CEST identicos."
    elif fiscal_conflict >= 2:
        recommendation = "BLOQUEAR_UNIAO"
        reason = "Divergencia fiscal forte entre os grupos."
        block_join = True
    elif desc_score >= 0.72 and ncm_equal:
        recommendation = "REVISAR"
        reason = "Texto semelhante com NCM igual, mas sem evidencia fiscal suficiente para unir automaticamente."

    if auto_join:
        final_score = 0.99
    elif recommendation == "UNIR_SUGERIDO":
        final_score = 0.8
    elif recommendation == "SEPARAR_SUGERIDO":
        final_score = 0.2
    elif recommendation == "BLOQUEAR_UNIAO":
        final_score = 0.05
    else:
        final_score = round((desc_score + ncm_score + cest_score + gtin_score) / 4, 6)

    return {
        "score_descricao": desc_score,
        "score_ncm": ncm_score,
        "score_cest": cest_score,
        "score_gtin": gtin_score,
        "score_final": final_score,
        "recomendacao": recommendation,
        "motivo_recomendacao": reason,
        "uniao_automatica_elegivel": auto_join,
        "bloquear_uniao": block_join,
    }
