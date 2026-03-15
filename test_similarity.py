import difflib
from functools import lru_cache
import re

STOP_WORDS = {"DE", "DA", "DO", "DAS", "DOS", "E", "COM", "SEM", "PARA", "UN", "PCT", "CX", "KG", "LT", "ML", "GR", "PC"}

@lru_cache(maxsize=10000)
def _normalize_similarity_text(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .replace("Á", "A")
        .replace("À", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )

@lru_cache(maxsize=10000)
def _normalize_similarity_tokens(value: str) -> tuple:
    clean_text = re.sub(r"[^A-Z0-9 ]+", " ", _normalize_similarity_text(value))
    return tuple([
        token for token in clean_text.split()
        if len(token) > 1 and token not in STOP_WORDS
    ])

@lru_cache(maxsize=10000)
def _jaccard(a: tuple, b: tuple) -> float:
    set_a, set_b = set(a), set(b)
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return 0.0 if union == 0 else intersection / union

@lru_cache(maxsize=10000)
def _sequence_match(a: str, b: str) -> float:
    # We join tokens to avoid checking the whole original strings with lots of spaces and garbage
    str_a = " ".join(_normalize_similarity_tokens(a))
    str_b = " ".join(_normalize_similarity_tokens(b))
    if not str_a and not str_b:
        return 1.0
    if not str_a or not str_b:
        return 0.0
    return difflib.SequenceMatcher(None, str_a, str_b).ratio()

@lru_cache(maxsize=10000)
def _similarity_score(a: str, b: str) -> float:
    if not a and not b: return 1.0
    if not a or not b: return 0.0

    a_str = str(a)
    b_str = str(b)

    tokens_a = _normalize_similarity_tokens(a_str)
    tokens_b = _normalize_similarity_tokens(b_str)

    token_score = _jaccard(tokens_a, tokens_b)
    sequence_score = _sequence_match(a_str, b_str)

    return 0.4 * token_score + 0.6 * sequence_score

print(_similarity_score("CERVEJA BRAHMA LATA 350ML", "CERVEJA BRAHMA LT 350ML"))
print(_similarity_score("CERVEJA SKOL LATA 350ML", "CERVEJA BRAHMA LT 350ML"))
print(_similarity_score("COCA COLA 2L", "REFRIGERANTE COCA COLA 2 LITROS"))
