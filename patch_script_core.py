import re

file = "server/python/core/produto_runtime.py"
with open(file, "r", encoding="utf-8") as f:
    content = f.read()

old_code_pattern = r"def _normalize_similarity_text.*?def _normalize_similarity_tokens.*?def _build_char_ngrams.*?def _jaccard.*?def _similarity_score.*?return 0\.6 \* token_score \+ 0\.4 \* ngram_score"
match = re.search(old_code_pattern, content, re.DOTALL)

if match:
    old_code = match.group(0)

    patch_code = """import difflib
from functools import lru_cache

_STOP_WORDS = {"DE", "DA", "DO", "DAS", "DOS", "E", "COM", "SEM", "PARA", "UN", "PCT", "CX", "KG", "LT", "ML", "GR", "PC", "LATA", "LITRO", "LITROS", "GARRAFA"}

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
def _normalize_similarity_tokens(value: str) -> tuple[str, ...]:
    clean_text = re.sub(r"[^A-Z0-9 ]+", " ", _normalize_similarity_text(value))
    return tuple(
        token for token in clean_text.split()
        if len(token) > 1 and token not in _STOP_WORDS
    )

@lru_cache(maxsize=10000)
def _jaccard(a: tuple[str, ...], b: tuple[str, ...]) -> float:
    set_a, set_b = set(a), set(b)
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return 0.0 if union == 0 else intersection / union

@lru_cache(maxsize=10000)
def _sequence_match(a: str, b: str) -> float:
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

    token_score = _jaccard(_normalize_similarity_tokens(a_str), _normalize_similarity_tokens(b_str))
    sequence_score = _sequence_match(a_str, b_str)

    return 0.4 * token_score + 0.6 * sequence_score"""

    if "import difflib" not in content:
        content = "import difflib\nfrom functools import lru_cache\n" + content

    new_content = content.replace(old_code, patch_code)
    with open(file, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"Patched {file}")
else:
    print(f"Pattern not found in {file}")
