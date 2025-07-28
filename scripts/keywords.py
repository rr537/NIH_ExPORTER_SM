import re
from typing import List, Tuple
from nltk.corpus import stopwords
from unidecode import unidecode
import logging

def generate_keyword_variants(keyword: str) -> List[str]:
    original = unidecode(keyword.strip().lower())
    variants = set()

    variants.add(original)

    # ➕ Pluralization
    if not original.endswith('s'):
        variants.add(original + 's')

    # ➕ Possessive form replacement
    if "'s" in original:
        variants.add(original.replace("'s", "s"))

    # ➕ Hyphen-to-space
    if "-" in original:
        hyphen_space = original.replace("-", " ")
        variants.add(hyphen_space)
        if not hyphen_space.endswith('s'):
            variants.add(hyphen_space + 's')

    # 🔄 Rule-based plural forms
    keyword_clean = re.sub(r"[^\w\s]", "", original)
    if keyword_clean.endswith('y') and not keyword_clean.endswith(('ay', 'ey', 'oy', 'uy')):
        variants.add(re.sub(r'y$', 'ies', keyword_clean))
    elif keyword_clean.endswith(('s', 'x', 'z', 'ch', 'sh')):
        variants.add(keyword_clean + 'es')
    else:
        variants.add(keyword_clean + 's')

    return list(variants)

def enrich_keywords(keywords: List[str], remove_stopwords: bool = False) -> List[str]:
    stopword_set = set(stopwords.words('english')) if remove_stopwords else set()
    enriched = set()

    for kw in keywords:
        kw_norm = unidecode(kw).strip().lower()
        if remove_stopwords and kw_norm in stopword_set:
            continue
        enriched.update(generate_keyword_variants(kw_norm))

    return sorted(enriched)

def load_keywords(config: dict, logger: logging.Logger, remove_stopwords: bool = False) -> Tuple[List[str], List[str]]:
    try:
        raw_treatments = config.get("keywords", {}).get("treatment", [])
        raw_diseases = config.get("keywords", {}).get("disease", [])

        logger.info(f" Loaded {len(raw_treatments)} treatments, {len(raw_diseases)} diseases")

        treatments = enrich_keywords(raw_treatments, remove_stopwords=remove_stopwords)
        diseases = enrich_keywords(raw_diseases, remove_stopwords=remove_stopwords)

        return treatments, diseases

    except Exception as e:
        logger.error(f" Failed to load keywords: {str(e)}", exc_info=True)
        return [], []

def prepare_keywords(
    config: dict,
    logger: logging.Logger,
    remove_stopwords: bool = False
) -> Tuple[List[str], List[str]]:
    """
    Loads and enriches keywords for treatment and disease tagging.
    Returns two lists of keyword variants.
    """
    from scripts.keywords import load_keywords

    treatments, diseases = load_keywords(config, logger, remove_stopwords=remove_stopwords)

    if not treatments and not diseases:
        logger.warning(" No enriched keywords returned — check config or enrichment logic.")
    else:
        logger.info(f" Keyword prep complete: {len(treatments)} treatment terms, {len(diseases)} disease terms.")

    return treatments, diseases
