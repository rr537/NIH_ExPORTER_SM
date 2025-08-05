import re
from typing import List, Tuple, Dict, Optional
from nltk.corpus import stopwords
from unidecode import unidecode
import logging

def generate_keyword_variants(keyword: str) -> List[str]:
    original = unidecode(keyword.strip().lower())
    variants = set([original])

    if not original.endswith('s'):
        variants.add(original + 's')

    if "'s" in original:
        variants.add(original.replace("'s", "s"))

    if "-" in original:
        hyphen_space = original.replace("-", " ")
        variants.update([hyphen_space, hyphen_space + 's'])

    keyword_clean = re.sub(r"[^\w\s]", "", original)
    if keyword_clean.endswith('y') and not keyword_clean.endswith(('ay', 'ey', 'oy', 'uy')):
        variants.add(re.sub(r'y$', 'ies', keyword_clean))
    elif keyword_clean.endswith(('s', 'x', 'z', 'ch', 'sh')):
        variants.add(keyword_clean + 'es')
    else:
        variants.add(keyword_clean + 's')

    return list(variants)

def enrich_keywords(
    keywords: List[str],
    remove_stopwords: bool = False
) -> List[str]:
    stopword_set = set(stopwords.words('english')) if remove_stopwords else set()
    enriched = set()

    for kw in keywords:
        kw_norm = unidecode(kw.strip().lower())
        if remove_stopwords and kw_norm in stopword_set:
            continue
        enriched.update(generate_keyword_variants(kw_norm))

    return sorted(enriched)

def load_keywords_from_config(
    config: Dict,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[str], List[str]]:
    try:
        keyword_section = config.get("keywords", {})
        remove_stopwords = keyword_section.get("remove_stopwords", False)

        raw_treatments = keyword_section.get("treatment", [])
        raw_diseases = keyword_section.get("disease", [])

        if logger:
            logger.info(f"Loaded {len(raw_treatments)} treatments, {len(raw_diseases)} diseases")

        treatments = enrich_keywords(raw_treatments, remove_stopwords)
        diseases = enrich_keywords(raw_diseases, remove_stopwords)

        return treatments, diseases

    except Exception as e:
        if logger:
            logger.error(f"Failed to load keywords: {str(e)}", exc_info=True)
        return [], []

def prepare_keywords(
    config: Dict,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[str], List[str], Dict[str, Dict]]:
    
    treatments, diseases = load_keywords_from_config(config, logger)

    summary = {
        "keyword_counts": {
            "treatment_terms": len(treatments),
            "disease_terms": len(diseases)
        },
        "keyword_lists": {
            "treatment_terms": treatments,
            "disease_terms": diseases
        }
    }

    if logger:
        if not treatments and not diseases:
            logger.warning("No enriched keywords returned — check config or enrichment logic.")
        else:
            logger.info(f"Keyword prep complete: {len(treatments)} treatment terms, {len(diseases)} disease terms.")

    return treatments, diseases, summary
