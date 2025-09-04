# meta_vri.py
import os, re
from typing import Dict, Optional
from lxml import etree

def _text(node):
    return ("".join(node.itertext()).strip()) if node is not None else None

# Regex aliases to normalize the <p rend="book"> text to a short code.
BOOK_ALIASES = [
    (r'\bMajjhima\b|Majjhima-?nik(?:ā|a)ya|Majjhimanik(?:ā|a)ya', 'MN'),
    (r'\bSa[mṃ]yutta\b|Sa[mṃ]yutta-?nik(?:ā|a)ya|Samyuttanikaya', 'SN'),
    (r'\bD[iī]gha\b|Digha-?nik(?:ā|a)ya|D[iī]ghanikaya', 'DN'),
    (r'\bA[nṅ]guttara\b|Anguttara-?nik(?:ā|a)ya', 'AN'),
    (r'\bVinaya\b', 'VIN'),
    (r'\bAbhidhamma\b', 'ABH'),
    # Khuddaka books (best-effort; keep adding as you encounter them)
    (r'Buddhava[mṃ]sa|BuddhavaCsa', 'Bv'),
    (r'Cariy[āa]pi[ṭt]aka|Cariya.*pitaka|CariyaCpi', 'Cp'),
    (r'Khuddakap[āa]ṭha|Khuddakapatha', 'Khp'),
    (r'Dhammapada', 'Dhp'),
    (r'Ud[āa]na', 'Ud'),
    (r'Itivuttaka', 'It'),
    (r'Suttanip[āa]ta', 'Snp'),
    (r'Vim[āa]navatthu', 'Vv'),
    (r'Petavatthu', 'Pv'),
    (r'Therag[āa]th[āa]', 'Thag'),
    (r'Therīg[āa]th[āa]|Therigatha', 'Thig'),
]

def _normalize_book(book_text: Optional[str]) -> Optional[str]:
    if not book_text:
        return None
    t = book_text.strip()
    for pat, code in BOOK_ALIASES:
        if re.search(pat, t, flags=re.IGNORECASE):
            return code
    return t  # fallback to raw if we don’t recognize it

def _normalize_basket_from_nikaya(nikaya_text: Optional[str]) -> Optional[str]:
    if not nikaya_text:
        return None
    t = nikaya_text.lower()
    if 'digha' in t: return 'sutta'
    if 'majjhima' in t: return 'sutta'
    if 'samyutta' in t or 'saṃyutta' in t: return 'sutta'
    if 'anguttara' in t or 'aṅguttara' in t: return 'sutta'
    if 'khuddaka' in t: return 'sutta'
    if 'vinaya' in t: return 'vinaya'
    if 'abhidhamma' in t: return 'abhidhamma'
    return None

def parse_meta(data: bytes, path: str) -> Dict[str, Optional[str]]:
    """
    Parse VRI/TEI XML to extract:
      - nikaya (raw), book (raw), book (normalized code), basket (inferred)
      - edition/lang
      - sutta_id (use filename stem since VRI filenames aren’t MN/SN ids)
    """
    # lxml handles the UTF-16 declared in the XML prolog
    root = etree.fromstring(data)

    # Look for <p rend="nikaya"> and <p rend="book">
    p_nik = root.xpath('.//p[@rend="nikaya"]')
    p_book = root.xpath('.//p[@rend="book"]')

    nikaya_raw = _text(p_nik[0]) if p_nik else None
    book_raw   = _text(p_book[0]) if p_book else None

    # Sometimes there is no <p rend="book">; try common fallbacks
    if not book_raw:
        # Some files encode book hints in <front>/<head>/<title>
        title_el = (
            root.find('.//title') or
            root.find('.//head') or
            root.find('.//front//head') or
            root.find('.//teiHeader//title')
        )
        if title_el is not None:
            book_raw = _text(title_el)

    book = _normalize_book(book_raw)
    basket = _normalize_basket_from_nikaya(nikaya_raw)

    # Use the filename stem as a stable local id (e.g., "s0511a.att")
    stem = os.path.basename(path)
    stem = re.sub(r'\.xml$', '', stem)

    return {
        "nikaya_raw": nikaya_raw,  # keep the raw string for later mapping/QA
        "book_raw": book_raw,
        "book": book,              # normalized short code or raw fallback
        "basket": basket,          # sutta/vinaya/abhidhamma (best-effort)
        "sutta_id": stem,          # no MN/SN id in these files — keep stem
        "edition": "vri",
        "lang": "pli",
        "source_file": os.path.relpath(path),
        "source_repo": "tipitaka_vri"
    }
