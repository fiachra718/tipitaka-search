#!/usr/bin/env python3
import os, re, sys, pathlib
from typing import Dict, Iterable, List, Tuple, Optional
from lxml import etree as ET
from elasticsearch import Elasticsearch, helpers

ES_INDEX = "tipitaka_segments"
ES_URL   = os.environ.get("ES_URL",  "http://localhost:9200")
ES_USER  = os.environ.get("ES_USER", "elastic")
ES_PASS  = os.environ.get("ES_PASS", "changeme")

# --- Diacritic-lite helpers ---
# ASCII_MAP = str.maketrans("āīūṅñṭḍṇḷṁĀĪŪṄÑṬḌṆḶṀ", "aiuangtnlmaiuANGNTDNLM")
ASCII_MAP = {
    ord("ā"): "a", ord("ī"): "i", ord("ū"): "u",
    ord("ṅ"): "n", ord("ñ"): "n",
    ord("ṭ"): "t", ord("ḍ"): "d", ord("ṇ"): "n",
    ord("ḷ"): "l", ord("ṁ"): "m",
    ord("Ā"): "A", ord("Ī"): "I", ord("Ū"): "U",
    ord("Ṅ"): "N", ord("Ñ"): "N",
    ord("Ṭ"): "T", ord("Ḍ"): "D", ord("Ṇ"): "N",
    ord("Ḷ"): "L", ord("Ṁ"): "M"
}

def to_ascii(s: str) -> str: return s.translate(ASCII_MAP) if s else s

# --- Banner/Nikaya → basket/collection ---
NIKAYA_MAP = {
    "dīghanikāyo": ("sutta","DN"), "dighanikayo": ("sutta","DN"),
    "majjhimanikāye": ("sutta","MN"), "majjhimanikaye": ("sutta","MN"),
    "saṃyuttanikāye": ("sutta","SN"), "saṁyuttanikāye": ("sutta","SN"), "samyuttanikaye": ("sutta","SN"),
    "aṅguttaranikāye": ("sutta","AN"), "anguttaranikaye": ("sutta","AN"),
    "khuddakanikāye": ("sutta","KN"), "khuddakanikaye": ("sutta","KN"),
    "vinayapiṭake": ("vinaya",None), "vinayapitake": ("vinaya",None),
    "abhidhammapiṭake": ("abhidhamma",None), "abhidhammapitake": ("abhidhamma",None),
}

LAYER_BY_SUFFIX = { ".mul.xml": "mula", ".att.xml": "atthakatha", ".tik.xml": "tika" }

COLL_RE = re.compile(r'^(dn|mn|sn|an|kp|dhp|ud|iti|snp|vv|pv|thag|thig|vin|bd|abh)', re.I)

def parse(path: str) -> ET._ElementTree:
    # lxml auto-detects UTF-16/UTF-8 via BOM or xml declaration
    return ET.parse(path)

def get_texts(node, xp: str) -> List[str]:
    vals = node.xpath(xp)
    out: List[str] = []
    for v in vals:
        if isinstance(v, str):
            v = v.strip()
            if v: out.append(v)
        else:
            t = (v.text or "").strip()
            if t: out.append(t)
    return out

def infer_layer_from_filename(path: str) -> str:
    name = os.path.basename(path).lower()
    for suf, layer in LAYER_BY_SUFFIX.items():
        if name.endswith(suf): return layer
    return "unknown"

def infer_banner(root) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    vals = get_texts(root, "//p[@rend='nikaya']/text()")
    if not vals: return None, None, None
    raw = " ".join(vals).strip()
    low = raw.lower(); low_ascii = to_ascii(low)
    for key,(basket,coll) in NIKAYA_MAP.items():
        if key in low or key in low_ascii:
            return basket, coll, raw
    return None, None, raw

def nearest_head(div) -> Optional[str]:
    for rend in ("title","chapter","book",None):
        xp = f"./head[@rend='{rend}']/text()" if rend else "./head/text()"
        h = get_texts(div, xp)
        if h: return h[0]
    for anc in div.iterancestors(tag="div"):
        h = get_texts(anc, "./head/text()")
        if h: return h[0]
    return None

def build_div_path(leaf_div) -> List[Dict[str,str]]:
    chain = []
    node = leaf_div
    while node is not None and node.tag == "div":
        head = nearest_head(node)
        chain.append({
            "type": node.get("type"),
            "id":   node.get("id") or node.get("n"),
            "head": head
        })
        par = node.getparent()
        node = par if (par is not None and par.tag == "div") else None
    chain.reverse()
    return chain

def leaf_text_divs(body) -> List[ET._Element]:
    divs_with_p = set(body.xpath(".//div[.//p]"))
    leaves = []
    for d in divs_with_p:
        if not d.xpath("./div[.//p]"):  # no child div with <p>
            leaves.append(d)
    return leaves

def collect_preceding_pbs(elem) -> List[Dict[str,str]]:
    pbs = []
    for sib in elem.itersiblings(preceding=True):
        if sib.tag == "p": break
        if sib.tag == "pb":
            pbs.append({"ed": sib.get("ed"), "n": sib.get("n")})
    pbs.reverse()
    return pbs

def text_of(elem) -> str:
    return "".join(elem.itertext())

def clean_paragraph_text(p) -> Tuple[Optional[str], str]:
    para_no = p.get("n")
    if not para_no:
        nums = p.xpath(".//hi[@rend='paranum']/text()")
        if nums:
            para_no = (nums[0] or "").strip() or None
    raw = text_of(p).strip()
    if para_no:
        pat = re.compile(rf"^\s*{re.escape(para_no)}\s*[\.\u00B7•:]?\s+")
        raw = pat.sub("", raw, count=1).strip()
    return para_no, raw

def docs_from_file(path: str) -> List[Dict]:
    tree = parse(path)
    root = tree.getroot()
    body = root.find(".//body")
    if body is None: return []

    layer = infer_layer_from_filename(path)
    basket, collection, banner_raw = infer_banner(root)

    docs: List[Dict] = []
    for leaf in leaf_text_divs(body):
        # hierarchy/path & heads
        div_path = build_div_path(leaf)
        title = get_texts(leaf, ".//p[@rend='title']/text()")
        subhead = get_texts(leaf, ".//p[@rend='subhead']/text()")
        head_title = title[0] if title else nearest_head(leaf)

        # collection/basket fallbacks via div IDs or filename prefix
        if not collection or not basket:
            # try ancestor IDs like dn3_1, mn2_5, vin2_1, abh01...
            for anc in [leaf] + list(leaf.iterancestors(tag="div")):
                did = (anc.get("id") or anc.get("n") or "").lower()
                m = COLL_RE.match(did)
                if m and not collection:
                    pref = m.group(1).upper()
                    if pref in {"DN","MN","SN","AN","KP","DHP","UD","ITI","SNP","VV","PV","THAG","THIG"}:
                        collection = pref
                        basket = basket or "sutta"
                    elif pref in {"VIN","BD"}:
                        basket = "vinaya"
                    elif pref == "ABH":
                        basket = "abhidhamma"
                    break

        # still missing? filename fallback
        if not basket or (basket == "sutta" and not collection):
            name = os.path.basename(path).lower()
            m = COLL_RE.match(name)
            if m:
                pref = m.group(1).upper()
                if pref in {"DN","MN","SN","AN","KP","DHP","UD","ITI","SNP","VV","PV","THAG","THIG"}:
                    collection = collection or pref
                    basket = "sutta"
                elif pref in {"VIN","BD"}:
                    basket = "vinaya"
                elif pref == "ABH":
                    basket = "abhidhamma"

        # no banner + no prefix ⇒ extracanonical (e-files etc.)
        if not basket:
            basket = "extracanonical"

        order = 0
        for p in leaf.xpath(".//p"):
            order += 1
            para_no, text = clean_paragraph_text(p)
            if not text: continue

            # IDs
            leaf_id = leaf.get("id") or leaf.get("n") or "div"
            seg_suffix = para_no if para_no else f"{order:04d}"
            segment_id = f"{leaf_id}.p.{seg_suffix}"

            docs.append({
                "basket": basket,
                "collection": collection,
                "text_layer": layer,
                "book":    get_texts(root, "//head[@rend='book']/text()")[0] if get_texts(root, "//head[@rend='book']/text()") else None,
                "chapter": get_texts(root, "//head[@rend='chapter']/text()")[0] if get_texts(root, "//head[@rend='chapter']/text()") else None,
                "title":   head_title,
                "subhead": subhead[0] if subhead else None,
                "hierarchy": div_path,
                "canonical_scheme": None,       # (optionally fill for MN/DN later)
                "canonical_ref":    None,
                "work_id":  None,               # not always available for these TEI ids
                "div_id":   leaf_id,
                "segment_id": segment_id,
                "order":    order,
                "para_no":  para_no,
                "rend":     p.get("rend"),
                "edition_pages": collect_preceding_pbs(p),
                "lang":     "pi-Latn",
                "text":     text,
                "html":     ET.tostring(p, encoding="unicode"),
                "source_file": os.path.basename(path),
                "source_path": str(path)
            })
    return docs

def bulk_index(paths: List[str]) -> None:
    es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS))
    actions = (
        {"_op_type": "index", "_index": ES_INDEX, "_source": doc}
        for g in paths
        for p in pathlib.Path().glob(g)
        for doc in docs_from_file(str(p))
    )
    helpers.bulk(es, actions, chunk_size=500, request_timeout=120)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: index_tipitaka.py <glob> [<glob> ...]")
        sys.exit(2)
    bulk_index(sys.argv[1:])
    print(f"Indexed from globs: {sys.argv[1:]}")
