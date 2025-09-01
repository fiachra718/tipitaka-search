#!/usr/bin/env python3
"""
Tipitaka XML -> Elasticsearch (concurrent, readable)

- Parses TEI-like XML from tipitaka.org
- Extracts leaf <div> blocks and paragraph <p> nodes
- Adds canonical refs for DN/MN/SN/AN
- Uses stable _id so reindexing overwrites, not duplicates
- Streams to Elasticsearch with helpers.parallel_bulk (threaded)

Usage:
  python index_tipitaka.py "../../tipitaka-xml/romn/*.xml" \
      --index tipitaka_segments --threads 6 --chunk-size 500
"""
import os
import re
import sys
import pathlib
from typing import Dict, Iterable, Iterator, List, Tuple, Optional

from lxml import etree as ET
from elasticsearch import Elasticsearch, helpers

# ------------------------------- Config ---------------------------------------

ES_INDEX_DEFAULT = "tipitaka_segments"
ES_URL   = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER  = os.getenv("ES_USER", "elastic")
ES_PASS  = os.getenv("ES_PASS", "changeme")

LAYER_BY_SUFFIX = { ".mul.xml": "mula", ".att.xml": "atthakatha", ".tik.xml": "tika" }
COLL_RE = re.compile(r'^(dn|mn|sn|an|kp|dhp|ud|iti|snp|vv|pv|thag|thig|vin|bd|abh)', re.I)

# Banner/Nikaya -> basket/collection
NIKAYA_MAP = {
    "dīghanikāyo": ("sutta","DN"), "dighanikayo": ("sutta","DN"),
    "majjhimanikāye": ("sutta","MN"), "majjhimanikaye": ("sutta","MN"),
    "saṃyuttanikāye": ("sutta","SN"), "saṁyuttanikāye": ("sutta","SN"), "samyuttanikaye": ("sutta","SN"),
    "aṅguttaranikāye": ("sutta","AN"), "anguttaranikaye": ("sutta","AN"),
    "khuddakanikāye": ("sutta","KN"), "khuddakanikaye": ("sutta","KN"),
    "vinayapiṭake": ("vinaya",None), "vinayapitake": ("vinaya",None),
    "abhidhammapiṭake": ("abhidhamma",None), "abhidhammapitake": ("abhidhamma",None),
}

# SN and AN lookups (extend as needed)
SN_SAMYUTTA_TO_NO = {
    "devata": 1, "devaputta": 2, "kosala": 3, "mara": 4, "bhikkhuni": 5,
    "khandha": 22, "radha": 23, "ditthi": 24,
    "salayatana": 35, "vedana": 36,
}
AN_NIPATA_HEAD_TO_NO = {
    "eka":1,"duka":2,"tika":3,"catukka":4,"pancaka":5,"chakka":6,"sattaka":7,
    "atthaka":8,"navaka":9,"dasaka":10,"ekadasaka":11
}

# -------------------------- Small helper utilities ---------------------------

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

def parse_xml(path: str) -> ET._ElementTree:
    """Parse XML; lxml will honor BOM/declared encodings."""
    return ET.parse(path)

def texts(node, xp: str) -> List[str]:
    """Return non-empty texts for an XPath, coalescing strings/elements."""
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
    n = os.path.basename(path).lower()
    for suf, layer in LAYER_BY_SUFFIX.items():
        if n.endswith(suf): return layer
    return "unknown"

def infer_banner(root) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (basket, collection, raw_banner)."""
    vals = texts(root, "//p[@rend='nikaya']/text()")
    if not vals: return None, None, None
    raw = " ".join(vals).strip()
    low = raw.lower(); lowa = to_ascii(low)
    for key,(b,c) in NIKAYA_MAP.items():
        if key in low or key in lowa: return b, c, raw
    return None, None, raw

def nearest_head(div) -> Optional[str]:
    """Pick a useful head from this div or ancestors."""
    for r in ("title","chapter","book",None):
        xp = f"./head[@rend='{r}']/text()" if r else "./head/text()"
        h = texts(div, xp)
        if h: return h[0]
    for anc in div.iterancestors(tag="div"):
        h = texts(anc, "./head/text()")
        if h: return h[0]
    return None

def build_div_path(leaf_div) -> List[Dict[str,str]]:
    """Build ordered chain of {type,id,head} from root to leaf div."""
    chain: List[Dict[str,str]] = []
    node = leaf_div
    while node is not None and node.tag == "div":
        chain.append({
            "type": node.get("type"),
            "id":   node.get("id") or node.get("n"),
            "head": nearest_head(node)
        })
        par = node.getparent()
        node = par if (par is not None and par.tag == "div") else None
    chain.reverse()
    return chain

def leaf_text_divs(body) -> List[ET._Element]:
    """Return divs that contain <p> but no child div that contains <p>."""
    divs_with_p = set(body.xpath(".//div[.//p]"))
    return [d for d in divs_with_p if not d.xpath("./div[.//p]")]

def collect_preceding_pbs(elem) -> List[Dict[str,str]]:
    """Collect preceding <pb> siblings up to previous <p>."""
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
    """Extract para number (from @n or paranum) and strip number from text."""
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

# ------------------------- Canonical numbering helpers ------------------------

def _norm(s):
    if not s: return ""
    t = to_ascii(s).lower()
    t = re.sub(r"(samyuttaṃ?|samyutta|sam yutta)", "", t)
    t = re.sub(r"(nipataṃ?|nipata)", "", t)
    t = re.sub(r"[^a-z0-9]+", "", t)
    return t

def _int_from_subhead(subhead):
    if not subhead: return None
    m = re.match(r"\s*(\d+)", to_ascii(subhead))
    return int(m.group(1)) if m else None

def _dn_number(hierarchy, s_idx):
    # dn1..dn3 are the three vaggas (13,10,11)
    vagga = None
    for n in hierarchy:
        i = (n.get("id") or "").lower()
        if re.fullmatch(r"dn[123]", i):
            vagga = int(i[2:]); break
    if not vagga or not s_idx: return None
    counts = [13,10,11]
    if s_idx < 1 or s_idx > counts[vagga-1]: return None
    return sum(counts[:vagga-1]) + s_idx  # 1..34

def _mn_number(hierarchy, s_idx):
    book = vagga = None
    for n in hierarchy:
        i = (n.get("id") or "").lower()
        if re.fullmatch(r"mn[123]", i): book = int(i[2:])
        m = re.fullmatch(r"mn(\d+)_(\d+)", i)
        if m: book, vagga = int(m.group(1)), int(m.group(2))
    if not (book and vagga and s_idx): return None
    return (book-1)*50 + (vagga-1)*10 + s_idx  # nominal 1..150 (152 exist)

def _sn_number(hierarchy, s_idx):
    sam = None
    for n in hierarchy:
        t = (n.get("type") or "").lower()
        h = (n.get("head") or "")
        if "samyutta" in t or "samyutta" in to_ascii(h).lower():
            sam = n; break
    if not sam or not s_idx: return None
    key = _norm(sam.get("head"))
    s_no = SN_SAMYUTTA_TO_NO.get(key)
    return f"{s_no}.{s_idx}" if s_no else None

def _an_number(hierarchy, s_idx):
    nip = None
    for n in hierarchy:
        i = (n.get("id") or "").lower()
        if i.startswith("an"):
            m = re.match(r"an(\d+)$", i)
            if m: nip = int(m.group(1)); break
    if not nip:
        for n in hierarchy:
            h = _norm(n.get("head"))
            for stem, no in AN_NIPATA_HEAD_TO_NO.items():
                if h.startswith(stem): nip = no; break
            if nip: break
    return f"{nip}.{s_idx}" if (nip and s_idx) else None

def attach_canonical(doc: Dict) -> None:
    """Mutates doc in place, setting canonical_scheme and canonical_ref if known."""
    coll = doc.get("collection")
    hier = doc.get("hierarchy") or []
    s_idx = _int_from_subhead(doc.get("subhead"))

    if coll == "DN":
        n = _dn_number(hier, s_idx)
        if n: doc["canonical_scheme"], doc["canonical_ref"] = "DN", f"DN {n}"
    elif coll == "MN":
        n = _mn_number(hier, s_idx)
        if n: doc["canonical_scheme"], doc["canonical_ref"] = "MN", f"MN {n}"
    elif coll == "SN":
        n = _sn_number(hier, s_idx)
        if n: doc["canonical_scheme"], doc["canonical_ref"] = "SN", f"SN {n}"
    elif coll == "AN":
        n = _an_number(hier, s_idx)
        if n: doc["canonical_scheme"], doc["canonical_ref"] = "AN", f"AN {n}"

# --------------------------- Document extraction ------------------------------

def docs_from_file(path: str) -> List[Dict]:
    """Extract per-paragraph docs from one XML file."""
    tree = parse_xml(path)
    root = tree.getroot()
    body = root.find(".//body")
    if body is None:
        return []

    layer = infer_layer_from_filename(path)
    basket, collection, _banner = infer_banner(root)

    docs: List[Dict] = []
    # iterate leaf text divs
    for leaf in leaf_text_divs(body):
        div_path = build_div_path(leaf)
        title = texts(leaf, ".//p[@rend='title']/text()")
        subheads = texts(leaf, ".//p[@rend='subhead']/text()")
        head_title = title[0] if title else nearest_head(leaf)

        # collection/basket fallbacks via div ids/filename
        if not collection or not basket:
            for anc in [leaf] + list(leaf.iterancestors(tag="div")):
                did = (anc.get("id") or anc.get("n") or "").lower()
                m = COLL_RE.match(did)
                if m and not collection:
                    pref = m.group(1).upper()
                    if pref in {"DN","MN","SN","AN","KP","DHP","UD","ITI","SNP","VV","PV","THAG","THIG"}:
                        collection = pref; basket = basket or "sutta"
                    elif pref in {"VIN","BD"}:
                        basket = "vinaya"
                    elif pref == "ABH":
                        basket = "abhidhamma"
                    break

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

        if not basket:
            basket = "extracanonical"

        # paragraphs
        order = 0
        for p in leaf.xpath(".//p"):
            order += 1
            para_no, text = clean_paragraph_text(p)
            if not text:
                continue

            leaf_id = leaf.get("id") or leaf.get("n") or "div"
            seg_suffix = para_no if para_no else f"{order:04d}"
            segment_id = f"{leaf_id}.p.{seg_suffix}"

            book = texts(root, "//head[@rend='book']/text()")
            chapter = texts(root, "//head[@rend='chapter']/text()")

            doc = {
                "basket": basket,
                "collection": collection,
                "text_layer": layer,
                "book":    book[0] if book else None,
                "chapter": chapter[0] if chapter else None,
                "title":   head_title,
                "subhead": subheads[0] if subheads else None,
                "hierarchy": div_path or [],
                "canonical_scheme": None,
                "canonical_ref": None,
                "work_id": None,
                "div_id": leaf_id,
                "segment_id": segment_id,
                "order": order,
                "para_no": para_no,
                "rend": p.get("rend"),
                "edition_pages": collect_preceding_pbs(p),
                "lang": "pi-Latn",
                "text": text,
                "html": ET.tostring(p, encoding="unicode"),
                "source_file": os.path.basename(path),
                "source_path": str(path),
            }

            # add canonical numbering
            attach_canonical(doc)

            # stable id = file + segment
            # doc["_id"] = f"{doc['source_file']}::{doc['segment_id']}"
            docs.append(doc)

    return docs

# ------------------------------ Bulk indexing --------------------------------

def iter_globs(globs: List[str]) -> Iterator[str]:
    """Yield file paths that match any of the provided globs."""
    for g in globs:
        for p in pathlib.Path().glob(g):
            if p.is_file():
                yield str(p)

def action_stream(globs: List[str], index: str) -> Iterator[Dict]:
    """
    Lazily produce bulk actions for all files.
    Using a generator keeps memory footprint small while parallel_bulk
    fans out indexing in threads.
    """
    for path in iter_globs(globs):
        for doc in docs_from_file(path):
            doc_id = f"{doc['source_file']}::{doc['segment_id']}"
            yield {"_op_type": "index", "_index": index, "_id": doc_id, "_source": doc}

def parallel_index(
    globs: List[str],
    index: str,
    es_url: str,
    es_user: str,
    es_pass: str,
    *,
    threads: int = 6,
    chunk_size: int = 500,
) -> None:
    """Index using parallel_bulk with N threads and specified chunk size."""
    es = Elasticsearch(es_url, basic_auth=(es_user, es_pass))
    actions = action_stream(globs, index)

    # parallel_bulk yields (ok, info) tuples we can tally/log if desired
    failed = 0
    for ok, info in helpers.parallel_bulk(
            es,
            actions,
            thread_count=threads,
            chunk_size=chunk_size,
            request_timeout=120,
            raise_on_error=False,  # <— don’t raise, stream failures
            raise_on_exception=False
    ):
        if not ok:
            failed += 1
            if failed <= 10:  # print first few; they include the ES reason
                print("FAIL:", info)
    print("Failed actions:", failed)

# ---------------------------------- Main -------------------------------------
def main(argv: List[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Index Tipitaka XML into Elasticsearch")
    ap.add_argument("globs", nargs="+", help="File globs, e.g. '../../tipitaka-xml/romn/*.xml'")
    ap.add_argument("--index", default=ES_INDEX_DEFAULT, help=f"Target index (default {ES_INDEX_DEFAULT})")
    ap.add_argument("--threads", type=int, default=6, help="Parallel indexing threads (default 6)")
    ap.add_argument("--chunk-size", type=int, default=500, help="Bulk chunk size (default 500)")
    args = ap.parse_args(argv)

    parallel_index(
        globs=args.globs,
        index=args.index,
        es_url=ES_URL,
        es_user=ES_USER,
        es_pass=ES_PASS,
        threads=args.threads,
        chunk_size=args.chunk_size,
    )
    print(f"Indexed from globs: {args.globs} → index={args.index} (threads={args.threads}, chunk={args.chunk_size})")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
