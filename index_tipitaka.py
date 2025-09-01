#!/usr/bin/env python3
"""
Tipitaka.org XML -> Elasticsearch (lean)

- No canonical numbering (we'll take canonical IDs from SuttaCentral later)
- Extracts layer (mula/att/tik), basket hint, collection hint, basic heads
- Tracks per-paragraph subhead (so each paragraph knows its nearest sutta header)
- Stable _id per paragraph (source_file :: segment_id) to avoid duplicates
- Parallel bulk indexing with a small memory footprint

Usage:
  python index_tipitaka.py "../../tipitaka-xml/romn/*.xml" \
      --index tipitaka_segments --threads 4 --chunk-size 300
"""

import os
import re
import sys
import pathlib
from typing import Dict, Iterator, List, Optional, Tuple

from lxml import etree as ET
from elasticsearch import Elasticsearch, helpers


# ------------------------------- Config ---------------------------------------

ES_INDEX_DEFAULT = "tipitaka_segments"
ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")

# filename → text_layer
LAYER_BY_SUFFIX = {
    ".mul.xml": "mula",
    ".att.xml": "atthakatha",
    ".tik.xml": "tika",
}

# quick hint from ids/filenames (do not treat as canonical)
COLL_RE = re.compile(r'^(dn|mn|sn|an|kp|dhp|ud|iti|snp|vv|pv|thag|thig|vin|bd|abh)', re.I)


# -------------------------- Small helper utilities ---------------------------

ASCII_MAP = {
    ord("ā"): "a", ord("ī"): "i", ord("ū"): "u",
    ord("ṅ"): "n", ord("ñ"): "n",
    ord("ṭ"): "t", ord("ḍ"): "d", ord("ṇ"): "n",
    ord("ḷ"): "l", ord("ṁ"): "m",
    ord("Ā"): "A", ord("Ī"): "I", ord("Ū"): "U",
    ord("Ṅ"): "N", ord("Ñ"): "N",
    ord("Ṭ"): "T", ord("Ḍ"): "D", ord("Ṇ"): "N",
    ord("Ḷ"): "L", ord("Ṁ"): "M",
}
def to_ascii(s: Optional[str]) -> Optional[str]:
    return s.translate(ASCII_MAP) if s else s

def parse_xml(path: str) -> ET._ElementTree:
    return ET.parse(path)

def texts(node, xp: str) -> List[str]:
    vals = node.xpath(xp)
    out: List[str] = []
    for v in vals:
        if isinstance(v, str):
            v = v.strip()
            if v:
                out.append(v)
        else:
            t = (v.text or "").strip()
            if t:
                out.append(t)
    return out

def infer_layer_from_filename(path: str) -> str:
    name = os.path.basename(path).lower()
    for suf, layer in LAYER_BY_SUFFIX.items():
        if name.endswith(suf):
            return layer
    return "unknown"

def infer_collection_hint(path: str, root) -> Optional[str]:
    """
    Very light hint (DN/MN/SN/AN/KN/VIN/ABH/etc.) from:
      1) any div id/n close to leafs, or
      2) filename.
    Do NOT treat this as canonical.
    """
    # try to find any obvious id on a leaf ancestor first
    ids = root.xpath("//div/@id | //div/@n")
    for raw in ids:
        m = COLL_RE.match((raw or "").lower())
        if m:
            return m.group(1).upper()
    # fallback to filename
    m = COLL_RE.match(os.path.basename(path).lower())
    return m.group(1).upper() if m else None

def infer_basket_hint(collection_hint: Optional[str]) -> Optional[str]:
    if not collection_hint:
        return None
    if collection_hint in {"DN","MN","SN","AN","KP","DHP","UD","ITI","SNP","VV","PV","THAG","THIG"}:
        return "sutta"
    if collection_hint in {"VIN","BD"}:
        return "vinaya"
    if collection_hint == "ABH":
        return "abhidhamma"
    return None

def nearest_head(div) -> Optional[str]:
    """Pick a useful head from this div or ancestors."""
    # try specific rends first
    for rend in ("title", "chapter", "book"):
        vals = texts(div, f"./head[@rend='{rend}']/text()")
        if vals:
            return vals[0]
    # any head on this div
    vals = texts(div, "./head/text()")
    if vals:
        return vals[0]
    # walk up
    for anc in div.iterancestors(tag="div"):
        vals = texts(anc, "./head/text()")
        if vals:
            return vals[0]
    return None

def build_div_path(leaf_div) -> List[Dict[str, Optional[str]]]:
    """Chain of {type,id,head} from root to this leaf."""
    chain: List[Dict[str, Optional[str]]] = []
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
    """Divs that contain <p> but no child div that contains <p>."""
    divs_with_p = set(body.xpath(".//div[.//p]"))
    return [d for d in divs_with_p if not d.xpath("./div[.//p]")]

def text_of(elem) -> str:
    return "".join(elem.itertext())

def collect_preceding_pbs(elem) -> List[Dict[str,str]]:
    """Collect preceding <pb> siblings up to previous <p>."""
    pbs: List[Dict[str, str]] = []
    for sib in elem.itersiblings(preceding=True):
        if sib.tag == "p":
            break
        if sib.tag == "pb":
            pbs.append({"ed": sib.get("ed"), "n": sib.get("n")})
    pbs.reverse()
    return pbs

def clean_paragraph_text(p) -> Tuple[Optional[str], str]:
    """
    Extract para number from @n or hi[rend='paranum'] and strip leading number from text.
    """
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


# --------------------------- Document extraction ------------------------------

def docs_from_file(path: str) -> List[Dict]:
    """
    Extract per-paragraph docs from one XML file.

    NO canonical numbering here. We keep only:
      - text_layer, basket (hint), collection_hint
      - nikaya banner text (raw)
      - book/chapter/title/subhead (per paragraph subhead tracking)
      - hierarchy, edition_pages
      - ids (div_id, segment_id, order, para_no, rend)
      - text/html, source_file/path
    """
    tree = parse_xml(path)
    root = tree.getroot()
    body = root.find(".//body")
    if body is None:
        return []

    layer = infer_layer_from_filename(path)
    collection_hint = infer_collection_hint(path, root)
    basket = infer_basket_hint(collection_hint) or "extracanonical"

    nikaya_banner_text = " ".join(texts(root, "//p[@rend='nikaya']/text()")).strip() or None

    docs: List[Dict] = []

    # iterate leaf text divs
    for leaf in leaf_text_divs(body):
        div_path = build_div_path(leaf)
        title = texts(leaf, ".//p[@rend='title']/text()")
        head_title = title[0] if title else nearest_head(leaf)
        book = texts(root, "//head[@rend='book']/text()")
        chapter = texts(root, "//head[@rend='chapter']/text()")

        # per-paragraph subhead tracking
        order = 0
        current_subhead: Optional[str] = None

        for child in leaf.iterchildren():
            if child.tag != "p":
                continue

            if (child.get("rend") or "").lower() == "subhead":
                current_subhead = text_of(child).strip() or None
                # do not index the header line as a body paragraph
                continue

            order += 1
            para_no, text = clean_paragraph_text(child)
            if not text:
                continue

            leaf_id = leaf.get("id") or leaf.get("n") or "div"
            seg_suffix = para_no if para_no else f"{order:04d}"
            segment_id = f"{leaf_id}.p.{seg_suffix}"

            doc = {
                "text_layer": layer,
                "basket": basket,
                "collection_hint": collection_hint,
                "work_hint": leaf_id,  # e.g. dn2_1, sn4_1 (hint only)
                "nikaya_banner_text": nikaya_banner_text,

                "book":    book[0] if book else None,
                "chapter": chapter[0] if chapter else None,
                "title":   head_title,
                "subhead": current_subhead,

                "hierarchy": div_path or [],
                "edition_pages": collect_preceding_pbs(child),

                "div_id": leaf_id,
                "segment_id": segment_id,
                "order": order,
                "para_no": para_no,
                "rend": child.get("rend"),
                "lang": "pi-Latn",

                "text": text,
                "html": ET.tostring(child, encoding="unicode"),

                "source_file": os.path.basename(path),
                "source_path": str(path),
            }

            docs.append(doc)

    return docs


# ------------------------------ Bulk indexing --------------------------------

def iter_globs(globs: List[str]) -> Iterator[str]:
    for g in globs:
        for p in pathlib.Path().glob(g):
            if p.is_file():
                yield str(p)

def action_stream(globs: List[str], index: str) -> Iterator[Dict]:
    """Yield bulk actions lazily; compute stable _id per paragraph."""
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
    threads: int = 4,
    chunk_size: int = 300,
) -> None:
    es = Elasticsearch(es_url, basic_auth=(es_user, es_pass))
    actions = action_stream(globs, index)

    failed = 0
    for ok, info in helpers.parallel_bulk(
        es,
        actions,
        thread_count=threads,
        chunk_size=chunk_size,
        request_timeout=120,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        if not ok:
            failed += 1
            if failed <= 8:
                print("FAIL:", info)
    if failed:
        print("Failed actions:", failed)


# ---------------------------------- Main -------------------------------------

def main(argv: List[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Index Tipitaka.org XML into Elasticsearch (lean)")
    ap.add_argument("globs", nargs="+", help="File globs, e.g. '../../tipitaka-xml/romn/*.xml'")
    ap.add_argument("--index", default=ES_INDEX_DEFAULT, help=f"Target index (default {ES_INDEX_DEFAULT})")
    ap.add_argument("--threads", type=int, default=4, help="Parallel indexing threads")
    ap.add_argument("--chunk-size", type=int, default=300, help="Bulk chunk size")
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
