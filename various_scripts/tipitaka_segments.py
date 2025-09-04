#!/usr/bin/env python3
import os, re, sys, pathlib, itertools
from typing import Dict, Iterable, List, Tuple, Optional
from lxml import etree as ET
from elasticsearch import Elasticsearch, helpers

ES_INDEX = "tipitaka_segments"

COLL_RE = re.compile(r'^(dn|mn|sn|an|kp|dhp|ud|iti|snp|vv|pv|thag|thig|vin|abh)', re.I)

def parse(path: str) -> ET._ElementTree:
    return ET.parse(path)  # auto-detects UTF-16/UTF-8 via BOM/declaration

def text_of(elem: ET._Element) -> str:
    return "".join(elem.itertext())

def clean_paragraph_text(p: ET._Element) -> Tuple[Optional[str], str]:
    """Extract para_no from p/@n or hi[rend='paranum'], and return (para_no, cleaned_text)."""
    para_no = p.get("n")
    # Find first hi@rend='paranum'
    paranums = p.xpath(".//hi[@rend='paranum']")
    if paranums and not para_no:
        t = (paranums[0].text or "").strip()
        para_no = t if t else None
    # Make a cleaned copy string with the paranum removed at the very start if present
    # Simple heuristic: if text starts with that number followed by punctuation/dot/space, strip it
    raw = text_of(p).strip()
    if para_no:
        # common forms: "1. …", "1 …", "1• …"
        pat = re.compile(rf'^\s*{re.escape(para_no)}\s*[\.\u00B7•:]?\s+')
        raw = pat.sub("", raw, count=1).strip()
    return para_no, raw

def first_title(div: ET._Element) -> Optional[str]:
    # Prefer explicit title
    t = div.xpath("./head[@rend='title']")
    if t and (t[0].text and t[0].text.strip()):
        return t[0].text.strip()
    # Fall back to any head text inside this level
    t = div.xpath("./head")
    if t and (t[0].text and t[0].text.strip()):
        return t[0].text.strip()
    return None

def basket_from_top(div_chain: List[ET._Element]) -> Optional[str]:
    # Look up the chain for known types
    for d in div_chain:
        t = (d.get("type") or "").lower()
        if t in ("sutta", "vinaya", "abhidhamma"):
            return "sutta" if t == "sutta" else t
    # Heuristic from IDs like 'dn1', 'mn3_4', 'abh01'
    for d in div_chain:
        did = (d.get("id") or d.get("n") or "").lower()
        if did.startswith(("dn","mn","sn","an","kp","dhp","ud","iti","snp","vv","pv","thag","thig")):
            return "sutta"
        if did.startswith(("vin","bd")):
            return "vinaya"
        if did.startswith(("abh","abhi")):
            return "abhidhamma"
    return None

def collection_and_workid(div: ET._Element) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (collection, work_id, div_id) for the current sutta-level div.
    div_id is TEI's own id/n like 'mn3_4'.
    work_id is normalized like 'mn10' when discernible; otherwise fall back to div_id.
    """
    did = (div.get("id") or div.get("n") or "")
    div_id = did if did else None
    coll = None
    work_id = None
    if did:
        m = COLL_RE.match(did)
        if m:
            coll = m.group(1).upper()
            work_id = did.lower()
    return coll, work_id, div_id

def collect_preceding_pbs(elem: ET._Element) -> List[Dict[str,str]]:
    """Collect page breaks encountered since previous paragraph within the same ancestor."""
    pbs = []
    # Look back among preceding siblings for pb; stop when we hit a previous p
    for sib in elem.itersiblings(preceding=True):
        if sib.tag == "p":
            break
        if sib.tag == "pb":
            pbs.append({"ed": sib.get("ed"), "n": sib.get("n")})
    pbs.reverse()
    return pbs

def walk_divs(body: ET._Element) -> Iterable[Dict]:
    """
    Yield documents per <p> within each <div type='sutta'> (or any div that contains text).
    """
    # Depth-first traversal maintaining ancestor chain
    for sutta_div in body.xpath(".//div[@type='sutta']"):
        # Build ancestor chain from root body to this sutta_div
        chain = []
        node = sutta_div
        while node is not None and node is not body:
            if node.tag == "div":
                chain.append(node)
            node = node.getparent()
        chain = list(reversed(chain))

        title = first_title(sutta_div)
        coll, work_id, div_id = collection_and_workid(sutta_div)
        basket = basket_from_top(chain) or "sutta"

        section_parts = ["Sutta"]
        if coll: section_parts.append(coll)
        if div_id: section_parts.append(div_id)
        section_path = "/".join(section_parts)

        order = 0
        for p in sutta_div.xpath(".//p"):
            order += 1
            para_no, text = clean_paragraph_text(p)
            if not text:
                continue
            seg_suffix = para_no if para_no else f"{order:04d}"
            segment_id = f"{(work_id or div_id or 'work')}.p.{seg_suffix}"

            # page breaks seen since last p (edition markers)
            edition_pages = collect_preceding_pbs(p)

            yield {
                "basket": basket,
                "collection": coll,
                "work_id": work_id or div_id,
                "div_id": div_id,
                "title": title,
                "section_path": section_path,
                "section_parts": section_parts,
                "segment_id": segment_id,
                "order": order,
                "para_no": para_no,
                "rend": p.get("rend"),
                "edition_pages": edition_pages,
                "lang": "pi-Latn",
                "text": text,
                "html": ET.tostring(p, encoding="unicode"),
            }

def docs_from_file(path: str) -> List[Dict]:
    tree = parse(path)
    root = tree.getroot()
    body = root.find(".//body")
    if body is None:
        return []
    docs = []
    for d in walk_divs(body):
        d["source_file"] = os.path.basename(path)
        d["source_path"] = str(path)
        docs.append(d)
    return docs

def bulk_index(paths: List[str]):
    es = Elasticsearch(os.environ.get("ES_URL","http://localhost:9200"))
    gen = (doc for p in paths for doc in docs_from_file(p))
    helpers.bulk(es, gen, index=ES_INDEX, chunk_size=500, request_timeout=120)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: index_tipitaka.py <glob> [...]")
        sys.exit(2)
    files = []
    for g in sys.argv[1:]:
        files.extend([str(p) for p in pathlib.Path().glob(g)])
    if not files:
        print("No files matched")
        sys.exit(1)
    bulk_index(files)
    print(f"Indexed from {len(files)} files into {ES_INDEX}")
