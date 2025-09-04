#!/usr/bin/env python3
import os, re, sys, json, glob, argparse, itertools
from typing import Dict, Any, List, Optional, Tuple
from elasticsearch import Elasticsearch, helpers

ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")

# Accept hyphens in work ids (e.g., pli-tv-kd10), and multi-level numeric sections (e.g., 1.1.1)
# allow digits, dots, hyphens, and lowercase letters after the colon
SEG_KEY_RE = re.compile(r'^([a-z\-]+[\d\.]+):([0-9][0-9a-z\.\-]*)$')
FILENAME_WORK_RE = re.compile(r"([a-z\-]+[\d\.]+)_?", re.I)
SPLIT_SCHEME_RE  = re.compile(r"^([a-z\-]+?)([\d\.]+)$", re.I)

# Collections inside Khuddaka Nikaya (treated as basket='sutta', collection='KN')
KN_PREFIXES = {"kp","dhp","ud","iti","snp","vv","pv","thag","thig","ja","ap","bv"}

# Buckets considered primarily verse-oriented (used for is_gatha defaulting)
VERSE_Y_WORK_PREFIX = {"snp","thag","thig","vv","pv","dhp"}

def infer_variant_from_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Return (layer, lang, translator) based on bilara path segments."""
    parts = path.replace("\\", "/").split("/")
    if "root" in parts:
        return ("root", "pli", None)
    if "translation" in parts:
        try:
            i = parts.index("translation")
            lang = parts[i+1] if i+1 < len(parts) else None
            translator = parts[i+2] if i+2 < len(parts) else None
            return ("translation", lang, translator)
        except Exception:
            return ("translation", None, None)
    return ("unknown", None, None)

def parse_work_id_from_filename(filename: str) -> Optional[str]:
    base = os.path.basename(filename)
    m = FILENAME_WORK_RE.search(base.lower())
    return m.group(1) if m else None

def split_scheme_and_number(work_id: str) -> Tuple[Optional[str], Optional[str]]:
    m = SPLIT_SCHEME_RE.match(work_id or "")
    if not m: return (None, None)
    return (m.group(1).upper(), m.group(2))

def seq_from_section(section: str) -> int:
    """Map dotted section to sortable integer (a*1e8 + b*1e4 + c*1e2 + d)."""
    try:
        parts = [int(x) for x in section.split(".")]
    except Exception:
        return 0
    # support up to 4 levels
    coeff = [1000000, 1000, 10, 1]
    total = 0
    for i, p in enumerate(parts[:4]):
        total += p * coeff[i]
    return total

def infer_basket_collection_from_work(work_id: str, scheme: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (basket, collection). Basket in {'sutta','vinaya','abhidhamma'}.
    Collection: DN/MN/SN/AN/KN for suttas; otherwise a high-level code for vinaya/abhidhamma if desired.
    """
    wid = (work_id or "").lower()
    scm = (scheme or "").upper()

    # Sutta Nikayas
    if scm in {"DN","MN","SN","AN"}:
        return ("sutta", scm)

    # Khuddaka Nikaya detection
    prefix = re.match(r"([a-z]+)", wid).group(1) if re.match(r"([a-z]+)", wid) else ""
    if prefix in KN_PREFIXES or scm == "KN":
        return ("sutta", "KN")

    # Vinaya: pli-tv-*
    if wid.startswith("pli-tv-") or scm.startswith("PLI-TV"):
        return ("vinaya", "VIN")

    # Abhidhamma (very rough heuristic via common work codes)
    if prefix in {"dhs","vibh","kvu","pug","yam","yp","patthana","abh"} or scm in {"ABH"}:
        return ("abhidhamma", "ABH")

    return (None, None)

def parse_division_chapter(work_id: str) -> Tuple[Optional[str], Optional[int]]:
    """
    For vinaya-like ids such as pli-tv-kd10 -> division_code='KD', division_num=10.
    For sutta-like ids, returns (None, None).
    """
    wid = (work_id or "").lower()
    m = re.match(r"pli-tv-([a-z]+)(\d+)", wid)
    if m:
        return (m.group(1).upper(), int(m.group(2)))
    return (None, None)

def is_title_section(section: str) -> bool:
    # 0.* are title/heading segments in Bilara
    return section.startswith("0.")

def is_gatha_boundary(section: str) -> bool:
    # Heuristic: verse starts when the section suffix ends with .1 (non-title)
    return (not is_title_section(section)) and section.endswith(".1")

def base_sutta_id(work_id: str) -> Optional[str]:
    """
    For suttas, the base id is work_id as-is (e.g., 'mn10', 'sn22.59', 'thag1.1').
    For vinaya/abhidhamma, returns None.
    """
    wid = (work_id or "").lower()
    if re.match(r"^(mn|dn|sn|an)[\d\.]+$", wid):
        return wid
    # Khuddaka "sutta-like" items will also be retained as sutta ids
    if re.match(r"^(" + "|".join(map(re.escape, KN_PREFIXES)) + r")[\d\.]+$", wid):
        return wid
    return None

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def gather_segments(filepaths: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Build one doc per segment_id, merging multiple variants (root/translation).
    Also denormalize: translator/lang (primary), basket, collection, vagga (from titles), sutta/sutta_num (when applicable),
    division_code/division_num for vinaya, basic gatha tracking.
    """
    segments: Dict[str, Dict[str, Any]] = {}
    # We maintain per-work context for titles and gatha counters
    work_context: Dict[str, Dict[str, Any]] = {}

    for fp in filepaths:
        layer, lang, translator = infer_variant_from_path(fp)
        data = load_json(fp)

        fname_work_id = parse_work_id_from_filename(fp)   # e.g., "mn10", "sn22.59", "pli-tv-kd10"
        scheme, work_number = split_scheme_and_number(fname_work_id or "")

        for seg_id, text in data.items():
            m = SEG_KEY_RE.match(seg_id)
            if not m:
                continue
            key_work_id, section = m.group(1), m.group(2)
            # prefer work_id embedded in the segment key
            if fname_work_id and key_work_id != fname_work_id:
                scheme, work_number = split_scheme_and_number(key_work_id)

            basket, collection = infer_basket_collection_from_work(key_work_id, scheme)
            sutta_id = base_sutta_id(key_work_id)
            sutta_num = None
            if sutta_id is not None:
                # Extract the trailing numeric part sensibly
                mnum = re.search(r"(\d+(?:\.\d+)*)$", sutta_id)
                if mnum:
                    # try to take the last numeric as canonical "sutta number"
                    tail = mnum.group(1)
                    try:
                        sutta_num = int(tail.split(".")[-1])
                    except Exception:
                        sutta_num = None

            division_code, division_num = parse_division_chapter(key_work_id)

            # is_title and seq
            title_flag = is_title_section(section)
            seq = seq_from_section(section)

            # Init per-work context
            ctx = work_context.setdefault(key_work_id, {
                "last_titles": {},       # map "0.x" -> title text
                "gatha_no": 0,
                "gatha_line": 0,
                "last_boundary_seen": False,
                "likely_verse": (base_sutta_id(key_work_id) or "").split(".")[0] in VERSE_Y_WORK_PREFIX
            })

            # Track titles if this is a title segment
            if title_flag:
                ctx["last_titles"][section] = text

            # Gatha tracking: increment when boundary seen; otherwise increment line if in a gatha
            if is_gatha_boundary(section):
                ctx["gatha_no"] += 1
                ctx["gatha_line"] = 1
            else:
                if ctx["gatha_no"] > 0 and not title_flag:
                    ctx["gatha_line"] += 1

            # Build/merge segment doc
            if seg_id not in segments:
                # Choose a primary translator/lang/text if a translation variant appears later
                segments[seg_id] = {
                    "segment_id": seg_id,
                    "segment_num": section,
                    "seq": seq,
                    "is_title": title_flag,

                    "basket": basket,
                    "collection": collection,

                    "sutta": sutta_id,
                    "sutta_num": sutta_num,
                    "vagga": None,  # to be inferred from titles (best-effort)

                    "division_code": division_code,
                    "division_num": division_num,

                    # denormalized primary layer info (prefer translation when present)
                    "translator": translator if layer == "translation" else None,
                    "lang": lang if layer != None else None,
                    "text": text if layer == "translation" else None,

                    # verse metadata
                    "is_gatha": (ctx["gatha_no"] > 0) or ctx["likely_verse"],
                    "gatha_no": ctx["gatha_no"] if ctx["gatha_no"] > 0 else None,
                    "gatha_line": ctx["gatha_line"] if ctx["gatha_no"] > 0 else None,

                    # keep all titles we have seen so far for this work
                    "titles": [{"section": k, "text": v} for k, v in sorted(ctx["last_titles"].items(), key=lambda kv: kv[0])],

                    # nested variants (root/translation layers)
                    "variants": []
                }
            else:
                # Update non-conflicting fields if missing
                doc = segments[seg_id]
                if doc.get("translator") is None and layer == "translation":
                    doc["translator"] = translator
                if doc.get("lang") is None and lang is not None:
                    doc["lang"] = lang
                if doc.get("text") is None and layer == "translation":
                    doc["text"] = text

                # Update verse counters for this segment id if doc created earlier this pass
                doc["is_gatha"] = (ctx["gatha_no"] > 0) or ctx["likely_verse"]
                doc["gatha_no"] = ctx["gatha_no"] if ctx["gatha_no"] > 0 else None
                doc["gatha_line"] = ctx["gatha_line"] if ctx["gatha_no"] > 0 else None

                # Titles dict can be updated (we always copy the latest snapshot)
                doc["titles"] = [{"section": k, "text": v} for k, v in sorted(ctx["last_titles"].items(), key=lambda kv: kv[0])]

            # Derive vagga best-effort from known title keys if not already set
            # Common: "0.2" or "0.3" includes a division/vagga title in Sutta collections
            vagga = segments[seg_id].get("vagga")
            if not vagga:
                for k in ("0.2","0.3","0.4"):
                    t = ctx["last_titles"].get(k)
                    if t and any(w in t.lower() for w in ["vagga","chapter","nipāta","saṁyutta","saṃyutta", "paññāsa", "paṇṇāsa"]):
                        segments[seg_id]["vagga"] = t.strip()
                        break

            # Append variant
            segments[seg_id]["variants"].append({
                "layer": layer,
                "lang": lang,
                "translator": translator,
                "text": text,
                "source_file": os.path.basename(fp)
            })

    # Post-pass: ensure required top-level fields exist (translator, basket, etc.)
    # - translator: if still None but any variant has a translator, pick the first non-null
    # - lang: if still None but any variant has lang, pick the first
    for seg_id, doc in segments.items():
        if doc.get("translator") is None:
            for v in doc["variants"]:
                if v.get("translator"):
                    doc["translator"] = v["translator"]
                    break
        if doc.get("lang") is None:
            for v in doc["variants"]:
                if v.get("lang"):
                    doc["lang"] = v["lang"]
                    break
        # If text is still None, pick any variant text (e.g., root), so docs are easily previewable
        if doc.get("text") is None:
            for v in doc["variants"]:
                if v.get("text"):
                    doc["text"] = v["text"]
                    break

    return segments

def bulk_index(segments: Dict[str, Dict[str, Any]], index: str, refresh: bool=False):
    es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS))
    actions = (
        {
            "_op_type": "index",
            "_index": index,
            "_id": seg_id,
            "_source": seg_doc
        }
        for seg_id, seg_doc in segments.items()
    )
    ok, errors = helpers.bulk(es, actions, chunk_size=1000, request_timeout=120, raise_on_error=False, stats_only=False)
    if isinstance(errors, list) and errors:
        # Show up to 10 error items for diagnosis
        import itertools, sys, json as _json
        print("\n--- Bulk indexing reported failures (showing up to 10) ---", file=sys.stderr)
        for item in itertools.islice(errors, 10):
            print(_json.dumps(item, ensure_ascii=False)[:2000], file=sys.stderr)
        print("--- End failures ---\n", file=sys.stderr)
    if refresh:
        es.indices.refresh(index=index)


def ensure_index(index: str):
    """Create index with a sensible mapping if it doesn't exist."""
    es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS))
    if es.indices.exists(index=index):
        return
    mapping = {
        "mappings": {
            "properties": {
                "segment_id":   {"type": "keyword"},
                "segment_num":  {"type": "keyword"},
                "seq": {"type": "long"},
                "is_title":     {"type": "boolean"},

                "basket":       {"type": "keyword"},
                "collection":   {"type": "keyword"},
                "vagga":        {"type": "keyword"},
                "sutta":        {"type": "keyword"},
                "sutta_num":    {"type": "integer"},

                "division_code":{"type": "keyword"},
                "division_num": {"type": "integer"},

                "translator":   {"type": "keyword"},
                "lang":         {"type": "keyword"},

                "is_gatha":     {"type": "boolean"},
                "gatha_no":     {"type": "integer"},
                "gatha_line":   {"type": "integer"},

                "text":         {"type": "text"},

                "titles": { "type": "nested", "properties": { "section": { "type": "keyword" }, "text": { "type": "text" } } },

                "variants": {
                    "type": "nested",
                    "properties": {
                        "layer":       {"type": "keyword"},
                        "lang":        {"type": "keyword"},
                        "translator":  {"type": "keyword"},
                        "text":        {"type": "text"},
                        "source_file": {"type": "keyword"}
                    }
                }
            }
        }
    }
    es.indices.create(index=index, **mapping)

def main():
    ap = argparse.ArgumentParser(description="Load Bilara JSON into ES with denormalized fields + nested variants.")
    ap.add_argument("globs", nargs="+", help="File globs for root and/or translations")
    ap.add_argument("--index", default="bilara_segments", help="Target ES index")
    ap.add_argument("--no-create", action="store_true", help="Do not attempt to create the index/mapping")
    ap.add_argument("--refresh", action="store_true", help="Refresh index after bulk indexing")
    args = ap.parse_args()

    files: List[str] = []
    for g in args.globs:
        files.extend(glob.glob(g))

    if not files:
        print("No files matched.", file=sys.stderr)
        return 1

    if not args.no_create:
        ensure_index(args.index)

    segs = gather_segments(files)
    bulk_index(segs, args.index, refresh=args.refresh)
    print(f"Indexed {len(segs)} segments into {args.index}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
