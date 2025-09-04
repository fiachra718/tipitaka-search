#!/usr/bin/env python3
import os, re, sys, json, glob, argparse
from elasticsearch import Elasticsearch, helpers

ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")

SEG_KEY_RE = re.compile(r"^([a-z\-]+[\d\.]+):([\d\.]+)$")  # e.g., mn10:1.2 or sn22.59:3.1

KN_PREFIXES = {"kp","dhp","ud","iti","snp","vv","pv","thag","thig"}  # useful later if you want sub-collections

def infer_variant_from_path(path: str):
    parts = path.replace("\\", "/").split("/")
    if "root" in parts:
        return ("root", "pli", None)                    # layer, lang, translator
    if "translation" in parts:
        try:
            i = parts.index("translation")
            lang = parts[i+1]        # e.g., "en"
            translator = parts[i+2]  # e.g., "sujato"
            return ("translation", lang, translator)
        except Exception:
            return ("translation", None, None)
    return ("unknown", None, None)

def parse_work_id_from_filename(filename: str):
    base = os.path.basename(filename)
    m = re.match(r"([a-z\-]+[\d\.]+)_", base.lower())
    return m.group(1) if m else None  # e.g., "mn10", "sn22.59"

def split_scheme_and_number(work_id: str):
    # noinspection RegExpRedundantEscape
    m = re.match(r"([a-z\-]+?)([\d\.]+)$", work_id)
    if not m: return (None, None)
    return (m.group(1).upper(), m.group(2))

def seq_from_section(section: str) -> int:
    try:
        parts = [int(x) for x in section.split(".")]
    except Exception:
        return 0
    a = parts[0] if len(parts) > 0 else 0
    b = parts[1] if len(parts) > 1 else 0
    c = parts[2] if len(parts) > 2 else 0
    return a*10000 + b*100 + c

def infer_basket_and_collection(scheme: str, work_id: str):
    """
    Bilara covers Sutta collections; treat as basket='sutta'.
    Collection: DN/MN/SN/AN; KN if work_id starts with a known KN prefix.
    """
    basket = "sutta" if scheme in {"DN","MN","SN","AN"} else None
    collection = scheme if scheme in {"DN","MN","SN","AN"} else None

    # KN handling (optional for later expansion):
    if not collection:
        # e.g., work_id could be "kn" + subcode in some sets; often it's the sub-collection code directly
        prefix = re.match(r"([a-z]+)", work_id or "")
        pref = prefix.group(1) if prefix else ""
        if pref in KN_PREFIXES:
            basket = "sutta"
            collection = "KN"
        elif scheme == "KN":
            basket = "sutta"
            collection = "KN"

    return basket, collection

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def gather_segments(filepaths):
    segments = {}
    for fp in filepaths:
        layer, lang, translator = infer_variant_from_path(fp)
        data = load_json(fp)

        fname_work_id = parse_work_id_from_filename(fp)   # "mn10", "sn22.59", etc.
        scheme, work_number = split_scheme_and_number(fname_work_id or "")

        for seg_id, text in data.items():
            m = SEG_KEY_RE.match(seg_id)
            if not m:
                continue
            key_work_id, section = m.group(1), m.group(2)

            # prefer the work_id parsed from the key itself
            if fname_work_id and key_work_id != fname_work_id:
                scheme, work_number = split_scheme_and_number(key_work_id)

            basket, collection = infer_basket_and_collection(scheme or "", key_work_id or "")

            is_title = section.startswith("0.")
            seq = seq_from_section(section)

            if seg_id not in segments:
                segments[seg_id] = {
                    "segment_id": seg_id,
                    "collection": scheme,  # e.g. MN
                    "sutta": key_work_id,  # e.g. mn10
                    "sutta_num": work_number,  # e.g. 10
                    "segment_num": section,  # e.g. 1.2
                    "is_title": is_title,
                    "seq": seq,
                    "basket": basket,
                    "variants": []
                }

            variant = {
                "layer": layer,                 # NEW: "root" or "translation"
                "lang": lang,
                "translator": translator,
                "text": text,
                "source_file": os.path.basename(fp)
            }
            segments[seg_id]["variants"].append(variant)

    return segments

def bulk_index(segments, index: str):
    es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS))
    actions = (
        {
            "_op_type": "index",
            "_index": index,
            "_id": seg_id,               # stable: segment_id
            "_source": seg_doc
        }
        for seg_id, seg_doc in segments.items()
    )
    helpers.bulk(es, actions, chunk_size=1000, request_timeout=120)

def main():
    ap = argparse.ArgumentParser(description="Load Bilara JSON into a unified ES index with nested variants.")
    ap.add_argument("globs", nargs="+", help="File globs for root and translations")
    ap.add_argument("--index", default="bilara_segments", help="Target ES index")
    args = ap.parse_args()

    files = []
    for g in args.globs:
        files.extend(glob.glob(g))

    if not files:
        print("No files matched.")
        return 1

    segs = gather_segments(files)
    bulk_index(segs, args.index)
    print(f"Indexed {len(segs)} segments into {args.index}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
