#!/usr/bin/env python3
"""
Load Bilara JSON (root + translations) into a unified ES index with nested variants.

Usage:
  python bilara_load.py "bilara-data/root/pli/ms/sutta/mn/*.json" \
                        "bilara-data/translation/en/sujato/sutta/mn/*.json" \
    --index bilara_segments

- One doc per segment_id (e.g. "mn10:1.2")
- Variants array contains multiple languages/translators
- Stable _id = segment_id, so reruns upsert safely
"""

import os, re, sys, json, glob, argparse
from collections import defaultdict
from elasticsearch import Elasticsearch, helpers

ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")

SEG_KEY_RE = re.compile(r"^([a-z]+[\d\.]+):(.+)$")  # e.g. mn10:1.2 or sn22.59:3.1

def infer_variant_from_path(path: str):
    """
    Infer (kind, lang, translator) from the bilara path.
    - kind: "root" or "translation"
    - lang: "pli", "en", etc.
    - translator: e.g., "sujato" for translations, None for root
    """
    parts = path.replace("\\", "/").split("/")
    # crude but effective pattern for standard bilara layout
    if "root" in parts:
        return ("root", "pli", None)
    if "translation" in parts:
        try:
            i = parts.index("translation")
            lang = parts[i+1]        # e.g. "en"
            translator = parts[i+2]  # e.g. "sujato"
            return ("translation", lang, translator)
        except Exception:
            return ("translation", None, None)
    return ("unknown", None, None)

def parse_work_id_from_filename(filename: str):
    # e.g., mn10_root-pli-ms.json -> "mn10"
    #       sn22.59_translation-en-sujato.json -> "sn22.59"
    base = os.path.basename(filename)
    m = re.match(r"([a-z]+[\d\.]+)_", base.lower())
    return m.group(1) if m else None

def split_scheme_and_number(work_id: str):
    # "mn10" -> ("MN","10"), "sn22.59" -> ("SN","22.59")
    m = re.match(r"([a-z]+)([\d\.]+)$", work_id)
    if not m:
        return (None, None)
    return (m.group(1).upper(), m.group(2))

def seq_from_section(section: str) -> int:
    """
    Build a sortable integer from a dotted section like "1.2" or "4.0.2".
    We pack up to 3 levels: a*10000 + b*100 + c.
    """
    if not section:
        return 0
    try:
        parts = [int(x) for x in section.split(".")]
    except ValueError:
        return 0
    a = parts[0] if len(parts) > 0 else 0
    b = parts[1] if len(parts) > 1 else 0
    c = parts[2] if len(parts) > 2 else 0
    return a*10000 + b*100 + c

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def gather_segments(filepaths):
    """
    Returns a dict: segments[segment_id] = {
        "segment_id", "scheme", "work_id", "work_number", "section", "is_title", "seq",
        "variants": [ { lang, translator, text, source_file }, ... ]
    }
    """
    segments = {}
    for fp in filepaths:
        kind, lang, translator = infer_variant_from_path(fp)
        data = load_json(fp)
        work_id = parse_work_id_from_filename(fp)
        scheme, work_number = split_scheme_and_number(work_id or "")

        for seg_id, text in data.items():
            m = SEG_KEY_RE.match(seg_id)
            if not m:
                continue
            work_id_key, section = m.group(1), m.group(2)
            # Sanity: ensure consistency between key and filename-derived work_id
            if work_id and work_id_key != work_id:
                # Different sutta in same file? Rare, but keep the key as truth.
                scheme, work_number = split_scheme_and_number(work_id_key)

            is_title = section.startswith("0.")
            seq = seq_from_section(section)

            if seg_id not in segments:
                segments[seg_id] = {
                    "segment_id": seg_id,
                    "scheme": scheme,
                    "work_id": work_id_key,
                    "work_number": work_number,
                    "section": section,
                    "is_title": is_title,
                    "seq": seq,
                    "variants": []
                }

            variant = {
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
    ap.add_argument("globs", nargs="+", help="File globs, e.g. 'bilara-data/root/pli/ms/sutta/mn/*.json' 'bilara-data/translation/en/sujato/sutta/mn/*.json'")
    ap.add_argument("--index", default="bilara_segments", help="Target ES index (default: bilara_segments)")
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
