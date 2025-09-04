#!/usr/bin/env python3
"""
Drop-in async ingester for VRI/TEI XML → Elasticsearch.

- Reads XML bytes (UTF-16 OK) and passes base64 to an ingest-attachment pipeline.
- Uses your meta_vri.parse_meta(data, path) to extract book/nikaya/etc from XML.
- Emits one doc per file (doc-level). Per-segment fan-out can be added later.

Env vars:
  ES_URL, ES_USER, ES_PASS
  ES_INDEX (default: canon_segments)
  ES_PIPELINE (default: xml_attach)
  XML_GLOB (e.g., /home/andrew/tipitaka-xml/romn/*.xml)
  CONCURRENCY (default: 32)
"""

import os, glob, base64, asyncio, aiofiles, datetime as dt
from typing import Dict, Any, List
from elasticsearch import AsyncElasticsearch, helpers
from meta_vri import parse_meta  # must return the fields used below

ES_URL       = os.getenv("ES_URL", "http://localhost:9200")
ES_USER      = os.getenv("ES_USER", "elastic")
ES_PASS      = os.getenv("ES_PASS", "changeme")
INDEX        = os.getenv("ES_INDEX", "canon_segments")     # unified index
PIPELINE     = os.getenv("ES_PIPELINE", "xml_attach")      # ingest-attachment pipeline
GLOB_PAT     = os.getenv("XML_GLOB", "/home/andrew/tipitaka-xml/romn/*.xml")
CONCURRENCY  = int(os.getenv("CONCURRENCY", "32"))

def _now_iso() -> str:
    return dt.datetime.utcnow().isoformat()

def _clean(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

def _compute_id(meta: Dict[str, Any], path: str) -> str:
    # Stable-ish per-file doc id: "{BOOK}:{FILENAME_STEM}:doc"
    book = meta.get("book") or "UNK"
    stem = meta.get("sutta_id") or os.path.basename(path).removesuffix(".xml")
    return f"{book}:{stem}:doc"

async def make_action(path: str) -> dict:
    """Read one XML file, extract meta via parse_meta(), send base64 for Tika."""
    async with aiofiles.open(path, "rb") as f:
        data = await f.read()

    meta = parse_meta(data, path) or {}
    meta = _clean(meta)

    # Build document matching the canon_segments schema (doc-level)
    doc = {
        # identity
        "kind": "pali",
        "book": meta.get("book"),               # e.g., "MN" / "SN" / "VIN" / "Bv" / etc
        "sutta_id": meta.get("sutta_id"),       # using file stem for VRI corpus
        "seg_id": None,                         # doc-level; per-seg fan-out can set this
        "edition": meta.get("edition", "vri"),
        "basket": meta.get("basket"),           # "sutta" | "vinaya" | "abhidhamma"
        # language / transliteration
        "lang": meta.get("lang", "pli"),
        "translator": None,                     # not applicable to Pāli source
        # content (ingest pipeline should fill "text" & maybe "title")
        "title": None,
        "text": None,
        "text_shingles": None,                  # optional (can be set by an ingest processor)
        "raw_xml": None,                        # skip storing full XML to avoid bloat
        # linkage
        "parallel_of": None,
        "parallels": [],
        # provenance
        "source_url": meta.get("source_url"),
        "source_file": meta.get("source_file") or os.path.relpath(path),
        "source_repo": meta.get("source_repo") or "tipitaka_vri",
        "nikaya_raw": meta.get("nikaya_raw"),   # keep raw strings for QA / later mapping
        "book_raw": meta.get("book_raw"),
        "date_indexed": _now_iso()
    }

    # Merge any extra fields parse_meta may provide (but don't overwrite known keys)
    for k, v in meta.items():
        if k not in doc:
            doc[k] = v

    _id = _compute_id(doc, path)

    return {
        "_index": INDEX,
        "_id": _id,
        **doc,
        # pipeline input for ingest-attachment
        "data": base64.b64encode(data).decode("ascii")
    }

async def action_stream(paths: List[str]):
    """Concurrency-limited async generator of bulk actions (one per file)."""
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(p: str):
        async with sem:
            return await make_action(p)

    tasks = [asyncio.create_task(one(p)) for p in paths]
    for t in asyncio.as_completed(tasks):
        yield await t

async def main():
    paths = glob.glob(GLOB_PAT)
    if not paths:
        print(f"No files found for pattern: {GLOB_PAT}")
        return

    async with AsyncElasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS)) as es:
        ok, errors = await helpers.async_bulk(
            es,
            action_stream(paths),
            pipeline=PIPELINE,          # runs ingest-attachment on "data"
            raise_on_error=False
        )
        print(f"ok={ok}, errors={len(errors)}")
        if errors:
            print("Sample error:", errors[0])

if __name__ == "__main__":
    asyncio.run(main())
