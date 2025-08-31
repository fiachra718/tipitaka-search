#!/usr/bin/env python3
from elasticsearch import Elasticsearch
import os

ES_URL    = os.getenv("ES_URL", "http://localhost:9200")
ES_USER   = os.getenv("ES_USER", "elastic")
ES_PASS   = os.getenv("ES_PASS", "changeme")
INDEX = "canon_segments"

MAPPING = {
    "settings": {
        "analysis": {
            "analyzer": {
                "english_shingles": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "shingle"]
                }
            },
            "normalizer": {
                "lc": {"type": "custom", "filter": ["lowercase"]}
            }
        }
    },
    "mappings": {
        "properties": {
            # identity
            "kind": {"type": "keyword"},              # "pali" | "translation"
            "book": {"type": "keyword", "normalizer": "lc"},   # MN, DN, SN, etc
            "sutta_id": {"type": "keyword"},          # mn10, sn56.11, etc
            "seg_id": {"type": "keyword"},            # segment key (if available)
            "edition": {"type": "keyword"},           # e.g. "vri"
            "basket": {"type": "keyword"},            # sutta/vinaya/abhidhamma

            # language / people
            "lang": {"type": "keyword"},              # pli/en
            "translator": {"type": "keyword"},        # e.g. nyanamoli, bodhi

            # content
            "title": {"type": "text",
                      "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "text": {"type": "text", "analyzer": "english"},
            "text_shingles": {"type": "text", "analyzer": "english_shingles"},
            "raw_html": {"type": "text", "index": False},
            "raw_xml": {"type": "text", "index": False},

            # linkage (for alignment/facets)
            "parallel_of": {"type": "keyword"},       # ES _id of source Pāli segment/doc (if 1:1)
            "parallels": {"type": "keyword"},         # list of related ids (n:1 or n:n)

            # provenance
            "source_url": {"type": "keyword"},
            "source_file": {"type": "keyword"},
            "source_repo": {"type": "keyword"},
            "date_indexed": {"type": "date"}
        }
    }
}

if __name__ == "__main__":
    es = Elasticsearch(hosts=ES_URL, basic_auth=(ES_USER, ES_PASS))
    if es.indices.exists(index=INDEX):
        print(f"{INDEX} already exists")
    else:
        es.indices.create(index=INDEX, body=MAPPING)
        print(f"Created {INDEX}")
