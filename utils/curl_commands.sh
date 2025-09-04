

# 1) attachment pipeline (requires ingest-attachment plugin)
curl -u elastic:changeme -X PUT 'http://localhost:9200/_ingest/pipeline/xml_attach' \
  -H 'Content-Type: application/json' -d '{
  "description":"Extract text from XML via Tika",
  "processors":[
    { "attachment": { "field":"data", "indexed_chars": -1 } },
    { "remove": { "field":"data" } }
  ]
}'

# 2) index with diacritic-friendly analyzer
curl -u elastic:changeme -X PUT 'http://localhost:9200/pali-xml' \
  -H 'Content-Type: application/json' -d '{
  "settings": { "analysis": { "analyzer": {
    "pali": { "type":"custom", "tokenizer":"standard", "filter":["lowercase","asciifolding"] }
  }}},
  "mappings": { "properties": {
    "path": { "type":"keyword" },
    "attachment": { "properties": {
      "content": { "type":"text", "analyzer":"pali" },
      "language": { "type":"keyword" },
      "content_length": { "type":"integer" }
    }}
  }}
}'

curl -u elastic:changeme -s http://localhost:9200/tipitaka_segments/_mapping | jq
curl -u elastic:changeme -s http://localhost:9200/tipitaka_segments/_settings | jq


curl -u elastic:changeme 'http://localhost:9200/pali-xml/_search' \
  -H 'Content-Type: application/json' -d '{
  "query": { "match": { "attachment.content": "evam me sutam" } },
  "_source": ["path"]
}'

curl -u elastic:changeme -X PUT "http://localhost:9200/_ingest/pipeline/xml_attach" \
  -H 'Content-Type: application/json' -d '{
    "processors": [
      {
        "attachment": {
          "field": "data",
          "indexed_chars": -1,
          "ignore_failure": true,
          "properties": [ "content", "title" ]
        }
      },
      { "set": { "field": "text",  "copy_from": "attachment.content", "ignore_empty_value": true } },
      { "set": { "field": "title", "copy_from": "attachment.title",   "ignore_empty_value": true } },
      { "remove": { "field": "attachment", "ignore_missing": true } }
    ]
  }'


# How many docs made it?
curl -u elastic:changeme -s http://localhost:9200/canon_segments/_count | jq

# Peek at a couple of Pāli docs
curl -u elastic:changeme -s http://localhost:9200/canon_segments/_search -H 'Content-Type: application/json' -d '{
  "size": 2,
  "query": { "term": { "kind": "pali" } }
}' | jq '.hits.hits[]._source | {book,sutta_id,basket,lang,source_file}'

# Search for a Pāli term
curl -u elastic:changeme -s http://localhost:9200/canon_segments/_search -H 'Content-Type: application/json' -d '{
  "size": 5,
  "query": {
    "bool": {
      "filter": [{ "term": { "kind": "pali" }}],
      "must":   [{ "match": { "text": "dukkha" }}]
    }
  }
}' | jq '.hits.hits[]._source | {book,sutta_id,excerpt:.text[0:140]}'


curl -u elastic:changeme -s http://localhost:9200/canon_segments/_search -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": { "by_book": { "terms": { "field": "book", "size": 50 } } }
}' | jq '.aggregations.by_book.buckets[0:20]'


curl -u elastic:changeme -s http://localhost:9200/canon_segments/_search -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": { "by_basket": { "terms": { "field": "basket", "size": 10 } } }
}' | jq '.aggregations.by_basket.buckets'

curl -u elastic:changeme -XPUT "http://localhost:9200/tipitaka_segments" \
  -H 'Content-Type: application/json' -d '{
  "settings": {
    "analysis": {
      "analyzer": {
        "pali_text": {
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "pali_syns"]
        }
      },
      "filter": {
        "pali_syns": {
          "type": "synonym",
          "lenient": true,
          "synonyms": [
            "attha,atthakatha",
            "mula,mulika",
            "tika,ṭīkā",
            "samyutta,saṃyutta,saṁyutta",
            "anguttara,aṅguttara"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "basket":           { "type": "keyword" },
      "collection":       { "type": "keyword" },
      "text_layer":       { "type": "keyword" },
      "book":             { "type": "text", "analyzer": "pali_text" },
      "chapter":          { "type": "text", "analyzer": "pali_text" },
      "title":            { "type": "text", "analyzer": "pali_text" },
      "subhead":          { "type": "text", "analyzer": "pali_text" },
      "hierarchy": {
        "type": "nested",
        "properties": {
          "type": { "type": "keyword" },
          "id":   { "type": "keyword" },
          "head": { "type": "text", "analyzer": "pali_text" }
        }
      },
      "canonical_scheme": { "type": "keyword" },
      "canonical_ref":    { "type": "keyword" },
      "work_id":          { "type": "keyword" },
      "div_id":           { "type": "keyword" },
      "segment_id":       { "type": "keyword" },
      "order":            { "type": "integer" },
      "para_no":          { "type": "keyword" },
      "rend":             { "type": "keyword" },
      "edition_pages": {
        "type": "nested",
        "properties": {
          "ed": { "type": "keyword" },
          "n":  { "type": "keyword" }
        }
      },
      "lang":             { "type": "keyword" },
      "text":             { "type": "text", "analyzer": "pali_text" },
      "html":             { "type": "text", "index": false },
      "source_file":      { "type": "keyword" },
      "source_path":      { "type": "keyword", "index": false }
    }
  }
}'


curl -u elastic:changeme -X PUT "http://localhost:9200/tipitaka_segments" \
  -H 'Content-Type: application/json' \
  -d '{
    "settings": {
      "index": {
        "analysis": {
          "analyzer": {
            "pali_text": { "type": "standard", "stopwords": "_none_" }
          }
        }
      }
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "basket":        { "type": "keyword" },
        "collection":    { "type": "keyword" },
        "work_id":       { "type": "keyword" },
        "div_id":        { "type": "keyword" },
        "title":         { "type": "text" },
        "section_path":  { "type": "keyword" },
        "section_parts": { "type": "keyword" },
        "segment_id":    { "type": "keyword" },
        "order":         { "type": "integer" },
        "para_no":       { "type": "keyword" },
        "rend":          { "type": "keyword" },
        "edition_pages": {
          "type": "nested",
          "properties": {
            "ed": { "type": "keyword" },
            "n":  { "type": "keyword" }
          }
        },
        "lang":          { "type": "keyword" },
        "text":          { "type": "text", "analyzer": "pali_text" },
        "html":          { "type": "text", "index": false },
        "source_file":   { "type": "keyword" },
        "source_path":   { "type": "keyword", "index": false }
      }
    }
  }'

curl -u elastic:changeme -XDELETE http://localhost:9200/tipitaka_segments


curl -u elastic:changeme -XPUT "http://localhost:9200/tipitaka_segments" \
 -H 'Content-Type: application/json' -d '{
  "settings": {
    "analysis": {
      "analyzer": {
        "pali_text": {
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "pali_syns"]
        }
      },
      "filter": {
        "pali_syns": {
          "type": "synonym", "lenient": true,
          "synonyms": ["attha,atthakatha","mula,mulika","tika,ṭīkā","samyutta,saṃyutta,saṁyutta","anguttara,aṅguttara"]
        }
      }
    }
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "text_layer":   { "type": "keyword" },
      "basket":       { "type": "keyword" },
      "collection_hint": { "type": "keyword" },
      "work_hint":    { "type": "keyword" },
      "nikaya_banner_text": { "type": "text", "analyzer": "pali_text" },

      "book":   { "type": "text", "analyzer": "pali_text" },
      "chapter":{ "type": "text", "analyzer": "pali_text" },
      "title":  { "type": "text", "analyzer": "pali_text" },
      "subhead":{ "type": "text", "analyzer": "pali_text" },

      "hierarchy": {
        "type": "nested",
        "properties": {
          "type": { "type": "keyword" },
          "id":   { "type": "keyword" },
          "head": { "type": "text", "analyzer": "pali_text" }
        }
      },
      "edition_pages": {
        "type": "nested",
        "properties": { "ed": { "type": "keyword" }, "n": { "type": "keyword" } }
      },

      "div_id":       { "type": "keyword" },
      "segment_id":   { "type": "keyword" },
      "order":        { "type": "integer" },
      "para_no":      { "type": "keyword" },
      "rend":         { "type": "keyword" },
      "lang":         { "type": "keyword" },

      "text":         { "type": "text", "analyzer": "pali_text" },
      "html":         { "type": "text", "index": false },

      "source_file":  { "type": "keyword" },
      "source_path":  { "type": "keyword", "index": false }
    }
  }
}'

curl -u elastic:changeme -XPUT "http://localhost:9200/bilara_seg_pli_sujato" \
  -H 'Content-Type: application/json' -d '{
  "settings": {
    "analysis": {
      "analyzer": {
        "pali_text": {
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "pali_syns"]
        }
      },
      "filter": {
        "pali_syns": {
          "type": "synonym",
          "lenient": true,
          "synonyms": [
            "samyutta,saṃyutta,saṁyutta",
            "anguttara,aṅguttara",
            "satipatthana,satipaṭṭhāna",
            "nibbana,nibbāna"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "segment_id":   { "type": "keyword" },     // e.g. "mn10:1.2"
      "scheme":       { "type": "keyword" },     // "MN" | "SN" | "DN" | "AN" | ...
      "work_id":      { "type": "keyword" },     // e.g. "mn10"
      "work_number":  { "type": "keyword" },     // e.g. "10" or "22.59"
      "lang":         { "type": "keyword" },     // "en" | "pli"
      "translator":   { "type": "keyword" },     // e.g. "sujato"
      "is_title":     { "type": "boolean" },     // true for keys like mn10:0.x
      "seq":          { "type": "integer" },     // sortable sequence (precomputed)
      "section":      { "type": "keyword" },     // optional (1.1, 4.0.2 ...)
      "text":         { "type": "text", "analyzer": "pali_text" },  // for en index
      "source_file":  { "type": "keyword", "index": false }
    }
  }
}'



curl -u elastic:changeme -X PUT "http://localhost:9200/bilara_seq_en_sujato" \
  -H 'Content-Type:application/json' -d'{
  "segment_id": "mn10:1.2",
  "scheme": "MN",
  "work_id": "mn10",
  "work_number": "10",
  "lang": "en",
  "translator": "sujato",
  "is_title": false,
  "seq": 1020,
  "section": "1.2",
  "text": "At one time the Buddha was staying in the land of the Kurus, near the Kuru town named Kammāsadamma.",
  "source_file": "mn10_translation-en-sujato.json"
  }'




  # ================= create ES index for bilara
# (Optional) delete first if you want a clean slate:
curl -u elastic:changeme -XDELETE "http://localhost:9200/bilara_segments"

curl -u elastic:changeme -XPUT "http://localhost:9200/bilara_segments" \
-H 'Content-Type: application/json' -d'{
  "mappings": {
    "properties": {
      "segment_id":   { "type": "keyword" },
      "segment_num":  { "type": "keyword" },
      "seq":          { "type": "integer" },
      "is_title":     { "type": "boolean" },

      "basket":       { "type": "keyword" },              // sutta | vinaya | abhidhamma
      "collection":   { "type": "keyword" },              // DN/MN/SN/AN/KN or vinaya code
      "vagga":        { "type": "keyword" },              // when present
      "sutta":        { "type": "keyword" },              // e.g., mn10
      "sutta_num":    { "type": "integer" },

      "division_code":{ "type": "keyword" },              // e.g., kd
      "division_num": { "type": "integer" },
      "chapter_num":  { "type": "integer" },
      "section_num":  { "type": "integer" },

      "translator":   { "type": "keyword" },
      "lang":         { "type": "keyword" },

      "is_gatha":     { "type": "boolean" },
      "gatha_no":     { "type": "integer" },
      "gatha_line":   { "type": "integer" },

      "text":         { "type": "text" },                 // active layer’s text
      "variants": {
        "type": "nested",
        "properties": {
          "layer":       { "type": "keyword" },           // root | translation
          "lang":        { "type": "keyword" },
          "translator":  { "type": "keyword" },
          "text":        { "type": "text" },
          "source_file": { "type": "keyword" }
        }
      },

      "titles": {
        "type": "object",
        "properties": {
          "0.1": { "type": "text" }, "0.2": { "type": "text" }, "0.3": { "type": "text" }, "0.4": { "type": "text" }
        }
      }
    }
  }
}'


