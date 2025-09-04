import os
from flask import Flask, request, render_template, redirect, url_for
from elasticsearch import Elasticsearch

ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")
ES_INDEX = os.getenv("ES_INDEX", "tipitaka_segments")

es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS), request_timeout=15)

app = Flask(__name__)

@app.route("/bilara")
def bilara():
    pass
    # q = request.args.get("q","").strip()
    # scheme =request.args.get("scheme") or None
    # translator = request.args.get("translator") or None
    # work_id = request.args.get("work_id") or None
    # work_number = request.args.get("work_number") or None
    # size = int(request.args.get("size", 20))
    # page = max(1, int(request.args.get("page", 1)))
    # from_ = (page - 1) * size
    #
    # hits, total = [], None
    # if q:
    #     filters = []
    #     if scheme:     filters.append({"term": {"scheme": scheme}})
    #     if translator: filters.append({"term": {"translator": translator}})
    #     if work_id:    filters.append({"term": {"work_id": work_id}})
    #     if work_number:filters.append({"term": {"work_number": work_number}})
    #
    #     body = {
    #         "from": from_, "size": size,
    #         "query": {"bool":{
    #             "filter": filters,
    #             "must": [{"simple_query_string":{
    #                 "query": q, "default_operator":"and",
    #                 "fields": ["text^3","title^3","subhead^2","book^1.5","chapter^1.5"]
    #             }}],
    #
    #
    #         }}
    #
    #     }
    #

@app.route("/")
@app.route("/tipitaka")
def search():
    q = request.args.get("q","").strip()
    basket = request.args.get("basket") or None
    collection = request.args.get("collection") or None
    layer = request.args.get("layer") or None
    size = int(request.args.get("size", 20))
    page = max(1, int(request.args.get("page", 1)))
    from_ = (page-1)*size

    hits, total = [], None
    if q:
        filters = []
        if basket:     filters.append({"term": {"basket": basket}})
        if collection: filters.append({"term": {"collection": collection}})
        if layer:      filters.append({"term": {"text_layer": layer}})

        body = {
          "from": from_, "size": size,
          "query": {"bool":{
            "filter": filters,
            "must": [{"simple_query_string":{
              "query": q, "default_operator":"and",
              "fields": ["title^4","subhead^3","hierarchy.head^2","book^1.5","chapter^1.5","text^3"]
            }}]
          }},
          "highlight": {"pre_tags":["<mark>"],"post_tags":["</mark>"],
            "fields":{"text": {}, "title": {}, "subhead": {}, "hierarchy.head": {}},
            "fragment_size":150,"number_of_fragments":2,"require_field_match":False
          },
          "_source": ["title","subhead","basket","collection","text_layer","text",
                      "source_file","div_id","segment_id","hierarchy"]
        }
        resp = es.search(index=ES_INDEX, body=body)
        hits = resp["hits"]["hits"]
        total = resp["hits"]["total"]["value"]

    return render_template("index.html",
        q=q, hits=hits, total=total, size=size, page=page,
        basket=basket, collection=collection, layer=layer)

if __name__ == "__main__":
    # FLASK_RUN_PORT=5000 flask run  (or just python app.py)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
