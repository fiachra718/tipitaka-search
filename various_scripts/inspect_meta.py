#!/usr/bin/env python3
import os, sys, re
from lxml import etree as ET

NIKAYA_MAP = {
    "dīghanikāyo": ("sutta", "DN"),
    "majjhimanikāye": ("sutta", "MN"),
    "saṃyuttanikāye": ("sutta", "SN"),
    "saṁyuttanikāye": ("sutta", "SN"),
    "aṅguttaranikāye": ("sutta", "AN"),
    "khuddakanikāye": ("sutta", "KN"),
    "vinayapiṭake": ("vinaya", None),
    "vinayapitake": ("vinaya", None),
    "abhidhammapiṭake": ("abhidhamma", None),
    "abhidhammapitake": ("abhidhamma", None),
}

def infer_layer(path):
    name = os.path.basename(path).lower()
    if name.endswith(".mul.xml"): return "mula"
    if name.endswith(".att.xml"): return "atthakatha"
    if name.endswith(".tik.xml"): return "tika"
    return "unknown"

def inspect_file(path):
    tree = ET.parse(path)
    root = tree.getroot()
    # basket / collection
    basket = collection = None
    nikaya_texts = root.xpath("//p[@rend='nikaya']/text()")
    if nikaya_texts:
        t = " ".join(nikaya_texts).lower()
        for key,(b,c) in NIKAYA_MAP.items():
            if key in t:
                basket, collection = b, c
                break

    # book / chapter / subhead
    book = next((h.text for h in root.xpath("//head[@rend='book']") if h.text), None)
    chapter = next((h.text for h in root.xpath("//head[@rend='chapter']") if h.text), None)
    sutta = next((h.text for h in root.xpath("//p[@rend='subhead']") if h.text), None)

    # first body paragraph
    para = next((p.text for p in root.xpath("//p[@rend='bodytext']") if p.text), None)

    layer = infer_layer(path)

    return {
        "file": os.path.basename(path),
        "basket": basket,
        "collection": collection,
        "layer": layer,
        "book": book.strip() if book else None,
        "chapter": chapter.strip() if chapter else None,
        "subhead": sutta.strip() if sutta else None,
        "sample_para": para.strip() if para else None,
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: inspect_meta.py <xml files>")
        sys.exit(1)
    for fname in sys.argv[1:]:
        try:
            info = inspect_file(fname)
            print(info)
        except Exception as e:
            print(f"Error parsing {fname}: {e}")
