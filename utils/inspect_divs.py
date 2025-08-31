#!/usr/bin/env python3
import os, sys, re, json
from lxml import etree as ET

ASCII = dict(zip("āīūṅñṭḍṇḷṁĀĪŪṄÑṬḌṆḶṀ","aiuangtnlmaiuANGNTDNLM"))
def to_ascii(s): return "".join(ASCII.get(ch, ch) for ch in s)

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

HEAD_TRANSLATE = [
    (r"\bvagga\b", "chapter"),
    (r"\bkaṇḍaṃ\b|\bkandam\b", "section"),
    (r"\bpāḷi\b|\bpali\b", "pali-text"),
    (r"\baṭṭhakathā\b|\battakatha\b", "commentary"),
    (r"\bṭīkā\b|\btika\b", "subcommentary"),
    (r"\bsuttaṃ\b|\bsuttam\b", "sutta"),
    (r"\bnidānakathā\b|\bnidanakatha\b", "prologue"),
    (r"\bvaṇṇanā\b|\bvannana\b", "exposition"),
    (r"\bsikkhāpada\b", "training-rule"),
]

def std_label(s):
    if not s: return None
    low, lowa = s.lower(), to_ascii(s.lower())
    for pat, rep in HEAD_TRANSLATE:
        if re.search(pat, low) or re.search(pat, lowa):
            return f"{s.strip()} [{rep}]"
    return s.strip()

def layer_from_filename(p):
    n = os.path.basename(p).lower()
    if n.endswith(".mul.xml"): return "mula"
    if n.endswith(".att.xml"): return "atthakatha"
    if n.endswith(".tik.xml"): return "tika"
    return "unknown"

def parse(p): return ET.parse(p)

def texts(node, xp):
    vals = node.xpath(xp)
    out = []
    for v in vals:
        if isinstance(v, str):
            v = v.strip()
            if v: out.append(v)
        else:
            t = (v.text or "").strip()
            if t: out.append(t)
    return out

def banner(root):
    vals = texts(root, "//p[@rend='nikaya']/text()")
    if not vals: return None, None, None, None
    t = " ".join(vals).strip()
    low, lowa = t.lower(), to_ascii(t.lower())
    b = c = None
    for k,(bb,cc) in NIKAYA_MAP.items():
        if k in low or k in lowa:
            b, c = bb, cc
            break
    return b, c, t, lowa

def nearest_head(div):
    for r in ("title","chapter","book",None):
        xp = f"./head[@rend='{r}']/text()" if r else "./head/text()"
        h = texts(div, xp)
        if h: return h[0]
    for anc in div.iterancestors(tag="div"):
        h = texts(anc, "./head/text()")
        if h: return h[0]
    return None

def collect_divs(root):
    arr = []
    for d in root.xpath("//text//div"):
        did = d.get("id") or d.get("n")
        dtype = d.get("type")
        head = nearest_head(d)
        arr.append({"id": did, "type": dtype, "head": head, "head_en": std_label(head) if head else None})
    return arr

def inspect_one(p):
    root = parse(p).getroot()
    bsk, coll, braw, basc = banner(root)
    info = {
        "file": os.path.basename(p),
        "layer": layer_from_filename(p),
        "banner": {"raw": braw, "ascii": basc, "basket": bsk, "collection": coll},
        "headings": {
            "book": (b:= (texts(root,"//head[@rend='book']/text()") or [None])[0]),
            "book_en": std_label(b) if b else None,
            "chapter": (c:= (texts(root,"//head[@rend='chapter']/text()") or [None])[0]),
            "chapter_en": std_label(c) if c else None,
            "title": (t:= (texts(root,"//head[@rend='title']/text()") or [None])[0]),
            "title_en": std_label(t) if t else None,
            "subheads": (sh:= texts(root,"//p[@rend='subhead']/text()")),
            "subheads_en": [std_label(s) for s in sh] if sh else []
        },
        "sample_para": (texts(root,"//p[@rend='bodytext']/text()") or [None])[0],
        "divs": collect_divs(root)
    }
    return info

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: inspect_divs_json.py <files or globs>")
        sys.exit(2)
    for a in sys.argv[1:]:
        try:
            print(json.dumps(inspect_one(a), ensure_ascii=False))
        except Exception as e:
            print(json.dumps({"file": os.path.basename(a), "error": str(e)}))
