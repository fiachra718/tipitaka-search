#!/usr/bin/env python3
import sys, os, json, collections, pathlib
import codecs
from xml.etree import ElementTree as ET

def sniff_encoding(path):
    # Fast sniff: use the XML declaration if present
    with open(path, 'rb') as f:
        head = f.read(200).decode('latin1', errors='ignore')
    if 'encoding="' in head:
        enc = head.split('encoding="',1)[1].split('"',1)[0]
        return enc
    return 'utf-8'  # fallback

def qname(elem):
    if elem.tag.startswith('{'):
        ns, _, local = elem.tag[1:].partition('}')
        return (ns, local)
    return (None, elem.tag)

def path_key(elem):
    parts = []
    while elem is not None:
        ns, local = qname(elem)
        parts.append(local)
        elem = elem.getparent() if hasattr(elem, 'getparent') else None
    return '/'.join(reversed(parts))

def safe_parse(path):
    try:
        # Try the simplest thing first
        tree = ET.parse(path)
        return tree.getroot(), "(auto)", None
    except ET.ParseError:
        pass  # fall through to manual handling

    with open(path, "rb") as f:
        data = f.read()

    enc = "utf-8"
    if data.startswith(codecs.BOM_UTF8):
        enc = "utf-8-sig"
    elif data.startswith(codecs.BOM_UTF16_LE):
        enc = "utf-16-le"
    elif data.startswith(codecs.BOM_UTF16_BE):
        enc = "utf-16-be"

    try:
        # If there is an XML declaration inside, ET will re-interpret;
        # otherwise we provide a correct unicode string.
        text = data.decode(enc, errors="replace")
        root = ET.fromstring(text)
        return root, enc, None
    except ET.ParseError as e:
        return None, enc, str(e)

def walk(e, counts, attr_counts, samples, depth=0):
    ns, local = qname(e)
    counts[local] += 1
    for a,v in e.attrib.items():
        attr_counts[(local,a)][v] += 1
    if (local not in samples) and (e.text and e.text.strip()):
        samples[local] = e.text.strip()[:250]
    for c in list(e):
        walk(c, counts, attr_counts, samples, depth+1)

def main(paths):
    files = []
    for p in paths:
        for path in sorted(pathlib.Path().glob(p)):
            files.append(str(path))
    if not files:
        print("No files matched.", file=sys.stderr)
        sys.exit(1)

    tag_counts = collections.Counter()
    attr_counts = collections.defaultdict(collections.Counter)
    samples = {}
    errors = []
    encodings = collections.Counter()

    for path in files:
        root, enc, err = safe_parse(path)
        encodings[enc] += 1
        if err:
            errors.append({"file": path, "encoding": enc, "error": err})
            continue
        walk(root, tag_counts, attr_counts, samples)

    report = {
        "total_files": len(files),
        "encodings": encodings,
        "top_tags": tag_counts.most_common(40),
        "top_attributes": [
            {"tag": t, "attr": a, "unique_values": len(vals), "some_values": [v for v,_ in vals.most_common(5)]}
            for (t,a), vals in list(attr_counts.items())
        ][:60],
        "example_text_by_tag": samples,
        "parse_errors": errors[:20]
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: inspect_xml.py <glob> [<glob> ...]")
        sys.exit(2)
    main(sys.argv[1:])
