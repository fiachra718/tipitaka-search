import asyncio, os, re, hashlib, datetime
from urllib.parse import urlparse
import httpx
from bs4 import BeautifulSoup
from elasticsearch import AsyncElasticsearch

ES_URL    = os.getenv("ES_URL", "http://localhost:9200")
ES_USER   = os.getenv("ES_USER", "elastic")
ES_PASS   = os.getenv("ES_PASS", "changeme")
PALI_INDEX     = os.getenv("ES_INDEX", "pali-translation")
TRANSLATION_INDEX = os.getenv("TRANSLATION_INDEX", "pali-trans")
CONCURRENCY = int(os.getenv("CONCURRENCY", "32"))

COLLECTION_MAP = {
    "dn" : "Digha Nikaya",
    "mn": "Majjhima Nikaya",
    "sn": "Samyutta Nikaya",
    "an": "Anguttara Nikaya"
}

def sc_url_to_raw(url: str) -> str:
    """
    Convert a GitHub html_text URL to raw content.
    I could do this on the filesystem, but I am a lazy bastard, so, github it is
    THere will be translations from access to Insight and elsewhere
    e.g. https://github.com/.../blob/main/html_text/... -> https://raw.githubusercontent.com/.../main/html_text/...
    """
    pu = urlparse(url)
    if pu.netloc == "github.com" and "/blob/" in pu.path:
        owner, repo, _, branch, *rest = pu.path.strip("/").split("/")
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/" + "/".join(rest)
    return url

