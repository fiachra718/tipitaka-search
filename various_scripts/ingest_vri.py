import os, glob, base64, asyncio, aiofiles, re
from elasticsearch import AsyncElasticsearch, helpers
from meta_vri import parse_meta

ES_URL    = os.getenv("ES_URL", "http://localhost:9200")
ES_USER   = os.getenv("ES_USER", "elastic")
ES_PASS   = os.getenv("ES_PASS", "changeme")
INDEX     = os.getenv("ES_INDEX", "pali-xml")
PIPELINE  = os.getenv("ES_PIPELINE", "xml_attach")
GLOB_PAT  = os.getenv("XML_GLOB", "/home/andrew/tipitaka-xml/romn/*.xml")
CONCURRENCY = int(os.getenv("CONCURRENCY", "32"))


async def make_action(path: str) -> dict:
    """Read one XML file, extract meta via parse_meta(), send base64 for Tika."""
    async with aiofiles.open(path, "rb") as f:
        data = await f.read()
    meta = parse_meta(data, path)
    # keep only non-None fields
    for k in list(meta.keys()):
        if meta[k] is None:
            del meta[k]
    return {
        "_index": INDEX,
        "_id": os.path.relpath(path),                 # stable id = relative path
        "path": path,
        **meta,                                       # basket/book/book_no/verse_no/notes/...
        "data": base64.b64encode(data).decode("ascii")# pipeline reads "data"
    }

async def action_stream(paths: list[str]):
    """Concurrency-limited async generator of bulk actions."""
    sem = asyncio.Semaphore(CONCURRENCY)
    async def one(p):
        async with sem:
            return await make_action(p)
    tasks = [asyncio.create_task(one(p)) for p in paths]
    for t in asyncio.as_completed(tasks):
        yield await t

async def main():
    paths = glob.glob(GLOB_PAT)
    if not paths:
        print("No files found")
        return
    async with AsyncElasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS)) as es:
        ok, errors = await helpers.async_bulk(
            es,
            action_stream(paths),
            pipeline=PIPELINE,  # <-- runs ingest-attachment on "data"
            raise_on_error=False
        )
        print(f"ok={ok}, errors={len(errors)}")
        if errors:
            # show a sample error item
            print("Sample error:", errors[0])

if __name__ == "__main__":
    asyncio.run(main())
