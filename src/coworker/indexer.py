import hashlib, json, os
from pathlib import Path
from .config import CFG, DATA_DIR, PROJECTS_ROOT

CHUNK,OVERLAP = 4096, 64 * 1024  # 4096 bytes = 4 KB, 64 KB overlap, tokens: ~500-1000 tokens depending on content
EMBED_MODEL = "BAAI/bge-small-en-v1.5"  # BAAI/bge-small-en-v1.5 | BAAI/bge-large-en-v1.5 | BAAI/bge-large-en-v1.5-zh
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
MANIFEST = DATA_DIR / "manifest.json"
_model = None  # lazy load model to avoid unnecessary imports

def collection():
    import chromadb
    client = chromadb.PersistentClient(path=str(DATA_DIR / "chroma"))
    return client.get_or_create_collection("projects", metadata={"hnsw:space": "cosine"})


def index_all():
    col = collection()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    seen, stats = set(), {"indexed": 0, "skipped": 0, "errors": 0}