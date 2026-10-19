"""Apply the ingest pipeline and index template, then create yt-trending."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from elasticsearch import Elasticsearch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import ES_INDEX, ES_URL  # noqa: E402

HERE = Path(__file__).resolve().parent
PIPELINE_NAME = "yt-trending-ingest"
TEMPLATE_NAME = "yt-trending-template"


def load_json(name: str) -> dict:
    with (HERE / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def get_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30)


def apply(es: Elasticsearch | None = None) -> None:
    """Idempotent: put pipeline, put template, create index if missing."""
    es = es or get_client()
    if not es.ping():
        raise SystemExit(f"Cannot reach Elasticsearch at {ES_URL}")

    pipeline = load_json("ingest_pipeline.json")
    es.ingest.put_pipeline(id=PIPELINE_NAME, body=pipeline)
    print(f"Upserted ingest pipeline '{PIPELINE_NAME}'")

    template = load_json("index_template.json")
    es.indices.put_index_template(name=TEMPLATE_NAME, body=template)
    print(f"Upserted index template '{TEMPLATE_NAME}'")

    if es.indices.exists(index=ES_INDEX):
        print(f"Index '{ES_INDEX}' already exists")
        return

    es.indices.create(index=ES_INDEX)
    print(f"Created index '{ES_INDEX}'")


if __name__ == "__main__":
    apply()
