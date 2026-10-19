"""Run saved Elasticsearch queries and print a compact summary."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from elasticsearch import Elasticsearch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import ES_INDEX, ES_URL  # noqa: E402
from streaming_algos.flajolet_martin import FlajoletMartin  # noqa: E402

QUERY_DIR = Path(__file__).resolve().parent / "queries"


def load(name: str) -> dict:
    with (QUERY_DIR / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def pretty(title: str, body: dict) -> None:
    print("\n===", title, "===")
    print(json.dumps(body, indent=2)[:4000])


def compare_fm(es: Elasticsearch) -> None:
    resp = es.search(
        index=ES_INDEX,
        size=10000,
        source=["channel_id"],
        query={"match_all": {}},
    )
    hits = resp.get("hits", {}).get("hits", [])
    channels = [h["_source"]["channel_id"] for h in hits if h.get("_source")]
    fm = FlajoletMartin()
    for ch in channels:
        fm.add(ch)
    exact = len(set(channels))
    card = es.search(index=ES_INDEX, body=load("agg_cardinality_channels.json"))
    hll = card["aggregations"]["distinct_channels_hll"]["value"]
    est = fm.estimate()
    print("\n=== Flajolet-Martin vs Elasticsearch cardinality ===")
    print(f"documents scanned : {len(channels)}")
    print(f"exact distinct    : {exact}")
    print(f"FM estimate       : {est:.1f}")
    print(f"ES HyperLogLog++  : {hll}")
    if exact:
        print(f"FM error %        : {abs(est - exact) / exact * 100:.2f}")
        print(f"HLL error %       : {abs(hll - exact) / exact * 100:.2f}")


def main() -> None:
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        raise SystemExit(f"Cannot reach Elasticsearch at {ES_URL}")
    if not es.indices.exists(index=ES_INDEX):
        raise SystemExit(f"Index {ES_INDEX} does not exist. Run: make index")

    pretty("category × country", es.search(index=ES_INDEX, body=load("agg_category_country.json")).body)
    pretty("hourly views", es.search(index=ES_INDEX, body=load("agg_hourly_views.json")).body)
    pretty("cardinality", es.search(index=ES_INDEX, body=load("agg_cardinality_channels.json")).body)
    pretty("significant tags", es.search(index=ES_INDEX, body=load("agg_significant_tags.json")).body)
    pretty("filtered search", es.search(index=ES_INDEX, body=load("filtered_search.json")).body)

    sample = es.search(index=ES_INDEX, size=1, query={"match_all": {}})
    hits = sample.get("hits", {}).get("hits", [])
    if hits:
        mlt = load("more_like_this.json")
        mlt["query"]["more_like_this"]["like"][0]["_id"] = hits[0]["_id"]
        mlt["query"]["more_like_this"]["like"][0]["_index"] = ES_INDEX
        pretty(f"more_like_this for {hits[0]['_id']}", es.search(index=ES_INDEX, body=mlt).body)
    else:
        print("Index is empty; run make replay && make consumer first.")

    compare_fm(es)


if __name__ == "__main__":
    main()
