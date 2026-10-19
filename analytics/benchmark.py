"""Timing and accuracy benchmarks for the report."""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analytics.mapreduce_wordcount import wordcount  # noqa: E402
from config.settings import ES_INDEX, ES_URL  # noqa: E402
from streaming_algos.dgim import DGIM  # noqa: E402
from streaming_algos.flajolet_martin import FlajoletMartin  # noqa: E402


def timed(label: str, fn) -> object:
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"{label:40s} {elapsed*1000:8.1f} ms")
    return result


def load_docs(es: Elasticsearch) -> list[dict]:
    resp = es.search(index=ES_INDEX, size=8000, query={"match_all": {}})
    return [h["_source"] | {"_id": h["_id"]} for h in resp["hits"]["hits"]]


def pandas_views_by_category(docs: list[dict]) -> pd.Series:
    frame = pd.DataFrame(docs)
    return frame.groupby("category_name")["views"].sum().sort_values(ascending=False)


def mapreduce_style_views(docs: list[dict]) -> list[tuple[str, int]]:
    mapped = [(d.get("category_name") or "Unknown", int(d.get("views") or 0)) for d in docs]
    grouped: dict[str, list[int]] = defaultdict(list)
    for key, value in mapped:
        grouped[key].append(value)
    reduced = [(k, sum(v)) for k, v in grouped.items()]
    reduced.sort(key=lambda kv: kv[1], reverse=True)
    return reduced


def es_views_by_category(es: Elasticsearch) -> dict:
    body = {
        "size": 0,
        "aggs": {
            "by_category": {
                "terms": {"field": "category_name", "size": 20},
                "aggs": {"total_views": {"sum": {"field": "views"}}},
            }
        },
    }
    return es.search(index=ES_INDEX, body=body).body


def main() -> None:
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        raise SystemExit(f"Cannot reach Elasticsearch at {ES_URL}")
    docs = load_docs(es)
    if not docs:
        raise SystemExit("Index is empty. Run make replay && make consumer first.")

    print(f"Benchmark on {len(docs)} documents\n")
    timed("Elasticsearch terms+sum aggregation", lambda: es_views_by_category(es))
    timed("pandas groupby views per category", lambda: pandas_views_by_category(docs))
    timed("MapReduce-style Python aggregation", lambda: mapreduce_style_views(docs))
    timed("MapReduce word count on titles", lambda: wordcount([d.get("title") or "" for d in docs]))

    channels = [d["channel_id"] for d in docs]
    exact = len(set(channels))
    fm = FlajoletMartin()
    for ch in channels:
        fm.add(ch)
    card = es.search(
        index=ES_INDEX,
        size=0,
        aggs={"c": {"cardinality": {"field": "channel_id", "precision_threshold": 40000}}},
    )
    hll = card["aggregations"]["c"]["value"]
    print("\nDistinct channels")
    print(f"  exact={exact}  FM={fm.estimate():.1f}  ES-HLL={hll}")

    print("\nDGIM vs exact ones-count")
    bits = [1] * len(docs)
    for window in (10, 50, 100, min(250, len(docs))):
        dgim = DGIM(window)
        for bit in bits:
            dgim.add_bit(bit)
        exact_w = min(window, len(bits))
        print(f"  window={window:4d}  exact={exact_w:4d}  DGIM={dgim.estimate(window):6.1f}")

    sentiment = pd.DataFrame(docs)
    if "likes" in sentiment and "views" in sentiment:
        sentiment["likes_ratio"] = sentiment["likes"] / sentiment["views"].clip(lower=1)
        grouped = sentiment.groupby("category_name")[["sentiment", "likes_ratio"]].mean()
        print("\nAverage sentiment and likes ratio by category")
        print(grouped.sort_values("sentiment", ascending=False).head(10).to_string())
        corr = sentiment["sentiment"].corr(sentiment["likes_ratio"])
        print(f"\ncorr(sentiment, likes/views) = {corr}")


if __name__ == "__main__":
    main()
