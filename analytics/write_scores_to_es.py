"""Partial-update Elasticsearch documents with graph scores."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config.settings import ES_INDEX, ES_URL


def channel_scores_from_nodes(
    pagerank: dict[str, float],
    betweenness: dict[str, float],
    communities: dict[str, int],
) -> dict[str, dict[str, Any]]:
    """Map graph node ids like 'channel:UCxxx' back to channel_id."""
    out: dict[str, dict[str, Any]] = defaultdict(dict)
    for node, value in pagerank.items():
        if node.startswith("channel:"):
            out[node.split(":", 1)[1]]["pagerank"] = float(value)
    for node, value in betweenness.items():
        if node.startswith("channel:"):
            out[node.split(":", 1)[1]]["betweenness"] = float(value)
    for node, value in communities.items():
        if node.startswith("channel:"):
            out[node.split(":", 1)[1]]["community_id"] = int(value)
        elif node.startswith("tag:"):
            continue
    return dict(out)


def write_channel_scores(
    docs: list[dict[str, Any]],
    channel_scores: dict[str, dict[str, Any]],
    es: Elasticsearch | None = None,
) -> int:
    es = es or Elasticsearch(ES_URL, request_timeout=30)
    actions = []
    for doc in docs:
        cid = doc.get("channel_id")
        scores = channel_scores.get(cid)
        if not scores:
            continue
        actions.append(
            {
                "_op_type": "update",
                "_index": ES_INDEX,
                "_id": doc["_id"],
                "doc": {
                    "pagerank": scores.get("pagerank", 0.0),
                    "betweenness": scores.get("betweenness", 0.0),
                    "community_id": scores.get("community_id", -1),
                },
            }
        )
    if not actions:
        print("No scores to write.")
        return 0
    success, errors = bulk(es, actions, raise_on_error=False)
    if errors:
        print(f"Write-back had {len(errors)} errors, first={errors[0]}")
    print(f"Updated {success} documents with graph scores.")
    return success


def main() -> None:
    print("Use analytics/run_all.py to compute scores and write them back.")


if __name__ == "__main__":
    main()
