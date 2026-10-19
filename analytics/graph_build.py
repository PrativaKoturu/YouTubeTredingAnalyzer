"""Build a channel–tag bipartite graph and a tag–tag co-occurrence graph."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import networkx as nx
from elasticsearch import Elasticsearch

from config.settings import ES_INDEX, ES_URL


def fetch_docs(es: Elasticsearch | None = None, size: int = 5000) -> list[dict[str, Any]]:
    es = es or Elasticsearch(ES_URL, request_timeout=30)
    resp = es.search(
        index=ES_INDEX,
        size=size,
        query={"match_all": {}},
        source=["video_id", "title", "tags", "channel_id", "channel_title", "region"],
    )
    return [hit["_source"] | {"_id": hit["_id"]} for hit in resp["hits"]["hits"]]


def bipartite_channel_tag(docs: list[dict[str, Any]]) -> nx.Graph:
    graph = nx.Graph()
    weights: dict[tuple[str, str], int] = defaultdict(int)
    for doc in docs:
        channel = f"channel:{doc.get('channel_id')}"
        title = doc.get("channel_title") or doc.get("channel_id")
        graph.add_node(channel, kind="channel", label=title)
        for tag in doc.get("tags") or []:
            tag_node = f"tag:{tag}"
            graph.add_node(tag_node, kind="tag", label=tag)
            weights[(channel, tag_node)] += 1
    for (u, v), w in weights.items():
        graph.add_edge(u, v, weight=w)
    return graph


def tag_cooccurrence(docs: list[dict[str, Any]]) -> nx.Graph:
    graph = nx.Graph()
    weights: dict[tuple[str, str], int] = defaultdict(int)
    for doc in docs:
        tags = sorted(set(doc.get("tags") or []))
        for tag in tags:
            graph.add_node(f"tag:{tag}", kind="tag", label=tag)
        for i, a in enumerate(tags):
            for b in tags[i + 1 :]:
                key = (f"tag:{a}", f"tag:{b}")
                weights[key] += 1
    for (u, v), w in weights.items():
        graph.add_edge(u, v, weight=w)
    return graph


def directed_channel_to_tag(docs: list[dict[str, Any]]) -> nx.DiGraph:
    graph = nx.DiGraph()
    for doc in docs:
        channel = f"channel:{doc.get('channel_id')}"
        graph.add_node(channel, kind="channel")
        for tag in doc.get("tags") or []:
            tag_node = f"tag:{tag}"
            graph.add_node(tag_node, kind="tag")
            if graph.has_edge(channel, tag_node):
                graph[channel][tag_node]["weight"] += 1
            else:
                graph.add_edge(channel, tag_node, weight=1)
    return graph


def main() -> None:
    docs = fetch_docs()
    bi = bipartite_channel_tag(docs)
    tags = tag_cooccurrence(docs)
    print(f"docs={len(docs)} bipartite_n={bi.number_of_nodes()} e={bi.number_of_edges()}")
    print(f"tag-tag n={tags.number_of_nodes()} e={tags.number_of_edges()}")


if __name__ == "__main__":
    main()
