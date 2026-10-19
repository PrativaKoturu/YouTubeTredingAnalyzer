"""Streamlit search UI over the yt-trending Elasticsearch index."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import ES_INDEX, ES_URL  # noqa: E402


@st.cache_resource
def es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30)


def search(query: str, region: str, min_views: int, size: int = 20) -> list[dict]:
    must = [{"match": {"title": query}}] if query.strip() else [{"match_all": {}}]
    filters = [{"range": {"views": {"gte": min_views}}}]
    if region != "ALL":
        filters.append({"term": {"region": region}})
    body = {
        "size": size,
        "query": {"bool": {"must": must, "filter": filters}},
        "sort": [{"views": {"order": "desc"}}],
    }
    resp = es_client().search(index=ES_INDEX, body=body)
    rows = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        src["_id"] = hit["_id"]
        src["_score"] = hit.get("_score")
        rows.append(src)
    return rows


def more_like(doc_id: str) -> list[dict]:
    body = {
        "size": 8,
        "query": {
            "more_like_this": {
                "fields": ["title", "description", "tags"],
                "like": [{"_index": ES_INDEX, "_id": doc_id}],
                "min_term_freq": 1,
                "min_doc_freq": 1,
            }
        },
    }
    resp = es_client().search(index=ES_INDEX, body=body)
    return [h["_source"] | {"_id": h["_id"]} for h in resp["hits"]["hits"]]


def main() -> None:
    st.set_page_config(page_title="YouTube Trending Analyzer", layout="wide")
    st.title("YouTube Trending Analyzer")
    st.caption("Search the Elasticsearch index built by the Kafka consumer.")

    es = es_client()
    if not es.ping():
        st.error(f"Cannot reach Elasticsearch at {ES_URL}. Run `make up`.")
        return

    col1, col2, col3 = st.columns(3)
    query = col1.text_input("Text query", value="cricket")
    region = col2.selectbox("Region", ["ALL", "IN", "US", "GB"])
    min_views = col3.number_input("Minimum views", min_value=0, value=1000, step=1000)

    if st.button("Search", type="primary") or True:
        hits = search(query, region, int(min_views))
        st.write(f"{len(hits)} hits")
        if hits:
            table = pd.DataFrame(hits)[
                [
                    "title",
                    "channel_title",
                    "region",
                    "category_name",
                    "views",
                    "sentiment_label",
                    "pagerank",
                ]
            ]
            st.dataframe(table, use_container_width=True)
            chosen = st.selectbox(
                "Show similar videos for",
                options=hits,
                format_func=lambda d: d.get("title", d["_id"]),
            )
            if chosen:
                st.subheader("Elasticsearch more_like_this")
                similar = more_like(chosen["_id"])
                if similar:
                    st.dataframe(
                        pd.DataFrame(similar)[["title", "channel_title", "tags", "views"]],
                        use_container_width=True,
                    )
                else:
                    st.info("No similar documents yet. Index more data.")
        else:
            st.info("No hits. Replay the sample CSV and run the consumer.")


if __name__ == "__main__":
    main()
