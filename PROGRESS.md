# Progress log — YouTube Trending Analyzer

This file is the team changelog. Update it when you finish a slice of work.

Last updated: 2026-10-19

---

## Status snapshot

| Stage | Name | Status |
|-------|------|--------|
| 1 | Repo skeleton, Docker, Makefile, config, schemas | Done |
| 2 | Elasticsearch template, ingest pipeline, `create_index.py` | Done |
| 3 | Producer (API + CSV replay) | Done |
| 4 | Streaming algorithms, tests, consumer | Done |
| 5 | Saved ES queries + runner + FM vs cardinality | Done |
| 6 | Analytics jobs + write scores back | Done |
| 7 | Benchmarks, Streamlit, README, remaining docs | Done |

Core pipeline can be demoed **offline** with `make replay` + `make consumer` (no YouTube quota). Live API needs `YOUTUBE_API_KEY` in `.env`.

---

## What is implemented

### Ingestion

- Docker Compose: Kafka 3.8 (KRaft, no ZooKeeper), Elasticsearch 8.15.3, Kibana 8.15.3.
- Security disabled **only** for local development (commented in `docker-compose.yml`).
- Kafka topics `yt.trending.raw` (3 partitions) and `yt.trending.dead` (1 partition).
- YouTube Data API v3 client with paging, retries, exponential backoff, quota handling.
- CSV replay from `data/sample/sample_trending.csv` with configurable delay.

### Processing

- Pydantic validation of raw events; failures go to the dead-letter topic.
- Bloom filter de-duplication on `video_id + region + date`.
- Cleaning: ISO-8601 duration, tags, category names, nulls.
- VADER sentiment on title → `sentiment` and `sentiment_label`.
- Flajolet–Martin on `channel_id`; DGIM on a stream of ones (video seen).
- Bulk index batches of 200; Kafka offset commit **after** a successful bulk.

### Elasticsearch

- Explicit mapping, `dynamic: strict`.
- Document `_id` = `{video_id}_{region}_{YYYY-MM-DD}` (idempotent reruns).
- Ingest pipeline: lowercase tags, trim title, set `ingested_at`.
- Query JSON files under `elastic/queries/` plus `elastic/run_queries.py`.

### Analytics

- MapReduce-style word count with map / combiner / shuffle / reduce.
- TF-IDF + cosine similarity from scratch (plus sklearn comparison).
- Channel–tag bipartite graph, tag–tag projection.
- PageRank with taxation (power iteration) vs `networkx.pagerank`.
- HITS hubs/authorities.
- Degree, closeness, betweenness, Louvain communities.
- Partial updates of scores into Elasticsearch.

### Demo / report support

- Kibana saved-objects export: `elastic/kibana_export.ndjson`.
- Streamlit search + similar videos.
- Benchmarks: ES agg vs pandas vs MapReduce; FM vs ES cardinality; DGIM vs exact.
- Tests: Bloom, FM, DGIM, cleaning, TF-IDF.
- Docs: architecture notes, report outline, Elasticsearch talk, viva questions.
- Team operating manual: `TEAM_GUIDE.md`.

---

## Not in this repo (and why)

| Item | Reason |
|------|--------|
| Real YouTube API key | Secrets stay in `.env` (git-ignored) |
| Full Kaggle dump in git | Too large; put files in `data/raw/` locally |
| Spark | Explicitly out of the core pipeline |
| Production Elasticsearch security | Disabled only for local Docker |

---

## How to tick a new item

Add a dated bullet under **Next updates**, then move it into the right section above.

Example:

```
### Next updates
- 2026-10-20 C: imported Kibana dashboard on campus Docker
```
