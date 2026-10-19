# YouTube Trending Analyzer — Team Guide

Follow this file from a clean clone. It is the shared operating manual for the NADPCL72 Big Data Computing mini project.

If you only have time to read one document, read this one. Implementation status lives in [`PROGRESS.md`](PROGRESS.md).

---

## 1. What we are building

A pipeline that:

1. Pulls YouTube trending videos (live API **or** Kaggle CSV replay).
2. Streams them through **Kafka**.
3. Cleans and enriches them in Python (Bloom filter, VADER sentiment, Flajolet–Martin, DGIM).
4. Indexes them into **Elasticsearch** with a strict mapping and an ingest pipeline.
5. Runs graph and recommendation analytics and writes scores back to Elasticsearch.
6. Shows results in **Kibana** plus an optional Streamlit search demo.

**Academic goal:** apply syllabus Modules 1–5 on a real stream, and demonstrate Elasticsearch as the content-beyond-syllabus topic.

---

## 2. One-sentence data flow

API or CSV → producer → Kafka `yt.trending.raw` → consumer (validate, Bloom, clean, sentiment, sketches) → Elasticsearch `yt-trending` → analytics jobs enrich the same index → Kibana and Streamlit read from it.

Invalid records go to Kafka `yt.trending.dead`.

---

## 3. Prerequisites

| Tool | Why |
|------|-----|
| Python 3.11+ (3.13 is fine) | Application code |
| Docker Desktop + Docker Compose | Kafka, Elasticsearch, Kibana |
| Make | Shortcut commands |
| A YouTube Data API v3 key | Live producer only (CSV replay works without it) |

Create a virtualenv and install dependencies:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`: put your API key in `YOUTUBE_API_KEY` if you will call the live API. CSV replay does not need a key.

---

## 4. Daily run order (the happy path)

From the repo root, with Docker running:

```bash
make up          # Kafka + Elasticsearch + Kibana
make topics      # create yt.trending.raw and yt.trending.dead
make index       # apply ingest pipeline + index template + create index
make replay      # stream sample CSV into Kafka (offline demo)
make consumer    # process the stream into Elasticsearch
make analytics   # PageRank, TF-IDF, centrality, write scores back
make test        # pytest for Bloom, FM, DGIM, cleaning, TF-IDF
```

Then open:

- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601
- Streamlit (optional): `make search`

Stop everything:

```bash
make down
```

---

## 5. Makefile targets

| Target | What it does |
|--------|----------------|
| `make up` | Start Kafka, Elasticsearch, Kibana |
| `make down` | Stop the stack |
| `make topics` | Create Kafka topics |
| `make index` | Create ingest pipeline and `yt-trending` index |
| `make producer` | Live YouTube API → Kafka |
| `make replay` | Sample CSV → Kafka |
| `make consumer` | Kafka → clean/enrich → Elasticsearch |
| `make analytics` | Graph + recommender + write scores |
| `make queries` | Run saved ES queries and print results |
| `make benchmark` | Timing + FM vs cardinality + DGIM windows |
| `make search` | Streamlit search UI |
| `make test` | `pytest -q` |

---

## 6. Repository map

Do not rename these folders. They match the project architecture.

| Path | Owner idea | Role |
|------|------------|------|
| `config/` | A | Env, regions, category names |
| `schemas/` | A/B | Pydantic models for Kafka events and ES docs |
| `producer/` | A | YouTube client, polling producer, CSV replay |
| `consumer/` | B | Validate, Bloom, clean, sentiment, bulk index |
| `streaming_algos/` | B | Bloom, Flajolet–Martin, DGIM, reservoir (from scratch) |
| `elastic/` | C | Mapping, ingest pipeline, queries, Kibana export |
| `analytics/` | D | MapReduce word count, TF-IDF, graph, scores |
| `search_demo/` | D / optional | Streamlit UI |
| `tests/` | B/D | Algorithm and cleaning tests |
| `data/sample/` | A | Small CSV committed to git |
| `data/raw/` | A | Large Kaggle files (git-ignored) |
| `docs/` | all | Report, viva, Elasticsearch talk |
| `notebooks/` | D | EDA and report figures |

---

## 7. Suggested team split

| Person | Owns |
|--------|------|
| **A — Ingestion** | `docker-compose.yml`, producer, CSV replay, Kafka topics |
| **B — Processing** | consumer, cleaning, sentiment, Bloom, FM, DGIM, tests |
| **C — Elasticsearch** | mapping, ingest pipeline, queries, Kibana, beyond-syllabus talk |
| **D — Analytics** | MapReduce, TF-IDF, graph jobs, benchmarks, report notebook |

- 3 people: merge A into B.
- 2 people: drop HITS, Louvain, and Streamlit if time is short. Core pipeline stays.

---

## 8. Build stages (how the repo was assembled)

Work was implemented in this order so each layer can be tested before the next.

1. **Skeleton** — `requirements.txt`, `.env.example`, Docker, Makefile, `config/`, `schemas/`.
2. **Elasticsearch** — index template, ingest pipeline, `create_index.py`.
3. **Producer** — YouTube client, live producer, CSV replay.
4. **Streaming + consumer** — Bloom, FM, DGIM, cleaning, sentiment, bulk index.
5. **Queries** — aggregations, `more_like_this`, cardinality comparison.
6. **Analytics** — MapReduce, TF-IDF, PageRank/HITS, centrality, write-back.
7. **Demo layer** — benchmarks, Streamlit, README, docs, sample data.

---

## 9. How to demo (viva / presentation)

1. `make up && make topics && make index`
2. `make replay` in one terminal, `make consumer` in another.
3. In Kibana, import `elastic/kibana_export.ndjson` (**Stack Management → Saved Objects → Import**).
4. `make analytics` then refresh the dashboard (PageRank / community panels).
5. `make queries` to show aggregations in the terminal.
6. Optional: `make search` and run three filtered searches.
7. Keep a screen recording. Docker or campus Wi‑Fi can fail on the day.

---

## 10. Syllabus mapping (talking points)

| Module | Where it lives in this repo |
|--------|-----------------------------|
| 1 — 5 Vs of Big Data | `docs/report_outline.md`, README |
| 2 — Kafka producer/consumer, topics, partitions, offsets | `producer/`, `consumer/`, `docker-compose.yml` |
| 2 — NoSQL document store | Elasticsearch mapping (`elastic/index_template.json`) |
| 3 — Bloom, FM, DGIM | `streaming_algos/` + tests |
| 3 — MapReduce idea | `analytics/mapreduce_wordcount.py` vs ES terms agg |
| 4 — PageRank, HITS, social graph | `analytics/pagerank_hits.py`, `graph_build.py` |
| 5 — Recommendation, sentiment | `analytics/tfidf_similarity.py`, `consumer/sentiment.py` |
| Beyond syllabus | `docs/beyond_syllabus_elasticsearch.md` |

ZooKeeper is **not** running in Docker. Kafka uses **KRaft**. In the report, still explain ZooKeeper’s classical role (controller election, cluster metadata) and why KRaft replaces it.

---

## 11. Constraints we must not break

1. Secrets only in `.env`. Never commit a real API key.
2. Elasticsearch security is **disabled for local Docker only**. Comment is in `docker-compose.yml`.
3. Consumer commits Kafka offsets **only after** a successful bulk index.
4. Mapping is explicit with `dynamic: strict`. Document `_id` is `{video_id}_{region}_{fetch_date}`.
5. API quota and network errors use retries with exponential backoff.
6. Syllabus algorithms are implemented from scratch, then compared to a library or Elasticsearch.
7. No Spark in the core pipeline.

---

## 12. Documents to write the report from

| File | Use it for |
|------|------------|
| [`README.md`](README.md) | Setup and architecture diagram |
| [`docs/report_outline.md`](docs/report_outline.md) | Report section order |
| [`docs/beyond_syllabus_elasticsearch.md`](docs/beyond_syllabus_elasticsearch.md) | 10–12 slide talk |
| [`docs/viva_questions.md`](docs/viva_questions.md) | Practice answers |
| [`PROGRESS.md`](PROGRESS.md) | What is already implemented |

---

## 13. If something fails

| Symptom | Check |
|---------|--------|
| Kafka connection refused | `make up` finished; wait ~15s; `localhost:9092` |
| ES `connection refused` | Wait for health; `curl http://localhost:9200` |
| `mapper_parsing_exception` | Field not in strict mapping; fix template and `make index` after `make down` data reset if needed |
| YouTube `403 quotaExceeded` | Use `make replay` instead of live producer |
| Bloom skipped everything | First run is fine; Bloom is in-memory and resets when the consumer restarts |
| Empty Kibana | Import `elastic/kibana_export.ndjson` and set time field `fetched_at` |
