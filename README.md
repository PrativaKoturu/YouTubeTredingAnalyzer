# YouTube Trending Analyzer

An academic Big Data Computing project (NADPCL72) that ingests YouTube trending video snapshots, processes them as a Kafka stream, stores searchable documents in Elasticsearch, and demonstrates streaming and batch analytics. The project can be demonstrated offline with the included CSV; a YouTube Data API key is only needed for live collection.

## Contents

- [Purpose and use case](#purpose-and-use-case)
- [Architecture and technical flow](#architecture-and-technical-flow)
- [Technology stack](#technology-stack)
- [Algorithms and concepts](#algorithms-and-concepts)
- [Data model and storage](#data-model-and-storage)
- [Setup and run](#setup-and-run)
- [Configuration](#configuration)
- [Repository map](#repository-map)
- [Tests and research material](#tests-and-research-material)
- [Scope and implementation notes](#scope-and-implementation-notes)
- [Related documentation](#related-documentation)

## Purpose and use case

The system provides an end-to-end teaching/demo workflow for exploring a changing collection of popular videos across regions. Kafka decouples collection from processing; the consumer enriches each snapshot; Elasticsearch supports text search and aggregations; analytics scripts calculate term, similarity, and graph measures. The same data can be replayed from a CSV or saved JSONL, so the pipeline can be shown without spending YouTube API quota.

This is a course-scale local demo, not a production YouTube analytics service. Its main value is illustrating how these components fit together and how approximate streaming algorithms trade exactness for bounded memory.

## Architecture and technical flow

```text
YouTube Data API ─┐
CSV replay ────────┼─> Python producer -> Kafka: yt.trending.raw
JSONL replay ─────┘                         (3 partitions, key=video_id)
                                                  |
                                                  v
                               Python consumer group (manual offset commits)
                    validate -> Bloom dedupe -> normalize -> VADER sentiment
                         -> FM/DGIM in-memory metrics -> bulk index
                              |                         |
                        invalid event                   v
                              v                Elasticsearch: yt-trending
                    Kafka: yt.trending.dead             |
                                                       +--> Kibana dashboards
                                                       +--> Streamlit search UI
                                                       +--> batch analytics
                                                            (MapReduce, TF-IDF,
                                                             graph algorithms)
```

The consumer validates incoming JSON with Pydantic, uses a per-process Bloom filter to skip likely repeated `(video_id, region, fetched day)` keys, converts valid events to a strict Elasticsearch document, and indexes in batches. It commits Kafka offsets after successful indexing (or after records were deliberately skipped/routed to the dead-letter topic). Elasticsearch document IDs use the same video/region/day identity, making indexing idempotent for that key.

Batch analytics read up to 5,000 documents from Elasticsearch by default. They print word counts and similarity results, build channel/tag and tag co-occurrence graphs, calculate graph scores and then partially update channel documents in Elasticsearch. Kibana and Streamlit query the index; they do not consume Kafka directly.

## Technology stack

| Technology | Role in this repository |
|---|---|
| Python 3.10+ | Producers, consumer, algorithms, analytics, and UI (the team guide recommends Python 3.11+). |
| Apache Kafka 3.8, KRaft | Durable event buffer and decoupling between producers and consumer; local Compose uses a single combined broker/controller, not ZooKeeper. |
| `kafka-python-ng` | Python Kafka producer, consumer, admin, and topic interactions. |
| Pydantic 2 | Validation/coercion of raw event payloads and typed document construction. |
| Elasticsearch 8.15.1 client/server | Strictly mapped document store, full-text search, aggregations, and analytics write-back. |
| Kibana 8.15.1 | Saved-object dashboard and visual exploration. |
| Streamlit | Optional local search and “more like this” interface. |
| NetworkX + python-louvain | Graph creation, PageRank comparison/support, centrality, HITS graph inputs, and Louvain communities. |
| pandas / NumPy / scikit-learn | CSV handling, tabular analysis, and reference TF-IDF comparison. |
| `mmh3` | MurmurHash3 hashes used by Bloom and Flajolet–Martin sketches. |
| VADER (`vaderSentiment`) | Lexicon/rule based sentiment scoring of video titles. |
| `isodate` | Parse ISO-8601 video durations. |
| Docker Compose | Local Kafka, Elasticsearch, and Kibana services with persistent named volumes. |
| pytest | Unit tests for algorithms, cleaning, and TF-IDF. |

The dependency versions and ranges are maintained in [`requirements.txt`](requirements.txt). Docker uses a single-node local configuration; Elasticsearch security is disabled there for development convenience only.

## Algorithms and concepts

### Streaming algorithms (`streaming_algos/`)

| Concept | Implementation and purpose | Practical limits in this project |
|---|---|---|
| Bloom filter | `BloomFilter` stores membership bits using `m` bits and `k` double-hashed positions. It cheaply screens already-seen event keys. | False positives can suppress a new record; false negatives are not expected. Consumer defaults are 100,000 bits and 5 hashes, adjustable by environment. The filter is in memory and resets on restart; it is not a durable deduplication registry. |
| Flajolet–Martin style distinct estimate | `FlajoletMartin` uses seeded 32-bit MurmurHash3 hashes, tracks trailing-zero maxima, groups estimates, and takes a median with the `phi` correction. It estimates distinct channel IDs with bounded state. | Defaults: 64 hashes divided into 8 groups. It is approximate and maintained in the running consumer's memory; consumer output reports it periodically. |
| DGIM | Buckets of power-of-two sizes estimate a count over a recent suffix of a binary stream. Old buckets merge to cap storage. | Consumer currently feeds a `1` for each accepted event, so this reports approximate processed-event count over the last 1,000 stream positions, not engagement or views. |
| Reservoir sampling | `Reservoir(k)` implements the standard one-pass uniform sample for an unknown-length stream. | Optional standalone syllabus example; it is not wired into the Kafka consumer. |

### Text, statistics, and graph analytics (`analytics/`)

- **MapReduce-style word count:** tokenizes lowercase alphanumeric words, emits `(word, 1)`, locally combines counts, groups by key (shuffle/sort), and reduces totals. This demonstrates the Map/Combine/Shuffle/Reduce pattern in one Python process; it is not a distributed Hadoop/Spark job.
- **TF-IDF and cosine similarity:** the from-scratch model uses normalized term frequency and smoothed inverse document frequency `log((N+1)/(df+1)) + 1`, then ranks documents by cosine similarity. Scikit-learn is used as a comparison implementation; the batch runner compares top-k overlap.
- **Graph construction:** `graph_build.py` creates a weighted channel–tag bipartite graph, a weighted tag co-occurrence graph, and a directed channel-to-tag graph. Edge weights count observations/co-occurrences in the fetched document sample.
- **PageRank with taxation:** power iteration with damping 0.85, uniform teleportation, dangling-node mass handling, iteration cap, and convergence tolerance. Compared against NetworkX. The current function uses unweighted outgoing degree/edges even though graph builders retain weights.
- **HITS:** iterative hub and authority updates with L2 normalization on the directed channel-to-tag graph.
- **Centrality:** NetworkX degree, closeness, and betweenness centrality; betweenness uses a sample of up to 50 nodes when applicable.
- **Louvain communities:** `python-louvain` partitions the tag co-occurrence graph using edge weights; a connected-components fallback exists if the optional import is unavailable.
- **VADER sentiment:** the consumer scores titles to a compound value in `[-1, 1]`; labels are `pos` at or above `0.05`, `neg` at or below `-0.05`, otherwise `neu`.

These algorithms serve different use cases: streaming sketches track high-volume stream properties with compact state; TF-IDF finds textually related videos; graph ranking/centrality describes structural importance; community detection groups co-occurring tags. They are demonstrations and should not be interpreted as YouTube's own ranking or recommendation system.

## Data model and storage

The producer event model is [`schemas/video_event.py`](schemas/video_event.py). It carries video/channel/category identifiers and titles, description, tags, region, published/fetched timestamps, ISO duration, views, likes, and comments. Numeric counters are coerced to integers and tags accept lists or delimited strings. The consumer converts it into [`schemas/video_doc.py`](schemas/video_doc.py), adds normalized tags, category name, duration seconds, sentiment, and graph fields.

Elasticsearch setup in [`elastic/`](elastic/) applies an ingest pipeline, index template, and index:

- Index default: `yt-trending`; one shard, no replicas for the single-node demo.
- Mapping uses `dynamic: strict`, date fields for published/fetched/ingested timestamps, numeric metrics, keyword fields for IDs/filters, and English-analyzed title/description fields.
- The ingest pipeline trims title, lowercases tags, and stamps `ingested_at`.
- `title.keyword` supports exact-value aggregations; the Streamlit interface searches titles and filters by region/minimum views.
- Saved queries demonstrate filtered search, `more_like_this`, hourly/category aggregations, significant tags, and cardinality estimation.
- `elastic/kibana_export.ndjson` contains the importable Kibana saved objects.

`bulk_fetch.py --comments` can fetch comment text into a raw enriched JSONL backup, but `top_comments` is outside the event/document schema and is stripped before Kafka indexing. Sentiment is calculated from the title, not comments.

## Setup and run

### Requirements

- Docker Desktop with Compose, running locally.
- Python 3.10 or newer and pip.
- Optional: YouTube Data API v3 key for live collection. Offline CSV/JSONL replay does not require one.

Install Python packages and optionally create a root `.env` (loaded by `config/settings.py`):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a root `.env` with `YOUTUBE_API_KEY=...` only if using live API collection. Do not commit secrets.

### Start and initialize local services

```powershell
docker compose up -d
python -c "from config.settings import create_topics; create_topics()"
python elastic/create_index.py
```

### Choose a producer

CSV replay is the simplest offline path:

```powershell
python producer/csv_replay.py
```

Other options:

```powershell
python producer/producer.py                 # continuously poll configured regions
python producer/bulk_fetch.py --trending     # one bulk trending fetch
python producer/bulk_fetch.py --dry-run      # fetch and save JSONL without Kafka
python producer/jsonl_replay.py              # replay data/raw/*.jsonl
```

The consumer runs continuously; use another terminal and stop it with Ctrl+C after processing the replay batch:

```powershell
python consumer/consumer.py
```

Run batch analytics after documents have been indexed:

```powershell
python analytics/run_all.py
python elastic/run_queries.py
python analytics/benchmark.py
```

Start optional interfaces:

```powershell
python -m streamlit run search_demo/app.py
```

Open Elasticsearch at `http://localhost:9200`, Kibana at `http://localhost:5601`, and Streamlit at `http://localhost:8501`. Import `elastic/kibana_export.ndjson` through Kibana Stack Management → Saved Objects. Stop containers with `docker compose down`; `docker compose down -v` also removes persisted Kafka and Elasticsearch data.

`Makefile` provides shortcuts such as `make up`, `make topics`, `make index`, `make replay`, `make consumer`, `make analytics`, `make queries`, `make benchmark`, `make search`, and `make test` (use a shell with `make` installed).

## Configuration

Settings are read from environment variables (and `.env` when present) in [`config/settings.py`](config/settings.py):

| Variable | Default | Purpose |
|---|---|---|
| `YOUTUBE_API_KEY` | empty | Enables live API requests. |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address. |
| `KAFKA_RAW_TOPIC` / `KAFKA_DEAD_TOPIC` | `yt.trending.raw` / `yt.trending.dead` | Event and dead-letter topics. |
| `KAFKA_GROUP_ID` | `yt-trending-consumer` | Consumer group. |
| `ES_URL` / `ES_INDEX` | `http://localhost:9200` / `yt-trending` | Elasticsearch endpoint and index. |
| `REGIONS` | `IN,US,GB` | Regions used by producer scripts unless overridden. |
| `POLL_INTERVAL_SEC` | `300` | Delay between live producer polls. |
| `YOUTUBE_MAX_RESULTS` | `50` | API page result setting. |
| `REPLAY_CSV` / `REPLAY_DELAY_SEC` | `data/sample/sample_trending.csv` / `0.2` | CSV replay file and per-record delay. |
| `BULK_SIZE` | `200` | Consumer bulk index batch threshold. |
| `BLOOM_M_BITS` / `BLOOM_K_HASHES` | `100000` / `5` | Consumer Bloom filter capacity parameters. |

`config/regions.yaml` documents a ten-region example, but the current settings module does not load that YAML: the effective default is the `REGIONS` environment value (`IN,US,GB`). `config/categories.json` is loaded by consumer cleaning to map category IDs to names.

## Repository map

| Path | Responsibility |
|---|---|
| `config/` | Environment settings, category mapping, documented sample regions. |
| `schemas/` | Pydantic raw event and Elasticsearch document models. |
| `producer/` | YouTube API client, continuous/bulk producers, CSV and JSONL replay. |
| `consumer/` | Kafka consumer, validation/DLQ, normalization, sentiment, indexing. |
| `streaming_algos/` | Bloom, Flajolet–Martin, DGIM, reservoir sampling implementations. |
| `elastic/` | Index/pipeline setup, saved queries, Kibana export, query runner. |
| `analytics/` | MapReduce word count, TF-IDF, graph algorithms, score write-back, benchmarks. |
| `search_demo/` | Streamlit Elasticsearch search UI. |
| `data/sample/` | Small committed CSV for offline replay. |
| `data/raw/` | Runtime JSONL captures; ignored by git except `.gitkeep`. |
| `tests/` | Tests for Bloom, FM, DGIM, cleaning, and TF-IDF. |
| `notebooks/` | EDA and report results notebooks. |
| `docs/` | Architecture image, report outline, Elasticsearch material, viva questions. |

## Tests and research material

Run the test suite with:

```powershell
python -m pytest -q
```

Tests are in `tests/`; they cover the listed algorithm and transformation behaviors. They do not validate a live YouTube API connection or a running Docker/Kafka/Elasticsearch deployment.

## Scope and implementation notes

- Docker Compose runs Kafka in KRaft mode; there is no ZooKeeper service. ZooKeeper belongs in the historical/course explanation of Kafka, not in this runtime architecture.
- Compose currently pins Elasticsearch and Kibana to 8.15.1. Security is disabled for local development; do not expose this configuration as a production service.
- The consumer’s sketches and Bloom membership are process-local and reset after restart. A Bloom positive can skip a non-duplicate because the structure permits false positives.
- The standard producer defaults to three regions from settings. The YAML region list is descriptive and not currently wired into settings.
- The app's `more_like_this` recommendation is Elasticsearch relevance behavior. The custom TF-IDF similarity is run by batch analytics and is not currently the Streamlit recommendation implementation.
- Analytics use a bounded Elasticsearch search (default 5,000 docs), so results reflect that fetched sample. The custom PageRank currently ignores edge weights; other graph operations may use weights where configured.
- Analytics write PageRank, betweenness, and community IDs to documents for channels found in the graph. Scores are illustrative features, not platform truth.
- Bulk API behavior is quota-sensitive. The client retries transient/rate-limit failures with exponential backoff and raises a distinct quota exhaustion error; use replay when quota is unavailable.
- No Spark, Hadoop cluster, distributed Python execution, comments-based sentiment, or production-grade authentication is part of the current codebase.

## Related documentation

- [`HOW_TO_RUN.md`](HOW_TO_RUN.md) — detailed run guide.
- [`PROJECT_EXPLANATION.md`](PROJECT_EXPLANATION.md) — expanded technical narrative and algorithm context.
- [`TEAM_GUIDE.md`](TEAM_GUIDE.md) — team workflow and demo guidance.
- [`PROGRESS.md`](PROGRESS.md) — implementation status snapshot (may lag source changes).
- [`docs/report_outline.md`](docs/report_outline.md) and [`docs/viva_questions.md`](docs/viva_questions.md) — report and presentation preparation.
- [`docs/beyond_syllabus_elasticsearch.md`](docs/beyond_syllabus_elasticsearch.md) — Elasticsearch concepts.
- [`docs/architecture.png`](docs/architecture.png) — architecture illustration.

## Course use

Prepared as a college mini project. Respect YouTube API terms and quota. Keep credentials in the ignored `.env` file and use an authenticated, secured Elasticsearch deployment outside the local demo environment.
