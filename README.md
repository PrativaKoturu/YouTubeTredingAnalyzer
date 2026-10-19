# YouTube Trending Analyzer

Big Data Computing mini project (NADPCL72). A streaming pipeline from YouTube trending videos (or a CSV replay) through Kafka into Elasticsearch, with syllabus algorithms implemented from scratch and a Kibana dashboard.

Team operating manual: [`TEAM_GUIDE.md`](TEAM_GUIDE.md)  
What is already built: [`PROGRESS.md`](PROGRESS.md)

## Architecture

```
YouTube Data API  (or Kaggle CSV replay)
        |
        v
 Kafka  yt.trending.raw          (3 partitions, key = video_id)
        |
        v
 Consumer: pydantic -> Bloom -> clean -> VADER -> FM / DGIM
        |                              \
        v                               v
 Elasticsearch yt-trending          yt.trending.dead
        |
        v
 Analytics (MapReduce word count, TF-IDF, PageRank, HITS, Louvain)
        |
        +----> Kibana dashboard
        +----> Streamlit search_demo/
```

A drawn version of this diagram is in [`docs/architecture.png`](docs/architecture.png).

## Quick start (offline demo)

Python 3.11 or newer (3.13 works), Docker Desktop, and Make.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

make up
make topics
make index
make replay
make consumer          # Ctrl+C after it drains the sample
make analytics
make queries
make test
```

Open Kibana at http://localhost:5601 and import `elastic/kibana_export.ndjson` (Stack Management → Saved Objects → Import). Optional UI: `make search`.

Live API mode: put a YouTube Data API v3 key in `.env` as `YOUTUBE_API_KEY`, then `make producer` instead of `make replay`.

## What maps to the syllabus

| Topic | Code |
|-------|------|
| Kafka producer / consumer, topics, offsets | `producer/`, `consumer/` |
| Bloom filter, Flajolet–Martin, DGIM | `streaming_algos/` |
| MapReduce word count | `analytics/mapreduce_wordcount.py` |
| PageRank (taxation) and HITS | `analytics/pagerank_hits.py` |
| TF-IDF + cosine | `analytics/tfidf_similarity.py` |
| Sentiment | `consumer/sentiment.py` |
| Elasticsearch (beyond syllabus) | `elastic/`, `docs/beyond_syllabus_elasticsearch.md` |

Kafka in Docker uses **KRaft** (no ZooKeeper process). The report should still explain ZooKeeper’s classical role.

Elasticsearch security is **disabled for local development only** (see comments in `docker-compose.yml`).

## Project layout

See section 4 of the original plan, or the table in `TEAM_GUIDE.md`. Folders are not renamed.

## Tests

```bash
make test
```

Covers Bloom filter, Flajolet–Martin, DGIM, cleaning, and TF-IDF.

## License / course use

Written for a college mini project. Do not disable Elasticsearch security outside this local Docker stack. Respect YouTube API quota and terms.
