# YouTube Trending Analyzer

Big Data Computing mini project (NADPCL72). A streaming pipeline from YouTube trending videos (or a CSV replay) through Kafka into Elasticsearch, with syllabus algorithms implemented from scratch and a Kibana dashboard.

Detailed step-by-step guide: [`HOW_TO_RUN.md`](HOW_TO_RUN.md)  
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

## Quick Start (Detailed guide in [`HOW_TO_RUN.md`](HOW_TO_RUN.md))

### 1. Prerequisites
- **Docker Desktop** running
- **Python 3.10+** (dependencies in `requirements.txt`)
- Configure `.env` with your `YOUTUBE_API_KEY` (or use the offline replay mode without an API key).

### 2. Step-by-Step Commands

#### Start Infrastructure
```bash
docker compose up -d
python -c "from config.settings import create_topics; create_topics()"
python elastic/create_index.py
```

#### Produce Data into Kafka
* **Live YouTube API (Bulk Trending):**
  ```bash
  python producer/bulk_fetch.py --trending
  ```
* *Or Continuous Live Polling:* `python producer/producer.py`
* *Or Offline CSV Replay (no API key needed):* `python producer/csv_replay.py`

#### Consume & Index into Elasticsearch
In another terminal:
```bash
python consumer/consumer.py
```
*(Press **Ctrl+C** after processing the batch).*

#### Run Analytics (PageRank, HITS, Louvain, TF-IDF)
```bash
python analytics/run_all.py
```

#### View in Kibana Dashboard
1. Open <http://localhost:5601>.
2. Go to **Stack Management** → **Saved Objects** → **Import**.
3. Upload [`elastic/kibana_export.ndjson`](elastic/kibana_export.ndjson) and open the **YouTube Trending Analyzer** dashboard.
4. **Tip:** If panels show *"No results found"*, change the time picker in the top-right from **"Last 15 minutes"** to **"Today"** or **"Last 24 hours"**.

#### Optional: Interactive Streamlit UI
```bash
python -m streamlit run search_demo/app.py
```
Open <http://localhost:8501> in your browser.

To stop services: `docker compose down`.

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
