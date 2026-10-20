# YouTube Trending Analyzer - Complete Technical Manual & Project Guide

This document provides a comprehensive, end-to-end explanation of the **YouTube Trending Analyzer** project: the architecture, data volume, streaming algorithms, storage layers, graph algorithms, and dashboard components.

---

## Architecture Diagram

```
                       [ DATA SOURCE ]
             YouTube Data API v3 (or CSV / JSONL Replay)
                                  │
                                  ▼ (JSON Events)
                        [ APACHE KAFKA ]
                     Topic: yt.trending.raw
                    (3 Partitions, Key: video_id)
                                  │
                     ┌────────────┴────────────┐
                     ▼                         ▼
              Valid Records             Malformed Records
                     │                         │
                     ▼                         ▼
            [ STREAMING CONSUMER ]       [ DEAD LETTER QUEUE ]
            - Pydantic Validation         yt.trending.dead
            - Bloom Filter (Deduplication)
            - Text Cleaning & Normalization
            - VADER Sentiment Analysis
            - Flajolet-Martin (Distinct Count)
            - DGIM (Sliding Window Bit Count)
                     │
                     ▼ (Bulk Indexing)
           [ ELASTICSEARCH (yt-trending) ]
            - Ingest Pipeline & Stemming
            - Idempotent ID: {video_id}_{region}_{day}
            - BM25 Full-Text Search
                     │
       ┌─────────────┴────────────────────────┐
       ▼                                      ▼
[ OFFLINE ANALYTICS ]                 [ VISUALIZATION ]
- MapReduce Word Count                - Kibana Dashboard (Port 5601)
- TF-IDF & Cosine Similarity          - Streamlit Search UI (Port 8501)
- PageRank (Random Walk + Taxation)
- HITS (Hubs & Authorities)
- Louvain Community Detection
       │
       └───── Updates ES Docs with ───────────┘
              pagerank & community_id
```

---

## 1. Data Ingestion: How Data Enters the System

### The Sources
The pipeline supports three distinct data sources:
1. **Live YouTube Data API v3 (`producer/bulk_fetch.py` & `producer/producer.py`)**:
   - Calls the YouTube Data API endpoint: `videos.list` with parameter `chart="mostPopular"`.
   - Iterates through 10 ISO-3166 regions: **IN, US, GB, JP, BR, DE, FR, KR, CA, AU**.
   - Paginates up to 4 pages per region (50 items per page) = up to **200 videos per region**.
   - A single bulk run fetches **~1,989 to 2,000 unique records**.
   - Optional search queries (`search.list`) and comment threads (`commentThreads.list`) enrich the dataset.
2. **Offline CSV Replay (`producer/csv_replay.py`)**:
   - Replays historical Kaggle YouTube trending datasets (`data/sample/sample_trending.csv`) into Kafka with simulated time intervals.
3. **JSONL Replay (`producer/jsonl_replay.py`)**:
   - Replays pre-saved raw JSON dumps from `data/raw/` without consuming API quota.

### Message Schema Validation (`schemas/video_event.py`)
Before events are sent to Kafka, raw JSON is validated into strongly typed Pydantic models:
* `video_id` (str): Unique YouTube video ID.
* `title` (str), `description` (str).
* `channel_id` (str), `channel_title` (str).
* `category_id` (str), `category_name` (str, mapped via `config/categories.json`).
* `publish_time` (datetime), `fetched_at` (datetime ISO 8601 UTC).
* `views`, `likes`, `dislikes`, `comment_count` (int).
* `tags` (list of strings).
* `region` (str): 2-letter country code.

### Apache Kafka (KRaft Mode)
* **Topic `yt.trending.raw`**:
  - Configured with **3 partitions** to allow parallel multi-worker consumer scaling.
  - **Partition Key = `video_id`**: Ensures that all updates for a specific video land on the exact same Kafka partition, guaranteeing strict per-video ordering.
* **Topic `yt.trending.dead`**:
  - Dead Letter Queue (DLQ). Malformed records or validation failures are automatically routed here with stack traces so corrupt records never break the pipeline.
* **KRaft Mode**: Uses Kafka Raft Metadata mode directly inside the container without requiring an external ZooKeeper service.

---

## 2. Real-Time Streaming Consumer (`consumer/consumer.py`)

The streaming consumer pulls messages off Kafka in micro-batches (`bulk=200`):

### A. Streaming Deduplication: Bloom Filter (`streaming_algos/bloom_filter.py`)
- **Problem**: Querying a database to check if an incoming event is a duplicate consumes too much I/O and memory at high stream velocities.
- **Solution**: A probabilistic data structure with zero false negatives.
- **Implementation**:
  - Bit array size: $m = 500,000$ bits.
  - Number of hash functions: $k = 7$.
  - Uses **MurmurHash3** (`mmh3`) with double-hashing:
    $$h_i(x) = (h_1(x) + i \cdot h_2(x)) \pmod m$$
  - Deduplication key: `f"{video_id}_{region}_{date}"`.
  - Theoretical false positive rate: $< 0.01\%$.

### B. Text Normalization & Sentiment Analysis (`consumer/cleaning.py` & `consumer/sentiment.py`)
- **Cleaning**: Cleans emojis, HTML entities, URLs, punctuation, and non-printable characters.
- **Sentiment Analysis**: Uses **VADER** (*Valence Aware Dictionary and sEntiment Reasoner*), which is tuned for social media text and titles.
  - Produces a compound score in range $[-1.0, +1.0]$.
  - Categorizes titles into:
    * **Positive** (score $\ge +0.05$)
    * **Negative** (score $\le -0.05$)
    * **Neutral** (score between $-0.05$ and $+0.05$)

### C. In-Memory Streaming Algorithms:

#### 1. Flajolet–Martin (FM) Algorithm (`streaming_algos/flajolet_martin.py`)
- **Purpose**: Approximates the number of **distinct YouTube channels** seen in an unbounded stream using $O(\log(\text{distinct elements}))$ memory.
- **How it works**:
  1. Hashes each incoming channel ID into a 32-bit binary integer.
  2. Counts the number of trailing zeros $R$ in the binary representation.
  3. Uses 32 independent hash functions grouped into buckets.
  4. Takes the geometric mean of bucket medians multiplied by the bias-correction constant $\phi \approx 0.77351$ to estimate distinct channels.

#### 2. Datar-Gionis-Indyk-Motwani (DGIM) Algorithm (`streaming_algos/dgim.py`)
- **Purpose**: Estimates the count of high-engagement video events that occurred within the **last $N$ elements of a sliding window** using $O(\log^2 N)$ space.
- **How it works**:
  - Treats incoming events as a bitstream ($1$ = event processed).
  - Maintains timestamps of buckets whose sizes are powers of $2$ ($1, 2, 4, 8, \dots$).
  - Rules: At most 2 buckets of any given size. When a third bucket of size $2^k$ arrives, the two oldest buckets merge into a single bucket of size $2^{k+1}$.
  - Error bounded to $\le 50\%$.

---

## 3. Storage & Search: Elasticsearch (`elastic/`)

### Role of Elasticsearch in the Architecture
Elasticsearch acts as the distributed search engine and document store:
1. **Idempotent Document Keying**:
   ```python
   document_id = f"{video_id}_{region}_{fetched_at[:10]}"
   ```
   If the pipeline ingests the same video in the same country on the same calendar day, it performs an **upsert**, keeping view counts and stats fresh without creating duplicate rows.
2. **Ingest Pipeline (`elastic/ingest_pipeline.json`)**:
   Automatically normalizes timestamps and applies lowercase transformations on arrival.
3. **Index Template & Analyzers (`elastic/index_template.json`)**:
   - `title`: Analyzed with standard English analyzer + custom n-gram filters for autocomplete and fuzzy matching.
   - `title.keyword`: Stored as raw keyword for exact aggregations and grouping in Kibana.
   - `tags`: Multi-value keyword array for fast tag-cloud aggregations.
   - `region`: 2-letter keyword bucket (`IN`, `US`, `GB`, etc.).
   - `pagerank`: Floating-point field dynamically updated by analytics jobs.

---

## 4. Graph & Batch Analytics (`analytics/run_all.py`)

After records are loaded into Elasticsearch, offline analytical jobs compute deeper relationships:

### A. MapReduce Word Count (`analytics/mapreduce_wordcount.py`)
- Simulates distributed MapReduce on video titles:
  - **Map Phase**: Emits `(word, 1)` pairs for all alphanumeric words (filtering standard stopwords).
  - **Shuffle & Sort**: Groups all values by word key.
  - **Reduce Phase**: Aggregates counts $\sum(\text{ones})$ to determine global trending vocabulary.

### B. TF-IDF & Cosine Similarity (`analytics/tfidf_similarity.py`)
- Computes **Term Frequency–Inverse Document Frequency**:
  $$\text{TF-IDF}(t, d, D) = \text{TF}(t, d) \times \log\left(\frac{|D|}{|\{d \in D : t \in d\}|}\right)$$
- Builds a sparse vector representation for every video's title and description.
- Evaluates pairwise **Cosine Similarity**:
  $$\text{Cosine Similarity}(\vec{A}, \vec{B}) = \frac{\vec{A} \cdot \vec{B}}{\|\vec{A}\| \|\vec{B}\|}$$
- Powers content-based recommendation (finding videos related to the current trending topic).

### C. Graph Analytics (`analytics/pagerank_hits.py` & `analytics/centrality_community.py`)
Constructs two bipartite and co-occurrence graphs:
1. **Video–Tag Graph**: A graph where edges link videos to their shared tags.
2. **Channel Network**: Channels sharing similar audiences and categories.

From these graphs, the system calculates:
* **PageRank (with Taxation)**:
  Models a random surfer exploring YouTube:
  $$\mathbf{r}^{(k+1)} = \beta M \mathbf{r}^{(k)} + \frac{1 - \beta}{|V|} \mathbf{e}$$
  (with damping factor $\beta = 0.85$). Channels and videos with the highest authoritative incoming links receive the top PageRank scores.
* **Kleinberg’s HITS Algorithm**:
  Computes two complementary scores per node:
  - **Hub Score**: Measures how well a node points to other good nodes.
  - **Authority Score**: Measures how valuable the content of the node is.
* **Louvain Community Detection**:
  Optimizes graph **Modularity ($Q$)** to identify clusters of related tags and creator communities that trend together.
* **Write-back**: The computed `pagerank` and `community_id` scores are batch-updated directly back into Elasticsearch documents.

---

## 5. Visual Exploration Layer

### A. Kibana Dashboard (Port `5601`)
Loads predefined visualizations from `elastic/kibana_export.ndjson`:
- **Views Over Time**: Date histogram showing trending velocity by hour/day.
- **Top Categories**: Horizontal bar chart breaking down views across Gaming, Music, Entertainment, News, etc.
- **Top Tags**: Word cloud visualization of the most frequent tags.
- **Sentiment Distribution**: Pie chart illustrating the proportion of Positive vs Neutral vs Negative titles.
- **Top Channels by PageRank**: Bar chart ranking channels by algorithmic authority.
- **Latest Trending Videos**: Aggregation table showing videos and the regions they are currently dominating.

### B. Streamlit Search UI (`search_demo/app.py`, Port `8501`)
A standalone Python web app connected to Elasticsearch that offers:
- BM25 full-text search with fuzzy tolerance (typo handling).
- Multi-filter sliders (filter by region, category, minimum views, and sentiment).
- Content-based similarity recommendations.

---

## 6. Summary: Key Metrics & Data Volume

| Pipeline Stage | Technology | What it Handles / Computes | Volume / Specs |
| :--- | :--- | :--- | :--- |
| **Ingestion** | YouTube Data API v3 | `chart=mostPopular` across 10 regions | ~2,000 videos / poll cycle |
| **Buffering** | Kafka (KRaft) | Topic `yt.trending.raw` | 3 partitions, key = `video_id` |
| **Deduplication** | Bloom Filter | In-memory probabilistic filter | $m=500,000$ bits, $k=7$ hashes |
| **Stream Metrics**| Flajolet-Martin & DGIM | Distinct channels & sliding window bit count | $O(\log N)$ space |
| **Sentiment** | VADER | Compound sentiment scoring | Range: $[-1.0, +1.0]$ |
| **Storage & Search**| Elasticsearch 8.15 | Document indexing, BM25 text search | Index: `yt-trending` |
| **Graph Analytics** | NetworkX / Louvain | PageRank ($\beta=0.85$), HITS, Communities | Updates ES docs |
| **Dashboard** | Kibana 8.15 | Real-time charts & aggregations | Port 5601 |
