# YouTube Trending Analyzer - Step-by-Step Run Guide

This guide gives the exact, sequential commands to run the entire pipeline from scratch on **Windows (PowerShell)** and **macOS / Linux / Git Bash**.

---

## Prerequisites
1. **Docker Desktop** installed and running.
2. **Python 3.10+** installed.

---

## Initial Setup (Run Once)

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Verify `.env` File
Ensure your `.env` file exists and has your YouTube Data API v3 key:
```ini
YOUTUBE_API_KEY=your_api_key_here
```
*(If you do not have an API key, you can still run the offline CSV replay mode without one).*

---

## Sequential Execution (Live YouTube Data Pipeline)

### Step 1: Start Docker Containers
Start Kafka, Elasticsearch, and Kibana in the background:
```powershell
docker compose up -d
```
> *Wait about 15–20 seconds for Elasticsearch and Kafka to finish initializing.*

---

### Step 2: Initialize Kafka Topics & Elasticsearch Index
Run these two commands in your terminal:
```powershell
python -c "from config.settings import create_topics; create_topics()"
python elastic/create_index.py
```
*Expected Output:*
- `Created topics: yt.trending.raw, yt.trending.dead` (or `Topics already exist.`)
- `Upserted ingest pipeline 'yt-trending-ingest'`
- `Upserted index template 'yt-trending-template'`
- `Created index 'yt-trending'`

---

### Step 3: Fetch Live YouTube Data into Kafka
In a PowerShell window, run:

* **Option A: One-time live bulk fetch** (Fetches current trending videos across all 10 regions):
  ```powershell
  python producer/bulk_fetch.py --trending
  ```
* **Option B: Continuous live streaming** (Polls YouTube API every 5 minutes):
  ```powershell
  python producer/producer.py
  ```

---

### Step 4: Run the Consumer
In another PowerShell window, start the consumer to ingest events from Kafka into Elasticsearch:
```powershell
python consumer/consumer.py
```
* The consumer performs Bloom filter deduplication, title cleaning, VADER sentiment analysis, and bulk indexes records into Elasticsearch.
* Once the batch has finished processing (or when you want to pause), press **Ctrl + C**.

---

### Step 5: Run Analytics & Graph Algorithms (Enrichment)
Run the analytics algorithms (PageRank, HITS, Louvain community detection, and TF-IDF):
```powershell
python analytics/run_all.py
```
* This writes computed PageRank scores and community IDs back into the Elasticsearch index for each video.

---

### Step 6: View the Kibana Dashboard

1. Open your browser and navigate to: **[http://localhost:5601](http://localhost:5601)**
2. In the left navigation menu, go to **Management** &rarr; **Stack Management**.
3. Under **Kibana**, click **Saved Objects**.
4. Click **Import** in the top-right corner.
5. Select and upload:
   ```
   elastic/kibana_export.ndjson
   ```
6. Click **Import** (check "Automatically overwrite conflicts" if prompted), then click **Done**.
7. In the left navigation, go to **Analytics** &rarr; **Dashboard** and open **YouTube Trending Analyzer**.

> [!IMPORTANT]
> **If charts show "No results found":**  
> Change the time filter in the top-right corner from **"Last 15 minutes"** to **"Today"**, **"Last 24 hours"**, or **"Last 7 days"**, then click **Refresh**.

---

### Step 7 (Optional): Interactive Search Web UI
To run the Streamlit search interface:
```powershell
python -m streamlit run search_demo/app.py
```
Open **[http://localhost:8501](http://localhost:8501)** in your browser.

---

## Alternative: Offline Mode (Without YouTube API Key)
If you ever want to run without hitting the YouTube API:

1. `docker compose up -d`
2. `python -c "from config.settings import create_topics; create_topics()"`
3. `python elastic/create_index.py`
4. `python producer/csv_replay.py` *(replays sample CSV into Kafka)*
5. `python consumer/consumer.py` *(Ctrl+C once sample is drained)*
6. `python analytics/run_all.py`

---

## How to Stop the Project (Teardown)
To shut down all Docker containers when finished:
```powershell
docker compose down
```
To shut down and wipe container volumes/data:
```powershell
docker compose down -v
```
