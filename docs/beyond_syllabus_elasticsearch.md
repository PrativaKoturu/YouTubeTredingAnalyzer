# Beyond syllabus: Elasticsearch (talk notes)

Ten to twelve slides plus a live demo. Each slide has a command you can run against the local stack (`make up`).

## 1. Inverted index and analyzers

An Elasticsearch `text` field is analysed into terms stored in an inverted index (term → posting list of document ids).

```http
POST yt-trending/_analyze
{
  "analyzer": "english",
  "text": "India vs Australia cricket world cup highlights"
}
```

Show that `highlights` may stem and stopwords disappear. Contrast with `keyword` on `tags`, which is not analysed.

## 2. BM25 vs TF-IDF

TF-IDF treats term frequency as linear. BM25 saturates TF (a term appearing 20 times is not 20× more relevant) and normalises for field length.

```http
GET yt-trending/_search
{
  "explain": true,
  "size": 1,
  "query": { "match": { "title": "cricket" } }
}
```

Point at `idf`, `tf` saturation and field-length norm in the explain output.

## 3. Mappings, text vs keyword, strict dynamic

`title` is `text` plus a `keyword` subfield. `tags` and `region` are `keyword` so aggregations work. `dynamic: strict` rejects unknown fields — a good viva point: schema-on-write inside a NoSQL store.

## 4. Shards, replicas, nodes

This demo uses 1 shard and 0 replicas (single Docker node). In a cluster, a primary shard plus replicas let a node fail without losing data. Replicas also serve reads.

Talk through: node down → replica promoted → cluster health yellow/red.

## 5. Aggregations

- Bucket: `terms` on `category_name` / `region`, `date_histogram` on `fetched_at`
- Metric: `sum` views, `avg` sentiment, `cardinality` channels
- Pipeline: e.g. `bucket_script` (optional extra)
- `significant_terms` on `tags` (foreground vs background surprise)

Run files in `elastic/queries/`.

## 6. Ingest pipelines

`yt-trending-ingest` trims titles, lowercases tags, sets `ingested_at`. This is server-side ETL, similar in spirit to a MapReduce mapper that runs at write time.

## 7. Near-real-time search and CAP

A refresh (default 1s here) copies the in-memory indexing buffer to a new searchable Lucene segment. Search is NRT, not instantly consistent.

CAP: Elasticsearch is a CP-leaning distributed system with eventual replica visibility. It is not a linearisable OLTP store. Prefer it for search and analytics, not for banking balances.

## 8. Kibana dashboard demo

Import `elastic/kibana_export.ndjson`. Walk through categories, views over time, tags, sentiment, PageRank, latest table.

## 9. Elasticsearch vs MongoDB vs HBase

| | Elasticsearch | MongoDB | HBase |
|--|---------------|---------|-------|
| Pattern | Document + inverted index | Document | Wide column |
| Strength | Full-text search, aggregations | Flexible JSON apps | Huge sparse tables, write throughput |
| Query | DSLs, BM25 | Filter + aggregations | Scans, row keys |
| Analytics | Near-real-time aggs | OK for modest data | Batch / Spark friendly |
| Use here | Search + dashboard | Would store JSON but weak search | Overkill, no text relevance |

## Live demo checklist

1. `_analyze` a title  
2. `explain=true` search  
3. Reject a bad field (`dynamic: strict`)  
4. Category × country aggregation  
5. `cardinality` vs our Flajolet–Martin  
6. `more_like_this` vs our TF-IDF  
7. Dashboard
