# Likely viva questions (short answers)

**Why Kafka between the API and Elasticsearch?**  
Decoupling, buffering, replay, consumer groups, and offsets. The API can produce even if Elasticsearch is briefly down. CSV replay uses the same topic.

**Why a Bloom filter and what is its failure mode?**  
Space-efficient “have I seen this key?” test for `video_id|region|date`. False positives are possible (we might skip a new video). False negatives are not (we never process the same key twice if it was inserted).

**How does Flajolet–Martin differ from HyperLogLog++ in Elasticsearch?**  
Both hash items and use the position of bits (trailing zeros / rho) to estimate distinct counts. FM is the textbook sketch (median of means of 2^R). HLL++ uses stochastic averaging, range correction and a dense/sparse representation; Elasticsearch `cardinality` is HLL++.

**How is an Elasticsearch aggregation similar to MapReduce?**  
Each shard maps documents to partial buckets (like map + combine). The coordinating node shuffles by bucket key and reduces (merges) those partials.

**Why is Elasticsearch a NoSQL database, which pattern, and CAP?**  
It is a document store with an inverted index, schema-flexible (we chose strict). It favours partition tolerance and availability of search with eventual consistency of replicas more than strict CP OLTP.

**text vs keyword?**  
`text` is analysed for full-text search. `keyword` is exact-value, used for aggregations, sorting and filters.

**What does BM25 fix over TF-IDF?**  
Term-frequency saturation and better document-length normalisation.

**What happens if an Elasticsearch node fails?**  
With replicas, a replica is promoted and search continues (cluster yellow until replica is rebuilt). This demo has 0 replicas, so a node loss loses the index — mention that as a lab limitation.

**Dead ends and spider traps in PageRank?**  
Dead ends have out-degree 0 and leak rank. Spider traps are cycles that absorb rank. Taxation / damping (`1-d` jump to a random node) fixes both. We implement this in `analytics/pagerank_hits.py`.

**Why KRaft if the syllabus mentions ZooKeeper?**  
ZooKeeper historically stored broker metadata and elected a controller. KRaft stores that metadata in a Kafka-internal quorum. Same job, fewer moving parts for a laptop demo.

**Why document `_id` = video_id + region + fetch date?**  
A video can trend in several countries and on several days. The composite id makes bulk index idempotent.

**Why commit Kafka offsets only after bulk success?**  
At-least-once processing: if Elasticsearch fails, the consumer restarts from the last committed offset and retries those messages. Bloom then skips true duplicates.
