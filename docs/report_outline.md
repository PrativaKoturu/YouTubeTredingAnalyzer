# Report outline (NADPCL72)

Use this as the skeleton of the mini-project report. Fill each heading with screenshots and numbers from `make benchmark`, `make queries`, and `make analytics`.

1. **Problem statement and motivation**  
   YouTube trending is a high-velocity, multi-region stream. We want a pipeline that can ingest, de-duplicate, store, search and analyse it.

2. **Big Data characteristics (5 Vs)**  
   Volume (many videos × regions × polls), Velocity (continuous polls), Variety (text, tags, counts, graph), Veracity (API nulls, duplicate trending days), Value (recommendations, category insights).

3. **System architecture and tools**  
   Paste the diagram from `docs/architecture.png`. Explain Kafka (KRaft) vs the ZooKeeper role in the textbook. Explain why Elasticsearch is the store.

4. **Data collection and schema**  
   `videos.list` chart=mostPopular, CSV replay, Kafka topics, Elasticsearch mapping (`dynamic: strict`), document `_id`.

5. **Implementation of each syllabus module**  
   - Module 2: producer API, topics, partitions, offsets, NoSQL document model  
   - Module 3: Bloom, Flajolet–Martin, DGIM, MapReduce-style word count  
   - Module 4: PageRank with taxation, HITS, centrality, communities  
   - Module 5: VADER sentiment, TF-IDF + cosine recommender  

6. **Elasticsearch deep dive (beyond syllabus)**  
   Expand `docs/beyond_syllabus_elasticsearch.md` into 8–10 pages or 10–12 slides.

7. **Results and graphs**  
   Table of Bloom FPR, FM vs HLL, DGIM windows, aggregation timings, PageRank top 10, overlap@10, sentiment vs likes ratio.

8. **Challenges**  
   API quota, Bloom false positives, strict mapping rejections, consumer memory, Kibana time field.

9. **Conclusion and future work**  
   Spark batch layer, trend prediction, multi-node Elasticsearch, production security.

10. **References**  
    Mining of Massive Datasets; Hadoop: The Definitive Guide; Kafka: The Definitive Guide; Elasticsearch official docs.
