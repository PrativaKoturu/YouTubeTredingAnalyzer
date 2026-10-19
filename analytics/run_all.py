"""Run MapReduce word count, TF-IDF examples, graph analytics, write scores."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analytics.centrality_community import centralities, louvain_partition  # noqa: E402
from analytics.graph_build import (  # noqa: E402
    bipartite_channel_tag,
    directed_channel_to_tag,
    fetch_docs,
    tag_cooccurrence,
)
from analytics.mapreduce_wordcount import wordcount  # noqa: E402
from analytics.pagerank_hits import (  # noqa: E402
    compare_with_networkx,
    hits_scores,
    pagerank_taxation,
    top_n,
)
from analytics.tfidf_similarity import overlap_at_k, sklearn_top_k, top_k_similar  # noqa: E402
from analytics.write_scores_to_es import channel_scores_from_nodes, write_channel_scores  # noqa: E402


def main() -> None:
    docs = fetch_docs()
    if not docs:
        raise SystemExit("No documents in Elasticsearch. Run make replay && make consumer first.")

    titles = [d.get("title") or "" for d in docs]
    tag_blob = [" ".join(d.get("tags") or []) for d in docs]
    print("\n--- MapReduce word count (titles) ---")
    for word, count in wordcount(titles)[:15]:
        print(f"{count:5}  {word}")

    print("\n--- MapReduce word count (tags) ---")
    for word, count in wordcount(tag_blob)[:15]:
        print(f"{count:5}  {word}")

    print("\n--- TF-IDF similar videos (first 5 docs as queries) ---")
    combined = [
        (d.get("title") or "") + " " + " ".join(d.get("tags") or [])
        for d in docs
    ]
    for q in range(min(5, len(docs))):
        ours = top_k_similar(combined, q, k=10)
        sk = sklearn_top_k(combined, q, k=10)
        ov = overlap_at_k([i for i, _ in ours], [i for i, _ in sk])
        print(f"query={docs[q].get('title')!r}")
        for idx, score in ours[:5]:
            print(f"  {score:.3f}  {docs[idx].get('title')}")
        print(f"  overlap@10 vs sklearn = {ov:.2f}")

    print("\n--- Graph analytics ---")
    bi = bipartite_channel_tag(docs)
    directed = directed_channel_to_tag(docs)
    tags = tag_cooccurrence(docs)
    pr = pagerank_taxation(bi)
    print("top PageRank nodes:")
    for node, score in top_n(pr, 10):
        print(f"  {score:.5f}  {node}")
    print("mean |ours - networkx| =", round(compare_with_networkx(bi, pr), 8))

    hubs, auth, iters = hits_scores(directed)
    print(f"HITS converged in {iters} iterations")
    print("top hubs:", top_n(hubs, 5))
    print("top authorities:", top_n(auth, 5))

    cents = centralities(tags if tags.number_of_nodes() else bi)
    comm = louvain_partition(tags if tags.number_of_edges() else bi)
    print("top betweenness (tag graph):", top_n(cents["betweenness"], 5))
    print(f"Louvain communities: {len(set(comm.values()))}")

    scores = channel_scores_from_nodes(pr, cents["betweenness"], comm)
    # Betweenness was computed on the tag graph; fill channel betweenness from bipartite.
    bi_cents = centralities(bi)
    scores = channel_scores_from_nodes(pr, bi_cents["betweenness"], comm)
    write_channel_scores(docs, scores)


if __name__ == "__main__":
    main()
