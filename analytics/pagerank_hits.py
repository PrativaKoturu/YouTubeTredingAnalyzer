"""PageRank with taxation and HITS, implemented with power iteration.

Dead ends (out-degree 0) leak rank. Spider traps (cycles with no escape)
hoard rank. Taxation / damping sends a (1-d) share of rank to every node
each step so the walk cannot get stuck.
"""

from __future__ import annotations

from collections import defaultdict

import networkx as nx


def pagerank_taxation(
    graph: nx.Graph | nx.DiGraph,
    damping: float = 0.85,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> dict[str, float]:
    nodes = list(graph.nodes())
    n = len(nodes)
    if n == 0:
        return {}
    if not graph.is_directed():
        graph = graph.to_directed()

    rank = {node: 1.0 / n for node in nodes}
    for _ in range(max_iter):
        new_rank = {node: (1.0 - damping) / n for node in nodes}
        dangling_mass = 0.0
        for node in nodes:
            out_deg = graph.out_degree(node)
            if out_deg == 0:
                dangling_mass += rank[node]
                continue
            share = rank[node] / out_deg
            for _, dest in graph.out_edges(node):
                new_rank[dest] += damping * share
        for node in nodes:
            new_rank[node] += damping * dangling_mass / n
        delta = sum(abs(new_rank[node] - rank[node]) for node in nodes)
        rank = new_rank
        if delta < tol:
            break
    return rank


def hits_scores(
    graph: nx.DiGraph,
    max_iter: int = 50,
    tol: float = 1e-6,
) -> tuple[dict[str, float], dict[str, float], int]:
    nodes = list(graph.nodes())
    if not nodes:
        return {}, {}, 0
    hubs = {node: 1.0 for node in nodes}
    auth = {node: 1.0 for node in nodes}
    iters = 0
    for iters in range(1, max_iter + 1):
        new_auth = defaultdict(float)
        for src, dst in graph.edges():
            new_auth[dst] += hubs[src]
        new_hubs = defaultdict(float)
        for src, dst in graph.edges():
            new_hubs[src] += new_auth[dst]
        def _norm(vec: dict[str, float]) -> dict[str, float]:
            mag = sum(v * v for v in vec.values()) ** 0.5 or 1.0
            return {k: vec.get(k, 0.0) / mag for k in nodes}

        new_auth = _norm(new_auth)
        new_hubs = _norm(new_hubs)
        delta = sum(abs(new_auth[n] - auth[n]) + abs(new_hubs[n] - hubs[n]) for n in nodes)
        auth, hubs = new_auth, new_hubs
        if delta < tol:
            break
    return hubs, auth, iters


def compare_with_networkx(graph: nx.Graph, ours: dict[str, float]) -> float:
    if graph.number_of_nodes() == 0:
        return 0.0
    theirs = nx.pagerank(graph, alpha=0.85)
    keys = list(ours)
    diffs = [abs(ours[k] - theirs.get(k, 0.0)) for k in keys]
    return sum(diffs) / len(diffs)


def top_n(scores: dict[str, float], n: int = 10) -> list[tuple[str, float]]:
    return sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:n]


def main() -> None:
    g = nx.DiGraph([("A", "B"), ("B", "C"), ("C", "A"), ("C", "D")])
    pr = pagerank_taxation(g)
    print("PageRank:", {k: round(v, 4) for k, v in pr.items()})
    print("mean abs diff vs networkx:", round(compare_with_networkx(g, pr), 6))
    hubs, auth, iters = hits_scores(g)
    print("HITS converged in", iters, "hubs", {k: round(v, 4) for k, v in hubs.items()})
    print("authorities", {k: round(v, 4) for k, v in auth.items()})


if __name__ == "__main__":
    main()
