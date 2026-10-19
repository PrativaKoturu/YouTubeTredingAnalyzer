"""Degree, closeness, betweenness centrality and Louvain communities."""

from __future__ import annotations

import networkx as nx

try:
    import community as community_louvain
except ImportError:  # pragma: no cover - dependency is in requirements.txt
    community_louvain = None


def centralities(graph: nx.Graph) -> dict[str, dict[str, float]]:
    if graph.number_of_nodes() == 0:
        return {"degree": {}, "closeness": {}, "betweenness": {}}
    # Weighted closeness can fail on disconnected graphs; use unweighted hops.
    return {
        "degree": nx.degree_centrality(graph),
        "closeness": nx.closeness_centrality(graph),
        "betweenness": nx.betweenness_centrality(graph, k=min(50, graph.number_of_nodes())),
    }


def louvain_partition(graph: nx.Graph) -> dict[str, int]:
    if graph.number_of_nodes() == 0:
        return {}
    if community_louvain is None:
        # Fallback: connected components as a stand-in if the extra package is missing.
        parts: dict[str, int] = {}
        for i, component in enumerate(nx.connected_components(graph)):
            for node in component:
                parts[node] = i
        return parts
    raw = community_louvain.best_partition(graph, weight="weight")
    return {str(node): int(part) for node, part in raw.items()}


def main() -> None:
    g = nx.path_graph(5)
    print(centralities(g))
    print(louvain_partition(g))


if __name__ == "__main__":
    main()
