from analytics.tfidf_similarity import cosine, overlap_at_k, top_k_similar


def test_cosine_identical() -> None:
    assert abs(cosine([1.0, 0.0], [1.0, 0.0]) - 1.0) < 1e-9


def test_cosine_orthogonal() -> None:
    assert abs(cosine([1.0, 0.0], [0.0, 1.0])) < 1e-9


def test_similar_docs_rank_first() -> None:
    docs = [
        "cricket world cup highlights india",
        "india cricket cup match",
        "space telescope astronomy photos",
    ]
    ranked = top_k_similar(docs, 0, k=2)
    assert ranked[0][0] == 1
    assert ranked[0][1] > ranked[1][1]


def test_overlap() -> None:
    assert overlap_at_k([1, 2, 3], [3, 2, 9]) == 2 / 3
