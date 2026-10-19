"""TF-IDF + cosine similarity from scratch, compared with scikit-learn.

tf-idf(t, d) = tf(t, d) * log((N + 1) / (df(t) + 1)) + 1
cosine(u, v) = (u·v) / (||u|| ||v||)
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict

WORD = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return WORD.findall((text or "").lower())


class TfidfModel:
    def __init__(self, documents: list[str]) -> None:
        self.docs = [tokenize(doc) for doc in documents]
        self.n = len(self.docs)
        self.df: dict[str, int] = defaultdict(int)
        for tokens in self.docs:
            for term in set(tokens):
                self.df[term] += 1
        self.vocab = sorted(self.df)
        self.index = {term: i for i, term in enumerate(self.vocab)}
        self.vectors = [self._vector(tokens) for tokens in self.docs]

    def _idf(self, term: str) -> float:
        return math.log((self.n + 1) / (self.df[term] + 1)) + 1.0

    def _vector(self, tokens: list[str]) -> list[float]:
        tf = Counter(tokens)
        vec = [0.0] * len(self.vocab)
        length = max(len(tokens), 1)
        for term, count in tf.items():
            if term not in self.index:
                continue
            vec[self.index[term]] = (count / length) * self._idf(term)
        return vec


def cosine(u: list[float], v: list[float]) -> float:
    dot = sum(a * b for a, b in zip(u, v))
    nu = math.sqrt(sum(a * a for a in u))
    nv = math.sqrt(sum(b * b for b in v))
    if nu == 0 or nv == 0:
        return 0.0
    return dot / (nu * nv)


def top_k_similar(documents: list[str], query_index: int, k: int = 10) -> list[tuple[int, float]]:
    model = TfidfModel(documents)
    q = model.vectors[query_index]
    scored = [
        (i, cosine(q, vec))
        for i, vec in enumerate(model.vectors)
        if i != query_index
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:k]


def sklearn_top_k(documents: list[str], query_index: int, k: int = 10) -> list[tuple[int, float]]:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    matrix = TfidfVectorizer().fit_transform(documents)
    sims = cosine_similarity(matrix[query_index], matrix).ravel()
    ranked = [
        (i, float(sims[i]))
        for i in range(len(documents))
        if i != query_index
    ]
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked[:k]


def overlap_at_k(a: list[int], b: list[int]) -> float:
    if not a:
        return 0.0
    return len(set(a) & set(b)) / len(a)


def main() -> None:
    docs = [
        "india cricket world cup highlights",
        "cricket match sixes and fours",
        "python programming tutorial",
        "world cup football finals",
        "beginner python data science",
    ]
    ours = top_k_similar(docs, 0, k=3)
    sk = sklearn_top_k(docs, 0, k=3)
    print("ours:", ours)
    print("sklearn:", sk)
    print("overlap@3:", overlap_at_k([i for i, _ in ours], [i for i, _ in sk]))


if __name__ == "__main__":
    main()
