"""Map / shuffle / reduce word count on titles and tags (no Spark)."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Iterable

WORD = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return WORD.findall((text or "").lower())


def mapper(doc_id: str, text: str) -> list[tuple[str, int]]:
    """Emit (word, 1) for each token. doc_id is unused but matches MapReduce shape."""
    _ = doc_id
    return [(token, 1) for token in tokenize(text)]


def combiner(pairs: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """Local pre-aggregation, the same idea as a MapReduce combiner."""
    counts: dict[str, int] = defaultdict(int)
    for word, n in pairs:
        counts[word] += n
    return list(counts.items())


def shuffle_sort(pairs: Iterable[tuple[str, int]]) -> dict[str, list[int]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for word, n in pairs:
        grouped[word].append(n)
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def reducer(word: str, values: list[int]) -> tuple[str, int]:
    return word, sum(values)


def wordcount(texts: list[str]) -> list[tuple[str, int]]:
    mapped: list[tuple[str, int]] = []
    for i, text in enumerate(texts):
        mapped.extend(combiner(mapper(str(i), text)))
    grouped = shuffle_sort(mapped)
    reduced = [reducer(word, values) for word, values in grouped.items()]
    reduced.sort(key=lambda kv: (-kv[1], kv[0]))
    return reduced


def main() -> None:
    sample = [
        "India cricket highlights world cup",
        "cricket match highlights",
        "gaming live stream",
    ]
    for word, count in wordcount(sample)[:10]:
        print(f"{word:15} {count}")


if __name__ == "__main__":
    main()
