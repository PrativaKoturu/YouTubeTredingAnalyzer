"""Reservoir sampling (optional Module 3 extra).

Keep a uniform random sample of size k from a stream of unknown length.
"""

from __future__ import annotations

import random
from typing import TypeVar

T = TypeVar("T")


class Reservoir:
    def __init__(self, k: int, seed: int | None = 42) -> None:
        if k <= 0:
            raise ValueError("k must be positive")
        self.k = k
        self.seen = 0
        self.sample: list[T] = []
        self.rng = random.Random(seed)

    def add(self, item: T) -> None:
        self.seen += 1
        if len(self.sample) < self.k:
            self.sample.append(item)
            return
        j = self.rng.randint(1, self.seen)
        if j <= self.k:
            self.sample[j - 1] = item


def main() -> None:
    reservoir = Reservoir(5)
    for i in range(100):
        reservoir.add(f"v{i}")
    print("reservoir sample:", reservoir.sample)


if __name__ == "__main__":
    main()
