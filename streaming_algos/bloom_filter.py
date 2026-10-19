"""Bloom filter from scratch (syllabus Module 3).

A Bloom filter is a bit array of size m plus k hash functions.
- add(x) sets k bits
- might_contain(x) is True if all k bits are set

Failure mode: false positives are possible; false negatives are not.
"""

from __future__ import annotations

import math
import random

import mmh3


class BloomFilter:
    def __init__(self, m_bits: int, k_hashes: int) -> None:
        if m_bits <= 0 or k_hashes <= 0:
            raise ValueError("m_bits and k_hashes must be positive")
        self.m = m_bits
        self.k = k_hashes
        self.bits = bytearray((m_bits + 7) // 8)
        self.n_inserted = 0

    def _bit(self, index: int) -> bool:
        byte, offset = divmod(index, 8)
        return bool(self.bits[byte] & (1 << offset))

    def _set_bit(self, index: int) -> None:
        byte, offset = divmod(index, 8)
        self.bits[byte] |= 1 << offset

    def _positions(self, item: str) -> list[int]:
        # Double hashing with two mmh3 seeds keeps k independent-looking hashes.
        h1 = mmh3.hash(item, seed=0, signed=False)
        h2 = mmh3.hash(item, seed=1, signed=False) or 1
        return [((h1 + i * h2) % self.m) for i in range(self.k)]

    def add(self, item: str) -> None:
        for pos in self._positions(item):
            self._set_bit(pos)
        self.n_inserted += 1

    def might_contain(self, item: str) -> bool:
        return all(self._bit(pos) for pos in self._positions(item))

    def theoretical_fpr(self, n: int | None = None) -> float:
        """(1 - e^(-kn/m))^k"""
        n = self.n_inserted if n is None else n
        if n <= 0:
            return 0.0
        return (1.0 - math.exp(-self.k * n / self.m)) ** self.k


def measure_false_positive_rate(
    n_insert: int = 1000,
    n_probe: int = 2000,
    m_bits: int = 10_000,
    k_hashes: int = 5,
    seed: int = 7,
) -> tuple[float, float]:
    """Insert n_insert items, then probe n_probe unseen items. Return (measured, theory)."""
    rng = random.Random(seed)
    bloom = BloomFilter(m_bits, k_hashes)
    inserted = {f"item-{i}" for i in range(n_insert)}
    for item in inserted:
        bloom.add(item)

    false_positives = 0
    probes = 0
    while probes < n_probe:
        candidate = f"other-{rng.randint(n_insert, n_insert + 10_000_000)}"
        if candidate in inserted:
            continue
        probes += 1
        if bloom.might_contain(candidate):
            false_positives += 1

    measured = false_positives / n_probe
    return measured, bloom.theoretical_fpr(n_insert)


def main() -> None:
    measured, theory = measure_false_positive_rate()
    print(f"Bloom filter demo  m=10000 k=5 n=1000")
    print(f"theoretical FPR = {theory:.4f}")
    print(f"measured    FPR = {measured:.4f}")


if __name__ == "__main__":
    main()
