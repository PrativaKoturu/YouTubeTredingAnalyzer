"""DGIM sliding-window count of ones (syllabus Module 3).

Stream is a sequence of bits. We keep buckets whose sizes are powers of two,
at most two buckets of each size. Estimate of ones in the last N bits is
the sum of fully-inside buckets plus half of the oldest overlapping bucket.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class Bucket:
    size: int
    timestamp: int  # age of the newest 1 in this bucket (stream position)


class DGIM:
    def __init__(self, window_size: int) -> None:
        if window_size <= 0:
            raise ValueError("window_size must be positive")
        self.window_size = window_size
        self.time = 0
        self.buckets: dict[int, deque[Bucket]] = {}

    def _drop_old(self) -> None:
        cutoff = self.time - self.window_size
        for size in list(self.buckets):
            kept = deque(b for b in self.buckets[size] if b.timestamp > cutoff)
            if kept:
                self.buckets[size] = kept
            else:
                del self.buckets[size]

    def _merge(self) -> None:
        size = 1
        while True:
            q = self.buckets.get(size)
            if q is None or len(q) <= 2:
                break
            older = q.popleft()
            newer = q.popleft()
            merged = Bucket(size=size * 2, timestamp=newer.timestamp)
            self.buckets.setdefault(size * 2, deque()).append(merged)
            size *= 2

    def add_bit(self, bit: int) -> None:
        if bit not in (0, 1):
            raise ValueError("bit must be 0 or 1")
        self.time += 1
        if bit == 1:
            self.buckets.setdefault(1, deque()).append(Bucket(size=1, timestamp=self.time))
            self._merge()
        self._drop_old()

    def estimate(self, last_n: int | None = None) -> float:
        n = self.window_size if last_n is None else last_n
        cutoff = self.time - n
        total = 0.0
        overlapping: list[Bucket] = []
        for size, q in self.buckets.items():
            for bucket in q:
                if bucket.timestamp <= cutoff:
                    continue
                # oldest timestamp in bucket is unknown; treat bucket as overlapping
                # if its timestamp is the oldest among remaining buckets of this size
                overlapping.append(bucket)
        if not overlapping:
            return 0.0
        overlapping.sort(key=lambda b: b.timestamp)
        oldest = overlapping[0]
        for bucket in overlapping:
            if bucket is oldest:
                total += bucket.size / 2.0
            else:
                total += bucket.size
        return total

    def exact_count_reference(self, bits: list[int], last_n: int) -> int:
        return sum(bits[-last_n:])


def main() -> None:
    window = 100
    dgim = DGIM(window)
    bits = [1 if i % 3 != 0 else 0 for i in range(250)]
    for bit in bits:
        dgim.add_bit(bit)
    est = dgim.estimate(100)
    exact = sum(bits[-100:])
    print(f"window=100 exact={exact} DGIM={est:.1f}")


if __name__ == "__main__":
    main()
