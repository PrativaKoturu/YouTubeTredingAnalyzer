"""Flajolet–Martin distinct-count sketch (syllabus Module 3).

Idea: hash each item to a bit string and remember the largest number of
trailing zeros seen. If that maximum is R, the estimate is 2^R.

Several hash functions are combined with median-of-means to cut variance.
"""

from __future__ import annotations

from statistics import median

import mmh3


def trailing_zeros(value: int, bit_width: int = 32) -> int:
    if value == 0:
        return bit_width
    zeros = 0
    while value & 1 == 0:
        zeros += 1
        value >>= 1
    return zeros


class FlajoletMartin:
    def __init__(self, n_hashes: int = 64, n_groups: int = 8) -> None:
        if n_hashes % n_groups != 0:
            raise ValueError("n_hashes must be divisible by n_groups")
        self.n_hashes = n_hashes
        self.n_groups = n_groups
        self.max_zeros = [0] * n_hashes

    def add(self, item: str) -> None:
        for seed in range(self.n_hashes):
            hashed = mmh3.hash(item, seed=seed, signed=False)
            zeros = trailing_zeros(hashed)
            if zeros > self.max_zeros[seed]:
                self.max_zeros[seed] = zeros

    def estimate(self) -> float:
        """Median of 2^(mean R) over groups, with the classical phi correction.

        Averaging R (not 2^R) first avoids a single lucky hash blowing up the
        estimate; that is the usual 'median of means' trick from MMDS.
        """
        group_size = self.n_hashes // self.n_groups
        group_estimates: list[float] = []
        for g in range(self.n_groups):
            chunk = self.max_zeros[g * group_size : (g + 1) * group_size]
            avg_r = sum(chunk) / group_size
            group_estimates.append(2 ** avg_r)
        phi = 0.77351
        return float(median(group_estimates) / phi)


def exact_distinct(items: list[str]) -> int:
    return len(set(items))


def main() -> None:
    items = [f"channel-{i % 250}" for i in range(5000)]
    fm = FlajoletMartin()
    for item in items:
        fm.add(item)
    exact = exact_distinct(items)
    est = fm.estimate()
    err = abs(est - exact) / exact * 100
    print(f"exact distinct = {exact}")
    print(f"FM estimate    = {est:.1f}")
    print(f"error          = {err:.1f}%")


if __name__ == "__main__":
    main()
