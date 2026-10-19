from streaming_algos.bloom_filter import BloomFilter, measure_false_positive_rate


def test_no_false_negatives() -> None:
    bloom = BloomFilter(m_bits=2048, k_hashes=4)
    items = [f"video-{i}" for i in range(50)]
    for item in items:
        bloom.add(item)
    for item in items:
        assert bloom.might_contain(item)


def test_fresh_filter_misses_unseen() -> None:
    bloom = BloomFilter(m_bits=2048, k_hashes=4)
    bloom.add("alpha")
    assert not bloom.might_contain("zzzz-not-inserted")


def test_theoretical_fpr_in_range() -> None:
    bloom = BloomFilter(m_bits=10_000, k_hashes=5)
    for i in range(200):
        bloom.add(str(i))
    fpr = bloom.theoretical_fpr()
    assert 0 < fpr < 0.2


def test_measured_fpr_near_theory() -> None:
    measured, theory = measure_false_positive_rate(
        n_insert=200, n_probe=1000, m_bits=4000, k_hashes=4, seed=1
    )
    assert abs(measured - theory) < 0.05
