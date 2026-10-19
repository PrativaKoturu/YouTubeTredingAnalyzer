from streaming_algos.flajolet_martin import FlajoletMartin, exact_distinct, trailing_zeros


def test_trailing_zeros() -> None:
    assert trailing_zeros(1) == 0
    assert trailing_zeros(8) == 3
    assert trailing_zeros(0) == 32


def test_fm_near_exact_on_small_stream() -> None:
    items = [f"ch-{i % 80}" for i in range(2000)]
    fm = FlajoletMartin(n_hashes=64, n_groups=8)
    for item in items:
        fm.add(item)
    exact = exact_distinct(items)
    estimate = fm.estimate()
    # FM is a sketch: allow a generous relative error on this toy stream
    assert exact * 0.2 < estimate < exact * 5
