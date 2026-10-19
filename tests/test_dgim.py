from streaming_algos.dgim import DGIM


def test_dgim_all_ones() -> None:
    dgim = DGIM(window_size=16)
    bits = [1] * 16
    for bit in bits:
        dgim.add_bit(bit)
    estimate = dgim.estimate(16)
    exact = 16
    # Classic DGIM error bound is a factor of about 0.5 on the oldest bucket
    assert abs(estimate - exact) / exact <= 0.5


def test_dgim_zeros_stay_zero() -> None:
    dgim = DGIM(window_size=10)
    for _ in range(10):
        dgim.add_bit(0)
    assert dgim.estimate(10) == 0


def test_dgim_tracks_recent_window() -> None:
    dgim = DGIM(window_size=8)
    bits = [1, 0, 1, 1, 0, 0, 1, 1, 1, 0]
    for bit in bits:
        dgim.add_bit(bit)
    exact = sum(bits[-8:])
    estimate = dgim.estimate(8)
    assert abs(estimate - exact) <= exact  # never worse than 100% on this toy
