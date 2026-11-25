import random

import pytest

from scheduler.services.backoff import retry_delay


@pytest.mark.parametrize(
    ("attempt", "base"), [(1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (6, 60), (12, 60)]
)
def test_delay_doubles_from_two_seconds_and_caps_at_a_minute(attempt, base):
    assert retry_delay(attempt, rng=lambda: 0.0) == pytest.approx(base)
    assert retry_delay(attempt, rng=lambda: 1.0) == pytest.approx(base * 1.2)


def test_jitter_stays_within_twenty_percent():
    rng = random.Random(7)
    delays = [retry_delay(2, rng=rng.random) for _ in range(500)]
    assert all(4 <= d <= 4.8 for d in delays)
    assert max(delays) - min(delays) > 0.5
