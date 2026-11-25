import random

MAX_DELAY_SECONDS = 60
JITTER = 0.2


def retry_delay(attempt, rng=random.random):
    base = min(MAX_DELAY_SECONDS, 2 ** min(max(attempt, 1), 6))
    return base * (1 + JITTER * rng())
