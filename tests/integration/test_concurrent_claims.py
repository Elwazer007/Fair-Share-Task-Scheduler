from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import redis

from scheduler.fairqueue.scripts import FairQueue
from tests.conftest import TEST_REDIS_URL


def test_parallel_claimers_never_share_a_job(fq):
    for tenant in ("a", "b", "c", "d"):
        fq.enqueue(tenant, [f"{tenant}-{i}" for i in range(125)])

    def drain(worker):
        queue = FairQueue(redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True))
        taken = []
        while batch := queue.claim(worker, 3):
            taken.extend(c.job_id for c in batch)
        return taken

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(drain, [f"w{i}" for i in range(8)]))

    counts = Counter(job for taken in results for job in taken)
    assert len(counts) == 500
    assert set(counts.values()) == {1}
    assert sum(1 for taken in results if taken) > 1
