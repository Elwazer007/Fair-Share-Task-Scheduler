from dataclasses import dataclass
from functools import cache
from pathlib import Path

import redis
from django.conf import settings

from scheduler.fairqueue.keys import PREFIX, Keys

LUA_DIR = Path(__file__).parent / "lua"
SCRIPT_NAMES = ("enqueue", "claim", "release")


@dataclass(frozen=True)
class Claim:
    job_id: str
    tenant: str
    attempt: int


@cache
def _client(url):
    return redis.Redis.from_url(url, decode_responses=True)


@cache
def _source(name):
    return (LUA_DIR / f"{name}.lua").read_text()


def _num(value):
    return "" if value is None else repr(float(value))


class FairQueue:
    def __init__(self, client, prefix=PREFIX):
        self.client = client
        self.prefix = prefix
        self.keys = Keys(prefix)
        self._scripts = {name: client.register_script(_source(name)) for name in SCRIPT_NAMES}

    @classmethod
    def default(cls):
        return cls(_client(settings.REDIS_URL))

    def _run(self, name, *args):
        return self._scripts[name](keys=[], args=[self.prefix, *args])

    def enqueue(self, tenant, job_ids, head=False):
        if not job_ids:
            return 0
        return int(self._run("enqueue", tenant, "1" if head else "0", *map(str, job_ids)))

    def claim(self, worker_id, n, lease_ttl=None, now=None, cost=None):
        rows = self._run(
            "claim",
            worker_id,
            int(n),
            lease_ttl or settings.LEASE_TTL_SECONDS,
            _num(now),
            _num(cost if cost is not None else settings.DEFAULT_COST_SECONDS),
        )
        return [Claim(job, tenant, int(attempt)) for job, tenant, attempt in rows]

    def release(self, job_id, attempt, outcome, run_after=None):
        if outcome not in ("done", "retry", "failed"):
            raise ValueError(f"unknown outcome {outcome!r}")
        if outcome == "retry" and run_after is None:
            raise ValueError("retry needs run_after")
        return self._run("release", str(job_id), int(attempt), outcome, _num(run_after))

    def set_weight(self, tenant, weight):
        self.client.hset(self.keys.weight, tenant, int(weight))
