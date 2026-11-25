from dataclasses import dataclass
from functools import cache
from pathlib import Path

import redis
from django.conf import settings

from scheduler.fairqueue.keys import PREFIX, Keys

LUA_DIR = Path(__file__).parent / "lua"
SCRIPT_NAMES = ("enqueue", "claim", "heartbeat", "release", "expired", "promote", "restore")
SCAN_LIMIT = 500


class QueueFull(Exception):
    pass


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
    return (LUA_DIR / "lib.lua").read_text() + "\n" + (LUA_DIR / f"{name}.lua").read_text()


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

    def enqueue(self, tenant, job_ids, head=False, max_queued=0):
        if not job_ids:
            return 0
        added = int(
            self._run("enqueue", tenant, "1" if head else "0", int(max_queued), *map(str, job_ids))
        )
        if added < 0:
            raise QueueFull(tenant)
        return added

    def claim(self, worker_id, n, lease_ttl=None, now=None, cost=None):
        rows = self._run(
            "claim",
            worker_id,
            int(n),
            lease_ttl or settings.LEASE_TTL_SECONDS,
            _num(now),
            _num(self._default_cost(cost)),
        )
        return [Claim(job, tenant, int(attempt)) for job, tenant, attempt in rows]

    def heartbeat(self, worker_id, held, lease_ttl=None, now=None):
        if not held:
            return []
        pairs = [str(value) for job_id, attempt in held for value in (job_id, int(attempt))]
        return self._run(
            "heartbeat", worker_id, lease_ttl or settings.LEASE_TTL_SECONDS, _num(now), *pairs
        )

    def release(self, job_id, attempt, outcome, run_after=None, now=None, cost=None):
        if outcome not in ("done", "retry", "failed"):
            raise ValueError(f"unknown outcome {outcome!r}")
        if outcome == "retry" and run_after is None:
            raise ValueError("retry needs run_after")
        return self._run(
            "release",
            str(job_id),
            int(attempt),
            outcome,
            _num(run_after),
            _num(now),
            _num(self._default_cost(cost)),
        )

    def expired(self, now=None, limit=SCAN_LIMIT):
        rows = self._run("expired", _num(now), int(limit))
        return [(job_id, int(attempt)) for job_id, attempt in rows]

    def promote(self, now=None, limit=SCAN_LIMIT):
        return int(self._run("promote", _num(now), int(limit)))

    def restore(self, tenant, jobs, now=None):
        if not jobs:
            return 0
        args = [
            str(value)
            for job_id, attempts, run_after in jobs
            for value in (job_id, int(attempts), _num(run_after))
        ]
        return int(self._run("restore", tenant, _num(now), *args))

    def set_weight(self, tenant, weight):
        self.client.hset(self.keys.weight, tenant, int(weight))

    def set_weights(self, weights):
        if weights:
            self.client.hset(self.keys.weight, mapping={t: int(w) for t, w in weights.items()})

    def has_epoch(self):
        return bool(self.client.exists(self.keys.epoch))

    def mark_epoch(self, value):
        self.client.set(self.keys.epoch, value)

    @staticmethod
    def _default_cost(cost):
        return cost if cost is not None else settings.DEFAULT_COST_SECONDS
