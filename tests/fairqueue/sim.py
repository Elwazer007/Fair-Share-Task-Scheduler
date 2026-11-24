import heapq
import itertools
from collections import defaultdict
from dataclasses import dataclass, field

START = 1_000_000.0


@dataclass
class Run:
    claims: list = field(default_factory=list)
    intervals: dict = field(default_factory=lambda: defaultdict(dict))

    def busy(self, tenant, since, until):
        return sum(
            max(0.0, min(end, until) - max(start, since))
            for start, end in self.intervals[tenant].values()
        )

    def shares(self, since, until):
        totals = {t: self.busy(t, since, until) for t in self.intervals}
        whole = sum(totals.values())
        return {t: v / whole for t, v in totals.items()}

    def picks_since(self, index):
        return [tenant for _, tenant in self.claims[index:]]


class Simulation:
    def __init__(self, fq, slots, durations, heartbeat_every=10.0, lease_ttl=30):
        self.fq = fq
        self.slots = slots
        self.durations = durations
        self.heartbeat_every = heartbeat_every
        self.lease_ttl = lease_ttl
        self.now = START
        self.running = []
        self.run = Run()
        self._seq = itertools.count()
        self._next_heartbeat = START + heartbeat_every

    def add_jobs(self, tenant, count, start=0):
        ids = [f"{tenant}-{i}" for i in range(start, start + count)]
        for i in range(0, len(ids), 1000):
            self.fq.enqueue(tenant, ids[i : i + 1000])

    def _fill(self):
        free = self.slots - len(self.running)
        if free <= 0:
            return
        for claim in self.fq.claim("sim", free, lease_ttl=self.lease_ttl, now=self.now):
            duration = self.durations[claim.tenant]
            heapq.heappush(
                self.running,
                (self.now + duration, next(self._seq), claim, self.now),
            )
            self.run.claims.append((self.now, claim.tenant))
            self.run.intervals[claim.tenant][claim.job_id, claim.attempt] = (self.now, float("inf"))

    def advance_to(self, until):
        while True:
            self._fill()
            next_finish = self.running[0][0] if self.running else None
            step = min(t for t in (next_finish, self._next_heartbeat, until) if t is not None)
            if step >= until and (next_finish is None or next_finish > until):
                self.now = until
                return self.run
            self.now = step
            if self.now >= self._next_heartbeat:
                held = [(c.job_id, c.attempt) for _, _, c, _ in self.running]
                if held:
                    self.fq.heartbeat("sim", held, lease_ttl=self.lease_ttl, now=self.now)
                self._next_heartbeat += self.heartbeat_every
            while self.running and self.running[0][0] <= self.now:
                _, _, claim, started = heapq.heappop(self.running)
                self.fq.release(claim.job_id, claim.attempt, "done", now=self.now)
                self.run.intervals[claim.tenant][claim.job_id, claim.attempt] = (started, self.now)
