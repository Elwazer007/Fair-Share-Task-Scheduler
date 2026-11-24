import logging
import queue
import random
import threading

from django.conf import settings
from django.db import close_old_connections, connections
from redis.exceptions import RedisError

from scheduler.fairqueue.scripts import FairQueue
from scheduler.runtime.handlers import run_payload
from scheduler.services import lifecycle

logger = logging.getLogger(__name__)

_DONE = object()


class Worker:
    def __init__(
        self, slots, worker_id, fair_queue=None, idle_sleep=(0.05, 0.2), heartbeat_every=None
    ):
        self.slots = slots
        self.worker_id = worker_id
        self.fair_queue = fair_queue or FairQueue.default()
        self.idle_sleep = idle_sleep
        self.heartbeat_every = heartbeat_every or settings.HEARTBEAT_SECONDS
        self.running = 0
        self.peak_running = 0
        self.lost = set()
        self._held = {}
        self._stopping = threading.Event()
        self._finished = threading.Event()
        self._slot_freed = threading.Condition()
        self._work = queue.SimpleQueue()

    def stop(self):
        self._stopping.set()
        with self._slot_freed:
            self._slot_freed.notify_all()

    def run(self):
        logger.info("worker %s started with %d slots", self.worker_id, self.slots)
        threads = [
            threading.Thread(target=self._slot_loop, name=f"slot-{i}", daemon=True)
            for i in range(self.slots)
        ]
        heartbeat = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        for thread in [*threads, heartbeat]:
            thread.start()
        try:
            self._claim_loop()
        finally:
            for _ in threads:
                self._work.put(_DONE)
            for thread in threads:
                thread.join()
            self._finished.set()
            heartbeat.join()
            connections.close_all()
            logger.info("worker %s stopped", self.worker_id)

    def _claim_loop(self):
        while not self._stopping.is_set():
            free = self._wait_for_free_slots()
            if not free:
                continue
            claims = self._claim(free)
            if not claims:
                self._stopping.wait(random.uniform(*self.idle_sleep))
                continue
            for claim in claims:
                self._take_slot(claim)
                self._work.put(claim)

    def _slot_loop(self):
        try:
            while (claim := self._work.get()) is not _DONE:
                try:
                    self._execute(claim)
                finally:
                    self._free_slot(claim)
        finally:
            connections.close_all()

    def _heartbeat_loop(self):
        while not self._finished.wait(self.heartbeat_every):
            with self._slot_freed:
                held = list(self._held.items())
            if not held:
                continue
            try:
                lost = self.fair_queue.heartbeat(self.worker_id, held)
            except RedisError:
                logger.exception("heartbeat failed")
                continue
            if lost:
                logger.warning("leases lost for %s", ", ".join(lost))
                with self._slot_freed:
                    self.lost.update(lost)

    def _wait_for_free_slots(self):
        with self._slot_freed:
            while self.running >= self.slots and not self._stopping.is_set():
                self._slot_freed.wait(timeout=1)
            return 0 if self._stopping.is_set() else self.slots - self.running

    def _claim(self, n):
        try:
            return self.fair_queue.claim(self.worker_id, n)
        except RedisError:
            logger.exception("claim failed")
            self._stopping.wait(1)
            return []

    def _take_slot(self, claim):
        with self._slot_freed:
            self.running += 1
            self.peak_running = max(self.peak_running, self.running)
            self._held[claim.job_id] = claim.attempt

    def _free_slot(self, claim):
        with self._slot_freed:
            self.running -= 1
            self._held.pop(claim.job_id, None)
            self._slot_freed.notify_all()

    def _execute(self, claim):
        try:
            close_old_connections()
            payload = lifecycle.start(claim.job_id, claim.attempt, self.worker_id)
            if payload is None:
                logger.info("job %s attempt %d is no longer startable", claim.job_id, claim.attempt)
                self.fair_queue.release(claim.job_id, claim.attempt, "failed")
                return
            try:
                run_payload(payload)
            except Exception:
                logger.exception("job %s attempt %d raised", claim.job_id, claim.attempt)
                return
            if lifecycle.succeed(claim.job_id, claim.attempt):
                self.fair_queue.release(claim.job_id, claim.attempt, "done")
        except Exception:
            logger.exception("job %s attempt %d not completed", claim.job_id, claim.attempt)
