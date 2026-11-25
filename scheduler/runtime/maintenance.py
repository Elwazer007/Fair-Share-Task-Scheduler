import logging
import threading
import time

from django.db import close_old_connections, connections

from scheduler.fairqueue.scripts import FairQueue
from scheduler.services.reconcile import rebuild, reconcile
from scheduler.services.recovery import handle_expired

logger = logging.getLogger(__name__)


class Maintenance:
    def __init__(self, fair_queue=None, tick=1.0, reconcile_every=30.0):
        self.fair_queue = fair_queue or FairQueue.default()
        self.tick = tick
        self.reconcile_every = reconcile_every
        self._last_reconcile = None
        self._stopping = threading.Event()

    def stop(self):
        self._stopping.set()

    def run(self):
        logger.info("maintenance started")
        try:
            while not self._stopping.is_set():
                started = time.monotonic()
                try:
                    close_old_connections()
                    self.run_once()
                except Exception:
                    logger.exception("maintenance pass failed")
                self._stopping.wait(max(0.0, self.tick - (time.monotonic() - started)))
        finally:
            connections.close_all()
            logger.info("maintenance stopped")

    def run_once(self):
        if not self.fair_queue.has_epoch():
            rebuild(self.fair_queue)
            self._last_reconcile = time.monotonic()
        self.reap()
        self.fair_queue.promote()
        if (
            self._last_reconcile is None
            or time.monotonic() - self._last_reconcile >= self.reconcile_every
        ):
            reconcile(self.fair_queue)
            self._last_reconcile = time.monotonic()

    def reap(self):
        handled = 0
        for job_id, attempt in self.fair_queue.expired():
            try:
                outcome = handle_expired(self.fair_queue, job_id, attempt)
                logger.warning("lease of job %s attempt %d expired: %s", job_id, attempt, outcome)
                handled += 1
            except Exception:
                logger.exception("could not recover job %s attempt %d", job_id, attempt)
        return handled
