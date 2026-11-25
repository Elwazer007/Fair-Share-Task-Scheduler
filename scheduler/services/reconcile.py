import logging
import time
from collections import defaultdict
from datetime import timedelta

from django.db.models import Q
from django.utils import timezone

from scheduler.models import TERMINAL_STATES, Job, JobState, Tenant
from scheduler.services import lifecycle

logger = logging.getLogger(__name__)

LEASE_LOST = "lease lost"
MIN_AGE_SECONDS = 30
BATCH = 1000


def reconcile(fair_queue, min_age=MIN_AGE_SECONDS, batch=BATCH):
    fair_queue.set_weights(dict(Tenant.objects.values_list("id", "weight")))
    open_jobs = Job.objects.exclude(state__in=TERMINAL_STATES)
    if min_age > 0:
        open_jobs = open_jobs.filter(submitted_at__lte=timezone.now() - timedelta(seconds=min_age))

    restored = 0
    after = None
    while True:
        page = open_jobs
        if after is not None:
            page = page.filter(
                Q(submitted_at__gt=after[0]) | Q(submitted_at=after[0], id__gt=after[1])
            )
        rows = list(page.order_by("submitted_at", "id").values_list("submitted_at", "id")[:batch])
        if not rows:
            break
        after = rows[-1]
        ids = [str(job_id) for _, job_id in rows]
        seen = fair_queue.client.smismember(fair_queue.keys.known, ids)
        missing = [job_id for job_id, known in zip(ids, seen, strict=True) if not known]
        if missing:
            restored += _restore(fair_queue, missing)

    if restored:
        logger.warning("restored %d jobs missing from the queue", restored)
    return restored


def rebuild(fair_queue):
    restored = reconcile(fair_queue, min_age=0)
    fair_queue.mark_epoch(repr(time.time()))
    logger.warning("rebuilt queues from the database, %d jobs restored", restored)
    return restored


def _restore(fair_queue, job_ids):
    rows = (
        Job.objects.filter(id__in=job_ids)
        .exclude(state__in=TERMINAL_STATES)
        .order_by("submitted_at", "id")
        .values("id", "tenant_id", "state", "attempts", "run_after")
    )
    by_tenant = defaultdict(list)
    for row in rows:
        job_id, attempts = str(row["id"]), row["attempts"]
        run_after = row["run_after"].timestamp() if row["run_after"] else None
        if row["state"] == JobState.RUNNING:
            decision = lifecycle.fail(job_id, attempts, LEASE_LOST)
            if decision is None or decision.outcome == "failed":
                continue
            run_after = decision.run_after
        by_tenant[row["tenant_id"]].append((job_id, attempts, run_after))
    return sum(fair_queue.restore(tenant, jobs) for tenant, jobs in by_tenant.items())
