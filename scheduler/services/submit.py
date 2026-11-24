import logging

from django.db import DatabaseError, transaction
from redis.exceptions import RedisError

from scheduler.fairqueue.scripts import FairQueue, QueueFull
from scheduler.models import Job, Tenant
from scheduler.services.errors import StoreUnavailable, TenantNotFound, TooManyQueuedJobs

logger = logging.getLogger(__name__)


def submit_jobs(tenant_id, payloads):
    try:
        max_queued = (
            Tenant.objects.filter(pk=tenant_id).values_list("max_queued", flat=True).first()
        )
        if max_queued is None:
            raise TenantNotFound(tenant_id)
        with transaction.atomic():
            jobs = Job.objects.bulk_create(
                [Job(tenant_id=tenant_id, payload=payload) for payload in payloads]
            )
    except DatabaseError as exc:
        raise StoreUnavailable("database unavailable") from exc

    ids = [str(job.id) for job in jobs]
    try:
        FairQueue.default().enqueue(tenant_id, ids, max_queued=max_queued)
    except QueueFull:
        Job.objects.filter(id__in=ids).delete()
        raise TooManyQueuedJobs(f"{tenant_id} has {max_queued} or more queued jobs") from None
    except RedisError as exc:
        logger.warning("enqueue failed for %d jobs of %s, rolling back", len(ids), tenant_id)
        Job.objects.filter(id__in=ids).delete()
        raise StoreUnavailable("queue unavailable") from exc
    return ids
