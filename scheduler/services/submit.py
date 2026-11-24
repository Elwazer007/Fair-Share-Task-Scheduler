import logging

from django.db import DatabaseError, transaction
from redis.exceptions import RedisError

from scheduler.fairqueue.scripts import FairQueue
from scheduler.models import Job, Tenant
from scheduler.services.errors import StoreUnavailable, TenantNotFound

logger = logging.getLogger(__name__)


def submit_jobs(tenant_id, payloads):
    try:
        if not Tenant.objects.filter(pk=tenant_id).exists():
            raise TenantNotFound(tenant_id)
        with transaction.atomic():
            jobs = Job.objects.bulk_create(
                [Job(tenant_id=tenant_id, payload=payload) for payload in payloads]
            )
    except DatabaseError as exc:
        raise StoreUnavailable("database unavailable") from exc

    ids = [str(job.id) for job in jobs]
    try:
        FairQueue.default().enqueue(tenant_id, ids)
    except RedisError as exc:
        logger.warning("enqueue failed for %d jobs of %s, rolling back", len(ids), tenant_id)
        Job.objects.filter(id__in=ids).delete()
        raise StoreUnavailable("queue unavailable") from exc
    return ids
