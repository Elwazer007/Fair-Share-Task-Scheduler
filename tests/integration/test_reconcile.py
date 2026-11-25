import time
from datetime import timedelta

import pytest
from django.db.models import F

from scheduler.models import Job, JobState
from scheduler.services import lifecycle
from scheduler.services.reconcile import rebuild, reconcile
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db

NOW = 1_000_000.0


def age(*job_ids, seconds=60):
    Job.objects.filter(id__in=job_ids).update(
        submitted_at=F("submitted_at") - timedelta(seconds=seconds)
    )


def test_committed_job_missing_from_redis_is_enqueued(make_tenant, fq, redis_client):
    make_tenant("acme")
    job = Job.objects.create(tenant_id="acme", payload={"kind": "sleep", "seconds": 0})
    age(job.id)

    reconcile(fq)

    assert redis_client.lrange(fq.keys.queue("acme"), 0, -1) == [str(job.id)]
    [claim] = fq.claim("w1", 1, now=NOW)
    assert claim.job_id == str(job.id)


def test_fresh_jobs_are_left_to_the_api(make_tenant, fq, redis_client):
    make_tenant("acme")
    Job.objects.create(tenant_id="acme", payload={"kind": "sleep", "seconds": 0})

    reconcile(fq)

    assert redis_client.llen(fq.keys.queue("acme")) == 0


def test_reconcile_twice_enqueues_once(make_tenant, fq, redis_client):
    make_tenant("acme")
    job = Job.objects.create(tenant_id="acme", payload={"kind": "sleep", "seconds": 0})
    age(job.id)

    reconcile(fq)
    reconcile(fq)

    assert redis_client.llen(fq.keys.queue("acme")) == 1


def test_rebuild_after_redis_loses_everything(make_tenant, fq, redis_client):
    make_tenant("big", weight=4)
    make_tenant("small")
    running, retrying, done = submit_jobs("small", [{"kind": "sleep", "seconds": 0}] * 3)
    for job_id in (running, retrying, done):
        fq.client.lrem(fq.keys.queue("small"), 0, job_id)
        fq.client.lpush(fq.keys.queue("small"), job_id)
        [claim] = fq.claim("w1", 1, now=NOW)
        assert claim.job_id == job_id
        lifecycle.start(job_id, claim.attempt, "w1")
    queued = submit_jobs("big", [{"kind": "sleep", "seconds": 0}] * 3)
    lifecycle.fail(retrying, 1, "boom")
    lifecycle.succeed(done, 1)
    age(*queued, running, retrying, done)

    redis_client.flushdb()
    rebuild(fq)

    assert redis_client.hget(fq.keys.weight, "big") == "4"
    assert redis_client.exists(fq.keys.epoch)
    assert redis_client.lrange(fq.keys.queue("big"), 0, -1) == queued
    assert redis_client.smembers(fq.keys.known) == {*queued, running, retrying}

    lost = Job.objects.get(pk=running)
    assert lost.state == JobState.RETRYING
    assert lost.last_error == "lease lost"
    delayed = {m.split("|")[1] for m in redis_client.zrange(fq.keys.delayed, 0, -1)}
    assert delayed == {running, retrying}

    fq.promote(now=time.time() + 120)
    claims = {c.job_id: c.attempt for c in fq.claim("w2", 10, now=NOW + 100)}
    assert claims[running] == 2
    assert claims[retrying] == 2
    assert all(claims[j] == 1 for j in queued)
    assert lifecycle.start(running, claims[running], "w2") is not None


def test_rebuild_twice_restores_each_job_once(make_tenant, fq, redis_client):
    make_tenant("acme")
    submit_jobs("acme", [{"kind": "sleep", "seconds": 0}] * 4)
    redis_client.flushdb()

    rebuild(fq)
    rebuild(fq)

    assert redis_client.llen(fq.keys.queue("acme")) == 4
