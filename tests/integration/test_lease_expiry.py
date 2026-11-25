import time

import pytest

from scheduler.models import Job, JobState
from scheduler.services import lifecycle
from scheduler.services.recovery import handle_expired
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db

NOW = 1_000_000.0


@pytest.fixture
def job_id(make_tenant, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 1}])
    return job_id


def test_lease_without_heartbeat_expires_and_job_runs_again(job_id, fq):
    [first] = fq.claim("w1", 1, lease_ttl=30, now=NOW)
    assert lifecycle.start(first.job_id, first.attempt, "w1") is not None

    assert fq.expired(now=NOW + 29) == []
    assert fq.expired(now=NOW + 31) == [(job_id, 1)]

    assert handle_expired(fq, job_id, 1) == "retry"
    job = Job.objects.get(pk=job_id)
    assert job.state == JobState.RETRYING
    assert job.attempts == 1
    assert job.last_error == "lease expired"
    assert job.run_after is not None
    assert fq.expired(now=NOW + 31) == []

    assert fq.promote(now=time.time() + 120) == 1
    [second] = fq.claim("w2", 1, now=NOW + 40)
    assert (second.job_id, second.attempt) == (job_id, 2)
    assert lifecycle.start(second.job_id, second.attempt, "w2") is not None
    assert Job.objects.get(pk=job_id).attempts == 2


def test_claim_that_never_started_is_requeued_without_using_an_attempt(job_id, fq):
    fq.claim("w1", 1, lease_ttl=30, now=NOW)

    assert handle_expired(fq, job_id, 1) == "requeued"
    job = Job.objects.get(pk=job_id)
    assert job.state == JobState.QUEUED
    assert job.attempts == 0

    assert fq.promote(now=time.time() + 1) == 1
    [again] = fq.claim("w2", 1, now=NOW + 40)
    assert again.attempt == 2
    assert lifecycle.start(again.job_id, again.attempt, "w2") is not None


def test_promoted_job_goes_to_the_head_of_its_tenant_queue(make_tenant, fq, redis_client):
    make_tenant("acme")
    first, second = submit_jobs("acme", [{"kind": "sleep", "seconds": 0}] * 2)
    [claim] = fq.claim("w1", 1, now=NOW)
    lifecycle.start(claim.job_id, claim.attempt, "w1")
    handle_expired(fq, first, 1)

    fq.promote(now=time.time() + 120)
    assert redis_client.lrange(fq.keys.queue("acme"), 0, -1) == [first, second]


def test_promote_leaves_jobs_whose_backoff_is_not_over(job_id, fq):
    [claim] = fq.claim("w1", 1, now=NOW)
    lifecycle.start(claim.job_id, claim.attempt, "w1")
    handle_expired(fq, job_id, 1)

    assert fq.promote(now=time.time()) == 0
    assert fq.claim("w2", 1, now=NOW + 40) == []
