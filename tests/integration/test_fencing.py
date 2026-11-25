import time

import pytest

from scheduler.models import Job, JobState
from scheduler.services import lifecycle
from scheduler.services.recovery import handle_expired
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db

NOW = 1_000_000.0


@pytest.fixture
def two_attempts(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 1}])
    [old] = fq.claim("old", 1, now=NOW)
    lifecycle.start(job_id, old.attempt, "old")
    handle_expired(fq, job_id, old.attempt)
    fq.promote(now=time.time() + 120)
    [new] = fq.claim("new", 1, now=NOW + 40)
    lifecycle.start(job_id, new.attempt, "new")
    return job_id, old.attempt, new.attempt


def test_late_result_from_an_old_attempt_changes_nothing(two_attempts, fq, redis_client):
    job_id, old, new = two_attempts
    before = Job.objects.values().get(pk=job_id)
    lease_before = redis_client.hgetall(fq.keys.lease(job_id))

    assert lifecycle.succeed(job_id, old) is False
    assert lifecycle.fail(job_id, old, "late failure") is None
    assert fq.release(job_id, old, "done") == "stale"
    assert fq.heartbeat("old", [(job_id, old)], now=NOW + 41) == [job_id]

    assert Job.objects.values().get(pk=job_id) == before
    assert before["state"] == JobState.RUNNING
    assert before["attempts"] == new
    assert before["worker_id"] == "new"
    assert redis_client.hgetall(fq.keys.lease(job_id)) == lease_before
    assert redis_client.sismember(fq.keys.known, job_id)


def test_current_attempt_still_finishes(two_attempts, fq, redis_client):
    job_id, _, new = two_attempts

    assert lifecycle.succeed(job_id, new) is True
    assert fq.release(job_id, new, "done") == "ok"
    assert Job.objects.get(pk=job_id).state == JobState.SUCCEEDED
    assert not redis_client.sismember(fq.keys.known, job_id)
