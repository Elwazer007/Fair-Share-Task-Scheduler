import pytest

from scheduler.models import Job, JobState
from scheduler.services import lifecycle
from scheduler.services.recovery import handle_expired
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db

NOW = 1_000_000.0


@pytest.fixture
def started(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 1}])
    [claim] = fq.claim("w1", 1, now=NOW)
    lifecycle.start(job_id, claim.attempt, "w1")
    return job_id


def test_reaper_finishes_a_retry_it_crashed_halfway_through(started, fq, redis_client):
    job_id = started
    lifecycle.fail(job_id, 1, "lease expired")
    first_decision = Job.objects.values("state", "attempts", "run_after").get(pk=job_id)

    assert handle_expired(fq, job_id, 1) == "retry"
    assert Job.objects.values("state", "attempts", "run_after").get(pk=job_id) == first_decision
    assert first_decision["attempts"] == 1
    assert redis_client.exists(fq.keys.lease(job_id)) == 0
    assert redis_client.zscore(fq.keys.delayed, f"acme|{job_id}") == pytest.approx(
        first_decision["run_after"].timestamp()
    )
    assert redis_client.sismember(fq.keys.known, job_id)


def test_repeating_the_reaper_step_is_harmless(started, fq, redis_client):
    job_id = started
    assert handle_expired(fq, job_id, 1) == "retry"
    snapshot = Job.objects.values().get(pk=job_id)
    delayed = redis_client.zrange(fq.keys.delayed, 0, -1, withscores=True)

    handle_expired(fq, job_id, 1)

    assert Job.objects.values().get(pk=job_id) == snapshot
    assert redis_client.zrange(fq.keys.delayed, 0, -1, withscores=True) == delayed


def test_job_that_finished_before_its_worker_died_is_only_released(started, fq, redis_client):
    job_id = started
    assert lifecycle.succeed(job_id, 1) is True

    assert handle_expired(fq, job_id, 1) == "done"
    assert Job.objects.get(pk=job_id).state == JobState.SUCCEEDED
    assert not redis_client.sismember(fq.keys.known, job_id)
    assert redis_client.zcard(fq.keys.delayed) == 0
