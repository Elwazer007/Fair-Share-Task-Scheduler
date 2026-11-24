import pytest

from scheduler.models import Job, JobState
from scheduler.services import lifecycle
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db


@pytest.fixture
def job_id(make_tenant, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 0}])
    return job_id


def test_claimed_job_runs_to_succeeded(job_id, fq):
    [claim] = fq.claim("w1", 1)

    payload = lifecycle.start(claim.job_id, claim.attempt, "w1")
    assert payload == {"kind": "sleep", "seconds": 0}
    running = Job.objects.get(pk=job_id)
    assert running.state == JobState.RUNNING
    assert running.worker_id == "w1"
    assert running.attempts == 1
    assert running.first_started_at is not None
    assert running.started_at == running.first_started_at

    assert lifecycle.succeed(claim.job_id, claim.attempt) is True
    done = Job.objects.get(pk=job_id)
    assert done.state == JobState.SUCCEEDED
    assert done.finished_at >= done.started_at
    assert done.runtime_seconds >= 0
    assert done.attempts == 1


def test_start_is_rejected_for_an_attempt_already_seen(job_id, fq):
    [claim] = fq.claim("w1", 1)
    assert lifecycle.start(claim.job_id, claim.attempt, "w1") is not None

    assert lifecycle.start(claim.job_id, claim.attempt, "w2") is None
    assert Job.objects.get(pk=job_id).worker_id == "w1"


def test_succeed_is_rejected_for_a_stale_attempt(job_id, fq):
    [claim] = fq.claim("w1", 1)
    lifecycle.start(claim.job_id, claim.attempt, "w1")
    Job.objects.filter(pk=job_id).update(state=JobState.RETRYING)
    assert lifecycle.start(claim.job_id, claim.attempt + 1, "w2") is not None

    assert lifecycle.succeed(claim.job_id, claim.attempt) is False
    job = Job.objects.get(pk=job_id)
    assert job.state == JobState.RUNNING
    assert job.worker_id == "w2"


def test_succeed_twice_does_nothing_the_second_time(job_id, fq):
    [claim] = fq.claim("w1", 1)
    lifecycle.start(claim.job_id, claim.attempt, "w1")
    assert lifecycle.succeed(claim.job_id, claim.attempt) is True
    assert lifecycle.succeed(claim.job_id, claim.attempt) is False
