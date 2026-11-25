import threading
import time

import pytest

from scheduler.models import Job, JobState
from scheduler.runtime.worker import Worker
from scheduler.services import lifecycle
from scheduler.services.submit import submit_jobs

NOW = 1_000_000.0


def run_one_failing_attempt(fq, job_id, worker_id):
    fq.promote(now=time.time() + 120)
    [claim] = fq.claim(worker_id, 1, now=NOW)
    assert claim.job_id == job_id
    assert lifecycle.start(job_id, claim.attempt, worker_id) is not None
    decision = lifecycle.fail(job_id, claim.attempt, "boom")
    assert fq.release(job_id, claim.attempt, decision.outcome, run_after=decision.run_after) == "ok"
    return claim.attempt, decision


@pytest.mark.django_db
def test_failing_job_is_tried_three_times_then_failed(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "fail"}])

    for expected in (1, 2):
        attempt, decision = run_one_failing_attempt(fq, job_id, f"w{expected}")
        assert attempt == expected
        assert decision.outcome == "retry"
        job = Job.objects.get(pk=job_id)
        assert job.state == JobState.RETRYING
        assert job.attempts == expected
        assert job.run_after is not None
        assert job.finished_at is None
        assert redis_client.sismember(fq.keys.known, job_id)

    attempt, decision = run_one_failing_attempt(fq, job_id, "w3")
    assert (attempt, decision.outcome) == (3, "failed")
    job = Job.objects.get(pk=job_id)
    assert job.state == JobState.FAILED
    assert job.attempts == 3
    assert job.last_error == "boom"
    assert job.finished_at is not None
    assert job.run_after is None
    assert not redis_client.sismember(fq.keys.known, job_id)
    assert redis_client.hget(fq.keys.attempt, job_id) is None
    assert fq.promote(now=time.time() + 120) == 0


@pytest.mark.django_db
def test_retry_waits_longer_each_time(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "fail"}])
    waits = []
    for n in (1, 2):
        _, decision = run_one_failing_attempt(fq, job_id, f"w{n}")
        waits.append(decision.run_after - time.time())
    assert 1.5 < waits[0] <= 2.4
    assert 3.5 < waits[1] <= 4.8


@pytest.mark.django_db
def test_long_errors_are_cut_to_four_kilobytes(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "fail"}])
    [claim] = fq.claim("w1", 1, now=NOW)
    lifecycle.start(job_id, claim.attempt, "w1")

    lifecycle.fail(job_id, claim.attempt, "x" * 10_000)
    assert len(Job.objects.get(pk=job_id).last_error) == 4096


@pytest.mark.django_db(transaction=True)
def test_worker_turns_a_handler_error_into_a_retry(make_tenant, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "fail", "message": "disk full"}])
    worker = Worker(slots=1, worker_id="w1")
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            job = Job.objects.get(pk=job_id)
            if job.state == JobState.RETRYING:
                break
            time.sleep(0.05)
    finally:
        worker.stop()
        thread.join(timeout=10)

    assert job.state == JobState.RETRYING
    assert job.attempts == 1
    assert "disk full" in job.last_error
    assert redis_client.zcard("fs:delayed") == 1
    assert redis_client.zcard("fs:leases") == 0
