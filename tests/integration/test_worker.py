import threading
import time

import pytest

from scheduler.models import Job, JobState
from scheduler.runtime.worker import Worker
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db(transaction=True)


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_worker_runs_jobs_on_several_slots(make_tenant, redis_client):
    make_tenant("acme")
    ids = submit_jobs("acme", [{"kind": "sleep", "seconds": 0.3}] * 6)
    worker = Worker(slots=3, worker_id="test-worker")
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert wait_for(lambda: worker.peak_running == 3)
        assert wait_for(
            lambda: Job.objects.filter(id__in=ids, state=JobState.SUCCEEDED).count() == 6
        )
    finally:
        worker.stop()
        thread.join(timeout=10)

    assert not thread.is_alive()
    jobs = Job.objects.filter(id__in=ids)
    assert {j.worker_id for j in jobs} == {"test-worker"}
    assert all(j.attempts == 1 and j.finished_at for j in jobs)
    assert redis_client.scard("fs:known") == 0


def test_worker_stop_waits_for_running_jobs(make_tenant, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 0.5}])
    worker = Worker(slots=1, worker_id="test-worker")
    thread = threading.Thread(target=worker.run)
    thread.start()
    assert wait_for(lambda: Job.objects.get(pk=job_id).state == JobState.RUNNING)

    worker.stop()
    thread.join(timeout=10)

    assert Job.objects.get(pk=job_id).state == JobState.SUCCEEDED


def test_worker_heartbeats_keep_long_jobs_leased(make_tenant, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 1.2}])
    worker = Worker(slots=1, worker_id="test-worker", heartbeat_every=0.2)
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert wait_for(lambda: redis_client.zscore("fs:leases", job_id) is not None)
        first_expiry = redis_client.zscore("fs:leases", job_id)
        assert wait_for(lambda: (redis_client.zscore("fs:leases", job_id) or 0) > first_expiry)
        assert wait_for(lambda: Job.objects.get(pk=job_id).state == JobState.SUCCEEDED)
    finally:
        worker.stop()
        thread.join(timeout=10)
    assert worker.lost == set()
