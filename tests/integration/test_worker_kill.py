import os
import signal
import subprocess
import sys
import threading
import time

import pytest
from django.conf import settings
from django.db import connection

from scheduler.models import Job, JobState
from scheduler.runtime.maintenance import Maintenance
from scheduler.runtime.worker import Worker
from scheduler.services.submit import submit_jobs

pytestmark = pytest.mark.django_db(transaction=True)


def database_url():
    db = connection.settings_dict
    return f"postgres://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:{db['PORT']}/{db['NAME']}"


def wait_until(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


def test_job_survives_its_worker_being_killed(make_tenant, fq, redis_client):
    make_tenant("acme")
    [job_id] = submit_jobs("acme", [{"kind": "sleep", "seconds": 3}])
    env = {
        **os.environ,
        "DATABASE_URL": database_url(),
        "REDIS_URL": settings.REDIS_URL,
        "LEASE_TTL_SECONDS": "2",
        "HEARTBEAT_SECONDS": "1",
    }
    doomed = subprocess.Popen(
        [sys.executable, "manage.py", "run_worker", "--slots", "1", "--worker-id", "doomed"],
        cwd=settings.BASE_DIR,
        env=env,
    )
    try:
        assert wait_until(lambda: Job.objects.get(pk=job_id).state == JobState.RUNNING, 30)
        doomed.send_signal(signal.SIGKILL)
        doomed.wait(timeout=10)
    finally:
        if doomed.poll() is None:
            doomed.kill()

    rescuer = Worker(slots=1, worker_id="rescuer")
    maintenance = Maintenance(fair_queue=fq)
    thread = threading.Thread(target=rescuer.run)
    thread.start()
    try:

        def finished():
            maintenance.run_once()
            return Job.objects.get(pk=job_id).state == JobState.SUCCEEDED

        assert wait_until(finished, 30)
    finally:
        rescuer.stop()
        thread.join(timeout=10)

    job = Job.objects.get(pk=job_id)
    assert job.attempts == 2
    assert job.worker_id == "rescuer"
    assert job.last_error == "lease expired"
    assert not redis_client.sismember(fq.keys.known, job_id)
