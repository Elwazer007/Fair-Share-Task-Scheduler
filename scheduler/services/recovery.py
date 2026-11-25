import time

from scheduler.models import Job, JobState
from scheduler.services import lifecycle

LEASE_EXPIRED = "lease expired"


def handle_expired(fair_queue, job_id, attempt, now=None):
    for _ in range(3):
        job = Job.objects.filter(pk=job_id).values("state", "attempts", "run_after").first()
        if job is None:
            fair_queue.release(job_id, attempt, "failed", now=now)
            return "gone"
        state, attempts = job["state"], job["attempts"]

        if state == JobState.RUNNING and attempts == attempt:
            decision = lifecycle.fail(job_id, attempt, LEASE_EXPIRED)
            if decision is None:
                continue
            fair_queue.release(
                job_id, attempt, decision.outcome, run_after=decision.run_after, now=now
            )
            return decision.outcome
        if state == JobState.SUCCEEDED:
            fair_queue.release(job_id, attempt, "done", now=now)
            return "done"
        if state == JobState.FAILED:
            fair_queue.release(job_id, attempt, "failed", now=now)
            return "failed"
        if state == JobState.RETRYING and attempts == attempt:
            run_after = job["run_after"].timestamp()
            fair_queue.release(job_id, attempt, "retry", run_after=run_after, now=now)
            return "retry"
        if state in (JobState.QUEUED, JobState.RETRYING) and attempts < attempt:
            fair_queue.release(job_id, attempt, "retry", run_after=time.time(), now=now)
            return "requeued"
        break

    fair_queue.release(job_id, attempt, "failed", now=now)
    return "orphaned"
