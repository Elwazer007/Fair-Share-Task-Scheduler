import json
import time
from dataclasses import dataclass

from django.db import connection

from scheduler.services.backoff import retry_delay

MAX_ERROR_LENGTH = 4096

START_SQL = """
UPDATE scheduler_job
SET state = 'running',
    attempts = %(attempt)s,
    worker_id = %(worker)s,
    started_at = now(),
    first_started_at = COALESCE(first_started_at, now()),
    run_after = NULL
WHERE id = %(id)s
  AND attempts < %(attempt)s
  AND state IN ('queued', 'retrying')
RETURNING payload
"""

SUCCEED_SQL = """
UPDATE scheduler_job
SET state = 'succeeded',
    finished_at = now(),
    runtime_seconds = runtime_seconds + EXTRACT(EPOCH FROM now() - started_at)
WHERE id = %(id)s
  AND attempts = %(attempt)s
  AND state = 'running'
"""

FAIL_SQL = """
UPDATE scheduler_job
SET state = CASE WHEN attempts < max_attempts THEN 'retrying' ELSE 'failed' END,
    run_after = CASE WHEN attempts < max_attempts THEN to_timestamp(%(run_after)s) END,
    finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE now() END,
    last_error = %(error)s,
    runtime_seconds = runtime_seconds + EXTRACT(EPOCH FROM now() - started_at)
WHERE id = %(id)s
  AND attempts = %(attempt)s
  AND state = 'running'
RETURNING state, EXTRACT(EPOCH FROM run_after)
"""


@dataclass(frozen=True)
class Decision:
    outcome: str
    run_after: float | None


def start(job_id, attempt, worker_id):
    with connection.cursor() as cursor:
        cursor.execute(START_SQL, {"id": str(job_id), "attempt": attempt, "worker": worker_id})
        row = cursor.fetchone()
    if row is None:
        return None
    return json.loads(row[0]) if isinstance(row[0], str) else row[0]


def succeed(job_id, attempt):
    with connection.cursor() as cursor:
        cursor.execute(SUCCEED_SQL, {"id": str(job_id), "attempt": attempt})
        return cursor.rowcount == 1


def fail(job_id, attempt, error):
    params = {
        "id": str(job_id),
        "attempt": attempt,
        "error": str(error)[:MAX_ERROR_LENGTH],
        "run_after": time.time() + retry_delay(attempt),
    }
    with connection.cursor() as cursor:
        cursor.execute(FAIL_SQL, params)
        row = cursor.fetchone()
    if row is None:
        return None
    state, run_after = row
    if state == "failed":
        return Decision("failed", None)
    return Decision("retry", float(run_after))
