import json

from django.db import connection

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
