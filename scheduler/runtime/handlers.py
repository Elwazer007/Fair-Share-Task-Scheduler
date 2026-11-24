import time

MAX_SLEEP_SECONDS = 600


class JobError(Exception):
    pass


def _sleep(payload):
    seconds = payload.get("seconds", 0)
    if not isinstance(seconds, int | float) or not 0 <= seconds <= MAX_SLEEP_SECONDS:
        raise JobError(f"seconds must be between 0 and {MAX_SLEEP_SECONDS}")
    time.sleep(seconds)


def _fail(payload):
    raise JobError(payload.get("message", "job failed on purpose"))


HANDLERS = {"sleep": _sleep, "fail": _fail}


def run_payload(payload):
    handler = HANDLERS.get(payload.get("kind"))
    if handler is None:
        raise JobError(f"unknown job kind {payload.get('kind')!r}")
    handler(payload)
