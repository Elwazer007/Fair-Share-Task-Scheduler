import time

import pytest

from scheduler.runtime.handlers import JobError, run_payload


def test_sleep_waits_for_given_seconds():
    started = time.monotonic()
    run_payload({"kind": "sleep", "seconds": 0.1})
    assert time.monotonic() - started >= 0.1


def test_fail_raises_with_message():
    with pytest.raises(JobError, match="boom"):
        run_payload({"kind": "fail", "message": "boom"})


@pytest.mark.parametrize("payload", [{}, {"kind": "teleport"}, {"kind": "sleep", "seconds": -1}])
def test_bad_payloads_raise(payload):
    with pytest.raises(JobError):
        run_payload(payload)
