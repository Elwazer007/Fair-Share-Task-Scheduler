import pytest

from scheduler.fairqueue.scripts import FairQueue

pytestmark = pytest.mark.django_db


def claim_order(api_client, redis_client, payload_for):
    redis_client.flushdb()
    for i in range(6):
        for tenant in ("a", "b"):
            api_client.post(
                "/api/jobs", {"tenant": tenant, "payload": payload_for(tenant, i)}, format="json"
            )
    queue = FairQueue(redis_client)
    return [c.tenant for c in queue.claim("w", 12, now=1_000_000.0)]


def test_payload_values_do_not_change_scheduling(api_client, make_tenant, redis_client):
    make_tenant("a")
    make_tenant("b")

    plain = claim_order(api_client, redis_client, lambda t, i: {"kind": "sleep", "seconds": 1})
    gamed = claim_order(
        api_client,
        redis_client,
        lambda t, i: {"kind": "sleep", "seconds": 0.001 if t == "a" else 300, "cost": 0.0001},
    )

    assert plain == gamed


def test_redis_holds_no_payload_data(api_client, make_tenant, redis_client):
    make_tenant("a")
    api_client.post("/api/jobs", {"tenant": "a", "payload": {"secret-marker": 1}}, format="json")
    for key in redis_client.scan_iter("*"):
        dumped = redis_client.dump(key)
        assert b"secret-marker" not in dumped
