import uuid

import pytest

from scheduler.models import Job

pytestmark = pytest.mark.django_db


@pytest.fixture
def tenant(make_tenant, redis_client):
    return make_tenant("acme")


def submit_batch(client, payloads, tenant="acme"):
    return client.post("/api/jobs/batch", {"tenant": tenant, "payloads": payloads}, format="json")


def test_batch_accepts_and_queues_in_order(api_client, tenant, fq, redis_client):
    payloads = [{"kind": "sleep", "seconds": 0.1, "n": i} for i in range(1000)]

    response = submit_batch(api_client, payloads)

    assert response.status_code == 201
    ids = response.json()["ids"]
    assert len(ids) == 1000
    assert redis_client.lrange(fq.keys.queue("acme"), 0, -1) == ids
    stored = dict(Job.objects.values_list("id", "payload"))
    assert [stored[uuid.UUID(i)]["n"] for i in ids] == list(range(1000))


@pytest.mark.parametrize("count", [0, 1001])
def test_batch_rejects_bad_sizes(api_client, tenant, count):
    response = submit_batch(api_client, [{"kind": "sleep"}] * count)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"
    assert Job.objects.count() == 0


def test_batch_rejects_non_object_payload(api_client, tenant):
    response = submit_batch(api_client, [{"kind": "sleep"}, 5])
    assert response.status_code == 400


def test_batch_unknown_tenant_is_404(api_client, redis_client):
    response = submit_batch(api_client, [{}], tenant="ghost")
    assert response.status_code == 404
