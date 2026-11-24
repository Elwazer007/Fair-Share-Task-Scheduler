import pytest

from scheduler.models import Job

pytestmark = pytest.mark.django_db


@pytest.fixture
def tenants(make_tenant, redis_client):
    make_tenant("flooder", max_queued=5)
    make_tenant("other")


def batch(client, tenant, n):
    payloads = [{"kind": "sleep", "seconds": 0}] * n
    return client.post("/api/jobs/batch", {"tenant": tenant, "payloads": payloads}, format="json")


def test_submissions_past_the_cap_get_429(api_client, tenants):
    assert batch(api_client, "flooder", 5).status_code == 201

    response = api_client.post("/api/jobs", {"tenant": "flooder", "payload": {}}, format="json")

    assert response.status_code == 429
    assert response.json()["error"] == "too_many_queued_jobs"
    assert Job.objects.filter(tenant_id="flooder").count() == 5


def test_a_batch_that_would_cross_the_cap_is_rejected_whole(api_client, tenants, fq, redis_client):
    batch(api_client, "flooder", 3)

    assert batch(api_client, "flooder", 3).status_code == 429
    assert Job.objects.filter(tenant_id="flooder").count() == 3
    assert redis_client.llen(fq.keys.queue("flooder")) == 3


def test_other_tenants_are_not_affected(api_client, tenants):
    batch(api_client, "flooder", 5)
    assert batch(api_client, "other", 10).status_code == 201
