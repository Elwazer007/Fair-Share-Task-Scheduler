import uuid

import pytest

from scheduler.models import Job

pytestmark = pytest.mark.django_db


@pytest.fixture
def tenant(make_tenant, redis_client):
    return make_tenant("acme", weight=2)


def submit(client, body):
    return client.post("/api/jobs", body, format="json")


def test_submit_returns_201_with_id_and_queued_state(api_client, tenant, fq, redis_client):
    response = submit(api_client, {"tenant": "acme", "payload": {"kind": "sleep", "seconds": 1}})

    assert response.status_code == 201
    body = response.json()
    assert body["state"] == "queued"
    job = Job.objects.get(pk=body["id"])
    assert job.tenant_id == "acme"
    assert job.payload == {"kind": "sleep", "seconds": 1}
    assert redis_client.lrange(fq.keys.queue("acme"), 0, -1) == [body["id"]]


@pytest.mark.parametrize(
    "body",
    [
        {"payload": {"kind": "sleep"}},
        {"tenant": "acme"},
        {"tenant": "acme", "payload": "not-an-object"},
        {"tenant": "acme", "payload": {"blob": "x" * 70_000}},
    ],
)
def test_submit_rejects_invalid_body(api_client, tenant, body):
    response = submit(api_client, body)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"
    assert Job.objects.count() == 0


def test_submit_rejects_malformed_json(api_client, tenant):
    response = api_client.post("/api/jobs", "{nope", content_type="application/json")
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_submit_unknown_tenant_is_404(api_client, redis_client):
    response = submit(api_client, {"tenant": "ghost", "payload": {}})

    assert response.status_code == 404
    assert response.json()["error"] == "tenant_not_found"


def test_submit_returns_503_and_keeps_nothing_when_redis_is_down(api_client, tenant, settings):
    settings.REDIS_URL = "redis://127.0.0.1:1/15"

    response = submit(api_client, {"tenant": "acme", "payload": {}})

    assert response.status_code == 503
    assert response.json()["error"] == "store_unavailable"
    assert Job.objects.count() == 0


def test_status_returns_job_fields(api_client, tenant):
    job_id = submit(api_client, {"tenant": "acme", "payload": {"kind": "sleep"}}).json()["id"]

    response = api_client.get(f"/api/jobs/{job_id}")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "id",
        "tenant",
        "state",
        "attempts",
        "max_attempts",
        "submitted_at",
        "first_started_at",
        "started_at",
        "finished_at",
        "run_after",
        "last_error",
    }
    assert body["id"] == job_id
    assert body["tenant"] == "acme"
    assert body["state"] == "queued"
    assert body["attempts"] == 0
    assert body["first_started_at"] is None
    assert body["last_error"] is None


@pytest.mark.parametrize("job_id", [str(uuid.uuid4()), "not-a-uuid"])
def test_status_unknown_job_is_404(api_client, db, job_id):
    response = api_client.get(f"/api/jobs/{job_id}")

    assert response.status_code == 404
    assert response.json()["error"] == "job_not_found"
