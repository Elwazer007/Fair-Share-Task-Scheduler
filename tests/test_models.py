from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from scheduler.models import Job, JobState, Tenant


def test_migrations_match_models(db):
    out = StringIO()
    call_command("makemigrations", "scheduler", "--check", "--dry-run", stdout=out)


def test_saving_tenant_pushes_weight_to_redis(make_tenant, fq, redis_client):
    make_tenant("acme", weight=4)
    assert redis_client.hget(fq.keys.weight, "acme") == "4"


def test_new_job_defaults(make_tenant, settings):
    tenant = make_tenant("acme")
    job = Job.objects.create(tenant=tenant, payload={"kind": "sleep", "seconds": 1})
    assert job.state == JobState.QUEUED
    assert job.attempts == 0
    assert job.max_attempts == settings.MAX_ATTEMPTS
    assert job.submitted_at is not None


def test_create_tenant_command_creates_then_updates(db, fq, redis_client):
    call_command("create_tenant", "acme", "--weight", "2", stdout=StringIO())
    call_command("create_tenant", "acme", "--weight", "4", "--max-queued", "10", stdout=StringIO())

    tenant = Tenant.objects.get(pk="acme")
    assert (tenant.weight, tenant.max_queued) == (4, 10)
    assert redis_client.hget(fq.keys.weight, "acme") == "4"


@pytest.mark.parametrize("tenant_id,weight", [("Bad_Id", 1), ("ok", 0), ("ok", 101)])
def test_create_tenant_command_validates(db, tenant_id, weight):
    with pytest.raises(CommandError):
        call_command("create_tenant", tenant_id, "--weight", str(weight), stdout=StringIO())
