import os
from dataclasses import dataclass

import pytest
import redis as redis_lib

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture(autouse=True)
def _test_redis_settings(settings):
    settings.REDIS_URL = TEST_REDIS_URL


@pytest.fixture
def redis_client():
    client = redis_lib.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    if client.connection_pool.connection_kwargs.get("db", 0) == 0:
        raise RuntimeError("tests must not run against Redis database 0")
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def make_tenant(db):
    from scheduler.models import Tenant

    def factory(tenant_id, weight=1, **fields):
        tenant, _ = Tenant.objects.update_or_create(
            id=tenant_id, defaults={"weight": weight, **fields}
        )
        return tenant

    return factory


@dataclass
class FixedClock:
    now: float = 1_000_000.0

    def advance(self, seconds):
        self.now += seconds
        return self.now


@pytest.fixture
def clock():
    return FixedClock()


@pytest.fixture
def fq(redis_client):
    from scheduler.fairqueue.scripts import FairQueue

    return FairQueue(redis_client)
