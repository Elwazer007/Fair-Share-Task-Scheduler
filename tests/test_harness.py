from django.db import connection


def test_postgres_reachable(db):
    with connection.cursor() as cursor:
        cursor.execute("select 1")
        assert cursor.fetchone() == (1,)
    assert connection.vendor == "postgresql"


def test_redis_is_isolated_and_empty(redis_client):
    assert redis_client.dbsize() == 0
    redis_client.set("probe", "1")
    assert redis_client.get("probe") == "1"


def test_clock_advances(clock):
    start = clock.now
    assert clock.advance(2.5) == start + 2.5
