import pytest

NOW = 1_000_000.0


def test_enqueue_pushes_in_order_and_marks_tenant_ready(fq, redis_client):
    assert fq.enqueue("a", ["j1", "j2", "j3"]) == 3
    assert redis_client.lrange(fq.keys.queue("a"), 0, -1) == ["j1", "j2", "j3"]
    assert redis_client.zscore(fq.keys.ready, "a") == 0
    assert redis_client.smembers(fq.keys.known) == {"j1", "j2", "j3"}


def test_enqueue_skips_known_ids(fq, redis_client):
    fq.enqueue("a", ["j1", "j2"])
    assert fq.enqueue("a", ["j2", "j3"]) == 1
    assert redis_client.lrange(fq.keys.queue("a"), 0, -1) == ["j1", "j2", "j3"]


def test_enqueue_at_head_keeps_given_order(fq, redis_client):
    fq.enqueue("a", ["j3"])
    fq.enqueue("a", ["j1", "j2"], head=True)
    assert redis_client.lrange(fq.keys.queue("a"), 0, -1) == ["j1", "j2", "j3"]


def test_claim_on_empty_returns_nothing(fq):
    assert fq.claim("w1", 4, now=NOW) == []


def test_claim_creates_lease_and_counts_running(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, lease_ttl=30, now=NOW, cost=1.0)

    assert (claim.job_id, claim.tenant, claim.attempt) == ("j1", "a", 1)
    lease = redis_client.hgetall(fq.keys.lease("j1"))
    assert lease["worker"] == "w1"
    assert lease["attempt"] == "1"
    assert redis_client.zscore(fq.keys.leases, "j1") == NOW + 30
    assert redis_client.hget(fq.keys.running, "a") == "1"


def test_claim_drops_tenant_from_ready_when_its_queue_empties(fq, redis_client):
    fq.enqueue("a", ["j1"])
    fq.claim("w1", 1, now=NOW)
    assert redis_client.zscore(fq.keys.ready, "a") is None
    assert redis_client.llen(fq.keys.queue("a")) == 0


def test_claim_serves_lowest_virtual_time_and_charges_by_weight(fq, redis_client):
    fq.set_weight("a", 1)
    fq.set_weight("b", 2)
    fq.enqueue("a", [f"a{i}" for i in range(10)])
    fq.enqueue("b", [f"b{i}" for i in range(10)])

    picks = [c.tenant for c in fq.claim("w1", 9, now=NOW, cost=1.0)]

    assert picks.count("b") == 6
    assert picks.count("a") == 3
    assert float(redis_client.hget(fq.keys.vtime, "a")) == 3.0
    assert float(redis_client.hget(fq.keys.vtime, "b")) == 3.0


def test_claim_stops_at_n(fq):
    fq.enqueue("a", [f"j{i}" for i in range(5)])
    assert len(fq.claim("w1", 2, now=NOW)) == 2


def test_attempt_number_increases_on_each_claim(fq):
    fq.enqueue("a", ["j1"])
    [first] = fq.claim("w1", 1, now=NOW)
    fq.release("j1", first.attempt, "retry", run_after=NOW)
    fq.client.lpush(fq.keys.queue("a"), "j1")
    fq.client.zadd(fq.keys.ready, {"a": 0})

    [second] = fq.claim("w2", 1, now=NOW)
    assert second.attempt == 2


def test_release_done_clears_lease_and_known(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, now=NOW)

    assert fq.release("j1", claim.attempt, "done") == "ok"
    assert not redis_client.exists(fq.keys.lease("j1"))
    assert redis_client.zscore(fq.keys.leases, "j1") is None
    assert not redis_client.sismember(fq.keys.known, "j1")
    assert redis_client.hget(fq.keys.running, "a") is None


def test_release_retry_moves_job_to_delayed_and_keeps_it_known(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, now=NOW)

    assert fq.release("j1", claim.attempt, "retry", run_after=NOW + 4) == "ok"
    assert redis_client.zscore(fq.keys.delayed, "a|j1") == NOW + 4
    assert redis_client.sismember(fq.keys.known, "j1")


def test_release_with_old_attempt_is_stale_and_changes_nothing(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, now=NOW)

    assert fq.release("j1", claim.attempt + 1, "done") == "stale"
    assert redis_client.exists(fq.keys.lease("j1"))
    assert redis_client.sismember(fq.keys.known, "j1")


def test_release_without_lease_is_stale(fq):
    assert fq.release("missing", 1, "done") == "stale"


def test_release_rejects_unknown_outcome(fq):
    with pytest.raises(ValueError):
        fq.release("j1", 1, "maybe")


def test_heartbeat_extends_lease_for_current_owner(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, lease_ttl=30, now=NOW)

    lost = fq.heartbeat("w1", [("j1", claim.attempt)], lease_ttl=30, now=NOW + 10)

    assert lost == []
    assert redis_client.zscore(fq.keys.leases, "j1") == NOW + 40


def test_heartbeat_reports_leases_it_no_longer_holds(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, lease_ttl=30, now=NOW)

    lost = fq.heartbeat(
        "w1", [("j1", claim.attempt + 1), ("missing", 1)], lease_ttl=30, now=NOW + 10
    )
    assert sorted(lost) == ["j1", "missing"]
    assert fq.heartbeat("w2", [("j1", claim.attempt)], now=NOW + 10) == ["j1"]
    assert redis_client.zscore(fq.keys.leases, "j1") == NOW + 30


def test_release_charges_actual_runtime(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, now=NOW, cost=1.0)

    fq.release("j1", claim.attempt, "done", now=NOW + 7)

    assert float(redis_client.hget(fq.keys.vtime, "a")) == pytest.approx(7.0)
    assert float(redis_client.hget(fq.keys.usage, "a")) == pytest.approx(7.0)
    assert float(redis_client.hget(fq.keys.est, "a")) == pytest.approx(0.8 * 1.0 + 0.2 * 7)


def test_release_refunds_an_unused_estimate(fq, redis_client):
    fq.enqueue("a", ["j1"])
    [claim] = fq.claim("w1", 1, now=NOW, cost=1.0)

    fq.release("j1", claim.attempt, "done", now=NOW + 0.25)

    assert float(redis_client.hget(fq.keys.vtime, "a")) == pytest.approx(0.25)
    assert float(redis_client.hget(fq.keys.usage, "a")) == pytest.approx(0.25)


def test_enqueue_handles_more_ids_than_fit_in_one_push(fq, redis_client):
    ids = [f"j{i}" for i in range(20_000)]
    assert fq.enqueue("a", ids) == 20_000
    assert redis_client.llen(fq.keys.queue("a")) == 20_000
