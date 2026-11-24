from tests.fairqueue.sim import START, Simulation


def test_backlogged_tenants_share_worker_time_by_weight(fq):
    for tenant, weight in (("a", 1), ("b", 2), ("c", 4)):
        fq.set_weight(tenant, weight)
    sim = Simulation(fq, slots=7, durations={"a": 2.0, "b": 0.5, "c": 1.0})
    for tenant in ("a", "b", "c"):
        sim.add_jobs(tenant, 5000)

    run = sim.advance_to(START + 300)

    assert len(run.claims) >= 700
    shares = run.shares(START + 60, START + 300)
    for tenant, target in (("a", 1 / 7), ("b", 2 / 7), ("c", 4 / 7)):
        assert abs(shares[tenant] - target) <= 0.1 * target, shares
