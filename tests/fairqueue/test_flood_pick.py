from tests.fairqueue.sim import START, Simulation

SMALL = {"small-a": 1, "small-b": 2, "small-c": 4}


def test_small_tenants_jump_ahead_of_a_flood(fq):
    for tenant, weight in SMALL.items():
        fq.set_weight(tenant, weight)
    durations = {"big": 0.5, **dict.fromkeys(SMALL, 0.2)}
    sim = Simulation(fq, slots=10, durations=durations)
    sim.add_jobs("big", 100_000)
    sim.advance_to(START + 60)

    arrived, seen = sim.now, len(sim.run.claims)
    for tenant in SMALL:
        sim.add_jobs(tenant, 5)
    run = sim.advance_to(arrived + 10)

    picks = run.picks_since(seen)
    first_small = {t: picks.index(t) for t in SMALL}
    assert max(first_small.values()) < 10, picks[:20]
    last_small = max(i for i, t in enumerate(picks) if t in SMALL)
    assert last_small < 30, picks[:40]
    finished = {t: max(i for i, p in enumerate(picks) if p == t) for t in SMALL}
    assert finished["small-c"] < finished["small-b"] < finished["small-a"], picks[:40]
    assert "big" in picks[:30]
