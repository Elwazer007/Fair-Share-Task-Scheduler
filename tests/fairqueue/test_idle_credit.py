from tests.fairqueue.sim import START, Simulation


def test_returning_tenant_gets_its_share_not_a_burst(fq):
    sim = Simulation(fq, slots=4, durations={"busy": 1.0, "idle": 1.0})
    sim.add_jobs("idle", 1)
    sim.advance_to(START + 5)
    sim.add_jobs("busy", 2000)
    sim.advance_to(START + 255)

    returned_at, seen = sim.now, len(sim.run.claims)
    sim.add_jobs("idle", 200, start=1)
    run = sim.advance_to(returned_at + 20)

    picks = run.picks_since(seen)[:40]
    assert 0.35 <= picks.count("idle") / len(picks) <= 0.65, picks
