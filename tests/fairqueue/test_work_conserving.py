from tests.fairqueue.sim import START, Simulation


def test_lone_tenant_fills_every_free_slot(fq):
    fq.set_weight("solo", 1)
    fq.enqueue("solo", [f"j{i}" for i in range(50)])

    assert len(fq.claim("w1", 16, now=START)) == 16


def test_lone_tenant_keeps_all_workers_busy(fq):
    sim = Simulation(fq, slots=8, durations={"solo": 0.5})
    sim.add_jobs("solo", 2000)

    run = sim.advance_to(START + 60)

    assert run.busy("solo", START, START + 60) >= 0.95 * 8 * 60
