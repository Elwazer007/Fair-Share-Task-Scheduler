from tests.fairqueue.sim import START, Simulation


def test_long_and_short_jobs_get_equal_worker_time_at_equal_weight(fq):
    sim = Simulation(fq, slots=4, durations={"long": 300.0, "short": 0.5})
    sim.add_jobs("long", 100)
    sim.add_jobs("short", 50_000)

    run = sim.advance_to(START + 6300)

    shares = run.shares(START + 300, START + 6300)
    assert 0.4 <= shares["long"] <= 0.6, shares
    assert 0.4 <= shares["short"] <= 0.6, shares


def test_short_jobs_keep_a_real_share_while_long_jobs_run(fq):
    sim = Simulation(fq, slots=4, durations={"long": 300.0, "short": 0.5})
    sim.add_jobs("long", 100)
    sim.add_jobs("short", 50_000)

    run = sim.advance_to(START + 1200)

    assert run.busy("short", START + 300, START + 1200) > 0.3 * 4 * 900
