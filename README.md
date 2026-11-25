# Fair-Share Task Scheduler

A multi-tenant job scheduler in Django. Each tenant gets worker time in proportion to its weight, so one tenant flooding the queue can't starve the others. Jobs survive worker crashes and are retried with backoff.

## How it works

- **API** (Django + DRF): accepts jobs and writes them to Postgres, then queues them in Redis.
- **Postgres**: the source of truth for tenants and jobs.
- **Redis**: scheduling state. Atomic Lua scripts pick the next tenant by weighted fair queuing on virtual time, charged by actual runtime.
- **Workers**: claim jobs under a 30 s lease, renew it every 10 s, and run each job in its own slot.
- **Maintenance**:
  - requeues jobs whose lease expired;
  - moves retries back into the queue once their backoff is over;
  - every 30 s, puts back any open job that's missing from Redis;
  - rebuilds Redis from Postgres if Redis loses its data.

Delivery is at least once. A job gets at most 3 attempts, waiting 2 s, then 4 s, and so on up to 60 s, plus jitter. Each attempt has a number, and a late result from an older attempt is ignored.

## Run

```bash
docker compose up -d --build
docker compose run --rm api python manage.py migrate
docker compose exec api python manage.py create_tenant acme --weight 2
```

Host ports: API `18000`, Postgres `15432`, Redis `16379` (override with `API_HOST_PORT`, `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`).

## API

```bash
# submit one job
curl -X POST localhost:18000/api/jobs -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","payload":{"kind":"sleep","seconds":5}}'

# submit up to 1000 jobs
curl -X POST localhost:18000/api/jobs/batch -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","payloads":[{"kind":"sleep","seconds":1},{"kind":"fail"}]}'

# job status
curl localhost:18000/api/jobs/<id>
```

Payload kinds: `sleep` (`seconds`, 0–600) and `fail` (`message`). States: `queued → running → succeeded`, or `retrying` between attempts and `failed` after the last one.

## Test

```bash
docker compose --profile tools run --rm test          # full test suite
./scripts/crash_test.sh                               # kills a worker mid-job, expects success on attempt 2
```

## Commands

| Command | Purpose |
| --- | --- |
| `create_tenant <id> --weight N --max-queued N` | create or update a tenant |
| `run_worker --slots N` | run a worker |
| `run_maintenance` | reaper, retry promoter, reconciler |
| `rebuild_queues` | restore every open job from Postgres into Redis |
