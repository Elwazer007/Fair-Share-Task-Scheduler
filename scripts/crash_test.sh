#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:${API_PORT:-18000}/api"
TENANT=crash-test
TIMEOUT=${TIMEOUT:-120}

job_field() {
  curl -fsS "$API/jobs/$1" | python3 -c "import sys, json; print(json.load(sys.stdin)['$2'])"
}

docker compose up -d --wait postgres redis api worker maintenance >/dev/null
docker compose exec -T api python manage.py create_tenant "$TENANT" >/dev/null

JOB=$(curl -fsS -X POST "$API/jobs" -H 'Content-Type: application/json' \
  -d "{\"tenant\": \"$TENANT\", \"payload\": {\"kind\": \"sleep\", \"seconds\": 20}}" |
  python3 -c "import sys, json; print(json.load(sys.stdin)['id'])")
echo "submitted $JOB"

for _ in $(seq 60); do
  [ "$(job_field "$JOB" state)" = running ] && break
  sleep 1
done
[ "$(job_field "$JOB" state)" = running ] || { echo "job never started"; exit 1; }

WORKER=$(docker compose exec -T postgres psql -U scheduler -d scheduler -tAc \
  "SELECT split_part(worker_id, ':', 1) FROM scheduler_job WHERE id = '$JOB'")
echo "killing worker container $WORKER"
docker kill --signal KILL "$WORKER" >/dev/null
docker compose up -d worker >/dev/null

START=$(date +%s)
while [ $(($(date +%s) - START)) -lt "$TIMEOUT" ]; do
  STATE=$(job_field "$JOB" state)
  if [ "$STATE" = succeeded ]; then
    ATTEMPTS=$(job_field "$JOB" attempts)
    if [ "$ATTEMPTS" = 2 ]; then
      echo "PASS: $JOB succeeded on attempt 2 after its worker was killed"
      exit 0
    fi
    echo "FAIL: succeeded with attempts=$ATTEMPTS"
    exit 1
  fi
  [ "$STATE" = failed ] && { echo "FAIL: job failed"; exit 1; }
  sleep 2
done
echo "FAIL: job not finished after ${TIMEOUT}s (state=$(job_field "$JOB" state))"
exit 1
