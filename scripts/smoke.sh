#!/bin/sh
# Run from the repository root. Leaves the stack running for inspection.
set -eu
docker compose up -d --build
metrics_url="http://$(docker compose port collector 8888)"
loki_url="http://$(docker compose port loki 3100)"
wait_for() {
  attempt=0
  until curl -fsS "$1" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
      echo "Timed out waiting for $1" >&2
      docker compose ps
      exit 1
    fi
    sleep 2
  done
}
wait_for "$metrics_url/readyz"
wait_for "$loki_url/ready"
before_exported=$(curl -fsS "$metrics_url/metrics" | awk '$1 == "collector_exported_total" {print $2}')
service="telemetry-smoke-$(date +%s)-$$"
docker compose run --rm --no-deps producer -endpoint collector:4317 -service "$service" -count 20 -batch 5 -interval 0s
attempt=0
while :; do
  result=$(curl -fsSG "$loki_url/loki/api/v1/query_range" \
    --data-urlencode "query={service_name=\"$service\"}" --data-urlencode 'limit=100')
  count=$(printf '%s' "$result" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(len(s["values"]) for s in d["data"]["result"]))')
  if [ "$count" -eq 20 ]; then
    after_exported=$(curl -fsS "$metrics_url/metrics" | awk '$1 == "collector_exported_total" {print $2}')
    python3 -c 'import sys; assert float(sys.argv[2])-float(sys.argv[1]) >= 20, "collector did not account for 20 successful exports"' "$before_exported" "$after_exported"
    echo "PASS: all 20 logs reached Loki (service_name=$service)"
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then echo "Expected 20 logs; found $count" >&2; exit 1; fi
  sleep 1
done
