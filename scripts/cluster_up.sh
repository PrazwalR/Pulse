#!/usr/bin/env bash
# Bring up the 3-node Cassandra cluster one node at a time, waiting for each to
# reach UN (Up/Normal). Racing them at boot causes join failures.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f extra/docker-compose.yml"

un() { docker exec cass1 nodetool status 2>/dev/null | grep -c '^UN' || true; }

wait_un() {
  local want=$1 tries=0
  until [ "$(un)" -ge "$want" ]; do
    tries=$((tries + 1))
    if [ "$tries" -ge 120 ]; then
      echo "timeout waiting for $want nodes UN (have $(un))" >&2
      exit 1
    fi
    sleep 5
  done
  echo "$(un)/$want nodes UN"
}

echo "starting cass1 (seed)..."; $COMPOSE up -d cass1; wait_un 1
echo "starting cass2...";        $COMPOSE up -d cass2; wait_un 2
echo "starting cass3...";        $COMPOSE up -d cass3; wait_un 3
docker exec cass1 nodetool status
