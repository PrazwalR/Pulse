#!/usr/bin/env bash
# Stop the cluster. Data persists under extra/data unless you pass -v.
#   scripts/cluster_down.sh       # stop, keep data
#   scripts/cluster_down.sh -v    # stop and wipe data
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f extra/docker-compose.yml down "$@"
