#!/usr/bin/env bash
# Stops PostgreSQL and removes its container, network, and data volume
# (i.e. a full teardown — all database data is permanently deleted).
#
# Usage:
#   ./scripts/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "Stopping and removing containers, network, and data volume..."
docker compose down -v

echo "Done. Postgres and its data volume have been removed."
