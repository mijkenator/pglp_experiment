#!/usr/bin/env bash
# Starts PostgreSQL (via docker-compose), ensures the `items` table exists,
# and empties it — leaving a clean slate before running
# `scripts/generate_events.sh`.
#
# Usage:
#   ./scripts/reset_items.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "Starting Postgres..."
docker compose up -d

echo "Waiting for Postgres to become healthy..."
for _ in $(seq 1 30); do
  status="$(docker inspect --format='{{.State.Health.Status}}' pglp_postgres 2>/dev/null || true)"
  [ "${status}" = "healthy" ] && break
  sleep 1
done

if [ "${status:-}" != "healthy" ]; then
  echo "Postgres did not become healthy in time (last status: ${status:-unknown})" >&2
  exit 1
fi

PSQL=(docker compose exec -T postgres psql -U postgres -d pglp_dev -v ON_ERROR_STOP=1 -q)

echo "Ensuring \"items\" table exists..."
"${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS items (id serial primary key, name text, updated_at timestamptz DEFAULT now());"
"${PSQL[@]}" -c "ALTER TABLE items ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();"

echo "Clearing \"items\" and resetting id sequence..."
"${PSQL[@]}" -c "TRUNCATE TABLE items RESTART IDENTITY;"

echo "Done. \"items\" is empty and ready for scripts/generate_events.sh."
