#!/usr/bin/env bash
# Generates INSERT and UPDATE events on the `items` table so you can watch
# them show up in the running app's console (logged via
# PglpExperiment.Replication.Consumer).
#
# Usage:
#   ./scripts/generate_events.sh [count] [sleep_seconds]
#
#   count          number of insert+update cycles to run (default: 10)
#   sleep_seconds  delay between cycles, in seconds (default: 1)
#
# Connects through `docker compose exec`, so the Postgres container started
# by docker-compose.yml must already be running (`docker compose up -d`).

set -euo pipefail

COUNT="${1:-10}"
SLEEP_SECONDS="${2:-1}"

PSQL=(docker compose exec -T postgres psql -U postgres -d pglp_dev -v ON_ERROR_STOP=1 -q)

"${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS items (id serial primary key, name text, updated_at timestamptz DEFAULT now());"
"${PSQL[@]}" -c "ALTER TABLE items ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();"

echo "Generating ${COUNT} insert+update cycles on \"items\" (every ${SLEEP_SECONDS}s)..."

for i in $(seq 1 "${COUNT}"); do
  "${PSQL[@]}" -c "INSERT INTO items (name) VALUES ('item-${i}');"
  "${PSQL[@]}" -c "UPDATE items SET name = 'item-${i}-updated', updated_at = now() WHERE name = 'item-${i}';"
  echo "[$i/${COUNT}] inserted + updated item-${i}"
  sleep "${SLEEP_SECONDS}"
done

echo "Done."
