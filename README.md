# pglp_experiment

An Elixir application that connects to PostgreSQL and subscribes to its
**logical replication** stream (using the native `pgoutput` plugin), decoding
every insert/update/delete/truncate and logging it to the console.

It uses [`Postgrex.ReplicationConnection`](https://hexdocs.pm/postgrex/Postgrex.ReplicationConnection.html)
under the hood — no external plugins (e.g. wal2json) are required, so the
stock `postgres` Docker image works as-is.

## How it works

- `PglpExperiment.Replication.Setup` — on boot, idempotently creates the
  publication (`CREATE PUBLICATION ... FOR ALL TABLES`) and the logical
  replication slot (`pg_create_logical_replication_slot/2` with `pgoutput`),
  skipping either step if they already exist.
- `PglpExperiment.Replication.Consumer` — opens a replication connection,
  runs `START_REPLICATION`, and streams binary `pgoutput` messages.
- `PglpExperiment.Replication.Decoder` — pure functions that decode the
  `pgoutput` binary protocol (Begin/Commit/Relation/Insert/Update/Delete/Truncate)
  into plain maps.

Decoded changes are logged like:

```
[info] INSERT into public.items: %{"id" => "1", "name" => "test"}
[info] UPDATE on public.items: %{"id" => "1", "name" => "test"} -> %{"id" => "1", "name" => "test2"}
[info] DELETE from public.items: %{"id" => "1", "name" => nil}
```

(Values that aren't part of the row's key/replica identity show up as `nil`
on UPDATE/DELETE unless the table's `REPLICA IDENTITY` is set to `FULL`.)

## Resuming after a crash (no missed events)

If the app goes down mid-run, restarting it does **not** lose or skip
events. Postgres logical replication slots always resume from the slot's
own `confirmed_flush_lsn`, and the consumer only advances that position
after fully processing a transaction (on each `Commit`, acked right away —
not just on the server's ~10s keepalive). On reconnect it logs
`Resuming replication from slot ... (confirmed_flush_lsn=...)`, and
Postgres redelivers everything committed after that point — nothing more,
nothing less.

This makes delivery **at-least-once**: a transaction that was fully
processed but whose ack hadn't reached Postgres yet before a crash may be
redelivered once. For console logging that's harmless; a real downstream
sink should dedupe on something like `{relation_oid, primary_key, xid}` if
that matters. See the moduledoc on `PglpExperiment.Replication.Consumer`
for the full explanation.

## Running it

1. Start PostgreSQL (configured with `wal_level=logical`):

   ```
   docker compose up -d
   ```

2. Fetch deps and run the app:

   ```
   mix deps.get
   mix run --no-halt
   ```

   (or `iex -S mix` for an interactive session)

   On boot it will log that it created (or found existing) the publication
   and replication slot, then connect and start streaming.

3. In another shell, make some changes and watch them show up in the app's
   console output:

   ```
   docker compose exec postgres psql -U postgres -d pglp_dev -c "
     CREATE TABLE items (id serial primary key, name text);
     INSERT INTO items (name) VALUES ('test');
     UPDATE items SET name = 'test2' WHERE id = 1;
     DELETE FROM items WHERE id = 1;
   "
   ```

   Or use `scripts/generate_events.sh` to continuously generate INSERT and
   UPDATE events on `items`:

   ```
   ./scripts/generate_events.sh          # 10 insert+update cycles, 1s apart
   ./scripts/generate_events.sh 50 0.2   # 50 cycles, 0.2s apart
   ```

   Use `scripts/reset_items.sh` beforehand if you want to start Postgres
   and get `items` into a clean, empty state (e.g. so ids start back at 1):

   ```
   ./scripts/reset_items.sh
   ```

4. When you're done, tear everything down. `docker compose stop` just stops
   the container (data preserved, resume with `docker compose up -d`);
   `docker compose down` also removes the container/network (data volume
   preserved). To remove **everything**, including the database data, use:

   ```
   ./scripts/teardown.sh
   ```

## RisingWave (optional, for comparing against a real CDC consumer)

`docker-compose.yml` also includes a single-node
[RisingWave](https://risingwave.com) container — a streaming database
with a built-in Postgres CDC connector — so you can compare a
production-grade logical replication consumer against this repo's
hand-rolled `Consumer`. It runs independently: RisingWave creates and
manages its own publication/slot (auto-named `rw_publication_*` /
`rw_cdc_*`), completely separate from the app's `pglp_publication` /
`pglp_slot`. Postgres allows any number of independent replication
slots on the same database, so both can read the WAL at the same time
without conflicting.

```
docker compose up -d
```

Connect to it (e.g. from inside the `postgres` container, which has
`psql`):

```
docker compose exec postgres psql -h risingwave -p 4566 -d dev -U root
```

Then set up a CDC source against `pglp_dev` and mirror a table:

```sql
CREATE SOURCE pg_source WITH (
  connector = 'postgres-cdc',
  hostname = 'postgres',
  port = '5432',
  username = 'postgres',
  password = 'postgres',
  database.name = 'pglp_dev'
);

CREATE TABLE items (
  id int PRIMARY KEY,
  name text
) FROM pg_source TABLE 'public.items';

SELECT * FROM items;
```

Inserts/updates/deletes on `items` in Postgres show up in RisingWave's
`items` within a couple of seconds. The RisingWave dashboard is at
<http://localhost:5691>.

## Performance testing

`mix pglp.perf` measures how many replication events the consumer can
handle per second. It drives its own fast, direct (no `psql` round trips)
load generator against a dedicated table/publication/slot — so it doesn't
touch your dev `items` table or the app's default slot — and reports
throughput, replication lag, and a correctness check (no missing/dropped
events):

```
docker compose up -d
mix pglp.perf                                    # 10,000 rows -> 20,000 events
mix pglp.perf --rows 100000                       # scale up
mix pglp.perf --rows 50000 --batch-size 1000
mix pglp.perf --rows 50000 --ack-every-commit 10  # ack every 10th commit instead of every one
```

Sample output:

```
== Results ==
Events received:   40000 / 40000
By type:           %{insert: 20000, update: 20000}
Consumption time:  722.6ms
Throughput:        55359.0 events/sec
Acks sent:         80 (for 80 commits)
Replication lag:   min=-6120µs mean=4.5ms p95=14.3ms max=19.2ms (n=80 commits)
Correctness:       OK, all 20000 row ids observed, no gaps
```

`--ack-every-commit N` (default `1`) controls how many commits the
consumer batches before proactively acknowledging them back to Postgres.
Raising it sends fewer acks (`Acks sent` above drops accordingly) at the
cost of a larger post-crash redelivery window — up to `N - 1` already
fully-processed commits could be replayed after a crash before the next
ack would have gone out.

See `mix help pglp.perf` for details on what's measured and how.

## Configuration

Connection and replication settings are read from environment variables
(see `config/runtime.exs`), with defaults matching `docker-compose.yml`:

| Env var            | Default              |
|---------------------|-----------------------|
| `PGHOST`            | `localhost`           |
| `PGPORT`            | `5432`                |
| `PGDATABASE`        | `pglp_dev`             |
| `PGUSER`            | `postgres`             |
| `PGPASSWORD`        | `postgres`             |
| `PUBLICATION_NAME`  | `pglp_publication`     |
| `SLOT_NAME`         | `pglp_slot`            |
