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

## RisingWave sinks (pushing data back out)

RisingWave can also push data the other way. See
[`docs/risingwave-sink.md`](docs/risingwave-sink.md) for the full
per-connector analysis, or
[`docs/sink-comparison.md`](docs/sink-comparison.md) for a head-to-head
comparison of HTTP sink vs MQTT sink vs polling on idempotency,
performance, and scalability (all verified live, not theoretical):

- **`CREATE SINK ... connector='postgres'`** — ordinary DML over a
  JDBC connection pool RisingWave manages internally, not the
  replication protocol in either direction. Covers the correct `WITH
  (...)` property names (several public docs describe different names
  than what the server actually accepts — confirmed by iterating on
  the server's own error messages) and an important correctness
  gotcha: an `append-only` sink from a table that supports
  updates/deletes silently drops `DELETE`s and converts `UPDATE`s into
  extra `INSERT`s unless you understand what `force_append_only='true'`
  actually does. Set up manually against `docker-compose`, not wired
  into any mix task.
- **`CREATE SINK ... connector='http'`** — a genuine push mechanism
  (RisingWave POSTs each row to a URL), evaluated as a possible
  alternative to `RisingWave.Consumer`'s poll loop. Verified live
  (first with a throwaway Python listener, then re-confirmed against a
  real embedded [Bandit](https://hex.pm/packages/bandit) server) that
  it retries with backoff on delivery failure, but does **not**
  guarantee delivery order — confirmed rows arriving out of order both
  times — which conflicts with `Consumer`'s reliance on strict
  ordering for `UpdateDelete`/`UpdateInsert` pairs. Kept the poll-based
  consumer for this reason; see the doc for the full comparison. Like
  the MQTT sink below, this *is* wired into a mix task
  (`PglpExperiment.Http.WebhookPlug`/`SinkSetup`, chosen to build on
  Bandit specifically because it uses `thousand_island`, already a dep
  from the MQTT work):

  ```
  docker compose up -d
  mix pglp.risingwave &   # mirrors `items` into RisingWave first (needed as the sink's source)
  mix pglp.http           # starts the embedded Bandit server + creates the payload view + sink
  ```

  In another shell, `./scripts/generate_events.sh 5 1` — webhook POSTs
  should appear as log lines.
- **`CREATE SINK ... connector='mqtt'`** — tested as a possible "best
  of both": a real pub/sub protocol with QoS semantics, pushed into an
  MQTT broker *embedded directly in this Elixir app*
  (`PglpExperiment.Mqtt.Broker`, via the `mqttx` library) — no separate
  broker service to run, unlike Kafka. Verified live: it does **not**
  solve the ordering problem either (RisingWave dispatches from ~17
  parallel compute actors with no ordering coordination between them,
  regardless of the push protocol underneath), and throughput lands in
  the same range as the HTTP sink (no batching lever). Unlike the
  Postgres/HTTP sinks, this one *is* wired into a mix task:

  ```
  docker compose up -d
  mix pglp.risingwave &   # mirrors `items` into RisingWave first (needed as the sink's source)
  mix pglp.mqtt           # starts the embedded broker + creates the sink
  ```

  In another shell, `./scripts/generate_events.sh 5 1` — publishes
  should appear as log lines on the `pglp/items` topic.

## RisingWave consumer (experimental second CDC path)

The section above shows the Elixir app's `Consumer` reading from
Postgres, with RisingWave as a second, independent CDC reader of the
same database. This section is the *other* direction: the Elixir app
consuming *from* RisingWave, via RisingWave's own
[subscription cursor](https://docs.risingwave.com/delivery/subscription)
feature (`CREATE SUBSCRIPTION` + `DECLARE ... SUBSCRIPTION CURSOR` +
`FETCH NEXT ... WITH (timeout = ...)`).

This can't use Postgrex — every Postgrex connection path unconditionally
runs a `pg_type` bootstrap query that RisingWave's catalog can't satisfy
(missing the `typsend` column it needs), which kills the connection
before any real query runs. See the moduledoc on
`PglpExperiment.RisingWave.Client` for the full explanation. Instead,
`PglpExperiment.RisingWave.Client` is a small hand-rolled Postgres
wire-protocol v3 client (`:gen_tcp`, zero extra deps) that skips that
query entirely.

`mix pglp.risingwave` automates the same `CREATE SOURCE`/`CREATE TABLE
... FROM ...`/`CREATE SUBSCRIPTION` steps the section above walks
through by hand, then streams the result:

```
docker compose up -d
./scripts/reset_items.sh          # ensure the mirrored table exists on Postgres
mix pglp.risingwave
```

In another shell, generate some changes and watch them appear:

```
./scripts/generate_events.sh 5 1
```

Under load, raise `--batch-size` (default `1`, i.e. one row per
`FETCH` round-trip — equivalent to `FETCH NEXT`) to request more rows
per round-trip:

```
mix pglp.risingwave --batch-size 100
```

Measured directly against a running RisingWave, draining a 20,000-row
backlog: batch size 1 sustains ~1,100 rows/sec; batch size 1000
sustains ~89,000 rows/sec — about **80x** higher throughput, with no
added latency under normal (non-backlogged) load, since `FETCH <N> ...
WITH (timeout = ...)` returns as soon as any row is available rather
than waiting to fill a full batch.

Expect log lines like:

```
[info] INSERT into items: %{"id" => "1", "name" => "item-1"}
[info] UPDATE (delete half) on items: %{"id" => "1", "name" => "item-1"}
[info] UPDATE (insert half) on items: %{"id" => "1", "name" => "item-1-updated"}
```

(An `UPDATE` emits two rows sharing the same `rw_timestamp` — the old
row as `UpdateDelete`, the new row as `UpdateInsert` — rather than one
combined row, unlike the Postgres `Consumer`'s single `UPDATE` line.)

**Resume model — coarser than the Postgres consumer's, but restarting
mid-run does not lose events.** Postgres tracks a durable
`confirmed_flush_lsn` against the replication slot, so
`Replication.Consumer` can resume exactly across a full process
restart. RisingWave's subscription cursor has no server-side
equivalent — it's session-scoped and gone the moment the connection
drops — so `RisingWave.Consumer` checkpoints the last `rw_timestamp` it
processed to a local file (`tmp/rising_wave_checkpoints/`) after every
row, and reads it back on the next start. A mid-session reconnect
(e.g. the container restarting) resumes from its in-memory position; a
full process restart (e.g. restarting `mix pglp.risingwave` itself)
resumes from that checkpoint file instead — so restarting while
`scripts/generate_events.sh` is still running does **not** skip
whatever happened while it was down. Only a first-ever run (no
checkpoint file yet), or a checkpoint that's aged out of the
subscription's `retention` window (RisingWave rejects the `DECLARE`
outright in that case — logged as a warning, with a fallback to
`SINCE now()`), fall back to the configured `:since` default. See the
moduledoc on `PglpExperiment.RisingWave.Consumer` for the complete
explanation.

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

`mix pglp.risingwave` reads its own set (see `config/runtime.exs`,
nested under `:risingwave`), split into "how this app reaches
RisingWave" and "how RisingWave reaches Postgres" — genuinely different
network hops (the app runs on the host, RisingWave runs inside
docker-compose's network):

| Env var                       | Default                    |
|--------------------------------|-----------------------------|
| `RW_HOST`                     | `localhost`                 |
| `RW_PORT`                     | `4566`                      |
| `RW_DATABASE`                 | `dev`                       |
| `RW_USER`                     | `root`                      |
| `RW_PASSWORD`                 | *(empty)*                   |
| `RW_TABLE_NAME`                | `items`                     |
| `RW_SUBSCRIPTION_NAME`         | `pglp_rw_subscription`      |
| `RW_SOURCE_NAME`               | `pglp_pg_source`            |
| `RW_RETENTION`                 | `1D`                        |
| `RW_FETCH_TIMEOUT_SECONDS`     | `5`                         |
| `RW_COLUMNS`                   | `id int primary key, name varchar, updated_at timestamptz` |
| `RW_PG_HOSTNAME`               | `postgres`                  |
| `RW_PG_PORT`                   | `5432`                      |
| `RW_PG_USERNAME`               | `postgres`                  |
| `RW_PG_PASSWORD`               | `postgres`                  |
| `RW_PG_DATABASE`               | `pglp_dev`                  |
| `RW_PG_TABLE`                  | `public.items`               |

`mix pglp.mqtt` reads its own set (nested under `:mqtt`), with a third
network hop on top of the two above: how *RisingWave's sink* reaches
back into this app's embedded MQTT broker:

| Env var                | Default                              |
|-------------------------|----------------------------------------|
| `MQTT_BROKER_PORT`     | `1883`                                 |
| `MQTT_SINK_URL`        | `tcp://host.docker.internal:1883`      |
| `MQTT_SINK_NAME`       | `pglp_mqtt_sink`                       |
| `MQTT_TOPIC`           | `pglp/items`                           |
| `MQTT_QOS`             | `at_least_once`                        |
| `MQTT_SOURCE_TABLE`    | `items`                                |

`mix pglp.http` reads its own set (nested under `:http_sink`), same
shape — how *RisingWave's HTTP sink* reaches back into this app's
embedded Bandit server:

| Env var                     | Default                                    |
|------------------------------|--------------------------------------------|
| `HTTP_SINK_PORT`            | `8080`                                      |
| `HTTP_SINK_URL`             | `http://host.docker.internal:8080/webhook`  |
| `HTTP_SINK_SINK_NAME`       | `pglp_http_sink`                            |
| `HTTP_SINK_VIEW_NAME`       | `pglp_http_src`                             |
| `HTTP_SINK_SOURCE_TABLE`    | `items`                                     |
| `HTTP_SINK_COLUMNS`         | `id,name,updated_at`                        |
