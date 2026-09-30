# How RisingWave's sinks work

This document explains RisingWave's `CREATE SINK ...` feature —
RisingWave pushing data **out** somewhere, as opposed to the other two
data paths already documented in this repo:

- `postgres-cdc` **source** (RisingWave ← Postgres): RisingWave manages
  its own publication/slot and reads via the actual Postgres replication
  protocol. See the README's "RisingWave (optional...)" section.
- `RisingWave.Consumer` (Elixir ← RisingWave): our hand-rolled client
  polling `FETCH NEXT` on a subscription cursor, because RisingWave
  doesn't implement the replication protocol as a *server*. See
  [`docs/risingwave-consumer.md`](risingwave-consumer.md).
- `postgres` **sink** (RisingWave → Postgres, this document): RisingWave
  acts as a normal Postgres *client*, using ordinary DML over a JDBC
  connection pool it manages internally — no replication protocol
  involved in either direction. This is the simplest of the three
  conceptually, but has real correctness sharp edges (see below) that
  the other two don't.

Nothing in this repo currently wires this up automatically — the
findings below come from manually creating sinks against the running
`docker-compose` stack and are recorded here for reference.

## Postgres sink

### Setup

```sql
CREATE SINK pg_sink FROM sink_source WITH (
  connector = 'postgres',
  type = 'upsert',           -- or 'append-only'
  host = 'postgres',
  port = '5432',
  user = 'postgres',
  password = 'postgres',
  database = 'pglp_dev',
  table = 'sink_target',
  primary_key = 'id'          -- required for type = 'upsert'
);
```

**Property names, confirmed live against a running container — several
public docs describe different names than what the server actually
accepts:**

| What you might expect | What actually works |
|---|---|
| `hostname` | `host` |
| `username` | `user` |
| `database.name` | `database` |
| `table.name` | `table` |

Each of these was discovered by iterating on the server's own
`missing field '<name>'` error message, one field at a time — that's
the authoritative source, not any doc page. `type` is also required
explicitly (`'upsert'` or `'append-only'`); omitting it fails with
`missing field 'type'`.

### Two sink types, and they behave very differently

#### `type = 'upsert'`

Requires `primary_key`. Every `INSERT`/`UPDATE`/`DELETE` on the
RisingWave side is translated into the equivalent operation on the
Postgres target, keyed by the primary key. Confirmed live — insert,
update, and delete all propagated correctly, and the target table
always reflects current state:

```sql
-- RisingWave side (sink_source)
INSERT INTO sink_source VALUES (1, 'alpha', now());
UPDATE sink_source SET name = 'alpha-updated' WHERE id = 1;
DELETE FROM sink_source WHERE id = 1;
```
propagates to `sink_target` on Postgres as an insert, then an update,
then a delete — the row is fully gone from `sink_target` after the
delete, same as it's gone from `sink_source`.

#### `type = 'append-only'`

RisingWave **refuses** to create this sink from a table that supports
updates/deletes (a "retract stream") unless you explicitly add
`force_append_only = 'true'`. The refusal error is explicit about the
consequence:

```
Invalid input syntax: The sink of retract stream cannot be
append-only. Please add "force_append_only='true'" in WITH options to
force the sink to be append-only. Notice that this will cause the sink
executor to drop DELETE messages and convert UPDATE messages to
INSERT.
```

Verified this exactly, side by side with an upsert sink from the same
source table:

```sql
INSERT INTO sink_source VALUES (2, 'beta', now());
INSERT INTO sink_source VALUES (3, 'gamma', now());
UPDATE sink_source SET name = 'gamma-updated' WHERE id = 3;
DELETE FROM sink_source WHERE id = 2;
```

| Target | Result |
|---|---|
| `sink_target` (upsert) | Only `(3, 'gamma-updated')` — id 2 correctly deleted. |
| `sink_target_ao` (append-only, `force_append_only='true'`) | Both `(2, 'beta')` **and** `(3, 'gamma')` remain — the delete of id 2 was silently dropped, and the update to id 3 shows up as an extra insert of the *old* value rather than replacing it. |

**Takeaway:** `append-only` is for genuinely insert-only streams (e.g.
event logs, append-only fact tables) — not for mirroring a mutable
table. Using it against a table with updates/deletes without
understanding `force_append_only`'s consequences will silently corrupt
the target (stale rows that should have been deleted or replaced stick
around forever).

### Operational notes

- `SHOW SINKS` / `SELECT * FROM rw_catalog.rw_sinks` shows each sink's
  `sink_type` (`SINK_TYPE_UPSERT` / append-only), its full `CREATE SINK`
  definition, and its `connector_props` (handy for confirming exactly
  what config it's running with, since — as above — the property names
  you write aren't always what gets echoed back verbatim elsewhere).
- On the Postgres side, the sink holds a small pool of persistent
  connections, visible in `pg_stat_activity` as idle `client backend`
  sessions with no `application_name` set — consistent with a JDBC
  driver default (RisingWave's Postgres sink is JDBC-based under the
  hood, not a custom wire-protocol client like our
  `PglpExperiment.RisingWave.Client`).
- `DROP SINK <name>` cleans it up; the target table on Postgres is
  untouched by the drop (it's a normal table, not something RisingWave
  manages the lifecycle of).

## The HTTP sink — could it replace long-polling from Elixir?

RisingWave also has a generic `connector = 'http'` sink that POSTs
each row to an arbitrary URL — a genuine push mechanism, unlike
everything else in this document. Since `RisingWave.Consumer` is a
poll loop (`FETCH NEXT ... WITH (timeout = ...)` in a cycle), it's
worth asking whether pointing an HTTP sink at a small Elixir HTTP
endpoint instead would be a better design. Tested this directly
against the running stack; the answer for this app is no, for
reasons that are verifiable, not just theoretical.

### Setup

```sql
CREATE TABLE http_src (payload jsonb);   -- HTTP sink requires a `payload` (varchar/jsonb) column
CREATE SINK http_sink FROM http_src WITH (
  connector = 'http',
  type = 'append-only',
  force_append_only = 'true',            -- same tradeoff as above
  url = 'http://<receiver-host>:<port>/webhook'
);
```

A row's `payload` value is POSTed as the request body to `url`,
one request per row.

### What was verified

- **Real push, with retry.** Stopping the receiver mid-stream and
  reinserting a row: RisingWave logged the delivery failure and
  retried with growing backoff (479ms → 1.9s → 2.2s → 2.8s → 15s...,
  confirmed in its own logs), and **did** eventually redeliver once
  the receiver came back — so it's not naive fire-and-forget.
- **No ordering guarantee.** Inserting three rows in a single
  statement (`VALUES (1), (2), (3)`) produced three POSTs that arrived
  as **1, 3, 2** — confirmed directly against a real HTTP listener.
  Rows are evidently delivered concurrently, not serialized in commit
  order.
- **Retry state lives only in RisingWave's memory**, not in a durable,
  inspectable log the way a replication slot or subscription's
  `retention` window is. There's no equivalent of `rw_timestamp` to
  tell a receiver "you're behind, and by exactly this much" — a
  request either arrives or it doesn't; the receiver has no way to
  ask what it might have missed.

### Why this doesn't fit `RisingWave.Consumer`'s design

`Consumer` explicitly relies on strict ordering: an `UPDATE` arrives as
a `UpdateDelete`/`UpdateInsert` pair sharing one `rw_timestamp`, and
the moduledoc is deliberate about *never* processing that pair out of
order or interleaved with unrelated rows (see "Contrast with the
Postgres consumer's resume model" in the `Consumer` moduledoc). An
HTTP sink's confirmed reordering would break that invariant directly —
a receiver could observe `UpdateInsert` before `UpdateDelete` for the
same row, or two different rows' changes arriving in a different order
than they committed.

| | HTTP sink → Elixir receiver | `RisingWave.Consumer` (current) |
|---|---|---|
| Elixir-side complexity | Need an HTTP server, plus handling for reordering/dedup | Already built — no server needed |
| Ordering | **Not guaranteed** (verified) | Guaranteed — `FETCH NEXT` returns commit order |
| Resume after an outage | Backoff retry from RisingWave's memory; untested how it behaves across a RisingWave restart mid-backlog | Solved: on-disk `Checkpoint` + `SINCE <ts>`, bounded by the subscription's `retention` window |
| Visibility into lag | None — a request either arrives or doesn't | `rw_timestamp` on every row |
| Latency | Lower (push as soon as committed) | Bounded by `fetch_timeout_seconds` (default 5s) |
| New failure surface | A whole HTTP server to run/secure/monitor | None — reuses the already-validated TCP client |

**Verdict for this app:** keep polling. The HTTP sink is a reasonable
choice if a consumer can tolerate reordering and dedupe independently,
and lower latency matters more than strict ordering — but that's a
different set of guarantees than what `RisingWave.Consumer` was built
to preserve.

### Sink decoupling: what happens if the receiver is down or slow

RisingWave inserts an internal buffer — the **sink log store** — between
its streaming engine and every sink connector (HTTP included),
controlled by the `sink_decouple` session variable (confirmed live:
`SET sink_decouple = true|false` is accepted; `SHOW ALL` reports it as
`"Enable decoupling sink and internal streaming graph or not"`,
defaulting to a tri-state `default` — RisingWave decides per-connector
unless forced). Its purpose: isolate the streaming engine from a slow
or down sink, so backpressure from the sink doesn't stall RisingWave's
own internal checkpointing.

**How big can that buffer get?** Effectively unbounded, up to your
storage backend's capacity — not a fixed row/byte cap. The log store is
backed by **Hummock**, RisingWave's own LSM-tree storage engine (the
same compute/storage-separated layer everything else in RisingWave
uses). There's a small, genuinely bounded staging buffer in front of it
in memory, but once flushed, data lives in Hummock, which is backed by
your configured object storage (S3/GCS/MinIO, or the local filesystem
in this repo's single-node `docker-compose` setup) — no hard size limit
at that layer. **A stalled sink does not cause data loss; it causes
unbounded storage growth and a growing backlog until the receiver comes
back** (or the storage backend itself runs out of space).

**Two findings worth flagging, verified directly rather than assumed:**

- **Decoupling is not what causes the HTTP sink's reordering.** Retested
  the earlier out-of-order result (3 rows in one statement arriving as
  1, 3, 2) with `sink_decouple` explicitly set to `false` — rows still
  arrived out of order. The reordering is a property of how the HTTP
  sink executor dispatches requests (evidently concurrently), not of
  the decoupling buffer sitting in front of it.
- **The documented 10–60s decoupled-commit latency did not reproduce
  on this local single-node setup** — a row inserted with
  `sink_decouple = true` showed up at the HTTP receiver in well under a
  second. This is very likely because a single-node dev container runs
  a much shorter internal checkpoint interval than whatever
  multi-node/production configuration that figure assumes; not
  something to rely on as a number in this environment specifically.

**Why this matters for an HTTP-sink-as-Elixir-receiver design:** if the
receiver goes down, RisingWave will keep the backlog in Hummock and
retry once it's back (matches the backoff/redelivery behavior verified
earlier) — but there is **no signal to the receiver that a backlog is
building**, unlike this repo's polling design, where `rw_timestamp` on
every row makes lag directly observable, and the subscription's
`retention` window is a known, hard ceiling on how far behind a
consumer can fall before losing the ability to resume cleanly (see
`PglpExperiment.RisingWave.Consumer`'s moduledoc). An HTTP-sink receiver
outage becomes a silent, growing liability on RisingWave's storage that
you'd only discover by separately monitoring RisingWave's own
Hummock/log-store metrics — not something visible from the Elixir side
at all.

### Throughput: which one actually scales better?

Measured directly, draining a 20,000-row backlog through each path
against the same running stack:

| Approach | Throughput |
|---|---|
| HTTP sink (one POST per row, no batching option exists) | ~1,250–1,365 rows/sec |
| Polling, `FETCH NEXT` / `FETCH 1` | ~1,129 rows/sec |
| Polling, `FETCH 10` | ~9,690 rows/sec |
| Polling, `FETCH 100` | ~52,493 rows/sec |
| Polling, `FETCH 1000` | ~89,285 rows/sec |

At the same (unbatched) request granularity, the two approaches are
roughly tied — both cost one network round-trip per row. But **only
polling has a scaling lever**: `FETCH <N> FROM cursor WITH (timeout =
...)` returns up to N rows in a single round-trip, confirmed to still
return as soon as *any* rows are available rather than waiting to fill
a full batch (so it costs nothing in latency under normal load — see
`PglpExperiment.RisingWave.Consumer`'s `:batch_size` option, which
implements exactly this). Batching to 1000 measured **~79x** higher
throughput than one-row-at-a-time. The HTTP sink has no equivalent —
confirmed against RisingWave's own connector docs: no batching,
concurrency, or connection-pool-size option exists for `connector =
'http'`. It is architecturally capped at one row per HTTP round-trip,
permanently.

Combined with the reordering issue above, **polling scales better on
every axis that matters here**: it has a real throughput lever the
push mechanism lacks, and it preserves ordering, which the HTTP sink
does not.

## The Kafka sink — the best push-based option, if push is required

RisingWave supports 22 sink connectors in total (Kafka, Pulsar, NATS,
MQTT, Elasticsearch, Redis, Iceberg, Snowflake, ClickHouse, and more —
see [RisingWave's data delivery overview](https://docs.risingwave.com/delivery/overview)
for the full list). Of all of them, `connector = 'kafka'` is the one
that would actually solve the HTTP sink's problems if a push-based
design were required instead of `RisingWave.Consumer`'s poll loop —
evaluated here on the same three axes as the rest of this document
(performance, ease of implementation, Elixir library support), without
having stood one up against the running stack the way the Postgres and
HTTP sinks were.

### Why it solves the HTTP sink's core problem: ordering

Kafka guarantees **strict ordering within a partition** — a
fundamental property of the log, not something a client library adds.
RisingWave's Kafka sink uses the sink's `primary_key` as the Kafka
message key when set (required for `UPSERT`/`DEBEZIUM` format, optional
for `PLAIN`); standard Kafka producer behavior hash-partitions by
message key, so all changes for a given row land in the same partition,
in commit order. This is strongly implied by RisingWave's own docs (the
`key_encode` property constrains `primary_key`'s type, which only makes
sense if `primary_key` *is* the message key) and matches how every
other Kafka-based CDC connector (e.g. Debezium's) behaves — but wasn't
independently re-verified against a running Kafka broker the way the
HTTP sink's reordering was directly observed. If this becomes a real
design, verify it the same way before relying on it.

Either way, this directly addresses the HTTP sink's confirmed failure
mode: no client-side reordering to guard against, because the ordering
guarantee is structural (per-partition), not best-effort.

### Performance

Kafka is a real distributed log, not one HTTP request per row. RisingWave's
producer batches writes into Kafka's own client-level buffering, and
Kafka consumers pull in large batches natively — the same
"amortize the round-trip cost across many rows" advantage this repo
measured for `FETCH <N>` (see "Throughput" above) applies at the
transport layer here for free, without needing an equivalent to
`:batch_size` at all.

### Ease of implementation / Elixir library support

This is where Kafka wins most clearly. [`broadway_kafka`](https://hex.pm/packages/broadway_kafka)
(built on [`:brod`](https://github.com/kafka4beam/brod), a long-established
Erlang Kafka client) plus [`Broadway`](https://hex.pm/packages/broadway)
is a mature, idiomatic Elixir stack for exactly this job — declarative
pipeline configuration, built-in backpressure, and automatic offset
commit-after-ack (`:offset_commit_on_ack`, defaults to `true`) so a
crash before processing completes results in reprocessing, not silent
loss — the same at-least-once shape `RisingWave.Consumer` already
provides, but without hand-rolling a wire-protocol client the way
`PglpExperiment.RisingWave.Client` had to be for the subscription-cursor
path (there was no Elixir Kafka gap to fill the way there was no way to
talk to RisingWave's subscription cursor with Postgrex — Kafka already
has one). This is meaningfully less code to write and maintain than the
polling consumer's `Protocol`/`Client`/`Setup`/`Consumer`/`Checkpoint`
stack.

### Delivery semantics and tradeoffs

- **At-least-once, not exactly-once** — RisingWave's Kafka sink writes
  non-transactionally, per RisingWave's own docs. Same duplicate-on-retry
  contract as everything else in this repo; `UPSERT` format dedups
  automatically downstream by key, `PLAIN`/append-only format needs the
  consumer to dedup itself (same pattern already documented for
  `RisingWave.Consumer`).
- **Still subject to sink decoupling** — a Kafka sink goes through the
  same `kv_log_store` buffer as the HTTP sink (see "Sink decoupling"
  above): up to ~10s added latency by default, and the same
  "backlog only visible via RisingWave's own metrics, not from the
  consumer side" blind spot. Not a regression versus the HTTP sink, but
  not eliminated either.
- **One more moving part** — a Kafka (or Kafka-compatible, e.g. Redpanda)
  broker must exist. Not a new requirement for a RisingWave deployment
  generally (RisingWave's own distributed `docker-compose` setup already
  runs Redpanda internally), but this repo's current single-node setup
  doesn't have one, so adopting this path means adding a broker service.

### Verdict

Of the 22 supported connectors, Kafka is the one that combines real
throughput headroom, a mature Elixir client stack, and — critically —
an actual ordering guarantee that the HTTP sink concretely failed to
provide (see "What was verified" above). If a push-based design is ever
adopted instead of `RisingWave.Consumer`'s poll loop, this is the
connector to reach for — but it would need its own hands-on
verification pass (ordering, throughput, decoupling latency) against a
real broker before treating any of the above as confirmed the way the
rest of this document's findings are.

## Related documentation

- [`docs/risingwave-consumer.md`](risingwave-consumer.md) — the
  opposite direction (Elixir consuming from RisingWave).
- [README.md](../README.md) — the `postgres-cdc` source walkthrough
  (RisingWave consuming from Postgres).
