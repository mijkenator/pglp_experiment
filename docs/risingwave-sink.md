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

A fourth path, MQTT, is also covered further below — unlike the
Postgres/HTTP/Kafka sections, it's wired up by `mix pglp.mqtt` (see
`PglpExperiment.Mqtt.Broker`/`PglpExperiment.Mqtt.SinkSetup`); the
Postgres and HTTP sinks below were set up manually against the running
`docker-compose` stack and are recorded here for reference, same as
Kafka's desk evaluation (never stood up against a real broker).

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
| Infrastructure required | **None beyond RisingWave + Elixir** — RisingWave POSTs straight to a Plug/Phoenix endpoint | None beyond RisingWave + Elixir |
| Elixir-side complexity | Need an HTTP server, plus handling for reordering/dedup | Already built — no server needed |
| Ordering | **Not guaranteed** (verified) | Guaranteed — `FETCH NEXT` returns commit order |
| Resume after an outage | Backoff retry from RisingWave's memory; untested how it behaves across a RisingWave restart mid-backlog | Solved: on-disk `Checkpoint` + `SINCE <ts>`, bounded by the subscription's `retention` window |
| Visibility into lag | None — a request either arrives or doesn't | `rw_timestamp` on every row |
| Latency | Lower (push as soon as committed) | Bounded by `fetch_timeout_seconds` (default 5s) |
| New failure surface | A whole HTTP server to run/secure/monitor | None — reuses the already-validated TCP client |

Note this table is HTTP sink vs. this repo's *own* polling consumer — both
require zero extra infrastructure beyond RisingWave and Elixir. The
"Kafka sink" section below adds a third option that trades that
simplicity for guaranteed ordering, at the cost of a whole new broker
service — see that section for the fuller three-way tradeoff.

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

## The Kafka sink — trades infrastructure for guaranteed ordering

RisingWave supports 22 sink connectors in total (Kafka, Pulsar, NATS,
MQTT, Elasticsearch, Redis, Iceberg, Snowflake, ClickHouse, and more —
see [RisingWave's data delivery overview](https://docs.risingwave.com/delivery/overview)
for the full list). Of all of them, `connector = 'kafka'` is the one
that would solve the HTTP sink's ordering problem — but **this is a
real tradeoff, not a strict upgrade**: the HTTP sink's biggest advantage
is that RisingWave delivers straight to an Elixir endpoint with *zero*
extra infrastructure (see the table above), and Kafka gives that up.
Evaluated here on the same three axes as the rest of this document
(performance, ease of implementation, Elixir library support), plus the
infrastructure cost that makes this a genuine either/or decision, not
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

- **A whole new service, not just a new dependency.** This is the
  headline cost, not a footnote: a Kafka (or Kafka-compatible, e.g.
  Redpanda) *broker* must exist and be run, monitored, secured, and
  upgraded — not just a library added to the Elixir app. The HTTP sink
  needs none of that; RisingWave talks straight to a Plug/Phoenix
  endpoint. For a deployment that doesn't already run Kafka for other
  reasons, this is a genuinely new piece of infrastructure to operate
  indefinitely, purely to relay data between two systems that could
  otherwise talk directly. This repo's current single-node
  `docker-compose` setup has no broker today (RisingWave's own
  distributed setup runs Redpanda internally, but that's a different,
  much heavier deployment than what this repo uses).
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

### Verdict: a real tradeoff, not a strict upgrade

**Choose based on what the receiver can tolerate, not on ordering
alone:**

- If the Elixir side can dedupe by `{primary_key, op}` and doesn't need
  strict commit-order delivery (true for a surprising number of use
  cases — e.g. anything that just needs "eventually consistent, latest
  wins"), the **HTTP sink is the better choice**: zero new
  infrastructure, RisingWave delivers directly, and `rw_timestamp` still
  arrives on every payload if the receiver wants to buffer-and-resort
  itself rather than trust arrival order.
- If strict per-row ordering is a hard requirement and the extra
  operational surface of running a broker is acceptable (e.g. a broker
  already exists for other reasons, or the deployment is large enough
  that one is justified anyway), **Kafka is the right connector** among
  the 22 supported — it has a real throughput lever the HTTP sink
  lacks, and a mature Elixir client stack (`broadway_kafka`/`:brod`)
  ready to use.

Either way, this needs its own hands-on verification pass (ordering,
throughput, decoupling latency against a real broker) before treating
the Kafka-specific claims above as confirmed the way the rest of this
document's findings — which *were* verified against the running
stack — are.

## The MQTT sink — tested as a "best of both" candidate; it isn't

The Kafka section above is a desk evaluation (never stood up against a
real broker). This section is the opposite: a real experiment, fully
verified against the running `docker-compose` stack, testing whether
`connector = 'mqtt'` could get Kafka's ordering guarantee *and* the HTTP
sink's "zero extra infrastructure" property at once — by embedding the
MQTT broker directly inside this Elixir app (via the
[`mqttx`](https://hex.pm/packages/mqttx) library) rather than running a
separate broker service. Elixir is the broker; RisingWave's sink is the
client connecting *into* it — the same "no new service to operate"
shape as the HTTP sink, but with a real pub/sub protocol and QoS
semantics instead of one POST per row.

See `PglpExperiment.Mqtt.Broker` and `PglpExperiment.Mqtt.SinkSetup`
(new `lib/pglp_experiment/mqtt/` subsystem — not under `rising_wave/`,
since the connection direction is inverted here: every `rising_wave/*`
module has Elixir dialing *out* to RisingWave, whereas here RisingWave
dials *into* Elixir) and `mix pglp.mqtt`.

### Setup

```sql
CREATE SINK pglp_mqtt_sink FROM items WITH (
  connector = 'mqtt',
  url = 'tcp://host.docker.internal:1883',
  topic = 'pglp/items',
  qos = 'at_least_once',
  type = 'append-only'
)
FORMAT PLAIN ENCODE JSON (force_append_only='true');
```

Unlike every other RisingWave connector tested in this repo's history
(Postgres sink, HTTP sink, the `postgres-cdc` source), **every property
name here worked on the first attempt** — `url`, `topic`, `qos`,
`type` all matched the public docs exactly, no `missing field`
iteration needed. The MQTT sink requires a `payload` concept the same
way the HTTP sink does — plain `FORMAT PLAIN ENCODE JSON` with
`force_append_only='true'` publishes each row as a JSON object to the
configured topic.

`host.docker.internal` also resolved cleanly from inside the RisingWave
container on this (Docker Desktop / macOS) setup — no repeat of the
HTTP sink investigation's container-to-host DNS failure. This isn't
guaranteed on every Docker setup (that earlier failure was
Linux-container-specific), so verify it on yours before assuming it
"just works."

### What was verified

- **Full publish flow works correctly.** Five sequential rows generated
  via `scripts/generate_events.sh 5 1` arrived at
  `PglpExperiment.Mqtt.Broker.handle_publish/4` in order, with correct
  JSON content, topic (`pglp/items`), and QoS (`1`).
- **Ordering: MQTT does NOT solve the problem either.** Twenty rows
  inserted in a single fast batch arrived completely out of order
  (`12, 9, 7, 15, 6, 8, 13, 14, 23, 20, 17, 24, 11, 19, 10, 18, 21, 16,
  22, 25` for ids `6..25`) — the same failure mode already confirmed
  for the HTTP sink. The MQTT broker logged **~17 concurrent client
  connections** from RisingWave (`risingwave_<n>_<generation>` client
  IDs) during the test: RisingWave dispatches from multiple parallel
  compute actors, each independently publishing whatever rows land in
  its shard, with no ordering coordination between them — regardless of
  which push protocol sits underneath. This is a property of
  RisingWave's sink execution model, not of HTTP or MQTT specifically,
  and nothing about MQTT's own ordering-within-a-topic semantics
  changes it, because RisingWave itself is the one publishing
  out of order.
- **Outage/restart behavior: a full history replay, not just the
  missed window.** Stopping `mix pglp.mqtt`, inserting 5 more rows
  while it was down, then restarting it, redelivered **all 30 rows**
  (the original 25 plus the 5 generated during the outage) — zero
  gaps, but the entire history, not just what was missed. This is a
  direct consequence of this repo's `DROP SINK` + `CREATE SINK`
  idempotency pattern (`SinkSetup.ensure!/1`): since `CREATE SINK` has
  no `IF NOT EXISTS`, every restart drops and recreates the sink from
  scratch, and RisingWave treats a freshly created sink as having
  nothing yet delivered — so it replays its entire buffered backlog
  (see "Sink decoupling" above for why that backlog can hold
  everything, unbounded by anything but storage). This is specific to
  *this repo's* setup pattern, not an inherent MQTT sink property — a
  sink that was never dropped/recreated would not have this behavior,
  but RisingWave's own replay-on-fresh-sink semantics are real and
  worth knowing either way.
- **Throughput: in the same range as the HTTP sink, not the polling
  consumer's batched throughput.** Measured ~1,388 messages/sec
  draining a 5,000-row backlog — close to the HTTP sink's measured
  ~1,250–1,365 rows/sec, nowhere near the polling consumer's `FETCH
  1000` throughput (~89,285 rows/sec). Confirmed against RisingWave's
  own MQTT connector docs: the full property list (`url`, `qos`,
  `username`, `password`, `client_prefix`, `clean_start`,
  `inflight_messages`, `tls.client_cert`, `tls.client_key`, `topic`,
  `topic.field`, `retain`, `type`) has no batching, concurrency, or
  connection-pool-size option — `inflight_messages` (default `100`)
  caps concurrent *unacknowledged* QoS 1/2 messages, it doesn't batch
  multiple rows into one publish. Same structural ceiling as the HTTP
  sink: one row per network operation, permanently.

### Infrastructure required: the one place MQTT delivers on its premise

This is the actual hypothesis under test, and it did pan out: the
embedded broker needed **zero separate services** — no broker process
to run, monitor, or secure beyond the Elixir app itself, matching the
HTTP sink's "RisingWave talks directly to our app" property and
avoiding Kafka's "stand up and operate a whole broker" cost entirely.
`{:mqttx, "~> 0.11"}` + `{:thousand_island, "~> 1.0"}` (the transport —
not declared as a hard dependency by `mqttx` itself; compilation fails
with `module ThousandIsland.Handler is not loaded` without adding it
explicitly) were the only new pieces, both pure-Elixir libraries, no
external process.

So MQTT genuinely delivers on half the "best of both" hypothesis — no
infrastructure cost beyond the HTTP sink's — but the other half
(ordering) didn't pan out, because the ordering problem was never in
the transport to begin with. It's in how RisingWave's sink executors
dispatch rows across parallel actors, and that's unrelated to whether
the wire protocol underneath is HTTP, MQTT, or anything else without
its own partition/key-based ordering guarantee the way Kafka has.

### Verdict

**MQTT is not a "best of both" — it inherits the HTTP sink's ordering
problem while adding genuine protocol complexity (QoS, topics, a new
dependency) for no corresponding benefit in this evaluation.** If the
goal is "push with zero extra infrastructure," the HTTP sink is simpler
to reason about and already documented in detail above — MQTT doesn't
improve on it here. If the goal is "guaranteed ordering," only Kafka's
per-partition guarantee (unverified in this repo, but structurally
real — see above) actually addresses it; embedding a broker instead of
running one doesn't change *why* RisingWave's own dispatch reorders
rows.

The one scenario where this experiment's MQTT setup might still be
worth it: a receiver that already needs MQTT's semantics for other
reasons (retained messages, existing MQTT-based tooling, QoS-aware
clients elsewhere in the system) and can tolerate reordering the same
way an HTTP-sink receiver would have to. Absent that, prefer the HTTP
sink for simplicity or Kafka for ordering — this experiment didn't find
a reason to reach for MQTT over either.

## Related documentation

- [`docs/risingwave-consumer.md`](risingwave-consumer.md) — the
  opposite direction (Elixir consuming from RisingWave).
- [README.md](../README.md) — the `postgres-cdc` source walkthrough
  (RisingWave consuming from Postgres).
