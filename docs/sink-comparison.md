# Comparison: HTTP Sink vs MQTT Sink vs Postgres Pulling

All findings in this document come from live testing against this
repo's running `docker-compose` stack — not from docs, not from
theoretical analysis. Each approach was implemented as a real, standing
Elixir module (not a throwaway test script) and verified end-to-end
with the same data generators (`scripts/generate_events.sh`, raw
multi-row `INSERT`s, and telemetry-counter-based throughput
measurements).

## Architecture overview

```
Postgres (source of truth: `items` table)
   │
   │  postgres-cdc source (RisingWave manages its own publication/slot)
   ▼
RisingWave (mirrored `items` table)
   │
   ├──► [HTTP sink]  ──POST──►  Elixir (Bandit + Plug)         mix pglp.http
   ├──► [MQTT sink]  ──MQTT──►  Elixir (mqttx embedded broker)  mix pglp.mqtt
   └──► [Subscription cursor] ◄──FETCH── Elixir (gen_tcp client) mix pglp.risingwave
```

All three paths read from the same RisingWave-side `items` table.
The first two are **push** (RisingWave drives delivery); the third is
**pull** (Elixir drives consumption). This difference shapes every
tradeoff below.

## Comparison table

| | HTTP Sink (`mix pglp.http`) | MQTT Sink (`mix pglp.mqtt`) | Postgres Pulling (`mix pglp.risingwave`) |
|---|---|---|---|
| **Direction** | Push (RisingWave → Elixir) | Push (RisingWave → Elixir) | Pull (Elixir → RisingWave) |
| **Transport** | HTTP POST, one row per request | MQTT PUBLISH (QoS 1), one row per message | Postgres wire protocol, `FETCH <N>` rows per round-trip |
| **Elixir library** | Bandit (Plug.Router) | mqttx (MqttX.Server) | Hand-rolled gen_tcp client (PglpExperiment.RisingWave.Client) |
| **Extra infrastructure** | None — Bandit is embedded | None — mqttx broker is embedded | None — gen_tcp client needs no server |
| **RisingWave connector** | `connector = 'http'` | `connector = 'mqtt'` | Subscription cursor (`DECLARE`/`FETCH`) — not a sink connector |

## Idempotency

How each approach handles restarts, crashes, and "exactly once" /
"at least once" / "missed events" guarantees.

### HTTP Sink

- **Delivery guarantee from RisingWave:** at-least-once with retry.
  RisingWave retries failed POSTs with exponential backoff (confirmed:
  479ms → 1.9s → 2.2s → 2.8s → 15s...), and **does** eventually
  redeliver once the receiver comes back. The retry buffer is backed
  by Hummock (RisingWave's LSM-tree storage engine) — effectively
  unbounded until your storage fills up, not a fixed row/byte cap.
- **On receiver restart:** because this repo's `SinkSetup.ensure!/1`
  does `DROP SINK IF EXISTS` + fresh `CREATE SINK` on every start
  (there is no `IF NOT EXISTS` for `CREATE SINK`), RisingWave treats
  it as a brand-new sink and **replays its entire buffered history** —
  not just what was missed during the downtime. Confirmed live: stopped
  `mix pglp.http`, inserted 5 rows while down, restarted → received
  all 30 rows (25 prior + 5 new), not just 5.
- **Deduplication:** the receiver's responsibility. RisingWave provides
  no delivery-receipt or offset mechanism for HTTP sinks. The receiver
  gets each row's JSON payload (including `id`, `name`, `updated_at`)
  but no `rw_timestamp` or commit-position equivalent — so dedup must
  be by primary key + content, not by a monotonic position token.
- **Lag visibility:** none from the Elixir side. A request either
  arrives or it doesn't; there's no way for the receiver to ask "am I
  behind, and by how much?" — that information lives only in
  RisingWave's own Hummock/log-store metrics.

### MQTT Sink

- **Delivery guarantee from RisingWave:** identical to HTTP — at-least-
  once with retry-on-failure, backed by the same Hummock buffer.
  `qos = 'at_least_once'` (QoS 1) was tested; RisingWave's MQTT
  connector sends one PUBLISH per row with the configured QoS.
- **On receiver restart:** identical behavior to HTTP — full history
  replay due to the same `DROP SINK` + `CREATE SINK` idempotency
  pattern. Confirmed live with the same 25+5=30 row scenario.
- **Deduplication:** same as HTTP — receiver's responsibility, no
  offset or position token provided by RisingWave. The JSON payload
  contains row content but no automatic `rw_timestamp`.
- **Lag visibility:** same as HTTP — none from the Elixir side.
- **QoS 1/2 doesn't help here.** MQTT's own QoS guarantees cover the
  transport between the MQTT client (RisingWave) and the MQTT broker
  (our embedded mqttx server) — they ensure a message isn't lost
  *in flight*. But they don't provide durable, resumable position
  tracking the way Postgres's replication slot or RisingWave's
  subscription `retention` window does. The broker's session state
  (including in-flight QoS 1/2 handshake state) is in-memory — it
  doesn't survive a full `mix pglp.mqtt` process restart.

### Postgres Pulling (Subscription Cursor)

- **Delivery guarantee:** at-least-once, with a real resumable position.
  `RisingWave.Consumer` tracks `last_seen_rw_timestamp` in memory
  during a session and checkpoints it to disk
  (`tmp/rising_wave_checkpoints/`) after every processed row (atomic
  temp+rename write). On restart, it reads the checkpoint and declares
  `SINCE <last_seen_rw_timestamp>` — confirmed inclusive (redelivers
  the exact row at that timestamp), which means the last row before a
  crash may be redelivered once (same at-least-once contract), but
  nothing is skipped.
- **On receiver restart:** resumes from the checkpointed timestamp, not
  from scratch. Confirmed live: stopped `mix pglp.risingwave` mid-run,
  inserted rows while down, restarted → only the missed rows (plus
  at most one duplicate of the last-processed row) were delivered, not
  the entire history. This is fundamentally different from the HTTP/MQTT
  sinks' full-replay behavior.
- **Retention-window safety net:** if the checkpointed timestamp has
  aged out of the subscription's `retention` window, RisingWave
  rejects the `DECLARE` outright (confirmed: `rw_timestamp is too
  small, need to be large than the current unix_millis -
  subscription's retention time`). The consumer detects this and falls
  back to `SINCE now()` with a logged warning — better to state the
  gap plainly than to loop forever on a doomed timestamp.
- **Deduplication:** built into the protocol. `rw_timestamp` arrives on
  every row, so the receiver always knows its exact position relative
  to the stream — dedup by `{primary_key, rw_timestamp, op}` is
  straightforward and doesn't require trusting arrival order.
- **Lag visibility:** direct. Every row carries `rw_timestamp` (Unix
  milliseconds), so `System.os_time(:millisecond) - rw_timestamp`
  gives the receiver a live, per-row lag measurement — no external
  monitoring needed.

### Idempotency summary

| | HTTP Sink | MQTT Sink | Postgres Pulling |
|---|---|---|---|
| Delivery guarantee | At-least-once (retry) | At-least-once (retry) | At-least-once (checkpoint) |
| Resume after restart | **Full history replay** (drop/create) | **Full history replay** (drop/create) | **From checkpoint** (only missed rows) |
| Position tracking | None (receiver-side only) | None (receiver-side only) | `rw_timestamp` + on-disk checkpoint |
| Dedup mechanism | By primary key + content | By primary key + content | By `{pk, rw_timestamp, op}` |
| Lag visibility | None from receiver | None from receiver | Per-row `rw_timestamp` |

## Performance

All measurements against the same running `docker-compose` stack
(single-node RisingWave v3.1.0, Postgres 16-alpine, Elixir 1.18.2 /
OTP 26), draining a pre-built backlog through each path.

### Raw throughput

| Approach | Throughput | Methodology |
|---|---|---|
| HTTP Sink (Bandit) | **~1,397 msgs/sec** | 5,000 rows, telemetry counter |
| MQTT Sink (mqttx) | **~1,388 msgs/sec** | 5,000 rows, telemetry counter |
| Polling, `FETCH 1` (default) | **~1,129 rows/sec** | 20,000 rows, drain timer |
| Polling, `FETCH 10` | **~9,690 rows/sec** | 20,000 rows, drain timer |
| Polling, `FETCH 100` | **~52,493 rows/sec** | 20,000 rows, drain timer |
| Polling, `FETCH 1000` | **~89,285 rows/sec** | 20,000 rows, drain timer |

### Why push sinks are capped at ~1,400 rows/sec

Both HTTP and MQTT sinks are architecturally limited to **one row per
network operation** — one HTTP POST per row, one MQTT PUBLISH per row.
Confirmed against RisingWave's own connector docs: neither connector
exposes a batching, concurrency, or connection-pool-size option.
`inflight_messages` (MQTT, default 100) caps concurrent *unacknowledged*
QoS 1/2 messages but doesn't batch multiple rows into one publish.

### Why pulling scales to ~89K rows/sec

`FETCH <N> FROM cursor WITH (timeout = ...)` returns up to N rows in a
single network round-trip, amortizing the fixed per-round-trip cost
across many rows. Confirmed live: `FETCH <N>` still returns as soon as
*any* rows are available (even just 1), not once N have accumulated —
so raising `batch_size` costs nothing in latency under normal load, it
only helps when there's a backlog to drain. This is the **~80x**
throughput lever that neither push sink has.

### Latency under normal (non-backlogged) load

| Approach | Expected latency |
|---|---|
| HTTP/MQTT Sink | Lower — push fires as soon as RisingWave commits (subject to the `sink_decouple` commit interval, ~1–10s depending on config) |
| Polling | Bounded by `fetch_timeout_seconds` (default 5s) — the consumer blocks waiting for rows, so new data arriving 1s into a 5s wait returns at ~1s, not 5s |

Push sinks have a latency advantage under light load (push-on-commit
vs. poll-with-timeout), but the gap narrows as load increases (both
saturate their respective round-trip budgets).

## Scalability

### Ordering under concurrent load

This is the single most important scalability finding in this repo:

| Approach | Ordering guaranteed? | Why |
|---|---|---|
| HTTP Sink | **No** | Confirmed: 20 rows in one batch arrived as 15,13,14,8,9,23... RisingWave dispatches from ~17 parallel compute actors, each independently POSTing. |
| MQTT Sink | **No** | Confirmed: same scrambling (12,9,7,15,6,8...) for the same reason — parallel dispatch, independent of transport. |
| Polling | **Yes** | `FETCH` returns rows in commit order from a single cursor — no parallelism on the consumer side. |

The reordering is a property of **RisingWave's sink execution model**
(multiple parallel compute actors dispatching independently), not of
HTTP or MQTT specifically. Confirmed by testing with `sink_decouple =
false` — rows still arrived out of order. Only the subscription cursor
path preserves ordering, because there the consumer (Elixir) drives
consumption sequentially from a single cursor, rather than RisingWave
pushing from multiple parallel actors.

### Horizontal scaling (multiple consumers)

| Approach | Can run multiple receivers? | Per-row ordering preserved? |
|---|---|---|
| HTTP Sink | No (sink has a single fixed `url`) | N/A (already unordered with one receiver) |
| MQTT Sink | Theoretically yes (multiple subscribers to a topic) but RisingWave's sink publishes, it doesn't subscribe — the broker would need to fan out, adding complexity | N/A (already unordered) |
| Polling | **No** — a subscription cursor is session-scoped, one consumer per cursor. Multiple cursors against the same subscription would each see the full stream (no partitioning). | Yes (per individual cursor) |

None of the three approaches supports native partitioned consumption
(the way Kafka consumer groups do). Scaling beyond a single consumer
would require an external partitioning layer (e.g. Kafka as
an intermediary — see `docs/risingwave-sink.md`'s Kafka section) or
application-level sharding.

### Backpressure

| Approach | What happens if the receiver can't keep up? |
|---|---|
| HTTP Sink | RisingWave retries with backoff; the `kv_log_store` (Hummock-backed, unbounded) buffers indefinitely. **No signal to the receiver** that a backlog is building — silent storage growth on RisingWave's side. |
| MQTT Sink | Same as HTTP — retry + unbounded buffer, no receiver-side visibility. |
| Polling | Natural backpressure — Elixir controls the consumption rate via `FETCH` cadence. `rw_timestamp` on every row provides live lag measurement. The subscription's `retention` window is a known, hard ceiling on how far behind you can fall. |

## Overall verdict

| Criterion | Winner | Why |
|---|---|---|
| **Idempotency / resume** | **Polling** | Real resumable checkpoint; push sinks replay everything on restart |
| **Throughput (peak)** | **Polling** | ~89K rows/sec (batched) vs ~1.4K (push sinks, no batching lever) |
| **Ordering** | **Polling** | Guaranteed; push sinks are unordered (confirmed for both HTTP and MQTT) |
| **Latency (light load)** | **Push sinks** | Push-on-commit vs poll-with-timeout; small advantage under light load |
| **Infrastructure** | **Tie** | All three need zero extra services beyond RisingWave + Elixir |
| **Lag visibility** | **Polling** | Per-row `rw_timestamp`; push sinks have no equivalent |
| **Backpressure** | **Polling** | Consumer-driven; push sinks buffer silently with no receiver visibility |
| **Implementation complexity** | **HTTP Sink** | A Plug is ~30 lines; the polling consumer is ~350 lines + a custom wire-protocol client |

**For this repo's requirements (correctness, ordering, resume-after-
crash), polling wins on every axis except latency under light load and
implementation simplicity.** The push sinks (HTTP and MQTT) are
genuinely simpler to set up and have a small latency advantage when
traffic is sparse, but they sacrifice ordering (confirmed), resumability
(full replay on restart), and throughput scalability (no batching lever)
— tradeoffs that matter more as data volume or reliability requirements
grow.

If ordering truly doesn't matter and the receiver can dedupe
independently, the **HTTP sink** is the simplest push option (fewer
moving parts than MQTT, no QoS/topic concepts to reason about, same
throughput). If ordering *does* matter, **polling is the only verified
option in this repo** — Kafka's per-partition guarantee is structurally
real but was never stood up against a real broker here (see
`docs/risingwave-sink.md`'s Kafka section).

## Related documentation

- [`docs/risingwave-sink.md`](risingwave-sink.md) — detailed per-
  connector analysis (Postgres sink, HTTP sink + Bandit subsection,
  sink decoupling/Hummock buffer, Kafka desk evaluation, MQTT
  experiment).
- [`docs/risingwave-consumer.md`](risingwave-consumer.md) — module-
  by-module architecture walkthrough of the polling consumer
  (`Protocol` → `Client` → `Setup` → `Consumer` → `Checkpoint`).
- [README.md](../README.md) — quickstart instructions for all three
  paths (`mix pglp.http`, `mix pglp.mqtt`, `mix pglp.risingwave`).
