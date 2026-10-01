# How getting data from RisingWave is implemented

This document explains the data path and module structure behind
`mix pglp.risingwave` — the Elixir app consuming change events *from*
RisingWave, as opposed to `PglpExperiment.Replication.Consumer`, which
reads Postgres's WAL directly.

For *why* this needed its own client and resume model instead of
reusing `Replication.Consumer`, see the "RisingWave consumer" and
"Why not just point `Replication.Consumer` at RisingWave?" sections in
the [README](../README.md), and the moduledoc on
`PglpExperiment.RisingWave.Client`.

## Architecture overview

```
Postgres (items table)
   │  postgres-cdc source (RisingWave manages its own publication/slot)
   ▼
RisingWave (mirrored table + subscription)
   │  DECLARE ... SUBSCRIPTION CURSOR + FETCH NEXT ... WITH (timeout)
   ▼
PglpExperiment.RisingWave.Consumer (Elixir GenServer)
```

RisingWave itself is the CDC reader of Postgres (via its own
`postgres-cdc` connector — separate from `Replication.Consumer`'s own
slot). The Elixir app then reads *from RisingWave*, one hop further
downstream, via RisingWave's subscription-cursor feature. Five modules
under `lib/pglp_experiment/rising_wave/` implement this, plus one Mix
task.

## 1. `Protocol` — pure wire-format encode/decode

`lib/pglp_experiment/rising_wave/protocol.ex`

Pure encode/decode functions for the subset of the Postgres wire
protocol v3 needed to drive RisingWave's simple query protocol: the
startup handshake, plain `Query` messages, and their text-format
results.

Encodes `StartupMessage`/`PasswordMessage`/`Query`/`Terminate` and
decodes the server's replies (`AuthenticationOk`, `RowDescription`,
`DataRow`, `CommandComplete`, `ErrorResponse`, etc.) into plain Elixir
tagged tuples. No I/O — just byte-level translation, mirroring the
"pure, testable" style of `PglpExperiment.Replication.Decoder` (which
does the same for the `pgoutput` binary protocol).

This exists because Postgrex can't be used at all: its mandatory
`pg_type` bootstrap query fails against RisingWave's catalog, and even
a client that skipped it would still hit RisingWave's lack of the
logical replication protocol — both documented in the `Client`
moduledoc and the README.

## 2. `Client` — the connection layer

`lib/pglp_experiment/rising_wave/client.ex`

```elixir
Client.connect(opts)              # -> {:ok, socket} | {:error, reason}
Client.query(socket, sql, timeout_ms \\ 30_000)
                                   # -> {:ok, %{columns: [...], rows: [...]}} | {:error, reason}
Client.close(socket)
```

`connect/1` opens a raw `:gen_tcp` socket and drives the startup
handshake by hand (send `StartupMessage`, loop reading messages until
`ReadyForQuery`, answering a cleartext-password challenge if RisingWave
asks for one — it doesn't in the local dev setup, which uses trust
auth). `query/3` sends a simple `Query` message and accumulates the
reply into `%{columns: [...], rows: [...]}}`, always draining through
to `ReadyForQuery` even after an error, so the connection stays
reusable for the next query.

This is the layer that replaces what Postgrex normally provides for
every other Postgres-wire-protocol connection in this repo.

## 3. `Setup` — idempotent provisioning

`lib/pglp_experiment/rising_wave/setup.ex`

On startup, `Setup.ensure!/1` runs three `IF NOT EXISTS` statements over
one `Client` connection:

1. `CREATE SOURCE IF NOT EXISTS <source> WITH (connector = 'postgres-cdc', ...)`
   — RisingWave itself connects to Postgres here, managing its own
   publication/slot.
2. `CREATE TABLE IF NOT EXISTS <table> (...) FROM <source> TABLE '<pg_table>'`
   — mirrors the upstream table into RisingWave.
3. `CREATE SUBSCRIPTION IF NOT EXISTS <subscription> FROM <table> WITH (retention = ...)`.

All three are genuinely idempotent on RisingWave — reruns just emit a
notice, not an error — so unlike the Postgres-side
`PglpExperiment.Replication.Setup`, no exception-catching is needed.

## 4. `Consumer` — the polling loop

`lib/pglp_experiment/rising_wave/consumer.ex`

A `GenServer` with two `handle_continue` phases:

- **`:connect`** — runs `Setup.ensure!/1`, opens a `Client` connection,
  and `DECLARE <cursor> SUBSCRIPTION CURSOR FOR <subscription> SINCE
  <ts>`. The `SINCE` value comes from, in priority order: the
  checkpointed timestamp on disk, the in-memory
  `last_seen_rw_timestamp`, or the configured `:since` default
  (`"now()"`). If the timestamp has aged out of the subscription's
  retention window, RisingWave rejects the `DECLARE` outright — the
  consumer catches that specific error and falls back to `SINCE now()`
  with a logged warning instead of looping forever.
- **`:fetch`** — the actual polling loop: `FETCH NEXT FROM cursor WITH
  (timeout = 'Ns')`, which blocks server-side up to N seconds and
  returns as soon as a row is available (or empty after the timeout).
  Each returned row is `<table columns...>, op, rw_timestamp` — `op` is
  `Insert`/`UpdateDelete`/`UpdateInsert`/`Delete` (an `UPDATE` emits two
  rows sharing one `rw_timestamp`). The row is decoded, logged (unless
  `:quiet`), emitted as `[:pglp_experiment, :risingwave, :change]`
  telemetry, checkpointed to disk, then it loops back into `:fetch` —
  this is a blocking poll, not a busy loop or `Process.sleep`.

On a connect/fetch failure it logs a warning, closes the socket, and
schedules a `:reconnect` message on a backoff — it never crashes on a
transient failure.

### Telemetry

- `[:pglp_experiment, :risingwave, :change]` — one per decoded row.
  Measurements: `%{count: 1}`. Metadata: `%{op: :insert | :update_delete
  | :update_insert | :delete, table: String.t(), subscription:
  String.t(), row: map(), rw_timestamp: integer()}`.
- `[:pglp_experiment, :risingwave, :fetch]` — one per `FETCH NEXT`
  round-trip, whether or not it returned a row. Measurements: `%{count:
  0 | 1}`. There is no `:ack` analog — RisingWave has nothing to ack.

## 5. `Checkpoint` — durable resume position

`lib/pglp_experiment/rising_wave/checkpoint.ex`

RisingWave's subscription protocol has no ack mechanism, and the
subscription cursor is session-scoped — gone the instant the TCP
connection drops. So `Consumer` writes the last-seen `rw_timestamp` to
`tmp/rising_wave_checkpoints/<subscription>.checkpoint` (atomic
temp-file + rename) after every processed row, and reads it back on
startup.

This is what makes restarting `mix pglp.risingwave` mid-run safe: a
full process restart resumes from that checkpoint instead of falling
back to `SINCE now()` (which would silently skip everything generated
during the downtime).

## 6. `mix pglp.risingwave` — the entry point

`lib/mix/tasks/pglp.risingwave.ex`

A standalone Mix task, mirroring `mix pglp.perf`'s pattern — it is
deliberately **not** wired into `PglpExperiment.Application`, so
RisingWave being down or misconfigured can never break the main app's
boot (which starts the working Postgres `Replication.Consumer`).

It reads config from `config/runtime.exs`'s nested `:risingwave`
namespace (`RW_*` env vars, split into "how this app reaches
RisingWave" vs. "how RisingWave reaches Postgres" — genuinely different
network hops), starts `Consumer` under a small local `Supervisor`
(last-resort restart safety net, not the primary reconnect mechanism —
that lives inside `Consumer` itself), and blocks with
`Process.sleep(:infinity)` until killed.

## Running it

```bash
docker compose up -d
./scripts/reset_items.sh      # ensure the mirrored Postgres table exists
mix pglp.risingwave           # sets up source/table/subscription, then streams
```

In another shell:

```bash
./scripts/generate_events.sh 5 1
```

Inserts/updates show up as log lines within a couple of seconds —
subject to RisingWave's own epoch-batching (see the README), which can
coalesce a fast insert+update into a single `UpdateInsert` line rather
than two separate events.

## Related documentation

- [README.md](../README.md) — setup instructions, the manual
  `CREATE SOURCE`/`CREATE TABLE`/`CREATE SUBSCRIPTION` walkthrough via
  `psql`, and the "why not Postgrex / why not the replication protocol"
  explanations.
- `PglpExperiment.RisingWave.Client` moduledoc — the Postgrex and
  replication-protocol incompatibilities, in full.
- `PglpExperiment.RisingWave.Consumer` moduledoc — the resume-model
  contrast with `Replication.Consumer`, in full.
