# How RisingWave's Postgres sink works

This document explains RisingWave's `CREATE SINK ... connector='postgres'`
— RisingWave pushing data **out to** Postgres — as a third data path
alongside the two already documented in this repo:

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
findings below come from manually creating a sink against the running
`docker-compose` stack and are recorded here for reference.

## Setup

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

## Two sink types, and they behave very differently

### `type = 'upsert'`

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

### `type = 'append-only'`

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

## Operational notes

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

## Related documentation

- [`docs/risingwave-consumer.md`](risingwave-consumer.md) — the
  opposite direction (Elixir consuming from RisingWave).
- [README.md](../README.md) — the `postgres-cdc` source walkthrough
  (RisingWave consuming from Postgres).
