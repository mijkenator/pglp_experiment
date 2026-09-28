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
