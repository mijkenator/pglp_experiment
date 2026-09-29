defmodule Mix.Tasks.Pglp.Risingwave do
  @shortdoc "Runs a standalone consumer of RisingWave subscription events"

  @moduledoc """
  Runs `PglpExperiment.RisingWave.Consumer` as a standalone process,
  outside the main app's supervision tree (see the moduledoc on
  `PglpExperiment.RisingWave.Consumer` for what it does and why it can't
  just reuse Postgrex).

  ## Usage

      mix pglp.risingwave

  This automates, for repeated use, the same `CREATE SOURCE` / `CREATE
  TABLE ... FROM ... TABLE '...'` / `CREATE SUBSCRIPTION` steps the
  README's "RisingWave (optional...)" section walks through by hand via
  `psql` — use the manual walkthrough to poke around first; use this
  once you just want the consumer running.

  ## Prerequisites

    * `docker compose up -d`, with both `postgres` and `risingwave`
      healthy.
    * The Postgres-side table being mirrored (`RW_PG_TABLE`, default
      `public.items`) must already exist — `CREATE TABLE ... FROM
      source TABLE '...'` fails otherwise. Run
      `./scripts/reset_items.sh` or `./scripts/generate_events.sh`
      first if starting fresh.

  ## Restarting mid-run

  Restarting this task while events are still being generated does
  **not** lose them: `PglpExperiment.RisingWave.Consumer` checkpoints
  its resume position to `tmp/rising_wave_checkpoints/` after every
  processed row, and reads it back on the next start. See the
  `Consumer` moduledoc for the full explanation (and its limits — a
  checkpoint that has aged out of the subscription's `retention` window
  falls back to `SINCE now()`, logging a warning that a gap is
  possible).

  ## Why not wired into `mix run`?

  Kept as a standalone task, same pattern as `mix pglp.perf`: the main
  app's boot (`PglpExperiment.Application`) must never depend on
  RisingWave being up or configured, so a RisingWave outage can never
  break the already-working Postgres consumer. See
  `config/runtime.exs` for the `RW_*` env vars this reads (`:risingwave`
  namespace under `:pglp_experiment`).
  """

  use Mix.Task

  alias PglpExperiment.RisingWave.Consumer

  @impl Mix.Task
  def run(_args) do
    # Deliberately `app.config` (loads config, compiles, does NOT start
    # the supervision tree) rather than `app.start` — mirrors
    # `mix pglp.perf`'s reasoning: we don't want the main app's own
    # Postgres Consumer starting here, just the config/deps we need.
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:telemetry)

    rw_config = Application.fetch_env!(:pglp_experiment, :risingwave)

    connection_opts = [
      hostname: rw_config[:hostname],
      port: rw_config[:port],
      database: rw_config[:database],
      username: rw_config[:username],
      password: rw_config[:password]
    ]

    setup_opts = [
      connection_opts: connection_opts,
      source_name: rw_config[:source_name],
      pg_hostname: rw_config[:pg_hostname],
      pg_port: rw_config[:pg_port],
      pg_username: rw_config[:pg_username],
      pg_password: rw_config[:pg_password],
      pg_database: rw_config[:pg_database],
      pg_table: rw_config[:pg_table],
      table_name: rw_config[:table_name],
      columns: rw_config[:columns],
      subscription_name: rw_config[:subscription_name],
      retention: rw_config[:retention]
    ]

    consumer_opts =
      connection_opts ++
        [
          table_name: rw_config[:table_name],
          subscription_name: rw_config[:subscription_name],
          fetch_timeout_seconds: rw_config[:fetch_timeout_seconds],
          setup_opts: setup_opts
        ]

    # A small local supervisor is the last line of defense against a
    # crash slipping past the Consumer's own reconnect logic — not the
    # primary reconnect mechanism, which lives in the Consumer itself.
    {:ok, _sup} =
      Supervisor.start_link([{Consumer, consumer_opts}],
        strategy: :one_for_one,
        max_restarts: 5,
        max_seconds: 10
      )

    Mix.shell().info("RisingWave consumer running (Ctrl-C to stop)...")
    Process.sleep(:infinity)
  end
end
