defmodule Mix.Tasks.Pglp.Http do
  @shortdoc "Runs an embedded HTTP server that RisingWave's http sink pushes into"

  @moduledoc """
  Starts `PglpExperiment.Http.WebhookPlug` (an embedded Bandit HTTP
  server) as a standalone process, then creates a RisingWave `CREATE
  SINK ... connector='http'` pointed at it (see
  `PglpExperiment.Http.SinkSetup`).

  This re-verifies `docs/risingwave-sink.md`'s existing HTTP sink
  findings — which were obtained with a throwaway Python `http.server`
  listener — against a real, permanent Elixir receiver. Built on
  [Bandit](https://hex.pm/packages/bandit), chosen because it builds on
  `thousand_island`, already a dependency here from the MQTT work (see
  `mix pglp.mqtt`).

  ## Usage

      mix pglp.http

  ## Prerequisites

    * `docker compose up -d`, with both `postgres` and `risingwave`
      healthy.
    * The RisingWave-side table being sunk `FROM` (`HTTP_SINK_SOURCE_TABLE`,
      default `items`) must already exist on RisingWave — run `mix
      pglp.risingwave` once (it mirrors `items` from Postgres via
      `postgres-cdc`), or follow the README's manual `CREATE
      SOURCE`/`CREATE TABLE` walkthrough, before running this task.

  ## The `payload` view

  RisingWave's HTTP sink requires a single `payload` column on its
  source — `items` has `id`/`name`/`updated_at`, not `payload`. This
  task's setup step (`PglpExperiment.Http.SinkSetup.ensure!/1`) first
  creates a view wrapping `HTTP_SINK_SOURCE_TABLE` into that shape via
  `jsonb_build_object` (confirmed live that Postgres-style
  `to_jsonb(row)` doesn't work on RisingWave), then creates the sink
  `FROM` that view.

  ## Docker networking

  RisingWave (in `docker-compose`) needs to reach this task's embedded
  server, which runs on the *host* (same as `mix pglp.risingwave`
  running on the host to reach RisingWave, just the reverse direction).
  `HTTP_SINK_URL` defaults to `http://host.docker.internal:8080/webhook`
  — the same `host.docker.internal` network hop already confirmed
  working for `mix pglp.mqtt`.

  ## Why not wired into `mix run`?

  Kept as a standalone task, same pattern as `mix pglp.mqtt` and `mix
  pglp.risingwave`: the main app's boot (`PglpExperiment.Application`)
  must never depend on RisingWave or this experiment being up. See
  `config/runtime.exs` for the `HTTP_SINK_*` env vars this reads
  (`:http_sink` namespace under `:pglp_experiment`).
  """

  use Mix.Task

  alias PglpExperiment.Http.{SinkSetup, WebhookPlug}

  @impl Mix.Task
  def run(_args) do
    # Deliberately `app.config` (loads config, compiles, does NOT start
    # the supervision tree) rather than `app.start` — mirrors `mix
    # pglp.mqtt`'s reasoning: we don't want the main app's own Postgres
    # Consumer starting here, just the deps we need.
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:telemetry)

    http_config = Application.fetch_env!(:pglp_experiment, :http_sink)

    # Unlike MqttX.Server (which needs an explicit %{id:, start:} map
    # since its start_link/3 takes positional args), Bandit provides its
    # own child_spec/1, so {Bandit, plug: ..., port: ...} works directly
    # as a children-list entry.
    {:ok, _sup} =
      Supervisor.start_link(
        [{Bandit, plug: WebhookPlug, port: http_config[:server_port]}],
        strategy: :one_for_one,
        max_restarts: 5,
        max_seconds: 10
      )

    Mix.shell().info("Embedded HTTP server listening on port #{http_config[:server_port]}...")

    setup_opts = [
      connection_opts: [
        hostname: http_config[:rw_hostname],
        port: http_config[:rw_port],
        database: http_config[:rw_database],
        username: http_config[:rw_username],
        password: http_config[:rw_password]
      ],
      sink_name: http_config[:sink_name],
      view_name: http_config[:view_name],
      source_table: http_config[:source_table],
      columns: http_config[:columns],
      sink_url: http_config[:sink_url]
    ]

    SinkSetup.ensure!(setup_opts)

    Mix.shell().info(
      "HTTP sink #{http_config[:sink_name]} created, RisingWave should connect shortly " <>
        "(Ctrl-C to stop)..."
    )

    Process.sleep(:infinity)
  end
end
