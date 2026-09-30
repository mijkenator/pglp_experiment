defmodule Mix.Tasks.Pglp.Mqtt do
  @shortdoc "Runs an embedded MQTT broker that RisingWave's mqtt sink pushes into"

  @moduledoc """
  Starts `PglpExperiment.Mqtt.Broker` (an embedded `MqttX.Server`) as a
  standalone process, then creates a RisingWave `CREATE SINK ...
  connector='mqtt'` pointed at it (see `PglpExperiment.Mqtt.SinkSetup`).

  This is the experiment described in `docs/risingwave-sink.md`'s MQTT
  section: RisingWave *pushes* rows to this app over MQTT, instead of
  `mix pglp.risingwave` *polling* RisingWave's subscription cursor, or
  the HTTP-sink push mechanism (confirmed unordered, see the same doc).
  Elixir is the MQTT broker here — no separate broker service to run,
  unlike the Kafka sink alternative also evaluated in that doc.

  ## Usage

      mix pglp.mqtt

  ## Prerequisites

    * `docker compose up -d`, with both `postgres` and `risingwave`
      healthy.
    * The RisingWave-side table being sunk `FROM` (`MQTT_SOURCE_TABLE`,
      default `items`) must already exist on RisingWave — run `mix
      pglp.risingwave` once (it mirrors `items` from Postgres via
      `postgres-cdc`), or follow the README's manual `CREATE
      SOURCE`/`CREATE TABLE` walkthrough, before running this task.

  ## Docker networking

  RisingWave (in `docker-compose`) needs to reach this task's embedded
  broker, which runs on the *host* (same as `mix pglp.risingwave`
  running on the host to reach RisingWave, just the reverse direction).
  `MQTT_SINK_URL` defaults to `tcp://host.docker.internal:1883`; see
  `docs/risingwave-sink.md`'s MQTT section for whether that resolved
  cleanly in practice, or what fallback was needed (this repo hit a
  similar container-to-host reachability problem testing the HTTP sink
  and had to run the test listener as a container on
  `pglp_experiment_default`'s network instead — the same fallback may
  apply here).

  ## Why not wired into `mix run`?

  Kept as a standalone task, same pattern as `mix pglp.risingwave` and
  `mix pglp.perf`: the main app's boot (`PglpExperiment.Application`)
  must never depend on RisingWave or this experiment being up. See
  `config/runtime.exs` for the `MQTT_*` env vars this reads (`:mqtt`
  namespace under `:pglp_experiment`).
  """

  use Mix.Task

  alias PglpExperiment.Mqtt.{Broker, SinkSetup}

  @impl Mix.Task
  def run(_args) do
    # Deliberately `app.config` (loads config, compiles, does NOT start
    # the supervision tree) rather than `app.start` — mirrors
    # `mix pglp.risingwave`'s reasoning: we don't want the main app's
    # own Postgres Consumer starting here, just the deps we need.
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:mqttx)
    {:ok, _} = Application.ensure_all_started(:telemetry)

    mqtt_config = Application.fetch_env!(:pglp_experiment, :mqtt)

    broker_spec = %{
      id: MqttX.Server,
      start:
        {MqttX.Server, :start_link,
         [
           Broker,
           [],
           [transport: MqttX.Transport.ThousandIsland, port: mqtt_config[:broker_port]]
         ]}
    }

    # A small local supervisor is the last line of defense against a
    # crash of the embedded broker -- not the primary reconnect
    # mechanism (unlike the polling Consumer, there's no reconnect logic
    # to speak of here: RisingWave's sink drives its own retry/backoff
    # against us, we just need to still be listening when it tries).
    {:ok, _sup} =
      Supervisor.start_link([broker_spec],
        strategy: :one_for_one,
        max_restarts: 5,
        max_seconds: 10
      )

    Mix.shell().info("Embedded MQTT broker listening on port #{mqtt_config[:broker_port]}...")

    setup_opts = [
      connection_opts: [
        hostname: mqtt_config[:rw_hostname],
        port: mqtt_config[:rw_port],
        database: mqtt_config[:rw_database],
        username: mqtt_config[:rw_username],
        password: mqtt_config[:rw_password]
      ],
      sink_name: mqtt_config[:sink_name],
      source_table: mqtt_config[:source_table],
      sink_url: mqtt_config[:sink_url],
      topic: mqtt_config[:topic],
      qos: mqtt_config[:qos]
    ]

    SinkSetup.ensure!(setup_opts)

    Mix.shell().info(
      "MQTT sink #{mqtt_config[:sink_name]} created, RisingWave should connect shortly " <>
        "(Ctrl-C to stop)..."
    )

    Process.sleep(:infinity)
  end
end
