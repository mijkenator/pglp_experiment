defmodule PglpExperiment.Mqtt.SinkSetup do
  @moduledoc """
  Idempotently ensures the RisingWave-side `CREATE SINK ...
  connector='mqtt'` used by this experiment exists, using a short-lived
  `PglpExperiment.RisingWave.Client` connection — reused here even though
  this module lives under `PglpExperiment.Mqtt`, because setting up the
  sink is Elixir talking *SQL* to RisingWave, the same direction as
  everything under `rising_wave/`. Only `PglpExperiment.Mqtt.Broker`
  (which RisingWave's sink connects *into*) is the inverted direction.

  Unlike `PglpExperiment.RisingWave.Setup` (whose `CREATE SOURCE`/`CREATE
  TABLE`/`CREATE SUBSCRIPTION` all genuinely support `IF NOT EXISTS`),
  `CREATE SINK` has **no `IF NOT EXISTS`** — confirmed against RisingWave
  directly. So idempotency here is drop-then-create instead: `DROP SINK
  IF EXISTS` (which does support `IF EXISTS`, same as everywhere else in
  this repo) followed by a fresh `CREATE SINK`.
  """

  require Logger
  alias PglpExperiment.RisingWave.Client

  @doc """
  Drops (if it exists) and recreates the MQTT sink.

  `opts` keys:

    * `:connection_opts` — RisingWave connection opts (`:hostname`,
      `:port`, `:database`, `:username`, `:password`) — how *this Elixir
      app* reaches RisingWave to run the DDL (a different network hop
      than `:sink_url` below, which is how *RisingWave's sink* reaches
      back into this app's embedded MQTT broker).
    * `:sink_name` — name of the sink to create.
    * `:source_table` — RisingWave-side table to sink `FROM` (e.g. the
      `items` table already mirrored by `mix pglp.risingwave` or the
      README's manual walkthrough).
    * `:sink_url` — the MQTT broker URL RisingWave's sink connects to,
      e.g. `"tcp://host.docker.internal:1883"`.
    * `:topic` — MQTT topic to publish to.
    * `:qos` — `"at_most_once"`, `"at_least_once"`, or `"exactly_once"`.

  Every property name here should be treated as unverified until a real
  `CREATE SINK` succeeds against a live RisingWave — this repo's
  established pattern (see `docs/risingwave-sink.md`) is that several
  connectors' actual property names differ from what public docs
  describe, discovered only by iterating on the server's own `missing
  field`/`unknown field` errors.
  """
  @spec ensure!(keyword()) :: :ok
  def ensure!(opts) do
    connection_opts = Keyword.fetch!(opts, :connection_opts)

    socket =
      case Client.connect(connection_opts) do
        {:ok, socket} -> socket
        {:error, reason} -> raise "Failed to connect to RisingWave: #{inspect(reason)}"
      end

    try do
      drop_sink!(socket, opts)
      create_sink!(socket, opts)
    after
      Client.close(socket)
    end

    :ok
  end

  defp drop_sink!(socket, opts) do
    sink_name = Keyword.fetch!(opts, :sink_name)
    sql = "DROP SINK IF EXISTS #{sink_name}"

    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Dropped sink #{sink_name} (if it existed)")

      {:error, %{message: message}} ->
        raise "Failed to drop sink #{sink_name}: #{message}"

      {:error, reason} ->
        raise "Failed to drop sink #{sink_name}: #{inspect(reason)}"
    end
  end

  defp create_sink!(socket, opts) do
    sink_name = Keyword.fetch!(opts, :sink_name)
    source_table = Keyword.fetch!(opts, :source_table)
    sink_url = Keyword.fetch!(opts, :sink_url)
    topic = Keyword.fetch!(opts, :topic)
    qos = Keyword.fetch!(opts, :qos)

    sql = """
    CREATE SINK #{sink_name} FROM #{source_table} WITH (
      connector = 'mqtt',
      url = '#{sink_url}',
      topic = '#{topic}',
      qos = '#{qos}',
      type = 'append-only'
    )
    FORMAT PLAIN ENCODE JSON (force_append_only='true')
    """

    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Created sink #{sink_name} (#{source_table} -> #{sink_url}/#{topic})")

      {:error, %{message: message}} ->
        raise "Failed to create sink #{sink_name}: #{message}"

      {:error, reason} ->
        raise "Failed to create sink #{sink_name}: #{inspect(reason)}"
    end
  end

  @doc "Drops the sink, if it exists. Accepts the same `opts` as `ensure!/1`."
  @spec drop!(keyword()) :: :ok
  def drop!(opts) do
    connection_opts = Keyword.fetch!(opts, :connection_opts)

    socket =
      case Client.connect(connection_opts) do
        {:ok, socket} -> socket
        {:error, reason} -> raise "Failed to connect to RisingWave: #{inspect(reason)}"
      end

    try do
      drop_sink!(socket, opts)
    after
      Client.close(socket)
    end

    :ok
  end
end
