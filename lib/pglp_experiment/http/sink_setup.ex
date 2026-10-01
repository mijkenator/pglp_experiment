defmodule PglpExperiment.Http.SinkSetup do
  @moduledoc """
  Idempotently ensures the RisingWave-side `payload` view and `CREATE
  SINK ... connector='http'` used by this experiment exist, using a
  short-lived `PglpExperiment.RisingWave.Client` connection — reused
  here even though this module lives under `PglpExperiment.Http`,
  because setting up the sink is Elixir talking *SQL* to RisingWave,
  the same direction as everything under `rising_wave/`. Only
  `PglpExperiment.Http.WebhookPlug` (which RisingWave's sink connects
  *into*) is the inverted direction.

  ## The `payload` column requirement

  RisingWave's HTTP sink requires its source to have a single
  `payload` column (confirmed live) — `:source_table` (e.g. `items`,
  with columns `id`/`name`/`updated_at`) doesn't have one. So this
  module first ensures a view wrapping `:source_table` into the
  required shape:

      CREATE VIEW <view_name> AS
        SELECT jsonb_build_object('id', id, 'name', name, ...) AS payload
        FROM <source_table>

  Confirmed live that RisingWave's Postgres-style whole-row cast
  (`to_jsonb(items)`) does **not** work (`Item not found: Invalid
  column`) — `jsonb_build_object/N` with each column named explicitly
  does.

  ## Idempotency

  Neither `CREATE SINK` nor `CREATE VIEW` support `IF NOT EXISTS` on
  RisingWave. `CREATE OR REPLACE VIEW` is also confirmed **not
  implemented** (`Feature is not yet implemented: CREATE OR REPLACE
  VIEW`). So, like `PglpExperiment.Mqtt.SinkSetup`, idempotency here is
  drop-then-create throughout: `DROP SINK IF EXISTS` and `DROP VIEW IF
  EXISTS` (both of which do support `IF EXISTS`) followed by fresh
  `CREATE`s, in sink-then-view order (the sink depends on the view, so
  it must be dropped first).
  """

  require Logger
  alias PglpExperiment.RisingWave.Client

  @doc """
  Drops (if they exist) and recreates the payload view and the HTTP sink.

  `opts` keys:

    * `:connection_opts` — RisingWave connection opts (`:hostname`,
      `:port`, `:database`, `:username`, `:password`) — how *this
      Elixir app* reaches RisingWave to run the DDL (a different
      network hop than `:sink_url` below, which is how *RisingWave's
      sink* reaches back into this app's embedded HTTP server).
    * `:sink_name` — name of the sink to create.
    * `:view_name` — name of the `payload`-shaped view to create.
    * `:source_table` — RisingWave-side table to build the view `FROM`
      (e.g. the `items` table already mirrored by `mix pglp.risingwave`
      or the README's manual walkthrough).
    * `:columns` — list of column name strings on `:source_table` to
      include in the JSON payload, e.g. `["id", "name", "updated_at"]`.
    * `:sink_url` — the HTTP URL RisingWave's sink POSTs to, e.g.
      `"http://host.docker.internal:8080/webhook"`.
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
      drop_view!(socket, opts)
      create_view!(socket, opts)
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

  defp drop_view!(socket, opts) do
    view_name = Keyword.fetch!(opts, :view_name)
    sql = "DROP VIEW IF EXISTS #{view_name}"

    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Dropped view #{view_name} (if it existed)")

      {:error, %{message: message}} ->
        raise "Failed to drop view #{view_name}: #{message}"

      {:error, reason} ->
        raise "Failed to drop view #{view_name}: #{inspect(reason)}"
    end
  end

  defp create_view!(socket, opts) do
    view_name = Keyword.fetch!(opts, :view_name)
    source_table = Keyword.fetch!(opts, :source_table)
    columns = Keyword.fetch!(opts, :columns)

    jsonb_pairs =
      columns
      |> Enum.map(fn column -> "'#{column}', #{column}" end)
      |> Enum.join(", ")

    sql = """
    CREATE VIEW #{view_name} AS
      SELECT jsonb_build_object(#{jsonb_pairs}) AS payload
      FROM #{source_table}
    """

    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Created view #{view_name} (payload shape of #{source_table})")

      {:error, %{message: message}} ->
        raise "Failed to create view #{view_name}: #{message}"

      {:error, reason} ->
        raise "Failed to create view #{view_name}: #{inspect(reason)}"
    end
  end

  defp create_sink!(socket, opts) do
    sink_name = Keyword.fetch!(opts, :sink_name)
    view_name = Keyword.fetch!(opts, :view_name)
    sink_url = Keyword.fetch!(opts, :sink_url)

    sql = """
    CREATE SINK #{sink_name} FROM #{view_name} WITH (
      connector = 'http',
      type = 'append-only',
      force_append_only = 'true',
      url = '#{sink_url}'
    )
    """

    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Created sink #{sink_name} (#{view_name} -> #{sink_url})")

      {:error, %{message: message}} ->
        raise "Failed to create sink #{sink_name}: #{message}"

      {:error, reason} ->
        raise "Failed to create sink #{sink_name}: #{inspect(reason)}"
    end
  end

  @doc """
  Drops the sink and view, if they exist (in that order). Accepts the
  same `opts` as `ensure!/1`.
  """
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
      drop_view!(socket, opts)
    after
      Client.close(socket)
    end

    :ok
  end
end
