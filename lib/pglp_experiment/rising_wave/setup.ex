defmodule PglpExperiment.RisingWave.Setup do
  @moduledoc """
  Idempotently ensures the RisingWave-side CDC source, mirrored table,
  and subscription used by `PglpExperiment.RisingWave.Consumer` exist,
  using a short-lived `PglpExperiment.RisingWave.Client` connection.

  Unlike `PglpExperiment.Replication.Setup` (which must catch a
  `duplicate_object` Postgrex error, since Postgres's `CREATE
  PUBLICATION` has no `IF NOT EXISTS`), RisingWave's `CREATE SOURCE`,
  `CREATE TABLE`, and `CREATE SUBSCRIPTION` all genuinely support `IF
  NOT EXISTS` — confirmed live: re-running any of them just returns a
  `NoticeResponse` ("already exists, skipping"), not an error. So there
  is no exception-catching idempotency dance needed here; the happy
  path is simply the statements as written.

  This automates, for repeated/scripted use, the same steps the
  README's "RisingWave (optional...)" section walks through manually
  via `psql` (`CREATE SOURCE ... connector = 'postgres-cdc'`, `CREATE
  TABLE ... FROM pg_source TABLE '...'`, `CREATE SUBSCRIPTION ...`). Use
  the manual walkthrough to poke around by hand; use this (via `mix
  pglp.risingwave`) once you just want the consumer running.
  """

  require Logger
  alias PglpExperiment.RisingWave.Client

  @doc """
  Creates the CDC source, mirrored table, and subscription, if they
  don't already exist.

  `opts` keys:

    * `:connection_opts` — RisingWave connection opts (`:hostname`,
      `:port`, `:database`, `:username`, `:password`).
    * `:source_name` — name of the `postgres-cdc` source to create.
    * `:pg_hostname`, `:pg_port`, `:pg_username`, `:pg_password`,
      `:pg_database` — how *RisingWave* reaches Postgres (this is a
      different network hop than `:connection_opts`, which is how
      *this Elixir app* reaches RisingWave — see `config/runtime.exs`).
    * `:pg_table` — schema-qualified source-side table, e.g.
      `"public.items"`.
    * `:table_name` — name of the RisingWave-side mirrored table.
    * `:columns` — the RisingWave-side column DDL fragment (must match
      `:pg_table`'s actual columns), e.g. `"id int primary key, name
      varchar, updated_at timestamptz"`.
    * `:subscription_name` — name of the subscription to create.
    * `:retention` — retention window, e.g. `"1D"` (default `"1D"`).
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
      ensure_source!(socket, opts)
      ensure_table!(socket, opts)
      ensure_subscription!(socket, opts)
    after
      Client.close(socket)
    end

    :ok
  end

  defp ensure_source!(socket, opts) do
    source_name = Keyword.fetch!(opts, :source_name)

    sql = """
    CREATE SOURCE IF NOT EXISTS #{source_name} WITH (
      connector = 'postgres-cdc',
      hostname = '#{Keyword.fetch!(opts, :pg_hostname)}',
      port = '#{Keyword.fetch!(opts, :pg_port)}',
      username = '#{Keyword.fetch!(opts, :pg_username)}',
      password = '#{Keyword.fetch!(opts, :pg_password)}',
      database.name = '#{Keyword.fetch!(opts, :pg_database)}'
    )
    """

    run!(socket, sql, "source #{source_name}")
  end

  defp ensure_table!(socket, opts) do
    table_name = Keyword.fetch!(opts, :table_name)
    source_name = Keyword.fetch!(opts, :source_name)
    pg_table = Keyword.fetch!(opts, :pg_table)
    columns = Keyword.fetch!(opts, :columns)

    sql =
      "CREATE TABLE IF NOT EXISTS #{table_name} (#{columns}) " <>
        "FROM #{source_name} TABLE '#{pg_table}'"

    run!(socket, sql, "table #{table_name}")
  end

  defp ensure_subscription!(socket, opts) do
    subscription_name = Keyword.fetch!(opts, :subscription_name)
    table_name = Keyword.fetch!(opts, :table_name)
    retention = Keyword.get(opts, :retention, "1D")

    sql =
      "CREATE SUBSCRIPTION IF NOT EXISTS #{subscription_name} FROM #{table_name} " <>
        "WITH (retention = '#{retention}')"

    run!(socket, sql, "subscription #{subscription_name}")
  end

  @doc """
  Drops the subscription, table, and source, if they exist. Mirrors
  `PglpExperiment.Replication.Setup.drop!/3`. Accepts the same `opts` as
  `ensure!/1` (only the names are actually needed).
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
      drop!(
        socket,
        "DROP SUBSCRIPTION IF EXISTS #{Keyword.fetch!(opts, :subscription_name)}",
        "subscription"
      )

      drop!(socket, "DROP TABLE IF EXISTS #{Keyword.fetch!(opts, :table_name)}", "table")
      drop!(socket, "DROP SOURCE IF EXISTS #{Keyword.fetch!(opts, :source_name)}", "source")
    after
      Client.close(socket)
    end

    :ok
  end

  defp run!(socket, sql, description) do
    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Ensured #{description}")

      {:error, %{message: message}} ->
        raise "Failed to ensure #{description}: #{message}"

      {:error, reason} ->
        raise "Failed to ensure #{description}: #{inspect(reason)}"
    end
  end

  defp drop!(socket, sql, description) do
    case Client.query(socket, sql, 30_000) do
      {:ok, _result} ->
        Logger.info("Dropped #{description}")

      {:error, %{message: message}} ->
        raise "Failed to drop #{description}: #{message}"

      {:error, reason} ->
        raise "Failed to drop #{description}: #{inspect(reason)}"
    end
  end
end
