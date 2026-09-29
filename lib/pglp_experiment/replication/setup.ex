defmodule PglpExperiment.Replication.Setup do
  @moduledoc """
  Idempotently ensures the publication and logical replication slot used by
  `PglpExperiment.Replication.Consumer` exist, using a short-lived plain
  Postgrex connection (i.e. not a replication connection).
  """

  require Logger

  @doc """
  Creates the publication (for all tables) and the `pgoutput` logical
  replication slot if they don't already exist.

  `connection_opts` are standard `Postgrex.start_link/1` options
  (hostname, port, database, username, password).
  """
  def ensure!(connection_opts, publication_name, slot_name) do
    {:ok, conn} = Postgrex.start_link(connection_opts)

    try do
      ensure_publication!(conn, publication_name)
      ensure_slot!(conn, slot_name)
    after
      GenServer.stop(conn)
    end

    :ok
  end

  defp ensure_publication!(conn, publication_name) do
    Postgrex.query!(conn, "CREATE PUBLICATION #{publication_name} FOR ALL TABLES", [])
    Logger.info("Created publication #{inspect(publication_name)}")
  rescue
    error in [Postgrex.Error] ->
      if error.postgres.code == :duplicate_object do
        Logger.info("Publication #{inspect(publication_name)} already exists, skipping")
      else
        reraise error, __STACKTRACE__
      end
  end

  defp ensure_slot!(conn, slot_name) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        [slot_name]
      )

    if rows == [] do
      Postgrex.query!(
        conn,
        "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')",
        [slot_name]
      )

      Logger.info("Created replication slot #{inspect(slot_name)}")
    else
      Logger.info("Replication slot #{inspect(slot_name)} already exists, skipping")
    end
  end

  @doc """
  Drops the publication and replication slot created by `ensure!/3`, if
  they exist. Used to tear down disposable publications/slots, e.g. the
  ones `mix pglp.perf` creates for a single test run.

  The replication slot cannot be dropped while a connection is still
  attached to it — callers must stop any `Consumer` using it first.
  """
  def drop!(connection_opts, publication_name, slot_name) do
    {:ok, conn} = Postgrex.start_link(connection_opts)

    try do
      Postgrex.query!(conn, "DROP PUBLICATION IF EXISTS #{publication_name}", [])
      Postgrex.query!(conn, "SELECT pg_drop_replication_slot($1)", [slot_name])
    rescue
      error in [Postgrex.Error] ->
        unless error.postgres.code == :undefined_object, do: reraise(error, __STACKTRACE__)
    after
      GenServer.stop(conn)
    end

    :ok
  end
end
