defmodule PglpExperiment.Perf.Generator do
  @moduledoc """
  Fast load generator used by `mix pglp.perf`.

  Unlike `scripts/generate_events.sh` (which shells out to `psql` per
  statement — fine for a manual demo, far too slow to stress-test a
  replication consumer), this drives a single `Postgrex` connection
  directly and batches many rows per `INSERT`/`UPDATE` statement, so it
  can produce WAL traffic much faster than the consumer can plausibly
  keep up with.
  """

  @doc """
  Ensures `table` exists with the schema this generator (and
  `scripts/generate_events.sh`) expects, and empties it.
  """
  def prepare_table!(conn, table) do
    Postgrex.query!(
      conn,
      "CREATE TABLE IF NOT EXISTS #{table} " <>
        "(id serial primary key, name text, updated_at timestamptz DEFAULT now())",
      []
    )

    Postgrex.query!(conn, "TRUNCATE TABLE #{table} RESTART IDENTITY", [])
    :ok
  end

  @doc """
  Inserts `count` rows into `table` in batches of `batch_size`, then
  updates every row once (also batched), each as its own statement — so
  each batch is one transaction/commit, not one commit per row.

  Returns `{inserted, updated}` counts.
  """
  def generate!(conn, table, count, batch_size \\ 500) do
    1..count
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn ids ->
      insert_batch!(conn, table, ids)
    end)

    1..count
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn ids ->
      update_batch!(conn, table, ids)
    end)

    {count, count}
  end

  defp insert_batch!(conn, table, ids) do
    {placeholders, params} =
      ids
      |> Enum.with_index(1)
      |> Enum.map_reduce([], fn {i, idx}, acc ->
        {"($#{idx})", ["item-#{i}" | acc]}
      end)

    values = Enum.join(placeholders, ", ")
    params = Enum.reverse(params)

    Postgrex.query!(conn, "INSERT INTO #{table} (name) VALUES #{values}", params)
  end

  defp update_batch!(conn, table, ids) do
    min_id = Enum.min(ids)
    max_id = Enum.max(ids)

    Postgrex.query!(
      conn,
      "UPDATE #{table} SET name = name || '-updated', updated_at = now() " <>
        "WHERE id BETWEEN $1 AND $2",
      [min_id, max_id]
    )
  end
end
