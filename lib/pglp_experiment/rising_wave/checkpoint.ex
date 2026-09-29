defmodule PglpExperiment.RisingWave.Checkpoint do
  @moduledoc """
  Persists `PglpExperiment.RisingWave.Consumer`'s `last_seen_rw_timestamp`
  to a small local file, so a **full process restart** can resume from
  roughly where it left off instead of always falling back to the
  configured `:since` default (see the Consumer moduledoc for why
  RisingWave's subscription model has nothing better than this to offer
  — there's no server-side durable position to resume from, unlike
  Postgres's `confirmed_flush_lsn`).

  This closes the gap where restarting `mix pglp.risingwave` while
  events are still being generated would silently skip everything
  produced during the downtime (the default `SINCE now()` behavior)
  from ever showing up, even though those changes were never actually
  processed.

  One file per subscription name (so multiple consumers, e.g. against
  different tables, don't clobber each other's checkpoints), written
  atomically (temp file + rename) so a crash mid-write can't corrupt it.
  """

  require Logger

  @doc """
  Returns the checkpoint file path for `subscription_name`, under
  `dir` (default `"tmp/rising_wave_checkpoints"`, relative to the
  current working directory — created if missing).
  """
  def path(subscription_name, dir \\ default_dir()) do
    File.mkdir_p!(dir)
    Path.join(dir, "#{subscription_name}.checkpoint")
  end

  @doc """
  Reads the last checkpointed `rw_timestamp` for `subscription_name`,
  if any. Returns `nil` if no checkpoint file exists yet or it can't be
  parsed (logged as a warning, not raised — a bad checkpoint should
  never prevent the consumer from starting; it just falls back to
  `:since`).
  """
  def read(subscription_name, dir \\ default_dir()) do
    path = path(subscription_name, dir)

    case File.read(path) do
      {:ok, contents} ->
        case Integer.parse(String.trim(contents)) do
          {timestamp, ""} ->
            timestamp

          _ ->
            Logger.warning("Ignoring unreadable checkpoint at #{path}: #{inspect(contents)}")
            nil
        end

      {:error, :enoent} ->
        nil

      {:error, reason} ->
        Logger.warning("Failed to read checkpoint at #{path}: #{inspect(reason)}")
        nil
    end
  end

  @doc """
  Atomically writes `rw_timestamp` as the checkpoint for
  `subscription_name` (temp file + rename, so a crash mid-write leaves
  the previous checkpoint intact rather than a truncated/corrupt one).
  """
  def write!(subscription_name, rw_timestamp, dir \\ default_dir()) do
    path = path(subscription_name, dir)
    tmp_path = path <> ".tmp"

    File.write!(tmp_path, Integer.to_string(rw_timestamp))
    File.rename!(tmp_path, path)
    :ok
  end

  defp default_dir, do: "tmp/rising_wave_checkpoints"
end
