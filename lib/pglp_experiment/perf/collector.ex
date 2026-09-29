defmodule PglpExperiment.Perf.Collector do
  @moduledoc """
  Attaches to the `PglpExperiment.Replication.Consumer` telemetry events
  and accumulates counts + lag samples for `mix pglp.perf` to report on.

  Runs as its own tiny `Agent` so telemetry handlers (which run in the
  caller's process — here, the `Consumer` process) can record data without
  blocking on anything but a message send.
  """

  use Agent

  @change_event [:pglp_experiment, :replication, :change]
  @commit_event [:pglp_experiment, :replication, :commit]
  @ack_event [:pglp_experiment, :replication, :ack]

  def start_link(_opts \\ []) do
    Agent.start_link(
      fn ->
        %{
          changes: 0,
          by_type: %{},
          ids_seen: MapSet.new(),
          lags_us: [],
          acks: 0,
          first_change_at: nil,
          last_change_at: nil
        }
      end,
      name: __MODULE__
    )
  end

  @doc "Attaches telemetry handlers. Call once after `start_link/1`."
  def attach! do
    :telemetry.attach_many(
      "pglp-perf-collector",
      [@change_event, @commit_event, @ack_event],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  def detach! do
    :telemetry.detach("pglp-perf-collector")
  end

  @doc false
  def handle_event(@change_event, %{count: count}, meta, _config) do
    now = System.monotonic_time(:microsecond)

    Agent.update(__MODULE__, fn state ->
      state
      |> Map.update!(:changes, &(&1 + count))
      |> Map.update!(:by_type, fn by_type -> Map.update(by_type, meta.type, 1, &(&1 + 1)) end)
      |> Map.update!(:first_change_at, &(&1 || now))
      |> Map.put(:last_change_at, now)
      |> record_id(meta)
    end)
  end

  def handle_event(@commit_event, %{commit_timestamp: commit_ts}, _meta, _config) do
    lag_us = System.os_time(:microsecond) - commit_timestamp_to_unix(commit_ts)

    Agent.update(__MODULE__, fn state ->
      Map.update!(state, :lags_us, &[lag_us | &1])
    end)
  end

  def handle_event(@ack_event, _measurements, _meta, _config) do
    Agent.update(__MODULE__, fn state -> Map.update!(state, :acks, &(&1 + 1)) end)
  end

  defp record_id(state, %{row: %{"id" => id}}) when is_binary(id) do
    Map.update!(state, :ids_seen, &MapSet.put(&1, String.to_integer(id)))
  end

  defp record_id(state, _meta), do: state

  defp commit_timestamp_to_unix(commit_ts) do
    PglpExperiment.Replication.Consumer.commit_timestamp_to_unix(commit_ts)
  end

  @doc "Returns the current accumulated snapshot."
  def snapshot do
    Agent.get(__MODULE__, & &1)
  end

  @doc """
  Checks that every id in `1..expected_count` was observed at least once
  (as an insert or update) — i.e. no gaps, nothing silently dropped.

  Returns `{:ok, count}` or `{:missing, [id]}`.
  """
  def check_no_gaps(expected_count) do
    %{ids_seen: ids_seen} = snapshot()
    expected = MapSet.new(1..expected_count)
    missing = MapSet.difference(expected, ids_seen)

    if MapSet.size(missing) == 0 do
      {:ok, expected_count}
    else
      {:missing, Enum.sort(MapSet.to_list(missing))}
    end
  end
end
