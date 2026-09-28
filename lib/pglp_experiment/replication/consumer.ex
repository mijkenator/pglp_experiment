defmodule PglpExperiment.Replication.Consumer do
  @moduledoc """
  Connects to Postgres in replication mode, starts streaming from a
  `pgoutput` logical replication slot, decodes each message, and logs
  every insert/update/delete/truncate to the console.
  """

  use Postgrex.ReplicationConnection
  require Logger

  alias PglpExperiment.Replication.{Decoder, Setup}

  @doc """
  Starts the replication connection.

  `opts` must contain the standard Postgrex connection options
  (`:hostname`, `:port`, `:database`, `:username`, `:password`) plus
  `:publication_name` and `:slot_name`.
  """
  def start_link(opts) do
    {publication_name, opts} = Keyword.pop!(opts, :publication_name)
    {slot_name, opts} = Keyword.pop!(opts, :slot_name)

    connection_opts = opts

    Setup.ensure!(connection_opts, publication_name, slot_name)

    state = %{
      publication_name: publication_name,
      slot_name: slot_name,
      relations: %{}
    }

    Postgrex.ReplicationConnection.start_link(
      __MODULE__,
      state,
      connection_opts ++ [auto_reconnect: true]
    )
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_connect(state) do
    Logger.info("Connected, starting replication from slot #{inspect(state.slot_name)}")

    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 " <>
        "(proto_version '1', publication_names '#{state.publication_name}')"

    {:stream, query, [], state}
  end

  @impl true
  def handle_disconnect(state) do
    Logger.warning("Replication connection disconnected, will attempt to reconnect")
    {:noreply, state}
  end

  # Primary keepalive message from the server. If the server requests a
  # reply we send one back so it knows we're alive and doesn't time us out.
  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64, reply::8>>, state) do
    messages =
      case reply do
        1 -> [standby_status_update(wal_end)]
        _ -> []
      end

    {:noreply, messages, state}
  end

  # XLogData message: `w` <starting LSN::64> <ending LSN::64> <clock::64> <payload>
  def handle_data(<<?w, _start_lsn::64, _end_lsn::64, _clock::64, payload::binary>>, state) do
    decoded = Decoder.decode(payload)
    {:noreply, [], handle_message(decoded, state)}
  end

  def handle_data(data, state) do
    Logger.debug("Unhandled replication message: #{inspect(data)}")
    {:noreply, [], state}
  end

  defp handle_message(%{type: :relation} = relation, state) do
    Logger.debug("Cached relation #{relation.namespace}.#{relation.name} (oid=#{relation.oid})")
    put_in(state.relations[relation.oid], relation)
  end

  defp handle_message(%{type: :insert} = msg, state) do
    log_change("INSERT", msg.relation_oid, nil, msg.tuple, state)
    state
  end

  defp handle_message(%{type: :update} = msg, state) do
    log_change("UPDATE", msg.relation_oid, msg.old_tuple, msg.tuple, state)
    state
  end

  defp handle_message(%{type: :delete} = msg, state) do
    log_change("DELETE", msg.relation_oid, msg.old_tuple, nil, state)
    state
  end

  defp handle_message(%{type: :truncate} = msg, state) do
    tables =
      msg.relation_oids
      |> Enum.map(&relation_label(state, &1))
      |> Enum.join(", ")

    Logger.info("TRUNCATE #{tables}")
    state
  end

  defp handle_message(%{type: :begin}, state), do: state
  defp handle_message(%{type: :commit}, state), do: state

  defp handle_message(%{type: type}, state) when type in [:origin, :pg_type, :unknown] do
    state
  end

  defp log_change(action, relation_oid, old_tuple, new_tuple, state) do
    table = relation_label(state, relation_oid)
    old_row = row_map(state, relation_oid, old_tuple)
    new_row = row_map(state, relation_oid, new_tuple)

    message =
      case {old_row, new_row} do
        {nil, row} -> "#{action} into #{table}: #{inspect(row)}"
        {row, nil} -> "#{action} from #{table}: #{inspect(row)}"
        {old, new} -> "#{action} on #{table}: #{inspect(old)} -> #{inspect(new)}"
      end

    Logger.info(message)
  end

  defp relation_label(state, relation_oid) do
    case Map.fetch(state.relations, relation_oid) do
      {:ok, %{namespace: namespace, name: name}} -> "#{namespace}.#{name}"
      :error -> "oid:#{relation_oid}"
    end
  end

  defp row_map(_state, _relation_oid, nil), do: nil

  defp row_map(state, relation_oid, values) do
    case Map.fetch(state.relations, relation_oid) do
      {:ok, %{columns: columns}} ->
        columns
        |> Enum.map(& &1.name)
        |> Enum.zip(values)
        |> Map.new()

      :error ->
        values
    end
  end

  defp standby_status_update(wal_end) do
    next = wal_end + 1
    <<?r, next::64, next::64, next::64, current_time()::64, 0>>
  end

  # Microseconds since 2000-01-01, per the replication protocol's timestamp
  # epoch. `System.os_time/1` avoids the `Date`/`DateTime` "no wall clock at
  # compile time" restriction some environments apply.
  defp current_time do
    System.os_time(:microsecond) - 946_684_800_000_000
  end
end
