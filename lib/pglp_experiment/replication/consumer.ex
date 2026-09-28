defmodule PglpExperiment.Replication.Consumer do
  @moduledoc """
  Connects to Postgres in replication mode, starts streaming from a
  `pgoutput` logical replication slot, decodes each message, and logs
  every insert/update/delete/truncate to the console.

  ## Resuming after a crash / restart

  Postgres logical replication slots always resume streaming from the
  slot's own `confirmed_flush_lsn` — the `0/0` passed to
  `START_REPLICATION` below is ignored for that purpose, it's just the
  conventional placeholder. What actually determines where we resume from
  is which LSN *we* have acknowledged back to the server.

  We only acknowledge (via a standby status update, sent right after every
  commit — not just in reply to the server's keepalive pings, which default
  to a ~10s interval) the LSN of the last **fully processed transaction** —
  i.e. we update `last_committed_lsn` when we see a `Commit` message, after
  every Insert/Update/Delete in that transaction has already been handled.
  We never acknowledge a position while a transaction is still in flight.

  This gives the standard logical-replication delivery contract:

    * **Nothing is lost.** If the app crashes mid-transaction (or even
      right after fully handling one, before the next ack goes out),
      Postgres will redeliver starting from the last *acknowledged*
      commit, never skipping anything past it.
    * **Delivery is at-least-once, not exactly-once.** The transaction
      that was in flight (or the last one acknowledged just before a
      crash but not yet flushed by Postgres) may be redelivered on
      reconnect. Anything downstream of `handle_message/2` should
      tolerate seeing the same insert/update/delete more than once — for
      plain console logging that's harmless, but a real sink (a DB
      table, a queue, ...) should dedupe on something like
      `{relation_oid, primary_key, xid}`.

  On every (re)connect we log the slot's current `confirmed_flush_lsn`
  before streaming, so you can see exactly where a restart resumes from.
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
      relations: %{},
      last_committed_lsn: 0,
      step: nil
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
    query =
      "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '#{state.slot_name}'"

    {:query, query, %{state | step: :check_confirmed_lsn}}
  end

  @impl true
  def handle_result([%Postgrex.Result{rows: rows}], %{step: :check_confirmed_lsn} = state) do
    case rows do
      [[confirmed_flush_lsn]] ->
        Logger.info(
          "Resuming replication from slot #{inspect(state.slot_name)} " <>
            "(confirmed_flush_lsn=#{confirmed_flush_lsn})"
        )

      [] ->
        Logger.info(
          "Starting replication from slot #{inspect(state.slot_name)} (no prior position)"
        )
    end

    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 " <>
        "(proto_version '1', publication_names '#{state.publication_name}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl true
  def handle_disconnect(state) do
    Logger.warning("Replication connection disconnected, will attempt to reconnect")
    {:noreply, state}
  end

  # Primary keepalive message from the server. If the server requests a
  # reply we send one back, acknowledging only up to the last fully
  # processed transaction (`last_committed_lsn`) — never the server's own
  # `wal_end`, which may be ahead of what we've actually finished handling.
  @impl true
  def handle_data(<<?k, _wal_end::64, _clock::64, reply::8>>, state) do
    messages =
      case reply do
        1 -> [standby_status_update(state.last_committed_lsn)]
        _ -> []
      end

    {:noreply, messages, state}
  end

  # XLogData message: `w` <starting LSN::64> <ending LSN::64> <clock::64> <payload>
  #
  # We ack (send a standby status update) immediately after every commit,
  # rather than waiting for the server's own keepalive ping — keepalives
  # default to a ~10s interval, which would otherwise leave a long window
  # where fully-processed transactions sit unacknowledged and would be
  # needlessly redelivered after a crash in that window.
  def handle_data(<<?w, _start_lsn::64, _end_lsn::64, _clock::64, payload::binary>>, state) do
    decoded = Decoder.decode(payload)
    new_state = handle_message(decoded, state)

    messages =
      case decoded do
        %{type: :commit} -> [standby_status_update(new_state.last_committed_lsn)]
        _ -> []
      end

    {:noreply, messages, new_state}
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

  # Only here — once every change in the transaction has already been
  # handled above — do we advance the position we'll acknowledge back to
  # Postgres. This is what makes restarts resume without gaps: we never
  # tell the server we've flushed a transaction we haven't fully applied.
  defp handle_message(%{type: :commit} = msg, state) do
    Logger.debug("Committed transaction, advancing confirmed LSN to #{msg.end_lsn}")
    %{state | last_committed_lsn: msg.end_lsn}
  end

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

  defp standby_status_update(lsn) do
    next = lsn + 1
    <<?r, next::64, next::64, next::64, current_time()::64, 0>>
  end

  # Microseconds since 2000-01-01, per the replication protocol's timestamp
  # epoch. `System.os_time/1` avoids the `Date`/`DateTime` "no wall clock at
  # compile time" restriction some environments apply.
  defp current_time do
    System.os_time(:microsecond) - 946_684_800_000_000
  end
end
