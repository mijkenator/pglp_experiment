defmodule PglpExperiment.RisingWave.Consumer do
  @moduledoc """
  Connects to RisingWave, declares a subscription cursor, and repeatedly
  calls `FETCH NEXT FROM cursor WITH (timeout = 'Ns')` — a blocking
  poll, not a busy loop: each call ties up the connection for up to N
  seconds server-side, returning immediately once a row is available.
  Decodes each row and logs/emits telemetry, analogous in spirit to
  `PglpExperiment.Replication.Consumer` but over RisingWave's much
  simpler (non-transactional, non-binary) subscription protocol.

  ## Contrast with the Postgres consumer's resume model

  Postgres's `Replication.Consumer` resumes from a server-side durable
  position (`confirmed_flush_lsn`) that survives a full process restart,
  because Postgres tracks it against the replication *slot*, and the
  consumer explicitly acks it back. RisingWave's subscription cursor has
  no equivalent: nothing is ever acked back to the server, and the
  cursor itself is session-scoped — gone the moment the TCP connection
  drops. The only position this consumer can resume from is the
  `rw_timestamp` of the last row *it* saw:

    * A **mid-session TCP reconnect** (socket dropped, process alive)
      resumes from `state.last_seen_rw_timestamp` (in memory) — as
      tight a resume as RisingWave's model supports, gated by the
      subscription's `retention` window still covering that timestamp.
    * A **full process restart** (in-memory state lost) resumes from a
      checkpoint written to disk after every processed row (see
      `PglpExperiment.RisingWave.Checkpoint`) — this is what closes the
      gap where restarting mid-run would otherwise silently skip
      everything produced while the process was down. Only if no
      checkpoint file exists yet (first ever run) does it fall back to
      the configured `:since` starting option (default `"now()"`; use
      `"begin()"` to replay everything still in the retention window
      instead).

  If the checkpointed (or otherwise resumed-from) timestamp has aged out
  of the subscription's `retention` window, RisingWave rejects the
  `DECLARE` outright (confirmed live: `rw_timestamp is too small, need
  to be large than the current unix_millis - subscription's retention
  time`) rather than silently clamping or dropping data. This consumer
  detects that specific error and falls back to `SINCE now()` for that
  reconnect, logging a warning that a gap is possible — better to state
  the gap plainly than to loop forever retrying a doomed timestamp.

  `SINCE ts` was confirmed (by hand, against a running container) to be
  **inclusive** — it redelivers the exact row at that timestamp — so a
  reconnect can redeliver the last row already processed. This consumer
  deliberately does NOT bump the resume timestamp by `+1` to avoid that,
  because an UPDATE's `UpdateDelete`/`UpdateInsert` pair share the exact
  same `rw_timestamp`; bumping past it risks silently dropping the
  second half of a pair if a reconnect lands between them. The result
  is the same at-least-once contract `Replication.Consumer` documents:
  harmless for logging, dedupe downstream (e.g. on `{table,
  primary_key, rw_timestamp, op}`) if it matters.

  This is a deliberately coarser guarantee than the Postgres side
  provides — not a bug to fix, but a real limitation of RisingWave's
  subscription model that this design does not attempt to paper over.
  The disk checkpoint narrows the gap (a full restart now resumes
  almost as tightly as a mid-session reconnect) without pretending to
  eliminate it — a checkpoint write can itself be lost between "row
  processed" and "checkpoint fsync'd" on a hard crash, same as any
  asynchronous ack.

  ## Telemetry

  Distinct event names from `Replication.Consumer` (the metadata shapes
  differ enough that reusing the same names would be misleading — there
  is no `relation_oid`/transaction/commit concept here, and RisingWave's
  row-level ops don't map 1:1 onto insert/update/delete/truncate):

    * `[:pglp_experiment, :risingwave, :change]` — one per decoded row.
      Measurements: `%{count: 1}`. Metadata: `%{op: :insert |
      :update_delete | :update_insert | :delete, table: String.t(),
      subscription: String.t(), row: map(), rw_timestamp: integer()}`
      (`rw_timestamp` is already Unix milliseconds — no epoch
      conversion needed, unlike the Postgres consumer's
      `commit_timestamp_to_unix/1`).
    * `[:pglp_experiment, :risingwave, :fetch]` — one per `FETCH NEXT`
      round-trip, whether or not it returned a row. Measurements:
      `%{count: 0 | 1}`. Metadata: `%{}`. There is no `:ack` analog —
      RisingWave has nothing to ack.
  """

  use GenServer
  require Logger

  alias PglpExperiment.RisingWave.{Checkpoint, Client, Setup}

  # RisingWave's own error for a DECLARE whose SINCE timestamp has aged
  # out of the subscription's retention window (confirmed live: "rw_timestamp
  # is too small, need to be large than the current unix_millis -
  # subscription's retention time").
  @retention_exceeded_pattern "rw_timestamp is too small"

  @default_cursor_name "pglp_rw_cursor"
  @default_fetch_timeout_seconds 5
  @default_reconnect_backoff_ms 1_000
  # Socket recv timeout must comfortably exceed the SQL-level FETCH
  # timeout (RisingWave genuinely blocks server-side for up to that
  # long) — see Client.query/3.
  @socket_timeout_grace_seconds 5

  @doc """
  Starts the RisingWave subscription consumer.

  `opts`:

    * `:hostname`, `:port`, `:database`, `:username`, `:password` — how
      this Elixir app reaches RisingWave.
    * `:table_name`, `:subscription_name` — required; identify what to
      subscribe to.
    * `:cursor_name` — default `#{inspect(@default_cursor_name)}`.
    * `:fetch_timeout_seconds` — default `#{@default_fetch_timeout_seconds}`.
    * `:since` — starting point for a fresh cursor when there's no
      checkpoint to resume from yet (first ever run): `"now()"`
      (default), `"begin()"`, or an integer Unix-ms literal.
    * `:setup_opts` — if given, passed to `Setup.ensure!/1` before the
      first connect (omit to skip auto-setup, e.g. if it's already been
      run separately).
    * `:quiet` — default `false`; skips the per-row `Logger.info/1` call.
    * `:name` — registers the process under that name.
    * `:reconnect_backoff_ms` — default `#{@default_reconnect_backoff_ms}`.
    * `:checkpoint_dir` — directory for the on-disk resume checkpoint
      (see `PglpExperiment.RisingWave.Checkpoint`); defaults to
      `Checkpoint`'s own default. Pass `false` to disable checkpointing
      entirely (every restart then falls back to `:since`).
  """
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_server_opts = if name, do: [name: name], else: []

    subscription_name = Keyword.fetch!(opts, :subscription_name)
    checkpoint_dir = Keyword.get(opts, :checkpoint_dir)

    state = %{
      connection_opts: Keyword.take(opts, [:hostname, :port, :database, :username, :password]),
      table_name: Keyword.fetch!(opts, :table_name),
      subscription_name: subscription_name,
      cursor_name: Keyword.get(opts, :cursor_name, @default_cursor_name),
      fetch_timeout_seconds:
        Keyword.get(opts, :fetch_timeout_seconds, @default_fetch_timeout_seconds),
      since: Keyword.get(opts, :since, "now()"),
      setup_opts: Keyword.get(opts, :setup_opts),
      quiet?: Keyword.get(opts, :quiet, false),
      reconnect_backoff_ms:
        Keyword.get(opts, :reconnect_backoff_ms, @default_reconnect_backoff_ms),
      checkpoint_dir: checkpoint_dir,
      socket: nil,
      last_seen_rw_timestamp: load_checkpoint(subscription_name, checkpoint_dir)
    }

    GenServer.start_link(__MODULE__, state, gen_server_opts)
  end

  defp load_checkpoint(_subscription_name, false), do: nil

  defp load_checkpoint(subscription_name, nil), do: Checkpoint.read(subscription_name)

  defp load_checkpoint(subscription_name, dir), do: Checkpoint.read(subscription_name, dir)

  @impl true
  def init(state), do: {:ok, state, {:continue, :connect}}

  @impl true
  def handle_continue(:connect, state) do
    with :ok <- maybe_run_setup(state),
         {:ok, socket} <- Client.connect(state.connection_opts),
         {:ok, state} <- declare_cursor_with_retention_fallback(socket, state) do
      Logger.info(
        "Connected to RisingWave, streaming subscription #{inspect(state.subscription_name)} " <>
          "(#{since_description(state)})"
      )

      {:noreply, %{state | socket: socket}, {:continue, :fetch}}
    else
      {:error, reason} ->
        Logger.warning(
          "Failed to connect/declare cursor for subscription #{inspect(state.subscription_name)}: " <>
            "#{inspect(reason)}, retrying in #{state.reconnect_backoff_ms}ms"
        )

        Process.send_after(self(), :reconnect, state.reconnect_backoff_ms)
        {:noreply, %{state | socket: nil}}
    end
  end

  @impl true
  def handle_continue(:fetch, state) do
    fetch_timeout_ms = (state.fetch_timeout_seconds + @socket_timeout_grace_seconds) * 1_000

    sql =
      "FETCH NEXT FROM #{state.cursor_name} WITH (timeout = '#{state.fetch_timeout_seconds}s')"

    case Client.query(state.socket, sql, fetch_timeout_ms) do
      {:ok, %{columns: columns, rows: [values]}} ->
        :telemetry.execute([:pglp_experiment, :risingwave, :fetch], %{count: 1}, %{})
        new_state = handle_row(columns, values, state)
        {:noreply, new_state, {:continue, :fetch}}

      {:ok, %{rows: []}} ->
        # FETCH NEXT ... WITH (timeout = ...) blocked up to the timeout
        # and simply had nothing new to return -- a normal, expected
        # outcome of the polling loop, not an error.
        :telemetry.execute([:pglp_experiment, :risingwave, :fetch], %{count: 0}, %{})
        {:noreply, state, {:continue, :fetch}}

      {:error, reason} ->
        Logger.warning(
          "FETCH failed for subscription #{inspect(state.subscription_name)}: #{inspect(reason)}"
        )

        Client.close(state.socket)
        Process.send_after(self(), :reconnect, state.reconnect_backoff_ms)
        {:noreply, %{state | socket: nil}}
    end
  end

  @impl true
  def handle_info(:reconnect, state), do: {:noreply, state, {:continue, :connect}}

  @impl true
  def terminate(_reason, state) do
    if state.socket, do: Client.close(state.socket)
    :ok
  end

  # If we're resuming from a checkpointed/remembered timestamp that has
  # since aged out of the subscription's retention window, RisingWave
  # rejects the DECLARE outright (rather than silently clamping). Rather
  # than loop forever retrying a doomed timestamp, fall back to `:since`
  # once and log plainly that a gap is possible.
  defp declare_cursor_with_retention_fallback(socket, %{last_seen_rw_timestamp: ts} = state)
       when is_integer(ts) do
    case declare_cursor(socket, state) do
      {:ok, _} ->
        {:ok, state}

      {:error, %{message: message}} when is_binary(message) ->
        if String.contains?(message, @retention_exceeded_pattern) do
          Logger.warning(
            "Checkpointed rw_timestamp=#{ts} for subscription " <>
              "#{inspect(state.subscription_name)} has aged out of the retention window " <>
              "(events committed between then and now for this consumer may have been " <>
              "missed) — falling back to SINCE #{state.since}"
          )

          fallback_state = %{state | last_seen_rw_timestamp: nil}

          case declare_cursor(socket, fallback_state) do
            {:ok, _} -> {:ok, fallback_state}
            {:error, reason} -> {:error, reason}
          end
        else
          {:error, %{message: message}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp declare_cursor_with_retention_fallback(socket, state) do
    case declare_cursor(socket, state) do
      {:ok, _} -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_run_setup(%{setup_opts: nil}), do: :ok

  defp maybe_run_setup(%{setup_opts: setup_opts}) do
    Setup.ensure!(setup_opts)
    :ok
  rescue
    error -> {:error, {:setup_failed, Exception.message(error)}}
  end

  defp declare_cursor(socket, state) do
    sql =
      "DECLARE #{state.cursor_name} SUBSCRIPTION CURSOR FOR #{state.subscription_name} " <>
        since_clause(state)

    Client.query(socket, sql, 30_000)
  end

  defp since_clause(%{last_seen_rw_timestamp: ts}) when is_integer(ts), do: "SINCE #{ts}"
  defp since_clause(%{since: since}), do: "SINCE #{since}"

  defp since_description(%{last_seen_rw_timestamp: ts}) when is_integer(ts),
    do: "resuming from rw_timestamp=#{ts}"

  defp since_description(%{since: since}), do: "starting SINCE #{since}"

  defp handle_row(columns, values, state) do
    row = Enum.zip(columns, values) |> Map.new()
    {op_str, row} = Map.pop(row, "op")
    {rw_timestamp_str, row} = Map.pop(row, "rw_timestamp")
    op = decode_op(op_str)
    rw_timestamp = String.to_integer(rw_timestamp_str)

    unless state.quiet?, do: log_change(op, state.table_name, row)

    :telemetry.execute(
      [:pglp_experiment, :risingwave, :change],
      %{count: 1},
      %{
        op: op,
        table: state.table_name,
        subscription: state.subscription_name,
        row: row,
        rw_timestamp: rw_timestamp
      }
    )

    checkpoint!(state.subscription_name, rw_timestamp, state.checkpoint_dir)

    %{state | last_seen_rw_timestamp: rw_timestamp}
  end

  defp checkpoint!(_subscription_name, _rw_timestamp, false), do: :ok

  defp checkpoint!(subscription_name, rw_timestamp, nil),
    do: Checkpoint.write!(subscription_name, rw_timestamp)

  defp checkpoint!(subscription_name, rw_timestamp, dir),
    do: Checkpoint.write!(subscription_name, rw_timestamp, dir)

  defp decode_op("Insert"), do: :insert
  defp decode_op("UpdateDelete"), do: :update_delete
  defp decode_op("UpdateInsert"), do: :update_insert
  defp decode_op("Delete"), do: :delete
  defp decode_op(other), do: {:unknown, other}

  defp log_change(:insert, table, row), do: Logger.info("INSERT into #{table}: #{inspect(row)}")
  defp log_change(:delete, table, row), do: Logger.info("DELETE from #{table}: #{inspect(row)}")

  defp log_change(:update_delete, table, row),
    do: Logger.info("UPDATE (delete half) on #{table}: #{inspect(row)}")

  defp log_change(:update_insert, table, row),
    do: Logger.info("UPDATE (insert half) on #{table}: #{inspect(row)}")

  defp log_change({:unknown, op}, table, row),
    do: Logger.info("#{op} on #{table}: #{inspect(row)}")
end
