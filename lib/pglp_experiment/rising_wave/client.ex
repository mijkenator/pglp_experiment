defmodule PglpExperiment.RisingWave.Client do
  @moduledoc """
  A minimal hand-rolled Postgres wire-protocol v3 client for talking to
  RisingWave over `:gen_tcp`.

  ## Why not Postgrex?

  Every Postgrex connection path (`Postgrex.start_link/1`,
  `Postgrex.ReplicationConnection.start_link/3`,
  `Postgrex.SimpleConnection.start_link/3`) unconditionally issues a
  `pg_type` bootstrap query immediately after the startup handshake —
  before any user code runs — to discover type OIDs for binary encoding
  (see `Postgrex.Protocol.connect/1` and `Postgrex.Types.bootstrap_query/2`
  in the `postgrex` dependency). There's no supported way to skip it
  (`disable_composite_types: true` doesn't help; passing `types: nil`
  raises `KeyError`). RisingWave's `pg_type` compatibility view is
  missing the `typsend` column that query selects, so the bootstrap
  query — and therefore every Postgrex connection to RisingWave — fails
  before a single real query can run:

      ERROR XX000 (internal_error): Failed to bind expression: t.typsend
        Item not found: missing FROM-clause entry for table "t"

  This module talks the wire protocol directly and simply never sends
  that query — we control the entire handshake ourselves.

  ## Scope

  Just enough of the protocol to run `DECLARE ... SUBSCRIPTION CURSOR`
  and `FETCH NEXT ... WITH (timeout = ...)`: the startup handshake
  (trust or cleartext-password auth) and the simple query protocol
  (`Query` / `'Q'`), text-format results only. No extended query
  protocol (Parse/Bind/Execute), no binary format, no replication
  protocol — none of that is needed for RisingWave subscriptions, which
  are plain SQL over the ordinary client connection.
  """

  require Logger
  alias PglpExperiment.RisingWave.Protocol

  @connect_timeout_ms 5_000

  @doc """
  Opens a TCP connection to RisingWave and performs the startup
  handshake. `opts`: `:hostname`, `:port`, `:database`, `:username`,
  `:password` (only sent if the server requests cleartext auth).
  """
  @spec connect(keyword()) :: {:ok, :gen_tcp.socket()} | {:error, term()}
  def connect(opts) do
    hostname = Keyword.fetch!(opts, :hostname) |> to_charlist()
    port = Keyword.fetch!(opts, :port)

    with {:ok, socket} <-
           :gen_tcp.connect(
             hostname,
             port,
             [:binary, packet: :raw, active: false, nodelay: true],
             @connect_timeout_ms
           ) do
      startup(socket, opts)
    end
  end

  defp startup(socket, opts) do
    params = %{
      "user" => Keyword.fetch!(opts, :username),
      "database" => Keyword.fetch!(opts, :database)
    }

    case :gen_tcp.send(socket, Protocol.encode_startup_message(params)) do
      :ok -> await_ready(socket, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp await_ready(socket, opts) do
    case recv_message(socket, @connect_timeout_ms) do
      {:ok, {:authentication_ok}} ->
        await_ready(socket, opts)

      {:ok, {:authentication_cleartext_password}} ->
        password = Keyword.get(opts, :password, "")

        case :gen_tcp.send(socket, Protocol.encode_password_message(password)) do
          :ok -> await_ready(socket, opts)
          {:error, reason} -> {:error, reason}
        end

      {:ok, {:authentication_md5_password, _salt}} ->
        {:error, :md5_auth_not_supported}

      {:ok, {:authentication_unsupported, code}} ->
        {:error, {:unsupported_auth_method, code}}

      {:ok, {:parameter_status, _name, _value}} ->
        await_ready(socket, opts)

      {:ok, {:backend_key_data, _pid, _secret_key}} ->
        await_ready(socket, opts)

      {:ok, {:ready_for_query, _status}} ->
        {:ok, socket}

      {:ok, {:error_response, fields}} ->
        {:error, {:startup_failed, fields}}

      {:ok, other} ->
        Logger.debug("Unexpected message during startup: #{inspect(other)}")
        await_ready(socket, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Sends a simple `Query` message and collects the result.

  `timeout_ms` is the *socket* recv timeout for this call. Callers
  issuing a query like `FETCH NEXT FROM cur WITH (timeout = 'Ns')` must
  pass a `timeout_ms` comfortably larger than `N * 1000` — RisingWave
  genuinely blocks server-side for up to that long before replying, and
  the socket must not time out first.
  """
  @spec query(:gen_tcp.socket(), String.t(), pos_integer()) ::
          {:ok, %{columns: [String.t()], rows: [[binary() | nil]]}}
          | {:error, %{severity: term(), code: term(), message: term()}}
          | {:error, :closed | :timeout | term()}
  def query(socket, sql, timeout_ms \\ 30_000) do
    case :gen_tcp.send(socket, Protocol.encode_query(sql)) do
      :ok -> query_recv(socket, timeout_ms, %{columns: nil, rows: [], outcome: nil})
      {:error, reason} -> {:error, reason}
    end
  end

  defp query_recv(socket, timeout_ms, acc) do
    case recv_message(socket, timeout_ms) do
      {:ok, {:row_description, columns}} ->
        query_recv(socket, timeout_ms, %{acc | columns: columns})

      {:ok, {:data_row, values}} ->
        query_recv(socket, timeout_ms, %{acc | rows: [values | acc.rows]})

      {:ok, {:command_complete, _tag}} ->
        query_recv(socket, timeout_ms, %{acc | outcome: :ok})

      {:ok, {:empty_query_response}} ->
        query_recv(socket, timeout_ms, %{acc | outcome: :ok})

      {:ok, {:error_response, fields}} ->
        query_recv(socket, timeout_ms, %{acc | outcome: {:error, fields}})

      {:ok, {:notice_response, fields}} ->
        Logger.debug("RisingWave notice: #{inspect(fields)}")
        query_recv(socket, timeout_ms, acc)

      # The simple query protocol always ends a round-trip with
      # ReadyForQuery, even after an error — only return once we see it,
      # so the connection is guaranteed idle for the next query.
      {:ok, {:ready_for_query, _status}} ->
        case acc.outcome do
          :ok -> {:ok, %{columns: acc.columns || [], rows: Enum.reverse(acc.rows)}}
          {:error, fields} -> {:error, fields}
          nil -> {:ok, %{columns: acc.columns || [], rows: Enum.reverse(acc.rows)}}
        end

      {:ok, other} ->
        Logger.debug("Unexpected message during query: #{inspect(other)}")
        query_recv(socket, timeout_ms, acc)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Sends `Terminate` and closes the socket."
  @spec close(:gen_tcp.socket()) :: :ok
  def close(socket) do
    _ = :gen_tcp.send(socket, Protocol.encode_terminate())
    :gen_tcp.close(socket)
  end

  # Reads exactly one framed message: a 1-byte type, a 4-byte length
  # (inclusive of itself), then (length - 4) more bytes.
  defp recv_message(socket, timeout_ms) do
    with {:ok, <<type::8, length::32>>} <- :gen_tcp.recv(socket, 5, timeout_ms),
         {:ok, payload} <- recv_payload(socket, length - 4, timeout_ms) do
      {:ok, Protocol.decode_message(type, payload)}
    end
  end

  defp recv_payload(_socket, 0, _timeout_ms), do: {:ok, <<>>}

  defp recv_payload(socket, length, timeout_ms) do
    :gen_tcp.recv(socket, length, timeout_ms)
  end
end
