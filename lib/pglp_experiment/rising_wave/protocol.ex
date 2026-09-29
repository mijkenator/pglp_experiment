defmodule PglpExperiment.RisingWave.Protocol do
  @moduledoc """
  Pure encode/decode functions for the subset of the Postgres wire
  protocol v3 needed to drive RisingWave's simple query protocol: the
  startup handshake, plain `Query` messages, and their text-format
  results.

  No I/O lives here — framing (reading exactly one message off a socket)
  is `PglpExperiment.RisingWave.Client`'s job, since that needs the
  socket. This module just turns bytes into tagged tuples and back,
  mirroring the "pure, testable" style of
  `PglpExperiment.Replication.Decoder` (which does the same for the
  `pgoutput` binary protocol).

  See `PglpExperiment.RisingWave.Client` for why we're hand-rolling this
  instead of using Postgrex.
  """

  @protocol_version_3_0 196_608

  ## Encoding (client -> server) ##

  @doc """
  Encodes a `StartupMessage`. `params` must include `"user"` and
  `"database"`; additional key/value pairs (e.g. `"application_name"`)
  are passed through as connection parameters.
  """
  def encode_startup_message(params) do
    body =
      for {key, value} <- params, into: <<>> do
        <<key::binary, 0, value::binary, 0>>
      end

    payload = <<@protocol_version_3_0::32, body::binary, 0>>
    <<byte_size(payload) + 4::32, payload::binary>>
  end

  @doc "Encodes a `PasswordMessage` (cleartext), sent in reply to an AuthenticationCleartextPassword request."
  def encode_password_message(password) do
    frame(?p, <<password::binary, 0>>)
  end

  @doc "Encodes a simple `Query` message."
  def encode_query(sql) do
    frame(?Q, <<sql::binary, 0>>)
  end

  @doc "Encodes a `Terminate` message."
  def encode_terminate do
    frame(?X, <<>>)
  end

  defp frame(type, payload) do
    <<type::8, byte_size(payload) + 4::32, payload::binary>>
  end

  ## Decoding (server -> client) ##

  @doc """
  Decodes a single already-framed message (the type byte plus its
  payload, with the leading length already stripped off by the caller)
  into a tagged tuple.
  """
  def decode_message(?R, <<0::32>>), do: {:authentication_ok}
  def decode_message(?R, <<3::32>>), do: {:authentication_cleartext_password}

  def decode_message(?R, <<5::32, salt::binary-size(4)>>),
    do: {:authentication_md5_password, salt}

  def decode_message(?R, <<code::32, _rest::binary>>), do: {:authentication_unsupported, code}

  def decode_message(?S, payload) do
    [name, value] = split_cstrings(payload, 2)
    {:parameter_status, name, value}
  end

  def decode_message(?K, <<pid::32, secret_key::32>>), do: {:backend_key_data, pid, secret_key}

  def decode_message(?Z, <<status::8>>) do
    {:ready_for_query, ready_status(status)}
  end

  def decode_message(?T, <<count::16, rest::binary>>) do
    {:row_description, decode_field_descriptions(rest, count, [])}
  end

  def decode_message(?D, <<count::16, rest::binary>>) do
    {:data_row, decode_data_row_values(rest, count, [])}
  end

  def decode_message(?C, payload) do
    [tag] = split_cstrings(payload, 1)
    {:command_complete, tag}
  end

  def decode_message(?I, <<>>), do: {:empty_query_response}

  def decode_message(?E, payload), do: {:error_response, decode_fields(payload)}
  def decode_message(?N, payload), do: {:notice_response, decode_fields(payload)}

  def decode_message(type, payload), do: {:unknown, type, payload}

  defp ready_status(?I), do: :idle
  defp ready_status(?T), do: :in_transaction
  defp ready_status(?E), do: :failed_transaction
  defp ready_status(other), do: other

  defp decode_field_descriptions(_rest, 0, acc), do: Enum.reverse(acc)

  defp decode_field_descriptions(rest, remaining, acc) do
    [name, rest] = :binary.split(rest, <<0>>)

    <<_table_oid::32, _attnum::16, _type_oid::32, _typlen::16, _type_modifier::32,
      _format_code::16, rest::binary>> = rest

    decode_field_descriptions(rest, remaining - 1, [name | acc])
  end

  defp decode_data_row_values(_rest, 0, acc), do: Enum.reverse(acc)

  defp decode_data_row_values(<<-1::signed-32, rest::binary>>, remaining, acc) do
    decode_data_row_values(rest, remaining - 1, [nil | acc])
  end

  defp decode_data_row_values(<<len::32, value::binary-size(len), rest::binary>>, remaining, acc) do
    decode_data_row_values(rest, remaining - 1, [value | acc])
  end

  # ErrorResponse/NoticeResponse: repeated (byte field_type, CString value),
  # terminated by a lone trailing 0 byte.
  defp decode_fields(payload), do: decode_fields(payload, %{})

  defp decode_fields(<<0>>, acc), do: normalize_fields(acc)
  defp decode_fields(<<>>, acc), do: normalize_fields(acc)

  defp decode_fields(<<field_type::8, rest::binary>>, acc) do
    [value, rest] = :binary.split(rest, <<0>>)
    decode_fields(rest, Map.put(acc, field_type, value))
  end

  defp normalize_fields(acc) do
    %{
      severity: Map.get(acc, ?S),
      code: Map.get(acc, ?C),
      message: Map.get(acc, ?M)
    }
  end

  defp split_cstrings(binary, count) do
    Enum.reduce(1..count, {binary, []}, fn _, {rest, acc} ->
      [value, rest] = :binary.split(rest, <<0>>)
      {rest, [value | acc]}
    end)
    |> elem(1)
    |> Enum.reverse()
  end
end
