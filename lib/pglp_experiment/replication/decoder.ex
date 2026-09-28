defmodule PglpExperiment.Replication.Decoder do
  @moduledoc """
  Pure decoding of `pgoutput` logical replication protocol messages.

  See the Postgres docs for the wire format:
  https://www.postgresql.org/docs/current/protocol-logical-replication.html
  """

  @doc """
  Decodes a single pgoutput message (the payload that follows the leading
  `w` XLogData byte and its LSN/timestamp header).
  """
  def decode(<<"B", lsn::64, commit_ts::64, xid::32>>) do
    %{type: :begin, final_lsn: lsn, commit_timestamp: commit_ts, xid: xid}
  end

  def decode(<<"C", _flags::8, commit_lsn::64, end_lsn::64, commit_ts::64>>) do
    %{type: :commit, commit_lsn: commit_lsn, end_lsn: end_lsn, commit_timestamp: commit_ts}
  end

  def decode(<<"R", oid::32, rest::binary>>) do
    {namespace, rest} = decode_cstring(rest)
    {name, rest} = decode_cstring(rest)
    <<_replica_identity::8, num_columns::16, rest::binary>> = rest
    columns = decode_columns(rest, num_columns, [])

    %{
      type: :relation,
      oid: oid,
      namespace: namespace,
      name: name,
      columns: columns
    }
  end

  def decode(<<"I", oid::32, "N", tuple_data::binary>>) do
    %{type: :insert, relation_oid: oid, tuple: decode_tuple(tuple_data)}
  end

  def decode(<<"U", oid::32, "K", rest::binary>>) do
    {old_tuple, rest} = decode_tuple_with_rest(rest)
    <<"N", new_tuple_data::binary>> = rest
    %{type: :update, relation_oid: oid, old_tuple: old_tuple, tuple: decode_tuple(new_tuple_data)}
  end

  def decode(<<"U", oid::32, "O", rest::binary>>) do
    {old_tuple, rest} = decode_tuple_with_rest(rest)
    <<"N", new_tuple_data::binary>> = rest
    %{type: :update, relation_oid: oid, old_tuple: old_tuple, tuple: decode_tuple(new_tuple_data)}
  end

  def decode(<<"U", oid::32, "N", tuple_data::binary>>) do
    %{type: :update, relation_oid: oid, old_tuple: nil, tuple: decode_tuple(tuple_data)}
  end

  def decode(<<"D", oid::32, "K", tuple_data::binary>>) do
    %{type: :delete, relation_oid: oid, old_tuple: decode_tuple(tuple_data)}
  end

  def decode(<<"D", oid::32, "O", tuple_data::binary>>) do
    %{type: :delete, relation_oid: oid, old_tuple: decode_tuple(tuple_data)}
  end

  def decode(<<"T", num_relations::32, _flags::8, oids::binary-size(num_relations * 4)>>) do
    relation_oids = for <<oid::32 <- oids>>, do: oid
    %{type: :truncate, relation_oids: relation_oids}
  end

  def decode(<<"O", _rest::binary>>), do: %{type: :origin}
  def decode(<<"Y", _rest::binary>>), do: %{type: :pg_type}

  def decode(other) do
    %{type: :unknown, raw: other}
  end

  defp decode_columns(_rest, 0, acc), do: Enum.reverse(acc)

  defp decode_columns(<<flags::8, rest::binary>>, remaining, acc) do
    {name, rest} = decode_cstring(rest)
    <<type_oid::32, type_modifier::32, rest::binary>> = rest

    column = %{
      name: name,
      key?: flags == 1,
      type_oid: type_oid,
      type_modifier: type_modifier
    }

    decode_columns(rest, remaining - 1, [column | acc])
  end

  defp decode_tuple(data) do
    {values, _rest} = decode_tuple_with_rest(data)
    values
  end

  defp decode_tuple_with_rest(<<num_columns::16, rest::binary>>) do
    decode_tuple_values(rest, num_columns, [])
  end

  defp decode_tuple_values(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp decode_tuple_values(<<"n", rest::binary>>, remaining, acc) do
    decode_tuple_values(rest, remaining - 1, [nil | acc])
  end

  defp decode_tuple_values(<<"u", rest::binary>>, remaining, acc) do
    decode_tuple_values(rest, remaining - 1, [:unchanged_toast | acc])
  end

  defp decode_tuple_values(
         <<"t", len::32, value::binary-size(len), rest::binary>>,
         remaining,
         acc
       ) do
    decode_tuple_values(rest, remaining - 1, [value | acc])
  end

  defp decode_tuple_values(
         <<"b", len::32, value::binary-size(len), rest::binary>>,
         remaining,
         acc
       ) do
    decode_tuple_values(rest, remaining - 1, [{:binary, value} | acc])
  end

  defp decode_cstring(binary) do
    [string, rest] = :binary.split(binary, <<0>>)
    {string, rest}
  end
end
