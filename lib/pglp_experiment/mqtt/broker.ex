defmodule PglpExperiment.Mqtt.Broker do
  @moduledoc """
  An `MqttX.Server` handler — this module IS the MQTT broker RisingWave's
  `connector = 'mqtt'` sink publishes into.

  This is the inverse of every module under `PglpExperiment.RisingWave.*`:
  there, Elixir is the *client* dialing RisingWave (`Client.connect/1`
  opens a TCP connection to RisingWave's SQL port). Here, Elixir is the
  *server* — RisingWave's sink is the one dialing us, over MQTT instead
  of the Postgres wire protocol. That's why this lives under
  `PglpExperiment.Mqtt.*` rather than being shoehorned into
  `rising_wave/`.

  The point of this experiment: RisingWave's `http` sink was confirmed
  (see `docs/risingwave-sink.md`) to deliver rows **out of order** with
  no batching lever, while its `kafka` sink guarantees per-partition
  ordering but requires standing up a separate broker service. MQTT is
  tested here as a potential "best of both": a real pub/sub protocol
  with QoS semantics, but the broker is *embedded in this Elixir app* —
  no external service to run, same "zero extra infrastructure" property
  the HTTP sink has.

  ## No `rw_timestamp`, no checkpoint

  Unlike `PglpExperiment.RisingWave.Consumer` (which gets a `rw_timestamp`
  on every row for free, straight from the subscription protocol), an
  MQTT sink's payload only contains whatever columns the sink's source
  query selects — there is no automatic commit-time field. If ordering
  visibility matters, the sink's `FROM` query would need to explicitly
  project a timestamp column into the JSON payload. There's also nothing
  analogous to `PglpExperiment.RisingWave.Checkpoint` here: MQTT has no
  resumable cursor concept the way the subscription-cursor path does —
  once a message is delivered (or lost), it's gone; QoS 1/2 redelivery
  covers in-flight messages during a live session, not a resumable
  position across a full broker restart. See "What was verified" in
  `docs/risingwave-sink.md`'s MQTT section for what this actually meant
  in practice when tested.

  ## Telemetry

  Distinct event names from both `Replication.Consumer` and
  `RisingWave.Consumer` (each transport in this repo gets its own event
  names, since their delivery models genuinely differ):

    * `[:pglp_experiment, :mqtt, :publish]` — one per received PUBLISH.
      Measurements: `%{count: 1}`. Metadata: `%{topic: String.t(), qos:
      0..2, retain: boolean(), payload: map() | binary()}` — `payload`
      is the JSON-decoded map if the publish body parses as JSON
      (RisingWave's `ENCODE JSON` sends JSON text), otherwise the raw
      binary.
    * `[:pglp_experiment, :mqtt, :connect]` — one per accepted client
      connection. Measurements: `%{count: 1}`. Metadata: `%{client_id:
      String.t()}`.
    * `[:pglp_experiment, :mqtt, :disconnect]` — one per client
      disconnect. Measurements: `%{count: 1}`. Metadata: `%{reason:
      term()}`.
  """

  use MqttX.Server
  require Logger

  @impl true
  def init(opts) do
    %{quiet?: Keyword.get(opts, :quiet, false)}
  end

  @impl true
  def handle_connect(client_id, _credentials, state) do
    Logger.info("MQTT client connected: #{inspect(client_id)}")

    :telemetry.execute(
      [:pglp_experiment, :mqtt, :connect],
      %{count: 1},
      %{client_id: client_id}
    )

    {:ok, state}
  end

  @impl true
  def handle_publish(topic, payload, opts, state) do
    topic_str = Enum.join(topic, "/")
    decoded = decode_payload(payload)

    unless state.quiet? do
      Logger.info(
        "MQTT publish on #{topic_str} (qos=#{opts.qos}, retain=#{opts.retain}): " <>
          "#{inspect(decoded)}"
      )
    end

    :telemetry.execute(
      [:pglp_experiment, :mqtt, :publish],
      %{count: 1},
      %{topic: topic_str, qos: opts.qos, retain: opts.retain, payload: decoded}
    )

    {:ok, state}
  end

  @impl true
  def handle_subscribe(topics, state) do
    # RisingWave's sink only ever publishes -- it never subscribes -- but
    # this callback is required by the behaviour, so grant whatever QoS
    # was requested for correctness in case anything else ever connects.
    {:ok, Enum.map(topics, & &1.qos), state}
  end

  @impl true
  def handle_disconnect(reason, _state) do
    Logger.info("MQTT client disconnected: #{inspect(reason)}")

    :telemetry.execute(
      [:pglp_experiment, :mqtt, :disconnect],
      %{count: 1},
      %{reason: reason}
    )

    :ok
  end

  defp decode_payload(payload) do
    case Jason.decode(payload) do
      {:ok, decoded} -> decoded
      {:error, _} -> payload
    end
  end
end
