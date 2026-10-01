defmodule PglpExperiment.Http.WebhookPlug do
  @moduledoc """
  A `Plug.Router` RisingWave's `connector = 'http'` sink POSTs into.

  Like `PglpExperiment.Mqtt.Broker`, this inverts the usual
  `rising_wave/*` direction: every module under `rising_wave/` has
  Elixir as the *client* dialing RisingWave (`Client.connect/1` opens a
  connection to RisingWave's SQL port). Here Elixir is the *server* —
  RisingWave's sink is the one dialing us, over plain HTTP instead of
  the Postgres wire protocol. That's why this lives under
  `PglpExperiment.Http.*` rather than `rising_wave/`, same reasoning as
  the `mqtt/` subsystem.

  ## Why re-verify the HTTP sink here?

  `docs/risingwave-sink.md` already documents a full investigation of
  RisingWave's HTTP sink — but using a throwaway Python `http.server`
  listener, not a real Elixir implementation. This module (plus
  `PglpExperiment.Http.SinkSetup` and `mix pglp.http`) re-verifies those
  findings (ordering, outage/restart behavior, throughput) against a
  real, permanent receiver built on Bandit — chosen because it builds on
  `thousand_island`, already a dependency here from the MQTT work.

  ## Telemetry

  Distinct event names from `Replication.Consumer`, `RisingWave.Consumer`,
  and `Mqtt.Broker` (each transport in this repo gets its own event
  names, since their delivery models genuinely differ):

    * `[:pglp_experiment, :http, :webhook]` — one per POST to
      `/webhook`. Measurements: `%{count: 1}`. Metadata: `%{method:
      "POST", path: String.t(), status: 200, payload: map() |
      binary()}` — `payload` is the JSON-decoded map if the body parses
      as JSON (RisingWave's HTTP sink POSTs its `payload` column as the
      raw body), otherwise the raw binary.
    * `[:pglp_experiment, :http, :unmatched]` — one per request that
      doesn't match `POST /webhook` (useful for catching RisingWave
      hitting an unexpected path/method while debugging setup).
      Measurements: `%{count: 1}`. Metadata: `%{method: String.t(),
      path: String.t()}`.
  """

  use Plug.Router
  require Logger

  plug(:match)
  plug(:dispatch)

  post "/webhook" do
    {:ok, body, conn} = read_body(conn)
    decoded = decode_payload(body)

    Logger.info("HTTP webhook POST #{conn.request_path}: #{inspect(decoded)}")

    :telemetry.execute(
      [:pglp_experiment, :http, :webhook],
      %{count: 1},
      %{method: "POST", path: conn.request_path, status: 200, payload: decoded}
    )

    send_resp(conn, 200, "{}")
  end

  match _ do
    Logger.debug("Unmatched HTTP request: #{conn.method} #{conn.request_path}")

    :telemetry.execute(
      [:pglp_experiment, :http, :unmatched],
      %{count: 1},
      %{method: conn.method, path: conn.request_path}
    )

    send_resp(conn, 404, "")
  end

  defp decode_payload(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> body
    end
  end
end
