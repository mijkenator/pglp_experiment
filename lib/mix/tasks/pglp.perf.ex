defmodule Mix.Tasks.Pglp.Perf do
  @shortdoc "Measures how many replication events the Consumer can consume per second"

  @moduledoc """
  Performance-tests `PglpExperiment.Replication.Consumer` by generating a
  large number of INSERT/UPDATE events directly against Postgres, then
  measuring how fast the running consumer decodes and processes them.

  ## Usage

      mix pglp.perf                  # 10_000 rows (10_000 inserts + 10_000 updates = 20_000 events)
      mix pglp.perf --rows 100_000
      mix pglp.perf --rows 50_000 --batch-size 1000
      mix pglp.perf --rows 50_000 --ack-every-commit 10

  ## What it measures

    * **Throughput** — events/sec, computed from telemetry timestamps
      between the first and last `[:pglp_experiment, :replication,
      :change]` event received by the *consumer* (not by the generator —
      so this measures consumption speed, not write speed).
    * **Replication lag** — time between a transaction's commit (WAL
      commit timestamp) and the consumer receiving/decoding it. Reported
      as min/mean/p95/max. On a local single-host run with light load,
      expect this near zero (even occasionally negative by a few
      hundred µs — that's clock/measurement noise, not a real ordering
      violation); it grows with batch size (larger transactions take
      longer to fully decode+process before the commit is observed) and
      with sustained throughput once the consumer can't fully keep up.
    * **Correctness** — confirms every generated row id was actually
      observed (no silently dropped events) before reporting numbers.

  `--ack-every-commit N` (default `1`) controls how many commits the
  consumer accumulates before proactively acknowledging them back to
  Postgres — see the `:ack_every_commit` option on
  `PglpExperiment.Replication.Consumer.start_link/1`. Raising it reduces
  the number of standby status updates sent, at the cost of a larger
  post-crash redelivery window; use this to explore that tradeoff under
  load.

  Uses its own dedicated publication/slot (`pglp_perf_publication` /
  `pglp_perf_slot`) and table (`pglp_perf_items`), separate from your
  normal dev setup, so it won't interfere with (or be skewed by) whatever
  else is running. Cleans up the slot/publication/table on exit.

  Requires Postgres to already be running (`docker compose up -d`) with
  `wal_level=logical` — same as the main app.
  """

  use Mix.Task

  alias PglpExperiment.Perf.{Collector, Generator}
  alias PglpExperiment.Replication.{Consumer, Setup}

  @table "pglp_perf_items"
  @publication "pglp_perf_publication"
  @slot "pglp_perf_slot"
  @consumer_name __MODULE__.Consumer

  @impl Mix.Task
  def run(args) do
    # Deliberately `app.config` (loads config, compiles, does NOT start the
    # supervision tree) rather than `app.start` — starting the full app
    # would also start `PglpExperiment.Application`'s own default
    # `Consumer` (against `pglp_publication FOR ALL TABLES`), which would
    # then *also* pick up changes on our perf table and double-count
    # telemetry events alongside the dedicated perf consumer below.
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:telemetry)

    {opts, _} =
      OptionParser.parse!(args,
        strict: [rows: :integer, batch_size: :integer, ack_every_commit: :integer],
        aliases: [r: :rows, b: :batch_size, a: :ack_every_commit]
      )

    row_count = opts[:rows] || 10_000
    batch_size = opts[:batch_size] || 500
    ack_every_commit = opts[:ack_every_commit] || 1

    connection_opts = [
      hostname: Application.fetch_env!(:pglp_experiment, :hostname),
      port: Application.fetch_env!(:pglp_experiment, :port),
      database: Application.fetch_env!(:pglp_experiment, :database),
      username: Application.fetch_env!(:pglp_experiment, :username),
      password: Application.fetch_env!(:pglp_experiment, :password)
    ]

    try do
      Mix.shell().info(
        "Preparing perf run: #{row_count} rows (#{row_count} inserts + #{row_count} updates = " <>
          "#{row_count * 2} expected events), batch size #{batch_size}, " <>
          "ack every #{ack_every_commit} commit(s)"
      )

      {:ok, setup_conn} = Postgrex.start_link(connection_opts)
      Generator.prepare_table!(setup_conn, @table)
      GenServer.stop(setup_conn)

      {:ok, _collector} = Collector.start_link()
      Collector.attach!()

      {:ok, _consumer} =
        Consumer.start_link(
          connection_opts ++
            [
              publication_name: @publication,
              slot_name: @slot,
              quiet: true,
              name: @consumer_name,
              ack_every_commit: ack_every_commit
            ]
        )

      Mix.shell().info("Consumer connected, generating load...")

      {:ok, gen_conn} = Postgrex.start_link(connection_opts)
      generate_started_at = System.monotonic_time(:microsecond)
      Generator.generate!(gen_conn, @table, row_count, batch_size)
      generate_ended_at = System.monotonic_time(:microsecond)
      GenServer.stop(gen_conn)

      Mix.shell().info(
        "Generated #{row_count * 2} events in " <>
          "#{format_us(generate_ended_at - generate_started_at)}, waiting for consumer to drain..."
      )

      expected_events = row_count * 2
      await_drain(expected_events)

      report(row_count, expected_events)
    after
      Collector.detach!()
      stop_consumer()
      cleanup(connection_opts)
    end
  end

  # The replication slot can't be dropped while a connection is still
  # attached to it, so the consumer must be stopped before `cleanup/1`
  # runs. Looked up by name (rather than threading a pid out of the `try`
  # block, which Elixir scoping doesn't allow into `after`).
  defp stop_consumer do
    case Process.whereis(@consumer_name) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  catch
    :exit, _ -> :ok
  end

  defp await_drain(expected_events, deadline \\ nil)

  defp await_drain(expected_events, nil) do
    await_drain(expected_events, System.monotonic_time(:millisecond) + 30_000)
  end

  defp await_drain(expected_events, deadline) do
    %{changes: changes} = Collector.snapshot()

    cond do
      changes >= expected_events ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Mix.shell().error(
          "Timed out waiting for consumer to drain: received #{changes}/#{expected_events} " <>
            "events after 30s"
        )

      true ->
        Process.sleep(50)
        await_drain(expected_events, deadline)
    end
  end

  defp report(row_count, expected_events) do
    %{changes: changes, by_type: by_type, lags_us: lags_us, acks: acks} = Collector.snapshot()

    duration_us = consumption_duration_us()
    events_per_sec = if duration_us > 0, do: changes / (duration_us / 1_000_000), else: 0.0

    Mix.shell().info("\n== Results ==")
    Mix.shell().info("Events received:   #{changes} / #{expected_events}")
    Mix.shell().info("By type:           #{inspect(by_type)}")
    Mix.shell().info("Consumption time:  #{format_us(duration_us)}")
    Mix.shell().info("Throughput:        #{Float.round(events_per_sec, 1)} events/sec")
    Mix.shell().info("Acks sent:         #{acks} (for #{length(lags_us)} commits)")

    report_lag(lags_us)

    case Collector.check_no_gaps(row_count) do
      {:ok, count} ->
        Mix.shell().info("Correctness:       OK, all #{count} row ids observed, no gaps")

      {:missing, missing} ->
        Mix.shell().error(
          "Correctness:       FAILED, #{length(missing)} row ids never observed " <>
            "(showing up to 20): #{inspect(Enum.take(missing, 20))}"
        )
    end
  end

  defp consumption_duration_us do
    case Collector.snapshot() do
      %{first_change_at: nil} -> 0
      %{first_change_at: first, last_change_at: last} -> last - first
    end
  end

  defp report_lag([]), do: Mix.shell().info("Replication lag:   no commits observed")

  defp report_lag(lags_us) do
    sorted = Enum.sort(lags_us)
    count = length(sorted)
    p95_index = max(0, ceil(count * 0.95) - 1)

    min_us = List.first(sorted)
    max_us = List.last(sorted)
    mean_us = Enum.sum(sorted) / count
    p95_us = Enum.at(sorted, p95_index)

    Mix.shell().info(
      "Replication lag:   min=#{format_us(min_us)} mean=#{format_us(round(mean_us))} " <>
        "p95=#{format_us(p95_us)} max=#{format_us(max_us)} (n=#{count} commits)"
    )
  end

  defp format_us(us) when us < 1_000, do: "#{us}µs"
  defp format_us(us) when us < 1_000_000, do: "#{Float.round(us / 1_000, 1)}ms"
  defp format_us(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  defp cleanup(connection_opts) do
    Setup.drop!(connection_opts, @publication, @slot)

    {:ok, conn} = Postgrex.start_link(connection_opts)
    Postgrex.query!(conn, "DROP TABLE IF EXISTS #{@table}", [])
    GenServer.stop(conn)
  rescue
    _ -> :ok
  end
end
