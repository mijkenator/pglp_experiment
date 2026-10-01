defmodule PglpExperiment.MixProject do
  use Mix.Project

  def project do
    [
      app: :pglp_experiment,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {PglpExperiment.Application, []}
    ]
  end

  defp deps do
    [
      {:postgrex, "~> 0.19"},
      {:telemetry, "~> 1.0"},
      {:mqttx, "~> 0.11"},
      {:thousand_island, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:bandit, "~> 1.0"}
    ]
  end
end
