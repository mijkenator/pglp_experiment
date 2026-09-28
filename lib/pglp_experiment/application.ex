defmodule PglpExperiment.Application do
  @moduledoc false

  use Application

  alias PglpExperiment.Replication.Consumer

  @impl true
  def start(_type, _args) do
    connection_opts = [
      hostname: Application.fetch_env!(:pglp_experiment, :hostname),
      port: Application.fetch_env!(:pglp_experiment, :port),
      database: Application.fetch_env!(:pglp_experiment, :database),
      username: Application.fetch_env!(:pglp_experiment, :username),
      password: Application.fetch_env!(:pglp_experiment, :password)
    ]

    consumer_opts =
      connection_opts ++
        [
          publication_name: Application.fetch_env!(:pglp_experiment, :publication_name),
          slot_name: Application.fetch_env!(:pglp_experiment, :slot_name)
        ]

    children = [
      {Consumer, consumer_opts}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PglpExperiment.Supervisor)
  end
end
