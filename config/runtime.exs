import Config

config :pglp_experiment,
  hostname: System.get_env("PGHOST", "localhost"),
  port: String.to_integer(System.get_env("PGPORT", "5432")),
  database: System.get_env("PGDATABASE", "pglp_dev"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  publication_name: System.get_env("PUBLICATION_NAME", "pglp_publication"),
  slot_name: System.get_env("SLOT_NAME", "pglp_slot")

# Nested under :risingwave (rather than flat top-level keys) because
# :hostname/:port/etc. above are already taken by the Postgres config.
#
# RW_HOST/RW_PORT is how *this Elixir app* (running on the host) reaches
# RisingWave -- distinct from RW_PG_HOSTNAME/RW_PG_PORT, which is how
# *RisingWave* (running inside docker-compose's network) reaches
# Postgres. These are genuinely different network hops; see
# PglpExperiment.RisingWave.Setup.
config :pglp_experiment, :risingwave,
  hostname: System.get_env("RW_HOST", "localhost"),
  port: String.to_integer(System.get_env("RW_PORT", "4566")),
  database: System.get_env("RW_DATABASE", "dev"),
  username: System.get_env("RW_USER", "root"),
  password: System.get_env("RW_PASSWORD", ""),
  table_name: System.get_env("RW_TABLE_NAME", "items"),
  subscription_name: System.get_env("RW_SUBSCRIPTION_NAME", "pglp_rw_subscription"),
  source_name: System.get_env("RW_SOURCE_NAME", "pglp_pg_source"),
  retention: System.get_env("RW_RETENTION", "1D"),
  fetch_timeout_seconds: String.to_integer(System.get_env("RW_FETCH_TIMEOUT_SECONDS", "5")),
  columns:
    System.get_env("RW_COLUMNS", "id int primary key, name varchar, updated_at timestamptz"),
  pg_hostname: System.get_env("RW_PG_HOSTNAME", "postgres"),
  pg_port: System.get_env("RW_PG_PORT", "5432"),
  pg_username: System.get_env("RW_PG_USERNAME", "postgres"),
  pg_password: System.get_env("RW_PG_PASSWORD", "postgres"),
  pg_database: System.get_env("RW_PG_DATABASE", "pglp_dev"),
  pg_table: System.get_env("RW_PG_TABLE", "public.items")

# RisingWave -> Elixir push experiment (mix pglp.mqtt): the app embeds an
# MQTT broker (PglpExperiment.Mqtt.Broker) and RisingWave's own
# `connector = 'mqtt'` sink connects INTO it -- the reverse direction of
# the :risingwave namespace above, where Elixir dials out to RisingWave.
#
# MQTT_BROKER_PORT is where *our* broker listens. MQTT_SINK_URL is how
# *RisingWave's sink* (running inside docker-compose's network) reaches
# back to that broker (running on the host, same as mix pglp.risingwave)
# -- a third network hop distinct from both RW_HOST/RW_PORT above (this
# app -> RisingWave, for running the CREATE SINK DDL) and
# RW_PG_HOSTNAME/RW_PG_PORT (RisingWave -> Postgres). See
# PglpExperiment.Mqtt.SinkSetup and docs/risingwave-sink.md for what
# reaching "the host from inside a container" actually required here.
config :pglp_experiment, :mqtt,
  broker_port: String.to_integer(System.get_env("MQTT_BROKER_PORT", "1883")),
  sink_url: System.get_env("MQTT_SINK_URL", "tcp://host.docker.internal:1883"),
  sink_name: System.get_env("MQTT_SINK_NAME", "pglp_mqtt_sink"),
  topic: System.get_env("MQTT_TOPIC", "pglp/items"),
  qos: System.get_env("MQTT_QOS", "at_least_once"),
  source_table: System.get_env("MQTT_SOURCE_TABLE", "items"),
  rw_hostname: System.get_env("RW_HOST", "localhost"),
  rw_port: String.to_integer(System.get_env("RW_PORT", "4566")),
  rw_database: System.get_env("RW_DATABASE", "dev"),
  rw_username: System.get_env("RW_USER", "root"),
  rw_password: System.get_env("RW_PASSWORD", "")

# RisingWave -> Elixir push experiment (mix pglp.http): the app embeds a
# Bandit HTTP server (PglpExperiment.Http.WebhookPlug) and RisingWave's
# own `connector = 'http'` sink POSTs INTO it -- same inverted direction
# as :mqtt above. This re-verifies docs/risingwave-sink.md's existing
# HTTP sink findings (ordering, outage/restart, throughput) against a
# real Elixir receiver instead of the throwaway Python listener used
# for the original investigation.
#
# RisingWave's HTTP sink requires a single `payload` column on its
# source -- :source_table (e.g. `items`) doesn't have one, so
# PglpExperiment.Http.SinkSetup first creates a view wrapping it into
# the right shape (:view_name, built from :columns via
# jsonb_build_object -- confirmed live that Postgres-style to_jsonb(row)
# doesn't work on RisingWave).
#
# HTTP_SINK_PORT is where *our* server listens. HTTP_SINK_URL is how
# *RisingWave's sink* reaches back to it -- same host.docker.internal
# network hop already confirmed working for :mqtt above.
config :pglp_experiment, :http_sink,
  server_port: String.to_integer(System.get_env("HTTP_SINK_PORT", "8080")),
  sink_url: System.get_env("HTTP_SINK_URL", "http://host.docker.internal:8080/webhook"),
  sink_name: System.get_env("HTTP_SINK_SINK_NAME", "pglp_http_sink"),
  view_name: System.get_env("HTTP_SINK_VIEW_NAME", "pglp_http_src"),
  source_table: System.get_env("HTTP_SINK_SOURCE_TABLE", "items"),
  columns: System.get_env("HTTP_SINK_COLUMNS", "id,name,updated_at") |> String.split(","),
  rw_hostname: System.get_env("RW_HOST", "localhost"),
  rw_port: String.to_integer(System.get_env("RW_PORT", "4566")),
  rw_database: System.get_env("RW_DATABASE", "dev"),
  rw_username: System.get_env("RW_USER", "root"),
  rw_password: System.get_env("RW_PASSWORD", "")
