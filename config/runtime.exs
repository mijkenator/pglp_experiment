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
