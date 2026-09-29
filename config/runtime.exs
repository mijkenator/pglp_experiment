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
