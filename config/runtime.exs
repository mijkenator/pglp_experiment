import Config

config :pglp_experiment,
  hostname: System.get_env("PGHOST", "localhost"),
  port: String.to_integer(System.get_env("PGPORT", "5432")),
  database: System.get_env("PGDATABASE", "pglp_dev"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  publication_name: System.get_env("PUBLICATION_NAME", "pglp_publication"),
  slot_name: System.get_env("SLOT_NAME", "pglp_slot")
