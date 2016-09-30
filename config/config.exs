use Mix.Config

config :swarm,
  debug: false,   # turn on debugging mode
  debug_opts: [], # [:trace] for detailed logging
  node_blacklist: [~r/^remsh.*$/],
  node_whitelist: []

config :logger,
  level: :info

config :porcelain,
  goon_warn_if_missing: false
