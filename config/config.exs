use Mix.Config

config :swarm,
  debug: true,
  registry: [log_level: :debug,
             broadcast_period: 10,
             max_silent_periods: 3,
             permdown_period: 30_000]

config :logger,
  level: :debug
