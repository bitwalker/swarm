use Mix.Config

config :swarm,
  pubsub: [name: Phoenix.PubSub.Test.PubSub,
           adapter: Phoenix.PubSub.PG2,
           opts: [pool_size: 1]],
  registry: [log_level: :debug,
             broadcast_period: 10,
             max_silent_periods: 3,
             permdown_period: 30_000]
