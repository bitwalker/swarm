use Mix.Config

config :swarm,
  nodes: [:"node1@127.0.0.1", :"node2@127.0.0.1"],
  sync_nodes_timeout: 0,
  anti_entropy_interval: 5_000,
  node_blacklist: [
    # the following blacklists nodes set up by exrm/relx/distillery
    # for remote shells (the first) and hot upgrade scripting (the second)
    ~r/^primary@.+$/,
    ~r/^remsh.*$/,
    ~r/^.+_upgrader_.+$/
    # or using strings..
    # "some_node" - literals
    # "^remsh.*$" - regex patterns
  ],
  node_whitelist: [
    # the same type of list as node_blacklist
  ]

config :logger, level: :warn

if System.get_env("SWARM_DEBUG") == "true" do
  config :swarm, debug: true
  config :logger, level: :debug
end

config :porcelain,
  goon_warn_if_missing: false
