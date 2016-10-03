use Mix.Config

config :swarm,
  debug: false,   # turn on debugging mode
  debug_opts: [], # [:trace] for detailed logging
  node_blacklist: [
    # the following blacklists nodes set up by exrm/relx/distillery
    # for remote shells (the first) and hot upgrade scripting (the second)
    ~r/^remsh.*$/,
    ~r/^.+_upgrader_.+$/
    # or using strings..
    # "some_node" - literals
    # "^remsh.*$" - regex patterns
  ],
  node_whitelist: [
    # the same type of list as node_blacklist
  ]

config :logger,
  level: :info

config :porcelain,
  goon_warn_if_missing: false
