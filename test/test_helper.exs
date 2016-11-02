exclude = Keyword.get(ExUnit.configuration(), :exclude, [])

unless :clustered in exclude do
  Swarm.Cluster.spawn()
end

ExUnit.start()
