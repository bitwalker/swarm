defmodule Swarm.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    # This table is the conflict-free view of the local registry state
    :ets.new(:swarm_registry, [
          :named_table,
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true,
          keypos: 1])

    cluster_strategy = case Application.get_env(:swarm, :autocluster, true) do
      :kubernetes -> Swarm.Cluster.Kubernetes
      true        -> Swarm.Cluster.Gossip
      false       -> Swarm.Cluster.Epmd
      mod when is_atom(mod) -> mod
      _           -> Swarm.Cluster.Epmd
    end

    default_pubsub_opts = [name: Swarm.PubSub,
                           adapter: Phoenix.PubSub.PG2,
                           opts: [pool_size: 1]]
    pubsub = Application.get_env(:swarm, :pubsub, [])
    pubsub = Keyword.merge(default_pubsub_opts, pubsub)
    Application.put_env(:swarm, :pubsub, pubsub, persistent: true)

    default_registry_opts = [name: Swarm.Registry,
                             log_level: false,
                             broadcast_period: 10,
                             max_silent_periods: 3,
                             permdown_period: 10_000]
    registry = Application.get_env(:swarm, :registry, [])
    registry = Keyword.merge(default_registry_opts, registry)
    Application.put_env(:swarm, :registry, registry, persistent: true)

    children = [
      worker(:hash_ring, []),
      supervisor(Task.Supervisor, [[name: Swarm.TaskSupervisor]]),
      supervisor(pubsub[:adapter], [pubsub[:name], pubsub[:opts]]),
      worker(Swarm.Ring, []),
      worker(Swarm.Registry, [registry]),
      worker(cluster_strategy, [])
    ]
    supervise(children, strategy: :rest_for_one)
  end
end
