defmodule Swarm.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    cluster_strategy = case Application.get_env(:swarm, :autocluster, true) do
      :kubernetes -> Swarm.Cluster.Kubernetes
      true        -> Swarm.Cluster.Gossip
      false       -> Swarm.Cluster.Epmd
      mod when is_atom(mod) -> mod
      _           -> Swarm.Cluster.Epmd
    end

    pubsub = Application.get_env(:swarm, :pubsub, [])

    children = [
      worker(:hash_ring, []),
      supervisor(Task.Supervisor, [[name: Swarm.TaskSupervisor]]),
      supervisor(pubsub[:adapter] || Phoenix.PubSub.PG2,
                 [pubsub[:name] || Phoenix.PubSub.Test.PubSub,
                  pubsub[:opts] || []]),
      worker(Swarm.Ring, []),
      worker(Swarm.Registry, [[name: Swarm.Registry]]),
      worker(cluster_strategy, [])
    ]
    supervise(children, strategy: :rest_for_one)
  end
end
