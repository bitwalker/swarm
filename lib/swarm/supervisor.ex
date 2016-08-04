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

    children = [
      worker(Swarm.ETS, []),
      worker(Swarm.Tracker, []),
      worker(cluster_strategy, [])
    ]
    supervise(children, strategy: :one_for_one)
  end
end
