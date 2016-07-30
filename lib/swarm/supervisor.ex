defmodule Swarm.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do

    autocluster? = Application.get_env(:swarm, :autocluster, true)
    children = cond do
      autocluster? ->
        [worker(Swarm.ETS, []),
         worker(Swarm.Tracker, []),
         worker(Swarm.Cluster.Gossip, [])]
      :else ->
        [worker(Swarm.ETS, []),
         worker(Swarm.Tracker, []),
         worker(Swarm.Cluster.Epmd, [])]
    end
    supervise(children, strategy: :one_for_one)
  end
end
