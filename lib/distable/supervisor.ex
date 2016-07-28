defmodule Distable.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do

    autocluster? = Application.get_env(:distable, :autocluster, true)
    children = cond do
      autocluster? ->
        [worker(Distable.ETS, []),
         worker(Distable.Tracker, []),
         worker(Distable.Cluster.Gossip, [])]
      :else ->
        [worker(Distable.ETS, []),
         worker(Distable.Tracker, []),
         worker(Distable.Cluster.Epmd, [])]
    end
    supervise(children, strategy: :one_for_one)
  end
end
