defmodule Swarm.App do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      supervisor(Task.Supervisor, [[name: Swarm.TaskSupervisor]]),
      worker(Swarm.Registry, []),
      worker(Swarm.Tracker, []),
    ]
    supervise(children, strategy: :one_for_one)
  end
end
