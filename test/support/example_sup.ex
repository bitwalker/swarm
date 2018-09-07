defmodule MyApp.WorkerSup do
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      worker(MyApp.Worker, [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def register() do
    {:ok, _pid} = Supervisor.start_child(__MODULE__, [])
  end
end
