defmodule MyApp.RestartWorker do
  @moduledoc false

  # A worker process that requests to be restarted during Swarm hand-off

  def start_link(name), do: GenServer.start_link(__MODULE__, name)

  def init(name), do: {:ok, name}

  def handle_call({:swarm, :begin_handoff}, _from, state) do
    {:reply, :restart, state}
  end

  def handle_info({:swarm, :die}, state) do
    {:stop, :shutdown, state}
  end
end
