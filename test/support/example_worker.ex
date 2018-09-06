defmodule MyApp.Worker do
  def start_link(), do: GenServer.start_link(__MODULE__, [])

  def init(_name) do
    # IO.inspect "starting #{inspect self()} on #{Node.self}"
    {:ok, {:rand.uniform(5_000), 0}, 0}
  end

  def handle_call({:swarm, :begin_handoff}, _from, {delay, count}) do
    {:reply, {:resume, {delay, count}}, {delay, count}}
  end

  def handle_call(:ping, _from, state) do
    {:reply, {:pong, self()}, state}
  end

  def handle_cast({:swarm, :end_handoff, {delay, count}}, {_, _}) do
    {:noreply, {delay, count}}
  end

  def handle_cast(_, state) do
    {:noreply, state}
  end

  def handle_info(:timeout, {delay, count}) do
    Process.send_after(self(), :timeout, delay)
    {:noreply, {delay, count + 1}}
  end

  # this message is sent when this process should die
  # because it's being moved, use this as an opportunity
  # to clean up
  def handle_info({:swarm, :die}, state) do
    {:stop, :shutdown, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  def terminate(_reason, _state) do
    # IO.inspect "stopping #{inspect self()} on #{Node.self}"
    :ok
  end
end
