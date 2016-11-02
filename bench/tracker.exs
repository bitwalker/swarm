Application.put_env(:swarm, :debug, false)
Application.ensure_started(:swarm)

defmodule SwarmTest.Worker do
  use GenServer

  def start_link(), do: GenServer.start_link(__MODULE__, [])
  def init(_), do: {:ok, nil}

  def handle_call(msg, _from, state) do
    {:reply, msg, state}
  end
end

:rand.seed(:exs64)

# Best single-node run so far 470ms for 10k
Benchee.run(%{time: 10}, %{
  "Swarm.register_name/4" => fn ->
    for i <- 1..10_000 do
      r = :rand.uniform(100_000_000)
      {:ok, _pid} = Swarm.register_name({:myapp, i, r}, SwarmTest.Worker, :start_link, [])
    end
  end
})
