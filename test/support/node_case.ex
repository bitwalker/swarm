defmodule Swarm.NodeCase do
  @timeout 5000
  @heartbeat 100
  @permdown 1500

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: true
      import unquote(__MODULE__)
      @moduletag :clustered

      @timeout unquote(@timeout)
      @heartbeat unquote(@heartbeat)
      @permdown unquote(@permdown)
    end
  end

  def start_tracker(node) do
    call_node(node, fn ->
      {:ok, result} = Application.ensure_all_started(:swarm)
      result
    end)
  end

  def spawn_worker(node, name) do
    call_node(node, fn ->
      Swarm.register_name(name, MyApp.Worker, :start_link, [])
    end)
  end

  def flush() do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  defp call_node(node, func) do
    parent = self()
    ref = make_ref()

    pid = Node.spawn_link(node, fn ->
      result = func.()
      send parent, {ref, result}
      ref = Process.monitor(parent)
      receive do
        {:DOWN, ^ref, :process, _, _} -> :ok
      end
    end)

    receive do
      {^ref, result} -> {pid, result}
    after
      @timeout -> {pid, {:error, :timeout}}
    end
  end
end
