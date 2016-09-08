defmodule Swarm.Ring do
  use GenServer
  require Logger

  def node_for_key(name) do
    {:ok, node} = :hash_ring.find_node(:swarm, "#{name}")
    :"#{node}"
  end

  def get_nodes(), do: GenServer.call(__MODULE__, :nodelist)

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    :ok = :hash_ring.create_ring(:swarm, 128)
    :ok = :hash_ring.add_node(:swarm, "#{Node.self}")
    :ok = :net_kernel.monitor_nodes(true, [node_type: :all])
    {:ok, []}
  end

  def handle_call(:nodelist, _from, nodelist), do: {:reply, nodelist, nodelist}

  def handle_info({:nodeup, node, _info}, nodelist) do
    case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
      {:ok, _} ->
        :ok = :hash_ring.add_node(:swarm, "#{node}")
        Swarm.Registry.redistribute!
        {:noreply, [node|nodelist]}
      err ->
        {:noreply, nodelist}
    end
  end
  def handle_info({:nodedown, node, _info}, nodelist) do
    :ok = :hash_ring.remove_node(:swarm, "#{node}")
    Swarm.Registry.redistribute!
    {:noreply, nodelist -- [node]}
  end
  def handle_info(_, nodelist) do
    {:noreply, nodelist}
  end

  def terminate(_reason, _state) do
    :ok = :hash_ring.delete_ring(:swarm)
  end
end
