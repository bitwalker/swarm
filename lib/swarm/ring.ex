defmodule Swarm.Ring do
  @moduledoc """
  This module defines a GenServer which manages
  the hash ring for the registry. The ring is setup
  as follows:

  - Each node in the cluster is given 128 replicas in the ring for
    even distribution
  - When nodeup events are detected, a node is inserted in the ring
    if it is running Swarm
  - When nodedown events are detected, a node is removed from the ring

  Registered names are then distributed around the ring based on the hash of their name.
  """
  use GenServer
  import Swarm.Logger

  @doc """
  Returns the node name for the given key based on it's hash and
  assignment within the ring
  """
  @spec node_for_key(key :: term) :: node
  def node_for_key(name) do
    {:ok, node} = :hash_ring.find_node(:swarm, "#{name}")
    :"#{node}"
  end

  @doc """
  Returns the list of nodes currently participating in the ring
  """
  @spec get_nodes() :: [node]
  def get_nodes(), do: GenServer.call(__MODULE__, :nodelist)

  ## GenServer implementation

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    :ok = :net_kernel.monitor_nodes(true, [node_type: :all])
    :ok = :hash_ring.create_ring(:swarm, 128)
    :ok = :hash_ring.add_node(:swarm, "#{Node.self}")
    {:ok, []}
  end

  # Returns the nodelist
  def handle_call(:nodelist, _from, nodelist), do: {:reply, nodelist, nodelist}
  def handle_call(_, _from, nodelist), do: {:noreply, nodelist}

  # Handles node up/down events
  def handle_info({:nodeup, node, _info}, nodelist) do
    handle_info({:nodeup, node}, nodelist)
  end
  def handle_info({:nodeup, node}, nodelist) do
    case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
      {:ok, _} ->
        debug "node added to ring: #{node}"
        :ok = :hash_ring.add_node(:swarm, "#{node}")
        Swarm.Registry.redistribute!
        {:noreply, [node|nodelist]}
      _err ->
        {:noreply, nodelist}
    end
  end
  def handle_info({:nodedown, node, _info}, nodelist) do
    handle_info({:nodedown, node}, nodelist)
  end
  def handle_info({:nodedown, node}, nodelist) do
    debug "node removed from ring: #{node}"
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
