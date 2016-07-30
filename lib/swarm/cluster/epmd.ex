defmodule Swarm.Cluster.Epmd do
  @moduledoc """
  This clustering strategy uses EPMD, which means connecting nodes in
  the cluster is a manual process. This strategy is primarily useful if
  you have a fixed set of nodes, you need a strategy you can use during testing,
  you want to cluster nodes running on the same host, or the gossip strategy
  is not an option for some other reason.

  This process will monitor the list of known nodes, and connect to newly detected
  nodes as they appear. It is still required that you make nodes visible to each other
  via EPMD, and if you manually establish connections to other nodes, those changes will
  be picked up automatically.
  """
  use GenServer
  import Swarm.Logger

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    nodes = Node.list
    connect_nodes(nodes)
    {:ok, MapSet.new(nodes), 0}
  end

  def handle_info(:timeout, nodelist) do
    new_nodelist = MapSet.new(Node.list)
    added        = MapSet.difference(new_nodelist, nodelist)
    removed      = MapSet.difference(nodelist, new_nodelist)
    for n <- removed do
      debug "disconnected from #{inspect n}"
    end
    connect_nodes(added)
    Process.send_after(self(), :timeout, 100)
    {:noreply, new_nodelist}
  end
  def handle_info(_, nodelist) do
    {:noreply, nodelist}
  end

  defp connect_nodes(nodes) do
    for n <- nodes do
      case :net_kernel.connect_node(n) do
        true ->
          debug "connected to #{inspect n}"
          :ok
        reason ->
          debug "attempted to connect to node (#{inspect n}) from heartbeat, but failed with #{reason}."
          :ok
      end
    end
    :ok
  end
end
