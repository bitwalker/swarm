defmodule Swarm.Cluster.Epmd do
  @moduledoc """
  This clustering strategy is effectively a no-op
  """
  use GenServer
  import Swarm.Logger

  def start_link(), do: :ignore
  def init(_) do
    :ok = :net_kernel.monitor_nodes(true, [node_type: :all])
    {:ok, []}
  end

  def handle_info({:nodeup, node, _info}, state) do
    handle_info({:nodeup, node}, state)
  end
  def handle_info({:nodeup, node}, state) do
    debug "nodeup #{node}"
    {:noreply, state}
  end
  def handle_info({:nodedown, node, _info}, state) do
    handle_info({:nodedown, node}, state)
  end
  def handle_info({:nodedown, node}, state) do
    debug "nodedown #{node}"
    {:noreply, state}
  end
  def handle_info(_, state), do: {:noreply, state}
end
