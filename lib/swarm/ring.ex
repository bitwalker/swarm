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

  def nodeup(node),   do: GenServer.cast(__MODULE__, {:nodeup, node})
  def nodedown(node), do: GenServer.cast(__MODULE__, {:nodedown, node})

  @doc """
  Returns the node name for the given key based on it's hash and
  assignment within the ring
  """
  @spec node_for_key(key :: term) :: node
  def node_for_key(name) do
    {:ok, node} = :hash_ring.find_node(:swarm, "#{inspect name}")
    :"#{node}"
  end

  @doc """
  Returns the list of nodes currently participating in the ring
  """
  @spec get_nodes() :: [node]
  def get_nodes() do
    [Node.self|Enum.map(:ets.tab2list(:swarm_nodes), fn {n,_,_} -> n end)]
  end

  def get_members() do
    [Process.whereis(Swarm.Registry)|Enum.map(:ets.tab2list(:swarm_nodes), fn {_,pid,_} -> pid end)]
  end

  def publish(msg) do
    Enum.each(get_members, &GenServer.cast(&1, msg))
  end

  def multi_call(msg, timeout \\ 5_000) do
    Enum.map(get_members, fn pid ->
      Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
        try do
          {:ok, pid, GenServer.call(pid, msg, timeout)}
        catch
          :exit, reason -> {:error, pid, reason}
        end
      end)
    end)
    |> Enum.map(&Task.await(&1, :infinity))
  end

  ## GenServer implementation

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    :ets.new(:swarm_nodes, [:named_table, :public, :set, keypos: 1, read_concurrency: true])
    :ok = :hash_ring.create_ring(:swarm, 128)
    :ok = :hash_ring.add_node(:swarm, "#{Node.self}")
    {:ok, nil}
  end

  # Handles node up/down events
  def handle_cast({:nodeup, node}, state) do
    IO.inspect {:nodeup, Node.self, node}
    case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
      {:ok, _} ->
        case :rpc.call(node, Process, :whereis, [Swarm.Registry]) do
          pid when is_pid(pid) ->
            :ok = :hash_ring.add_node(:swarm, "#{node}")
            ref = Process.monitor(pid)
            :ets.insert(:swarm_nodes, {node, pid, ref})
            notify({:join, pid})
            debug "node added to ring: #{node}"
        end
        {:noreply, state}
      _err ->
        {:noreply, state}
    end
  end
  def handle_cast({:nodedown, node}, state) do
    IO.inspect {:nodedown, Node.self, node}
    case :ets.lookup(:swarm_nodes, node) do
      [] ->
        IO.inspect {:nodedown, :exists?, false}
      [{_node, _pid, ref}] ->
        Process.demonitor(ref, [:flush])
        :ets.delete(:swarm_nodes, node)
        :ok = :hash_ring.remove_node(:swarm, "#{node}")
        notify({:leave, node})
        debug "node removed from ring: #{node}"
    end
    {:noreply, state}
  end
  def handle_info({:DOWN, _ref, _type, pid, _info}, state) do
    IO.inspect {:DOWN, Node.self, pid}
    case :ets.match_object(:swarm_nodes, {:'$1', pid, :'$2'}) do
      [] ->
        :ok
      [{node, ^pid, _ref}] ->
        true = :ets.delete(:swarm_nodes, node)
        :ok = :hash_ring.remove_node(:swarm, "#{node}")
        notify({:leave, pid})
        debug "node removed from ring: #{node}"
    end
    {:noreply, state}
  end
  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_reason, _state) do
    true = :ets.delete(:swarm_nodes)
    :ok = :hash_ring.delete_ring(:swarm)
  end

  defp notify(msg) do
    [{nil, Process.whereis(Swarm.Registry), nil}|:ets.tab2list(:swarm_nodes)]
    |> Enum.each(fn {node, pid, _ref} ->
      send(pid, msg)
    end)
  end
end
