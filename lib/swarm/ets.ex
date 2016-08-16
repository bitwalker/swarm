defmodule Swarm.ETS do
  @moduledoc """
  This process is the backing store for the registry. It should
  not be accessed directly, except by the Tracker process.

  It starts and owns an ETS table where the registry information is
  stored. It also exposes the registry via an API consumed by the Tracker.
  """
  use GenServer
  import Swarm.Logger

  @type groups :: [term]
  @type named :: {term, pid(), reference(), mfa, groups}

  @name_table :swarm_names # {name, pid, monitor_ref, mfa, groups}
  @node_table :swarm_nodes # {name, metadata :: Keyword.t}

  @spec nodeup(node()) :: :ok
  def nodeup(node) do
    case :ets.lookup(@node_table, node) do
      [_] -> :ok
      _   ->
        true = :ets.insert(@node_table, {node, node})
        :ok
    end
  end

  @spec nodedown(node()) :: :ok
  def nodedown(node) do
    true = :ets.delete(@node_table, node)
    :ok
  end

  @spec nodelist() :: [{node(), :connected | :pending}]
  def nodelist do
    :ets.tab2list(@node_table)
    |> Enum.map(fn {n, _} -> n end)
  end

  @spec get_name(term) :: named :: nil
  def get_name(name) do
    case :ets.lookup(@name_table, name) do
      [found] -> found
      _       -> nil
    end
  end

  @spec get_names() :: [named]
  def get_names() do
    :ets.tab2list(@name_table)
  end

  @spec get_names(node()) :: [named]
  def get_names(node) do
    :ets.tab2list(@name_table)
    |> Enum.filter(fn n -> node(elem(n, 1)) == node end)
  end

  @spec get_local_names() :: [named]
  def get_local_names() do
    this_node = Node.self
    Enum.filter_map(
      :ets.tab2list(@name_table),
      fn named -> node(elem(named, 1)) == this_node end,
      fn {name, pid, _ref, mfa, groups} -> {name, pid, nil, mfa, groups} end)
  end

  @spec register_name(term, mfa | pid, groups) :: :ok | {:error, term}
  def register_name(name, mfa, groups \\ []) do
    GenServer.call(__MODULE__, {:register_name, name, mfa, groups})
  end

  @spec unregister_name(term) :: :ok
  def unregister_name(name), do: GenServer.call(__MODULE__, {:unregister_name, name})

  @spec join_group(term, pid()) :: :ok
  def join_group(group, pid), do: GenServer.call(__MODULE__, {:join_group, group, pid})
  @spec leave_group(term, pid()) :: :ok
  def leave_group(group, pid), do: GenServer.call(__MODULE__, {:leave_group, group, pid})

  ## GenServer API

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    {:ok, nil, 0}
  end

  def handle_call({:register_name, name, pid, groups}, _from, state) when is_pid(pid) do
    debug "registering #{inspect name}"
    ref = Process.monitor(pid)
    :ets.insert(@name_table, {name, pid, ref, nil, groups})
    # Track this name on all other nodes
    :rpc.abcast(Node.list(:connected), __MODULE__, {:track_name, name, pid, nil, groups})
    {:reply, {:ok, pid}, state}
  end
  def handle_call({:register_name, name, {m,f,a}=mfa, groups}, _from, state) do
    try do
      pid = case apply(m, f, a) do
        {:ok, pid}           -> {:ok, pid}
        pid when is_pid(pid) -> {:ok, pid}
        {:error, _} = err    -> err
        other                -> {:error, other}
      end
      case pid do
        {:ok, pid} ->
          debug "registering #{inspect name}"
          ref = Process.monitor(pid)
          :ets.insert(@name_table, {name, pid, ref, mfa, groups})
          # Track this name on all other nodes
          :rpc.abcast(Node.list(:connected), __MODULE__, {:track_name, name, pid, mfa, groups})
          {:reply, {:ok, pid}, state}
        {:error, _} = err ->
          {:reply, err, state}
      end
    catch
      type, reason ->
        {:reply, {:error, {type, reason}}, state}
    end
  end
  def handle_call({:unregister_name, name}, _from, state) do
    case :ets.lookup(@name_table, name) do
      [{_name, _pid, ref, _mfa}] ->
        debug "unregistering #{inspect name}"
        Process.demonitor(ref)
        :ets.delete(@name_table, name)
        # Untrack this name on all other nodes
        :rpc.abcast(Node.list(:connected), __MODULE__, {:untrack_name, name})
      _ ->
        :ok
    end
    {:reply, :ok, state}
  end
  def handle_call({:join_group, group, pid}, _from, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [[name, groups]] ->
        debug "joining #{inspect name} to group #{inspect group}"
        new_groups = [group|groups]
        :ets.update_element(@name_table, name, [{5, new_groups}])
        # Update other nodes
        :rpc.abcast(Node.list(:connected), __MODULE__, {:track_group, group, pid})
      _ ->
        :ok
    end
    {:reply, :ok, state}
  end
  def handle_call({:leave_group, group, pid}, _from, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [[name, groups]] ->
        debug "parting #{inspect name} from group #{inspect group}"
        new_groups = Enum.filter(groups, fn ^group -> false; _ -> true end)
        :ets.update_element(@name_table, name, [{5, new_groups}])
        # Update other nodes
        :rpc.abcast(Node.list(:connected), __MODULE__, {:untrack_group, group, pid})
      _ ->
        :ok
    end
    {:reply, :ok, state}
  end
  def handle_call(_, _from, state) do
    {:noreply, state}
  end

  def handle_info(:timeout, _state) do
    names_t = create_or_get_table(@name_table)
    nodes_t = create_or_get_table(@node_table)
    {:noreply, %{names: names_t, nodes: nodes_t}}
  end
  def handle_info({:track_name, name, pid, mfa, groups}, state) do
    debug "tracking name #{inspect name}"
    :ets.insert(@name_table, {name, pid, nil, mfa, groups})
    {:noreply, state}
  end
  def handle_info({:untrack_name, name}, state) do
    debug "stopped tracking name #{inspect name}"
    :ets.delete(@name_table, name)
    {:noreply, state}
  end
  def handle_info({:track_group, group, pid}, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [[name, groups]] ->
        debug "tracking group #{inspect group} for #{inspect pid}"
        new_groups = [group|groups]
        :ets.update_element(@name_table, name, [{5, new_groups}])
        :ok
      _ ->
        :ok
    end
    {:noreply, state}
  end
  def handle_info({:untrack_group, group, pid}, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [[name, groups]] ->
        debug "untracking group #{inspect group} for #{inspect pid}"
        new_groups = Enum.filter(groups, fn ^group -> false; _ -> true end)
        :ets.update_element(@name_table, name, [{5, new_groups}])
        :ok
      _ ->
        :ok
    end
    {:noreply, state}
  end
  def handle_info({:DOWN, _monitor, _, pid, _reason}, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :_}) do
      [[name]] ->
        # Untrack this name on all other nodes
        :rpc.abcast(Node.list(:connected), __MODULE__, {:untrack_name, name})
        :ets.delete(@name_table, name)
      _ ->
        :ok
    end
    {:noreply, state}
  end
  def handle_info(_, state), do: {:noreply, state}

  defp create_or_get_table(name) do
    heir = {:heir, Process.whereis(Swarm.Supervisor), ""}
    case :ets.info(name) do
      :undefined ->
        :ets.new(name, [:named_table, :public, :set, {:keypos, 1}, heir])
      _ ->
        name
    end
  end
end
