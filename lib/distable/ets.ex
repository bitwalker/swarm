defmodule Distable.ETS do
  @moduledoc false
  use GenServer
  import Distable.Logger

  @type props :: [term]
  @type named :: {term, pid(), reference(), mfa, props}

  @name_table :distable_names # {name, pid, monitor_ref, mfa, props}

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
      fn {name, pid, _ref, mfa, props} -> {name, pid, nil, mfa, props} end)
  end

  @spec register_name(term, mfa, props) :: :ok | {:error, term}
  def register_name(name, mfa, props \\ []) do
    GenServer.call(__MODULE__, {:register_name, name, mfa, props})
  end

  @spec unregister_name(term) :: :ok
  def unregister_name(name), do: GenServer.call(__MODULE__, {:unregister_name, name})

  @spec register_property(pid(), term) :: :ok
  def register_property(pid, prop), do: GenServer.call(__MODULE__, {:register_property, pid, prop})

  ## GenServer API

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    {:ok, nil, 0}
  end

  def handle_call({:register_name, name, {m,f,a}=mfa, props}, _from, state) do
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
          :ets.insert(@name_table, {name, pid, ref, mfa, props})
          # Track this name on all other nodes
          connected = Node.list(:connected)
          :rpc.abcast(Node.list(:connected), __MODULE__, {:track_name, name, pid, mfa, props})
          {:reply, {:ok, pid}, state}
        {:error, _} = err ->
          {:reply, err, state}
      end
    rescue
      e ->
        reason = Exception.message(e)
        {:reply, {:error, reason}, state}
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
  def handle_call({:register_property, pid, prop}, _from, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [name, props] ->
        debug "adding #{prop} to #{inspect name} metadata"
        new_props = [prop|props]
        :ets.update_element(@name_table, name, [{4, new_props}])
        # Update other nodes
        :rpc.abcast(Node.list(:connected), __MODULE__, {:track_property, pid, prop})
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
    {:noreply, names_t}
  end
  def handle_info({:track_name, name, pid, mfa, props}, state) do
    debug "tracking name #{inspect name}"
    :ets.insert(@name_table, {name, pid, nil, mfa, props})
    {:noreply, state}
  end
  def handle_info({:untrack_name, name}, state) do
    debug "stopped tracking name #{inspect name}"
    :ets.delete(@name_table, name)
    {:noreply, state}
  end
  def handle_info({:track_property, pid, prop}, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_, :'$2'}) do
      [name, props] ->
        new_props = [prop|props]
        :ets.update_element(@name_table, name, [{4, new_props}])
        :ok
      _ ->
        :ok
    end
    {:noreply, state}
  end
  def handle_info({:DOWN, _monitor, _, pid, _reason}, state) do
    case :ets.match(@name_table, {:'$1', pid, :_, :_}) do
      [name] -> :ets.delete(@name_table, name)
      _      -> :ok
    end
    {:noreply, state}
  end
  def handle_info(_, state), do: {:noreply, state}

  defp create_or_get_table(name) do
    heir = {:heir, Process.whereis(Distable.Supervisor), ""}
    case :ets.info(name) do
      :undefined ->
        :ets.new(name, [:named_table, :public, :set, {:keypos, 1}, heir])
      _ ->
        name
    end
  end
end
