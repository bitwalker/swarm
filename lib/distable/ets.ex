defmodule Distable.ETS do
  @moduledoc false
  use GenServer

  defmodule State do
    defstruct [:nodes, :names]
    @type t :: %__MODULE__{
      nodes: [atom()],
      names: pid | atom,
    }
  end

  @name_table :distable_names # {name, pid}

  def get_name(name),           do: :ets.lookup(@name_table, name)
  def get_names(),              do: :ets.tab2list(@name_table)
  def get_nodes(),              do: GenServer.call(__MODULE__, :nodes)
  def set_nodes(nodes),         do: GenServer.call(__MODULE__, {:nodes, nodes})
  def register_name(name, mfa), do: GenServer.call(__MODULE__, {:register_name, name, mfa})
  def unregister_name(name),    do: GenServer.call(__MODULE__, {:unregister_name, name})
  def register_property(pid, prop), do: GenServer.call(__MODULE__, {:register_property, pid, prop})
  def get_by_property(prop),    do: GenServer.call(__MODULE__, {:get_by_property, prop})

  ## GenServer API

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: {:local, __MODULE__})

  def init(_) do
    {:ok, %State{nodes: {Node.self}}, 0}
  end

  def handle_call(:nodes, _from, state) do
    {:reply, state.nodes, state}
  end
  def handle_call({:nodes, nodes}, _from, state) do
    {:reply, :ok, %{state | :nodes => nodes}}
  end
  def handle_call({:register_name, name, {m,f,a}=mfa}, _from, state) do
    try do
      pid = case apply(m, f, a) do
        {:ok, pid}           -> {:ok, pid}
        pid when is_pid(pid) -> {:ok, pid}
        {:error, _} = err    -> err
        other                -> {:error, other}
      end
      case pid do
        {:ok, pid} ->
          ref = Process.monitor(pid)
          :ets.insert(state.names, {name, pid, ref, mfa})
          {:reply, pid, state}
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
    case :ets.lookup(state.names, name) do
      [{_name, _pid, ref, _mfa}] ->
        Process.demonitor(ref)
        :ets.delete(state.names, name)
      _ ->
        :ok
    end
    {:reply, :ok, state}
  end
  def handle_call({:register_property, pid, prop}, _from, state) do
    case :ets.match(state.names, {:'$1', pid, :_, :_, :'$2'}) do
      [name, props] ->
        new_props = [prop|props]
        :ets.update_element(state.names, name, [{4, new_props}])
      _ ->
        :ok
    end
  end
  def handle_call({:get_by_property, prop}, _from, state) do
    results = :ets.tab2list(state.names)
    |> Enum.filter_map(fn {_n,_pid,_ref,_mfa,props} -> prop in props end,
                       fn {_n, pid,_ref,_mfa,_props} -> pid end)
    {:reply, results, state}
  end
  def handle_call(_, _from, state) do
    {:noreply, state}
  end

  def handle_info(:timeout, state) do
    names_t = create_or_get_table(@name_table)
    state = %{state | :nodes => {}, :names => names_t}
    {:noreply, state}
  end
  def handle_info({:DOWN, _monitor, _, pid, _reason}, state) do
    case :ets.match(state.names, {:'$1', pid, :_, :_}) do
      [name] -> :ets.delete(state.names, name)
      _      -> :ok
    end
    {:noreply, state}
  end
  def handle_info(_, state), do: {:noreply, state}

  defp create_or_get_table(name) do
    heir = {:heir, Process.whereis(Distable.Supervisor), ""}
    case :ets.info(name) do
      :undefined ->
        :ets.new(name, [:named_table, :protected, :set, {:keypos, 1}, heir])
      _ ->
        :ok = GenServer.call(Distable.Supervisor, {:change_ets_owner, name, self()})
        name
    end
  end
end
