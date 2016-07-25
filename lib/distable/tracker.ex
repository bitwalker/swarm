defmodule Distable.Tracker do
  use GenServer
  alias Distable.ETS

  @max_hash  134_217_728 # :math.pow(2,27)

  def register(name, {m,f,a})
    when is_atom(m) and is_atom(f) and is_list(a) do
    GenServer.call(__MODULE__, {:register, name, {m,f,a}})
  end

  def unregister(name) do
    GenServer.call(__MODULE__, {:unregister, name})
  end

  def register_property(pid, prop) do
    GenServer.call({__MODULE__, :erlang.node(pid)}, {:register_property, pid, prop})
  end

  def get_by_property(prop) do
    GenServer.call(__MODULE__, {:get_by_property, prop})
  end

  def whereis(name) do
    case ETS.get_name(name) do
      {_name, {pid, _}} ->
        pid
      _ ->
        this_node = Node.self
        case node_for_name(name) do
          ^this_node ->
            :undefined
          node ->
            :rpc.call(node, __MODULE__, :whereis, [name], 5_000)
        end
    end
  end

  def call(name, message, timeout) do
    case whereis(name) do
      :undefined ->
        {:error, {:name_not_found, name}}
      pid when is_pid(pid) ->
        :gen_server.call(pid, message, timeout)
    end
  end

  def cast(name, message) do
    case whereis(name) do
      :undefined ->
        {:error, {:name_not_found, name}}
      pid when is_pid(pid) ->
        :gen_server.cast(pid, message)
    end
  end

  ## GenServer API

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    :net_kernel.monitor_nodes(true, [:nodedown_reason])
    {:ok, nil}
  end

  def handle_call({:register, name, mfa}, _from, state) do
    case whereis(name) do
      pid when is_pid(pid) ->
        {:reply, {:error, {:already_registered, pid}}, state}
      :undefined ->
        this_node = Node.self
        case node_for_name(name) do
          ^this_node ->
            res = ETS.register_name(name, mfa)
            {:reply, res, state}
          node ->
            res = GenServer.call({__MODULE__, node}, {:register, name, mfa}, 5_000)
            {:reply, res, state}
        end
    end
  end
  def handle_call({:unregister, name}, _from, state) do
    case whereis(name) do
      pid when is_pid(pid) ->
        this_node = Node.self
        case node_for_name(name) do
          ^this_node ->
            ETS.unregister_name(name)
          node ->
            GenServer.call({__MODULE__, node}, {:unregister, name}, 5_000)
        end
      :undefined ->
        :ok
    end
    {:reply, :ok, state}
  end
  def handle_call({:register_property, pid, prop}, _from, state) do
    res = ETS.register_property(pid, prop)
    {:reply, res, state}
  end
  def handle_call({:get_by_property, prop}, _from, state) do
    {res, _} = :rpc.multicall(ETS, :get_by_property, [prop])
    results = List.flatten(res)
    {:reply, results, state}
  end
  def handle_call({:handoff, name, mfa}, _from, state) do
    case ETS.register_name(name, mfa) do
      {:ok, pid} ->
        {:reply, {:ok, pid}, state}
      err ->
        {:reply, err, state}
    end
  end
  def handle_call({:handoff, name, mfa, handoff_state}, _from, state) do
    case ETS.register_name(name, mfa) do
      {:ok, pid} ->
        GenServer.call(pid, {:distable, :end_handoff, handoff_state})
        {:reply, {:ok, pid}, state}
      err ->
        {:reply, err, state}
    end
  end
  def handle_call(_, _from, state), do: {:noreply, state}

  def handle_info({:nodeup, node, _info}, state) do
    old_nodes = ETS.get_nodes()
    nodelist = Tuple.to_list(old_nodes)
    cond do
      node in nodelist -> :ok
      :else ->
        new_nodes = Tuple.append(old_nodes, node)
        ETS.set_nodes(new_nodes)
        redistribute(old_nodes, new_nodes)
    end
    {:noreply, state}
  end
  def handle_info({:nodedown, node, _info}, state) do
    old_nodes = ETS.get_nodes()
    nodelist = Tuple.to_list(old_nodes)
    cond do
      node in nodelist ->
        ni = Enum.find_index(nodelist, fn n -> n == node end)
        new_nodes = Tuple.delete_at(old_nodes, ni)
        ETS.set_nodes(new_nodes)
        redistribute(old_nodes, new_nodes)
    end
    {:noreply, state}
  end
  def handle_info(_, state) do
    {:noreply, state}
  end

  defp node_for_name(name) do
    case ETS.get_nodes() do
      [nodes: nodes] ->
        node_for_hash(nodes, :erlang.phash2(name))
      _ ->
        :undefined
    end
  end
  defp node_for_hash(nodes, hash) do
    range = div(@max_hash, tuple_size(nodes))
    node_pos = div(hash, range+1)
    elem(min(tuple_size(nodes), node_pos), nodes)
  end

  defp redistribute(nodes, nodes), do: :ok
  defp redistribute(_old_nodes, new_nodes) do
    this_node = Node.self
    new_nodes = Enum.sort(Tuple.to_list(new_nodes))
    pos = Enum.find_index(new_nodes, fn n -> n == this_node end)
    range = div(@max_hash, length(new_nodes))
    my_range_from = (pos*range)-range
    my_range_to = pos*range
    my_range_to = case (my_range_to + length(new_nodes)) > @max_hash do
                    true -> @max_hash
                    false -> my_range_to
                  end
    for {name, {pid, _ref, {m,f,a}}} <- ETS.get_names() do
      name_hash = :erlang.phash2(name)
      dest_node = node_for_hash(new_nodes, name_hash)
      cond do
        name_hash >= my_range_from and name_hash <= my_range_to ->
          :ok
        :erlang.node(pid) == dest_node ->
          :ok
        :else ->
          case GenServer.call(pid, {:distable, :begin_handoff}) do
            :restart ->
              send(pid, {:distable, :die})
              ETS.unregister_name(name)
              GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}})
            {:resume, state} ->
              send(pid, {:distable, :die})
              ETS.unregister_name(name)
              GenServer.call({__MODULE__, dest_node}, {:handoff, name, {m,f,a}, state})
            :ignore ->
              nil
          end
      end
    end
  end
end
