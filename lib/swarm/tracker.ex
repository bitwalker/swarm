defmodule Swarm.Tracker do
  @moduledoc """
  The Tracker process is responsible for watching for changes to
  the cluster, and shifting processes around accordingly. It also
  handles talking to the registry (ETS) to register names, properties,
  unregister names, etc.

  This API should be considered internal-use only.
  """
  use GenServer
  alias Swarm.ETS
  import Swarm.Logger

  @max_hash   134_217_728 # :math.pow(2,27)
  @name_table :swarm_names # {name, pid, monitor_ref, mfa, props}

  @doc """
  Register an MFA with the given name. If a pid is provided, the process will only be
  tracked, it will not be moved, or restarted when it's parent node goes
  down.
  """
  @spec register(term, mfa | pid) :: {:ok, pid} | {:error, term}
  def register(name, {m,f,a})
    when is_atom(m) and is_atom(f) and is_list(a) do
    GenServer.call(__MODULE__, {:register, name, {m,f,a}})
  end
  def register(name, pid) when is_pid(pid) do
    GenServer.call(__MODULE__, {:register, name, pid})
  end

  @doc """
  Unregister the given name
  """
  @spec unregister(term) :: :ok | {:error, term}
  def unregister(name) do
    GenServer.call(__MODULE__, {:unregister, name})
  end

  @doc """
  Register a property in the metadata for `pid`
  """
  @spec register_property(pid(), term) :: :ok
  def register_property(pid, prop) do
    GenServer.cast({__MODULE__, :erlang.node(pid)}, {:register_property, pid, prop})
  end

  @doc """
  Get all pids which have the provided property in their metadata
  """
  @spec get_by_property(term) :: [pid()]
  def get_by_property(prop) do
    ETS.get_names()
    |> Enum.filter_map(fn {_n,_pid,_ref,_mfa,props} -> prop in props end,
                       fn {_n, pid,_ref,_mfa,_props} -> pid end)
  end

  @doc """
  Get the PID for a given name. If the name is not registered,
  then `:undefined` is returned.
  """
  @spec whereis(term) :: pid() | :undefined
  def whereis(name) do
    case ETS.get_name(name) do
      [{_name, pid, _ref, _mfa, _props}] ->
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

  ## GenServer API

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    :net_kernel.monitor_nodes(true, [:nodedown_reason])
    {:ok, nil}
  end

  def handle_call({:register, name, mfa}, from, state) do
    handle_call({:register, name, mfa, []}, from, state)
  end
  def handle_call({:register, name, mfa, props}, _from, state) do
    case whereis(name) do
      pid when is_pid(pid) ->
        {:reply, {:error, {:already_registered, pid}}, state}
      :undefined ->
        this_node = Node.self
        case node_for_name(name) do
          ^this_node ->
            res = ETS.register_name(name, mfa, props)
            {:reply, res, state}
          node ->
            res = GenServer.call({__MODULE__, node}, {:register, name, mfa, props}, 5_000)
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
  def handle_call({:handoff, name, mfa, props}, _from, state) do
    case ETS.register_name(name, mfa, props) do
      {:ok, pid} ->
        {:reply, {:ok, pid}, state}
      err ->
        {:reply, err, state}
    end
  end
  def handle_call({:handoff, name, mfa, props, handoff_state}, _from, state) do
    case ETS.register_name(name, mfa, props) do
      {:ok, pid} ->
        case mfa do
          nil ->
            {:reply, {:ok, pid}, state}
          _ ->
            GenServer.call(pid, {:swarm, :end_handoff, handoff_state})
            {:reply, {:ok, pid}, state}
        end
      err ->
        {:reply, err, state}
    end
  end
  def handle_call({:sync, from_node, names}, _from, state) do
    local_names = ETS.get_local_names()
    synchronize_from_node(from_node, names)
    {:reply, local_names, state}
  end
  def handle_call(_, _from, state), do: {:noreply, state}

  def handle_cast({:register_property, pid, prop}, state) do
    ETS.register_property(pid, prop)
    {:noreply, state}
  end

  def handle_info({:nodeup, node, _info}, state) do
    debug "nodeup: #{inspect node}"
    Task.async(fn ->
      nodes = Enum.sort(all_nodes())
      redistribute(nodes)
      synchronize(nodes)
    end)
    {:noreply, state}
  end
  def handle_info({:nodedown, node, _info}, state) do
    debug "nodedown: #{inspect node}"
    Task.async(fn ->
      redistribute(Enum.sort(all_nodes()))
    end)
    {:noreply, state}
  end
  def handle_info(_, state) do
    {:noreply, state}
  end

  defp node_for_name(name) do
    node_for_name(Enum.sort(all_nodes()), name)
  end
  defp node_for_name(nodes, name) do
    node_for_hash(nodes, :erlang.phash2(name))
  end
  defp node_for_hash(nodes, hash) do
    range = div(@max_hash, length(nodes))
    node_pos = div(hash, range+1)
    Enum.at(nodes, min(length(nodes), node_pos))
  end

  defp synchronize(nodes) do
    debug "synchronizing with #{inspect nodes}"
    local_names = ETS.get_local_names()
    {replies, _badnodes} = GenServer.multi_call(nodes, __MODULE__, {:sync, Node.self, local_names})
    for {node, names} <- replies do
      synchronize_from_node(node, names)
      debug "synchronized with #{inspect node}"
    end
    :ok
  end

  defp synchronize_from_node(node, names) do
    # process adds/updates
    for named <- names do
      name = elem(named, 0)
      case :ets.lookup(@name_table, name) do
        [^named] ->
          :ok # in sync
        [_outdated] ->
          :ets.insert(@name_table, named) # need to sync
        [] ->
          :ets.insert(@name_table, named) # missing entirely
      end
    end
    # process stale entries
    old = MapSet.new(ETS.get_names(node))
    new = MapSet.new(names)
    stale = MapSet.difference(old, new)
    for named <- stale do
      name = elem(named, 0)
      :ets.delete(@name_table, name)
    end
    :ok
  end

  defp redistribute(new_nodes) do
    debug "redistributing across #{inspect new_nodes}"
    this_node = Node.self
    pos = Enum.find_index(new_nodes, fn n -> n == this_node end)
    range = div(@max_hash, length(new_nodes))
    my_range_from = (pos*range)-range
    my_range_to = pos*range
    my_range_to = case (my_range_to + length(new_nodes)) > @max_hash do
                    true -> @max_hash
                    false -> my_range_to
                  end
    to_move = ETS.get_names()
    for {name, pid, _ref, mfa, props} <- to_move do
      case mfa do
        nil ->
          :ok
        {m,f,a} ->
          name_hash = :erlang.phash2(name)
          dest_node = node_for_hash(new_nodes, name_hash)
          cond do
            name_hash >= my_range_from and name_hash <= my_range_to ->
              debug "nothing to do for #{inspect name}, already home"
              :ok
            :erlang.node(pid) == dest_node ->
              debug "nothing to do for #{inspect name}, already home on #{inspect dest_node}"
              :ok
            Node.ping(node(pid)) == :pong ->
              try do
                case GenServer.call(pid, {:swarm, :begin_handoff}) do
                  :restart ->
                    debug "handoff requested restart of #{inspect name}"
                    send(pid, {:swarm, :die})
                    ETS.unregister_name(name)
                    GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}, props})
                  {:resume, state} ->
                    debug "handoff requested resume of #{inspect name}"
                    send(pid, {:swarm, :die})
                    ETS.unregister_name(name)
                    GenServer.call({__MODULE__, dest_node}, {:handoff, name, {m,f,a}, props, state})
                  :ignore ->
                    debug "handoff ignored for #{inspect name}"
                    nil
                  _ ->
                    # bad return value, so we're going to restart
                    debug "handoff return value was bad, restarting #{inspect name}"
                    send(pid, {:swarm, :die})
                    ETS.unregister_name(name)
                    GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}, props})
                end
              catch
                _exit, {:noproc, _} ->
                  debug "cannot handoff response (:noproc), restarting #{inspect name}"
                  ETS.unregister_name(name)
                  GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}, props})
                _exit, {:timeout, _} ->
                  debug "cannot handoff response (:timeout), restarting #{inspect name}"
                  send(pid, {:swarm, :die})
                  ETS.unregister_name(name)
                  GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}, props})
              end
            :else ->
              debug "cannot handoff, restarting #{inspect name}"
              ETS.unregister_name(name)
              GenServer.call({__MODULE__, dest_node}, {:register, name, {m,f,a}, props})
          end
        end
      end
    :ok
  end

  defp all_nodes(), do: [Node.self|Node.list(:connected)]
end
