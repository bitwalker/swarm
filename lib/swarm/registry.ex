defmodule Swarm.Registry do
  import Swarm.Logger
  alias Swarm.Tracker
  use GenServer

  # Registry API

  @spec register_name(term, pid) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register_name(name, pid) do
    case :ets.lookup(:swarm_registry, name) do
      [] ->
        set(name, {pid, nil})
        {:ok, pid}
      [{_, {^pid, _}}] ->
        {:ok, pid}
      [{_, {pid, _}}] ->
        {:error, {:already_registered, pid}}
    end
  end

  @spec register_name(term, module, atom, [any()]) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register_name(name, module, fun, args) do
    try do
      node = Node.self
      case Swarm.Ring.node_for_key(name) do
        ^node ->
          case :ets.lookup(:swarm_registry, name) do
            [] ->
              case apply(module, fun, args) do
                {:ok, pid} ->
                  set(name, {pid, {module, fun, args}})
                  {:ok, pid}
                other ->
                  {:error, {:bad_return, other}}
              end
            [{_, {pid, _}}] ->
              {:error, {:already_registered, pid}}
          end
        other_node ->
          :rpc.call(other_node, __MODULE__, :register_name, [name, module, fun, args])
      end
    catch
      {_, err} ->
        {:error, err}
    end
  end

  @spec unregister_name(name :: term) :: :ok
  def unregister_name(name) do
    del(name)
    :ok
  end

  @spec register_property(prop :: term, pid) :: :ok
  def register_property(prop, pid) do
    set({:prop, prop}, pid)
    :ok
  end

  @spec unregister_property(prop :: term, pid) :: :ok
  def unregister_property(prop, pid) do
    del({:prop, prop, pid})
    :ok
  end

  @spec get_by_name(name :: term) :: :undefined | pid
  def get_by_name(name) do
    case get(name) do
      nil -> :undefined
      pid when is_pid(pid) -> pid
    end
  end

  @spec get_by_property(prop :: term) :: [pid]
  def get_by_property(prop) do
    case get({:prop, prop}) do
      nil -> []
      pids when is_list(pids) ->
        pids
    end
  end

  # Phoenix.Tracker implementation

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    {:ok, nil}
  end

  defp set(k, v) do
    Swarm.Ring.publish({:set, k, v})
  end

  defp get(k, default \\ nil) do
    res = Swarm.Ring.multi_call({:get, k})
    resolve_conflicts(k, res, default)
  end

  defp del(k) do
    Swarm.Ring.multi_call({:delete, k})
    :ok
  end

  defp resolve_conflicts(key, res, default) do
    debug "resolve_conflicts"
    {value, pids} = Enum.reduce(res, {default, []}, fn
      {:ok, pid, nil}, {current, pids} ->
        {current, [pid | pids]}
      {:ok, _pid, val}, {^default, pids} when val != default ->
        {val, pids}
      {:ok, pid, val}, {current, pids} when val != current or val == default ->
        {current, [pid | pids]}
      _, acc -> acc
    end)
    update_replicas(key, value, pids, default)
  end

  defp update_replicas(key, value, pids, default) do
    debug "update_replicas"
    if value != default do
      Enum.each(pids, fn pid ->
        Task.start(fn ->
          GenServer.cast(pid, {:set, key, value})
          debug "updated replica #{inspect pid} with #{inspect {key, value}}"
        end)
      end)
    end
    value
  end

  def handle_cast({:set, {:prop, prop}, pid}, state) do
    ref = Process.monitor(pid)
    :ets.insert(:swarm_monitors, {pid, ref, {:prop, prop}})
    case :ets.lookup(:swarm_props, prop) do
      [] ->
        :ets.insert(:swarm_props, {prop, [pid]})
      [{_prop, pids}] ->
        :ets.insert(:swarm_props, {prop, [pid|pids]})
    end
    {:noreply, state}
  end
  def handle_cast({:set, name, {pid, _} = val}, state) do
    ref = Process.monitor(pid)
    :ets.insert(:swarm_monitors, {pid, ref, name})
    :ets.insert(:swarm_registry, {name, val})
    {:noreply, state}
  end

  def handle_call({:delete, {:prop, prop, pid}}, state) do
    case :ets.match_object(:swarm_monitors, {pid, :'$2', :'$3'}) do
      [] -> :ok
      monitors ->
        for {_pid, ref, _name} <- monitors, do: Process.demonitor(ref, [:flush])
    end
    case :ets.lookup(:swarm_props, prop) do
      [] ->
        :ok
      [{_prop, pids}] ->
        :ets.insert(:swarm_props, {prop, Enum.filter(pids, fn ^pid -> false; _ -> true end)})
    end
    {:reply, :ok, state}
  end
  def handle_call({:delete, name}, from, state) do
    Task.start(fn ->
      case :ets.match_object(:swarm_monitors, {:'$1', :'$2', name}) do
        [] ->
          :ets.delete(:swarm_registry, name)
        monitors ->
          for {pid, ref, _name} <- monitors do
            Process.demonitor(ref, [:flush])
            :ets.delete(:swarm_monitors, pid)
          end
      end
      :ets.delete(:swarm_registry, name)
      GenServer.reply(from, :ok)
    end)
    {:noreply, state}
  end
  def handle_call({:get, {:prop, prop}}, from, state) do
    Task.start(fn ->
      case :ets.lookup(:swarm_props, prop) do
        [] -> GenServer.reply(from, nil)
        [{_prop, pids}] -> GenServer.reply(from, pids)
      end
    end)
    {:noreply, state}
  end
  def handle_call({:get, key}, from, state) do
    Task.start(fn ->
      case :ets.lookup(:swarm_registry, key) do
        [] ->
          GenServer.reply(from, nil)
        [v] ->
          GenServer.reply(from, v)
      end
    end)
    {:noreply, state}
  end

  def handle_info({:join, pid}, state) do
    Task.start(fn ->
      :ets.foldl(fn {key, value}, _ ->
        GenServer.cast(pid, {:set, key, value})
      end, nil, :swarm_registry)
    end)
    redistribute!
    {:noreply, state}
  end
  def handle_info({:leave, _pid}, state) do
    redistribute!
    {:noreply, state}
  end
  def handle_info({:DOWN, _ref, _type, pid, _info}, state) do
    Task.start(fn ->
      :ets.delete(:swarm_monitors, pid)
      true = :ets.match_delete(:swarm_registry, {:'$1', {pid, :'$1'}})
    end)
    {:noreply, state}
  end
  def handle_info(_msg, state), do: {:noreply, state}

  @doc false
  def redistribute! do
    Task.start(fn ->
      current_node = Node.self
      :ets.foldl(fn
        {name, {_pid, nil}}, _ ->
          Task.start(fn -> del(name) end)
        {name, {pid, {m,f,a}}}, _ ->
          Task.start(fn -> try do
            case Swarm.Ring.node_for_key(name) do
              # this node and target node are the same
              ^current_node ->
                case node(pid) do
                  # pid node matches the target node, nothing to do
                  ^current_node ->
                    :ok
                  # pid node does not match the target node, restart the pid
                  # on the correct node
                  other_node ->
                    # if we're no longer connected to the other node, restart the pid
                    unless Enum.member?(Node.list, other_node) do
                      debug "restarting #{inspect name} on #{current_node}: connection to previous node #{other_node} was lost"
                      restart_pid(pid, name, {m,f,a})
                    end
                end
              # this node and the target node are not the same
              other_node ->
                case node(pid) do
                  # pid node does not match the target node, and the pid is local
                  ^current_node ->
                    if Process.alive?(pid) do
                      debug "handoff: initiating for #{inspect name} on #{current_node}, new parent is #{other_node}"
                      # perform handoff
                      case GenServer.call(pid, {:swarm, :begin_handoff}, :infinity) do
                        :restart ->
                          debug "handoff: #{inspect name} requested restart"
                          restart_pid(pid, name, {m,f,a})
                        {:resume, state} ->
                          debug "handoff: #{inspect name} requested to be resumed, restarting.."
                          {:ok, new_pid} = restart_pid(pid, name, {m,f,a})
                          debug "handoff: #{inspect name} restart complete, resuming"
                          GenServer.cast(new_pid, {:swarm, :end_handoff, state})
                        :ignore ->
                          debug "handoff: #{inspect name} requested to be ignored"
                          :ok
                      end
                    end
                  # pid node may or may not match the target node, but the pid is
                  # non-local, so ignore it
                  _other_node ->
                    :ok
                end
            end
          catch
            _type, err ->
              if node(pid) == Node.self do
                error "failed to redistribute #{inspect pid} (#{Process.alive?(pid)}) on #{Node.self}: #{inspect err}"
              else
                error "failed to redistribute #{inspect pid} on #{Node.self}: #{inspect err}"
              end
          end end)
      end, nil, :swarm_registry)
    end)
  end

  # Performs the steps necessary to properly restart a process
  defp restart_pid(pid, name, {mod, fun, args}) do
    send(pid, {:swarm, :die})
    unregister_name(name)
    debug "unregistered name #{inspect name}"
    res = register_name(name, mod, fun, args)
    debug "registered name #{inspect name}: #{inspect res}"
    res
  end
end
