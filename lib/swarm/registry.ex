defmodule Swarm.Registry do
  @moduledoc false
  import Swarm.Entry
  alias Swarm.Tracker

  defdelegate register(name, pid), to: Tracker, as: :track
  defdelegate register(name, module, fun, args), to: Tracker, as: :track

  @spec unregister(term) :: :ok
  def unregister(name) do
    case get_by_name(name) do
      :undefined -> :ok
      entry(pid: pid) -> Tracker.untrack(pid)
    end
  end

  @spec whereis(term) :: :undefined | pid
  def whereis(name) do
    case get_by_name(name) do
      :undefined -> :undefined
      entry(pid: pid) -> pid
    end
  end

  @spec join(term, pid) :: :ok
  def join(group, pid) do
    Tracker.add_meta(group, true, pid)
  end

  @spec leave(term, pid) :: :ok
  def leave(group, pid) do
    Tracker.remove_meta(group, pid)
  end

  @spec members(group :: term) :: [pid]
  def members(group) do
    :ets.tab2list(:swarm_registry)
    |> Enum.filter_map(fn entry(meta: %{^group => _}) -> true; _ -> false end,
                       fn entry(pid: pid) -> pid end)
  end

  @spec registered() :: [{name :: term, pid}]
  def registered() do
    :ets.tab2list(:swarm_registry)
    |> Enum.map(fn entry(name: name, pid: pid) -> {name, pid} end)
  end

  @spec publish(term, term) :: :ok
  def publish(group, msg) do
    for pid <- members(group), do: Kernel.send(pid, msg)
  end

  @spec multi_call(term, term, pos_integer) :: [term]
  def multi_call(group, msg, timeout \\ 5_000) do
    Enum.map(members(group), fn member ->
      Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
        GenServer.call(member, msg, timeout)
      end)
    end)
    |> Enum.map(&Task.await(&1, :infinity))
  end

  @spec send(name :: term, msg :: term) :: :ok
  def send(name, msg) do
    case get_by_name(name) do
      :undefined -> :ok
      entry(pid: pid) ->
        GenServer.cast(pid, msg)
    end
  end

  def all() do
    :ets.tab2list(:swarm_registry)
    |> Enum.map(fn entry(name: name, pid: pid) -> {name, pid} end)
  end

  def get_by_name(name) do
    case :ets.lookup(:swarm_registry, name) do
      []    -> :undefined
      [obj] -> obj
    end
  end

  def get_by_pid(pid) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: pid, ref: :'$2', meta: :'$3', clock: :'$4')) do
      []    -> :undefined
      [obj] -> obj
    end
  end

  def get_by_ref(ref) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: :'$2', ref: ref, meta: :'$3', clock: :'$4')) do
      []    -> :undefined
      [obj] -> obj
    end
  end
end
