defmodule Swarm.Registry do
  @moduledoc false
  import Swarm.Entry
  alias Swarm.Tracker

  @spec register(term, pid) :: {:ok, pid} | {:error, term}
  def register(name, pid), do: Tracker.track(name, pid)

  @spec register(term, module, atom, [any()]) :: {:ok, pid} | {:error, term}
  def register(name, module, fun, args), do: Tracker.track(name, module, fun, args)

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
  def join(_group, _pid) do
    :ok
  end

  @spec leave(term, pid) :: :ok
  def leave(_group, _pid) do
    :ok
  end

  @spec members(group :: term) :: [pid]
  def members(_group) do
    []
  end

  @spec registered() :: [{name :: term, pid}]
  def registered() do
    :ets.tab2list(:swarm_registry)
    |> Enum.map(fn entry(name: name, pid: pid) -> {name, pid} end)
  end

  @spec publish(term, term) :: :ok
  def publish(_group, _msg) do
    :ok
  end

  @spec multi_call(term, term, pos_integer) :: [term]
  def multi_call(_group, msg, timeout \\ 5_000) do
    members = []
    Enum.map(members, fn member ->
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
