defmodule Swarm.Registry do
  @moduledoc false
  import Swarm.Entry
  alias Swarm.{Tracker, Entry}
  use GenServer

  ## Public API

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
  def join(group, pid), do: Tracker.add_meta(group, true, pid)

  @spec leave(term, pid) :: :ok
  defdelegate leave(group, pid), to: Tracker, as: :remove_meta

  @spec members(group :: term) :: [pid]
  def members(group) do
    :ets.select(:swarm_registry, [{entry(name: :'$1', pid: :'$2', ref: :'$3', meta: %{group => :'$4'}, clock: :'$5'), [], [:'$_']}])
    |> Enum.map(fn entry(pid: pid) -> pid end)
    |> Enum.uniq
  end

  @spec registered() :: [{name :: term, pid}]
  def registered(), do: all()

  @spec publish(term, term) :: :ok
  def publish(group, msg) do
    for pid <- members(group), do: Kernel.send(pid, msg)
    :ok
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

  @spec all() :: [{name :: term(), pid()}]
  def all() do
    :ets.tab2list(:swarm_registry)
    |> Enum.map(fn entry(name: name, pid: pid) -> {name, pid} end)
  end

  @spec get_by_name(term()) :: :undefined | Entry.entry
  def get_by_name(name) do
    case :ets.lookup(:swarm_registry, name) do
      []    -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_pid(pid) :: :undefined | [Entry.entry]
  def get_by_pid(pid) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: pid, ref: :'$2', meta: :'$3', clock: :'$4')) do
      [] -> :undefined
      list when is_list(list) -> list
    end
  end

  @spec get_by_pid_and_name(pid(), term()) :: :undefined | Entry.entry
  def get_by_pid_and_name(pid, name) do
    case :ets.match_object(:swarm_registry, entry(name: name, pid: pid, ref: :'$2', meta: '$3', clock: :'$4')) do
      [] -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_ref(reference()) :: :undefined | Entry.entry
  def get_by_ref(ref) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: :'$2', ref: ref, meta: :'$3', clock: :'$4')) do
      []    -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_meta(term()) :: :undefined | [Entry.entry]
  def get_by_meta(key) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: :'$2', ref: :'$3', meta: %{key => :'$4'}, clock: :'$5')) do
      []    -> :undefined
      list when is_list(list) -> list
    end
  end

  @spec get_by_meta(term(), term()) :: :undefined | [Entry.entry]
  def get_by_meta(key, value) do
    case :ets.match_object(:swarm_registry, entry(name: :'$1', pid: :'$2', ref: :'$3', meta: %{key => value}, clock: :'$4')) do
      []    -> :undefined
      list when is_list(list) -> list
    end
  end

  ## GenServer Implementation

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    # start ETS table for registry
    t = :ets.new(:swarm_registry, [
          :set,
          :named_table,
          :public,
          keypos: 2,
          read_concurrency: true,
          write_concurrency: true])
    {:ok, t}
  end
end
