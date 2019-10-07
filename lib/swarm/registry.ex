defmodule Swarm.Registry do
  @moduledoc false
  import Swarm.Entry
  alias Swarm.{Tracker, Entry}
  use GenServer

  @table_name :swarm_registry

  ## Public API

  defdelegate register(name, pid), to: Tracker, as: :track
  defdelegate register(name, module, fun, args, timeout), to: Tracker, as: :track

  @spec unregister(term) :: :ok
  def unregister(name) do
    case get_by_name(name) do
      :undefined -> :ok
      entry(pid: pid) when is_pid(pid) -> Tracker.untrack(pid)
    end
  end

  @spec fast_unregister(term) :: :ok
  def fast_unregister(name) do
    Tracker.fast_untrack(name)
  end

  @spec whereis(term) :: :undefined | pid
  def whereis(name) do
    case get_by_name(name) do
      :undefined ->
        Tracker.whereis(name)

      entry(pid: pid) when is_pid(pid) ->
        pid
    end
  end

  @spec whereis_or_register(term, atom(), atom(), [term]) :: {:ok, pid} | {:error, term}
  def whereis_or_register(name, m, f, a, timeout \\ :infinity)

  @spec whereis_or_register(term, atom(), atom(), [term], non_neg_integer() | :infinity) ::
          {:ok, pid} | {:error, term}
  def whereis_or_register(name, module, fun, args, timeout) do
    with :undefined <- whereis(name),
         {:ok, pid} <- register(name, module, fun, args, timeout) do
      {:ok, pid}
    else
      pid when is_pid(pid) ->
        {:ok, pid}

      {:error, {:already_registered, pid}} ->
        {:ok, pid}

      {:error, _} = err ->
        err
    end
  end

  @spec join(term, pid) :: :ok
  def join(group, pid), do: Tracker.add_meta(group, true, pid)

  @spec leave(term, pid) :: :ok
  defdelegate leave(group, pid), to: Tracker, as: :remove_meta

  @spec members(group :: term) :: [pid]
  def members(group) do
    :ets.select(@table_name, [
      {entry(name: :"$1", pid: :"$2", ref: :"$3", meta: %{group => :"$4"}, clock: :"$5"), [],
       [:"$_"]}
    ])
    |> Enum.map(fn entry(pid: pid) -> pid end)
    |> Enum.uniq()
  end

  @spec registered() :: [{name :: term, pid}]
  defdelegate registered(), to: __MODULE__, as: :all

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
    case whereis(name) do
      :undefined ->
        :ok

      pid when is_pid(pid) ->
        Kernel.send(pid, msg)
    end
  end

  ### Low-level ETS manipulation functions

  @spec all() :: [{name :: term(), pid()}]
  def all() do
    :ets.tab2list(@table_name)
    |> Enum.map(fn entry(name: name, pid: pid) -> {name, pid} end)
  end

  @spec snapshot() :: [Entry.entry()]
  def snapshot() do
    :ets.tab2list(@table_name)
  end

  @doc """
  Inserts a new registration, and returns true if successful, or false if not
  """
  @spec new(Entry.entry()) :: boolean
  def new(entry() = reg) do
    :ets.insert_new(@table_name, reg)
  end

  @doc """
  Like `new/1`, but raises if the insertion fails.
  """
  @spec new!(Entry.entry()) :: true | no_return
  def new!(entry() = reg) do
    true = :ets.insert_new(@table_name, reg)
  end

  @spec remove(Entry.entry()) :: true
  def remove(entry() = reg) do
    :ets.delete_object(@table_name, reg)
  end

  @spec remove_by_name(term()) :: true
  def remove_by_name(name) do
    :ets.delete(@table_name, name)
  end

  @spec remove_by_pid(pid) :: true
  def remove_by_pid(pid) when is_pid(pid) do
    case get_by_pid(pid) do
      :undefined ->
        true

      entries when is_list(entries) ->
        Enum.each(entries, &:ets.delete_object(@table_name, &1))
        true
    end
  end

  @spec get_by_name(term()) :: :undefined | Entry.entry()
  def get_by_name(name) do
    case :ets.lookup(@table_name, name) do
      [] -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_pid(pid) :: :undefined | [Entry.entry()]
  def get_by_pid(pid) do
    case :ets.match_object(
           @table_name,
           entry(name: :"$1", pid: pid, ref: :"$2", meta: :"$3", clock: :"$4")
         ) do
      [] -> :undefined
      list when is_list(list) -> list
    end
  end

  @spec get_by_pid_and_name(pid(), term()) :: :undefined | Entry.entry()
  def get_by_pid_and_name(pid, name) do
    case :ets.match_object(
           @table_name,
           entry(name: name, pid: pid, ref: :"$1", meta: :"$2", clock: :"$3")
         ) do
      [] -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_ref(reference()) :: :undefined | Entry.entry()
  def get_by_ref(ref) do
    case :ets.match_object(
           @table_name,
           entry(name: :"$1", pid: :"$2", ref: ref, meta: :"$3", clock: :"$4")
         ) do
      [] -> :undefined
      [obj] -> obj
    end
  end

  @spec get_by_meta(term()) :: :undefined | [Entry.entry()]
  def get_by_meta(key) do
    case :ets.match_object(
           @table_name,
           entry(name: :"$1", pid: :"$2", ref: :"$3", meta: %{key => :"$4"}, clock: :"$5")
         ) do
      [] -> :undefined
      list when is_list(list) -> list
    end
  end

  @spec get_by_meta(term(), term()) :: :undefined | [Entry.entry()]
  def get_by_meta(key, value) do
    case :ets.match_object(
           @table_name,
           entry(name: :"$1", pid: :"$2", ref: :"$3", meta: %{key => value}, clock: :"$4")
         ) do
      [] -> :undefined
      list when is_list(list) -> list
    end
  end

  @spec reduce(term(), (Entry.entry(), term() -> term())) :: term()
  def reduce(acc, fun) when is_function(fun, 2) do
    :ets.foldl(fun, acc, @table_name)
  end

  @spec update(term(), Keyword.t()) :: boolean
  defmacro update(key, updates) do
    fields = Enum.map(updates, fn {k, v} -> {Entry.index(k) + 1, v} end)

    quote bind_quoted: [table_name: @table_name, key: key, fields: fields] do
      :ets.update_element(table_name, key, fields)
    end
  end

  ## GenServer Implementation

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    # start ETS table for registry
    t =
      :ets.new(@table_name, [
        :set,
        :named_table,
        :public,
        keypos: 2,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, t}
  end
end
