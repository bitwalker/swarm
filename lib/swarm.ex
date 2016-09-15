defmodule Swarm do
  @moduledoc """
  This is the public Elixir API for `:swarm`.
  """
  use Application

  def start(_type, _args) do
    Swarm.Supervisor.start_link()
  end

  @doc """
  Registers the given name to the given pid, however names
  registered this way will not be shifted when the cluster
  topology changes, but this allows you to use `:swarm` as
  a distributed process registry, including registering names
  with `{:via, :swarm, name}`.
  """
  @spec register_name(term, pid) :: :yes | :no
  def register_name(name, pid) when is_pid(pid) do
    case Swarm.Registry.register_name(name, pid) do
      {:ok, _} -> :yes
      _ -> :no
    end
  end

  @doc """
  Similar to register_name/2, except this version takes module/function/args
  parameters, and starts the process, registers the pid with the given name,
  and handles cluster topology changes by restarting the process on it's new
  node using the given MFA.

  This version also returns an ok tuple with the pid if it registers successfully,
  or an error tuple if registration fails. You cannot use this with processes which
  are already started, it must be started by `:swarm`.
  """
  @spec register_name(term, atom(), atom(), [term]) :: {:ok, pid} | {:error, term}
  defdelegate register_name(name, m, f, a), to: Swarm.Registry

  @doc """
  Unregisters the given name from the registry.
  """
  @spec unregister_name(term) :: :ok
  defdelegate unregister_name(name), to: Swarm.Registry

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(term) :: pid | :undefined
  def whereis_name(name) do
    case :ets.lookup(:swarm_registry, name) do
      [] -> :undefined
      [{_, {pid, _}}] -> pid
    end
  end

  @doc """
  Join a process to a group
  """
  @spec join(term, pid) :: :ok
  def join(group, pid) when is_pid(pid) do
    Swarm.Registry.register_property(group, pid)
  end

  @doc """
  Leave a process group
  """
  @spec leave(term, pid) :: :ok
  def leave(group, pid) when is_pid(pid) do
    Swarm.Registry.unregister_property(group, pid)
  end

  @doc """
  Returns a list of pids which are members of the given group.
  """
  @spec members(term) :: [pid]
  def members(group) do
    Swarm.Registry.get_by_property(group)
  end

  @doc """
  Publish a message to all members of a group.
  """
  @spec publish(term, term) :: :ok
  def publish(group, msg) do
    members(group)
    |> Enum.each(fn pid -> Kernel.send(pid, msg) end)
  end

  @doc """
  Call all members of a group and return the results as a list.

  Takes an optional timeout value. Any responses not received within
  this period are ignored. Default is 5s.
  """
  @spec multicall(term, term) :: [any()]
  @spec multicall(term, term, pos_integer) :: [any()]
  def multicall(group, msg, timeout \\ 5_000) do
    members(group)
    |> Enum.map(fn pid ->
      Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
        try do
          {:ok, pid, GenServer.call(pid, msg, timeout)}
        catch
          :exit, reason -> {:error, pid, reason}
        end
      end)
    end)
    |> Enum.map(&Task.await(&1, :infinity))
  end

  @doc """
  This function sends a message to the process registered to the given name.
  It is intended to be used by GenServer when using `GenServer.cast/2`, but you
  may use it to send any message to the desired process.
  """
  @spec send(term, term) :: :ok
  def send(name, msg) do
    case whereis_name(name) do
      pid when is_pid(pid) -> Kernel.send(pid, msg)
      :undefined -> :ok
    end
  end
end
