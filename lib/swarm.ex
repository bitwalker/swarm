defmodule Swarm do
  @moduledoc """
  This is the public Elixir API for `:swarm`.
  """
  use Application

  def start(_type, _args) do
    Swarm.Supervisor.start_link()
  end

  @doc """
  Explicitly join this node to the cluster.
  If `autojoin: true` is set in the config, this call does nothing.
  """
  @spec join!() :: :ok | no_return
  def join! do
    Swarm.Tracker.join!
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
    case Swarm.Tracker.register(name, pid) do
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
  def register_name(name, module, function, args)
  when is_atom(module) and is_atom(function) and is_list(args) do
    Swarm.Tracker.register(name, {module, function, args})
  end

  @doc """
  Unregisters the given name from the registry.
  """
  @spec unregister_name(term) :: :ok
  def unregister_name(name) do
    Swarm.Tracker.unregister(name)
  end

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(term) :: pid | :undefined
  def whereis_name(name), do: Swarm.Tracker.whereis(name)

  @doc """
  Join a process to a group
  """
  @spec join(term, pid) :: :ok
  def join(group, pid) when is_pid(pid) do
    Swarm.Tracker.join_group(group, pid)
  end

  @doc """
  Leave a process group
  """
  @spec leave(term, pid) :: :ok
  def leave(group, pid) when is_pid(pid) do
    Swarm.Tracker.leave_group(group, pid)
  end

  @doc """
  Returns a list of pids which are members of the given group.
  """
  @spec members(term) :: [pid]
  def members(group) do
    Swarm.Tracker.group_members(group)
  end

  @doc """
  Publish a message to all members of a group.
  """
  @spec publish(term, term) :: :ok
  def publish(group, msg) do
    Swarm.Tracker.publish(group, msg)
  end

  @doc """
  Call all members of a group and return the results as a list.

  Takes an optional timeout value. Any responses not received within
  this period are ignored. Default is 5s.
  """
  @spec multicall(term, term) :: [any()]
  @spec multicall(term, term, pos_integer) :: [any()]
  def multicall(group, msg, timeout \\ 5_000) do
    Swarm.Tracker.multicall(group, msg, timeout)
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
