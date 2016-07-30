defmodule Swarm do
  @moduledoc """
  This is the public Elixir API for `:distable`.
  """
  use Application

  def start(_type, _args) do
    Swarm.Supervisor.start_link()
  end

  @doc """
  Registers the given name to the given pid, however names
  registered this way will not be shifted when the cluster
  topology changes, but this allows you to use `:distable` as
  a distributed process registry, including registering names
  with `{:via, :distable, name}`.
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
  are already started, it must be started by `:distable`.
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
  Add metadata to a registered process.
  """
  @spec register_property(pid, term) :: :ok
  def register_property(pid, prop) do
    Swarm.Tracker.register_property(pid, prop)
  end

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(term) :: pid | :undefined
  def whereis_name(name), do: Swarm.Tracker.whereis(name)

  @doc """
  Get a list of pids which have the given property in their
  metadata.
  """
  @spec get_by_property(term) :: [pid]
  def get_by_property(prop) do
    Swarm.Tracker.get_by_property(prop)
  end
end
