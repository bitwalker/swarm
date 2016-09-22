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
    case Swarm.Registry.register(name, pid) do
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
  defdelegate register_name(name, m, f, a), to: Swarm.Registry, as: :register

  @doc """
  Unregisters the given name from the registry.
  """
  @spec unregister_name(term) :: :ok
  defdelegate unregister_name(name), to: Swarm.Registry, as: :unregister

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(term) :: pid | :undefined
  defdelegate whereis_name(name), to: Swarm.Registry, as: :whereis

  @spec join(term, pid) :: :ok
  defdelegate join(group, pid), to: Swarm.Registry

  @spec leave(term, pid) :: :ok
  defdelegate leave(group, pid), to: Swarm.Registry

  @spec members(term) :: [pid]
  defdelegate members(group), to: Swarm.Registry

  @spec registered() :: [{name :: term, pid}]
  defdelegate registered, to: Swarm.Registry

  @spec publish(term, term) :: :ok
  defdelegate publish(group, msg), to: Swarm.Registry

  @spec multi_call(term, term) :: [any()]
  defdelegate multi_call(group, msg), to: Swarm.Registry

  @spec multi_call(term, term, pos_integer) :: [any()]
  defdelegate multi_call(group, msg, timeout), to: Swarm.Registry

  @spec send(term, term) :: :ok
  defdelegate send(name, msg), to: Swarm.Registry
end
